package pl.edu.agh.provider

import java.util.concurrent.atomic.AtomicBoolean
import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}

import java.nio.ByteBuffer
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
import org.apache.spark.sql.execution.streaming.LongOffset
import pl.edu.agh.model._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import okhttp3._
import okio.ByteString

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.{Try, Success, Failure}
import io.circe.Decoder
import io.circe.Encoder
import reflect.runtime.universe.TypeTag

case class WSMicroBatchStreamer[T <: Product: TypeTag](
    numPartitions: Int,
    schemaTag: String,
    websocketUrl: String
)(implicit decoder: Decoder[T], encoder: Encoder[T]) extends MicroBatchStream
    with Logging {

  private var currentOffset = LongOffset(-1)

  private var active = true

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  @GuardedBy("this")
  private val batches = new ListBuffer[(T, Long)]

  @GuardedBy("this")
  protected val messageQueue: BlockingQueue[(T, Long)] =
    new ArrayBlockingQueue[(T, Long)](1000)

  @GuardedBy("this")
  @transient
  var socket: Option[WebSocket] = None

  @GuardedBy("this")
  @transient
  var worker: Option[Thread] = None

  private def initialize(): Unit =
    synchronized {
      var initialMessageProcessed: Boolean = false

      val client = new OkHttpClient.Builder()
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .build()

      val ws = client.newWebSocket(
        new Request.Builder()
          .url(websocketUrl)
          .build(),
        new WebSocketListener {

          override def onOpen(
              webSocket: WebSocket,
              response: Response
          ): Unit = {
            log.debug("Opened websocket connection...")
            // Send out initial messages which we will get echoed back
            // val subscribeMessage = ProtocolMessage(
            //   "subscribe",
            //   List(Channel("ticker", List("ETH-BTC", "ETH-USD")))
            // )
            // webSocket.send(Serializer(subscribeMessage))
            val jsonToSubscribe = schemaTag match {
              case "heartbeat" => """{"type": "subscribe","channels": [{ "name": "heartbeat", "product_ids": ["ETH-EUR"] }]}"""
              case "status" => ""
              case "ticker" => """{"type": "subscribe","channels": [{"name": "ticker","product_ids": ["ETH-BTC","ETH-USD"]}]}"""
              case "level2" => ""
              case "auction" => """{"type": "auction","channels": [{ "name": "auctionfeed"}]}"""
            }
            webSocket.send(jsonToSubscribe)
          }

          override def onClosed(
              webSocket: WebSocket,
              code: Int,
              reason: String
          ): Unit = {
            log.info(s"Websocket closed: $reason ($code) ")
            if (code == 1000) {
              val unsubscribeMessage =
                ProtocolMessage("unsubscribe", List(Channel("ticker")))
              Try(webSocket.send(Serializer(unsubscribeMessage)))
            } else if (socket.isDefined) {
              log.warn("Attempting to reconnect in 1s...")
              Thread.sleep(1000)
              initialize()
            }
          }

          override def onFailure(
              webSocket: WebSocket,
              t: Throwable,
              response: Response
          ): Unit = {
            log.warn(s"Websocket failed: ${t.getMessage}")
            if (socket.isDefined && active) {
              log.warn("Attempting to reconnect in 1s...")
              Thread.sleep(1000)
              initialize()
            }
          }

          override def onMessage(webSocket: WebSocket, str: String): Unit = {
            if (!initialMessageProcessed) handleProtocolMessage(str)
            else handleDataMessage(str)
          }

          def handleProtocolMessage(message: String): Unit =
            Parser[ProtocolMessage](message) match {
              case Left(exception) =>
                log.warn(
                  "Failed to subscribe to websocket: " + exception.getMessage + " \n message: " + message
                )
              case Right(_) =>
                initialMessageProcessed = true
                log.info("Successfully subscribed to websocket")
            }

          def handleDataMessage(message: String): Unit =
            Parser[T](message) match {
              case Right(message) =>
                currentOffset = currentOffset + 1
                messageQueue.put((message, currentOffset.offset))
              case Left(exception) =>
                log.warn("Couldn't parse message " + message)
            }
        }
      )

      socket = Some(ws)

      worker = {
        val thread = new Thread("Queue Worker") {
          setDaemon(true)

          override def run(): Unit = {
            while (socket.isDefined && active) {
              Try(messageQueue.poll(1000, TimeUnit.MILLISECONDS)) match {
                case Success(event) => 
                  if (event != null) {
                    batches.append(event)
                  }
                case Failure(_) => 
                  log.warn("MessageQueue was interrupted")
              }
              
            }
          }
        }
        thread.start()
        Some(thread)
      }
    }

  override def planInputPartitions(
      start: Offset,
      end: Offset
  ): Array[InputPartition] = {
    val startOrdinal = start.asInstanceOf[LongOffset].offset.toInt + 1
    val endOrdinal = end.asInstanceOf[LongOffset].offset.toInt + 1

    // Internal buffer only holds the batches after lastOffsetCommitted
    val rawList = synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }

      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1

      batches.slice(sliceStart, sliceEnd)
    }

    val slices =
      Array.fill(numPartitions)(new ListBuffer[(T, Long)])

    rawList.zipWithIndex.foreach {
      case (r, idx) =>
        slices(idx % numPartitions).append(r)
    }

    slices.map(WSInputPartition[T])
  }

  override def createReaderFactory(): PartitionReaderFactory =
    (partition: InputPartition) => {
      val slice = partition.asInstanceOf[WSInputPartition[T]].slice
      new PartitionReader[InternalRow] {
        private var currentIdx = -1

        override def next(): Boolean = {
          currentIdx += 1
          currentIdx < slice.size
        }

        override def get(): InternalRow = {
          InternalRow(slice(currentIdx)._1, slice(currentIdx)._2)
          encodeMessage(slice(currentIdx)._1)
        }

        override def close(): Unit = {}
      }
    }

  private def encodeMessage(message: T): InternalRow = {
    val messageEncoder = Encoders.product[T]
    val messageExprEncoder =
      messageEncoder.asInstanceOf[ExpressionEncoder[T]]
    messageExprEncoder.createSerializer()(message)
  }

  override def stop(): Unit = {
    log.info("Stopping streamer")
    active = false
    socket.foreach(_.close(1000, "Closing websocket normally"))
    if (worker.exists(_.isAlive)) {
      Try(worker.foreach(_.join()))
      worker.foreach(_.stop())
    }
    log.info("Streamer stopped succesfully")
  }

  override def commit(end: Offset): Unit =
    synchronized {
      val newOffset = end.asInstanceOf[LongOffset]

      val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

      if (offsetDiff < 0) {
        sys.error(
          s"Offsets committed out of order: $lastOffsetCommitted followed by $end"
        )
      }

      batches.trimStart(offsetDiff)
      lastOffsetCommitted = newOffset
    }

  override def latestOffset(): Offset = currentOffset

  override def initialOffset(): Offset = LongOffset(-1)

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }
}

case class WSInputPartition[T](slice: ListBuffer[(T, Long)])
    extends InputPartition
