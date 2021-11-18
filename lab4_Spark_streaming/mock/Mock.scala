// using scala 3.0.2
// using lib org.java-websocket:Java-WebSocket:1.5.2
// using lib org.slf4j:slf4j-simple:1.7.25

import scala.util._
import scala.io.StdIn.readLine
import java.net.InetSocketAddress
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.nio.channels.SelectionKey
import collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import java.time.temporal.ChronoUnit.{SECONDS, MILLIS, MINUTES}

object Mock extends App:
  val s = new WebSocketServer(new InetSocketAddress("mock", 8025)):
    override def onConnect(key: SelectionKey): Boolean = true
    override def onOpen(webSocket: WebSocket, clientHandshake: ClientHandshake): Unit =
      println("Opening connection...")
      webSocket.send("""{"type":"subscriptions","channels":[{"name":"ticker","product_ids":["ETH-BTC","ETH-USD"]}]}""")
      println("Opened connection successfully!")
    override def onClose(webSocket: WebSocket, code: Int, reason: String, remote: Boolean): Unit = println("Conection has been closed.")
    override def onError(webSocket: WebSocket, ex: Exception): Unit = println(s"Error $ex")
    override def onMessage(webSocket: WebSocket, msg: String): Unit = println("Received message for subscription")
    override def onStart(): Unit = println("Starting. Waiting for subscriptions...")
  s.start()

  val now = java.time.Instant.parse("2021-11-01T12:00:00.123123Z")

  def msg(ts: String, price: Double) =
    s"""{"type":"ticker","sequence":0,"product_id":"ETH-USD","price":"$price","open_24h":"0","volume_24h":"0","low_24h":"0","high_24h":"0","volume_30d":"0","best_bid":"0","best_ask":"0","side":"buy","time":"$ts","trade_id":0,"last_size":"0"}"""

  def sendFrames(list: List[String]) = 
    def price() = scala.util.Random.nextDouble() * 1000
    list.foreach { ts =>
      val p = price()
      println(s"$ts - $p")
      s.broadcast(msg(ts, p))
    }

  def step(list: List[Int]) =
    println(s"Sending frames with ${list.map(t => s"T0+${t}s").mkString(", ")} timestamps.")
    sendFrames(list.map(diff))

  def diff(n: Int) = now.plus(n, SECONDS).toString

  while s.getConnections.isEmpty do Thread.sleep(1000)

  step(list = List(0, 14, 7))
  Thread.sleep(10000)
  step(list = List(15, 8, 21))
  Thread.sleep(10000)
  step(list = List(4, 17))

  s.getConnections.asScala.foreach(_.close(1000))
  s.stop(1000)
