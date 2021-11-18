package pl.edu.agh.provider

import java.util

import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  Table,
  TableCapability
}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import pl.edu.agh.model._

class WSStreamer(val schema: StructType, numPartitions: Int, schemaTag: String)
    extends Table
    with SupportsRead {

  override def name(): String = "ws://..."

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.MICRO_BATCH_READ
    ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    () => {
      val wsMicroBatchStreamer = schemaTag match {
        case "heartbeat" => WSMicroBatchStreamer[HeartbeatMessage](numPartitions, schemaTag, options.getOrDefault("url", "wss://ws-feed.exchange.coinbase.com"))
        case "status" => WSMicroBatchStreamer[StatusMessage](numPartitions, schemaTag,options.getOrDefault("url", "wss://ws-feed.exchange.coinbase.com"))
        case "ticker" => WSMicroBatchStreamer[TickerMessage](numPartitions, schemaTag, options.getOrDefault("url", "wss://ws-feed.exchange.coinbase.com"))
        case "level2" => WSMicroBatchStreamer[Level2Message](numPartitions, schemaTag, options.getOrDefault("url", "wss://ws-feed.exchange.coinbase.com"))
        case "auction" => WSMicroBatchStreamer[AuctionMessage](numPartitions, schemaTag, options.getOrDefault("url", "wss://ws-feed.exchange.coinbase.com"))
      }
      new Scan {
        override def readSchema(): StructType = schema

        override def toMicroBatchStream(
            checkpointLocation: String
        ): MicroBatchStream = 
          wsMicroBatchStreamer
      }
    }
}
