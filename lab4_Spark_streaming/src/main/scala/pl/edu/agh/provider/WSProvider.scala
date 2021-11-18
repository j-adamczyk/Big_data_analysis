package pl.edu.agh.provider

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.google.flatbuffers.Struct

class WSProvider extends DataSourceRegister with TableProvider with Logging {

  override def shortName(): String = "ws"

  private val heartbeatSchema = StructType(
    StructField("type", StringType, false) ::
    StructField("sequence", LongType, false) ::
    StructField("last_trade_id", LongType, false) ::
    StructField("product_id", StringType, false) ::
    StructField("time", TimestampType, false) :: Nil
  )

  private val statusSchema = StructType(
    StructField("type", StringType, false) ::
    StructField("products", ArrayType(StructType(
      StructField("id", StringType, false) ::
      StructField("base_currency", StringType, false) ::
      StructField("quote_currency", StringType, false) ::
      StructField("base_min_size", DoubleType, false) ::
      StructField("base_max_size", DoubleType, false) ::
      StructField("base_increment", DoubleType, false) ::
      StructField("quote_increment", DoubleType, false) ::
      StructField("display_name", StringType, false) ::
      StructField("status", StringType, false) ::
      StructField("status_message", StringType, true) ::
      StructField("min_market_funds", DoubleType, false) ::
      StructField("max_market_funds", DoubleType, false) ::
      StructField("post_only", BooleanType, false) ::
      StructField("limit_only", BooleanType, false) ::
      StructField("cancel_only", BooleanType, false) ::
      StructField("fx_stablecoin", BooleanType, false) :: Nil
    ), false), false) ::
    StructField("currencies", ArrayType(StructType(
      StructField("id", StringType, false) ::
      StructField("name", StringType, false) ::
      StructField("min_size", DoubleType, false) ::
      StructField("status", StringType, false) ::
      StructField("status_message", StringType, true) ::
      StructField("max_precision", DoubleType, false) ::
      StructField("convertible_to", ArrayType(StringType, false), false) ::
      StructField("details", StringType, false) :: Nil
    ), false), false) :: Nil
  )

  private val tickerSchema = StructType(
    StructField("type", StringType, false) ::
    StructField("trade_id", LongType, false) ::
    StructField("sequence", LongType, false) ::
    StructField("time", TimestampType, false) ::
    StructField("product_id", StringType, false) ::
    StructField("price", DoubleType, false) ::
    StructField("side", StringType, false) ::
    StructField("last_size", DoubleType, false) ::
    StructField("best_bid", DoubleType, false) ::
    StructField("best_ask", DoubleType, false) :: Nil
  )

  private val level2Schema = StructType(
    StructField("type", StringType, false) ::
    StructField("product_id", StringType, false) ::
    StructField("time", TimestampType, false) ::
    StructField("changes", StructType(
      StructField("side", StringType, false) ::
      StructField("price", DoubleType, false) ::
      StructField("size", DoubleType, false) :: Nil
    ), false) :: Nil
  )

  private val auctionSchema = StructType(
    StructField("type", StringType, false) ::
    StructField("product_id", StringType, false) ::
    StructField("sequence", LongType, false) ::
    StructField("auction_state", StringType, false) ::
    StructField("best_bid_price", DoubleType, false) ::
    StructField("best_bid_size", DoubleType, false) ::
    StructField("best_ask_price", DoubleType, false) ::
    StructField("best_ask_size", DoubleType, false) ::
    StructField("open_price", DoubleType, false) ::
    StructField("open_size", DoubleType, false) ::
    StructField("can_open", StringType, false) ::
    StructField("time", TimestampType, false) :: Nil
  )

  override def getTable(
      x: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    assert(partitioning.isEmpty)
    new WSStreamer(inferSchema(new CaseInsensitiveStringMap(properties)), SparkSession.active.sparkContext.defaultParallelism, properties.get("schema"))
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = options.get("schema") match {
    case "heartbeat" => heartbeatSchema
    case "status" => statusSchema
    case "ticker" => tickerSchema
    case "level2" => level2Schema
    case "auction" => auctionSchema
  }
}
