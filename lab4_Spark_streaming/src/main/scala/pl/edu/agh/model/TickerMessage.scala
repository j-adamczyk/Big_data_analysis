package pl.edu.agh.model

import io.circe.generic.JsonCodec
import java.sql.Timestamp
import Parser._

@JsonCodec
case class TickerMessage(
    `type`: Option[String],
    trade_id: Option[Long],
    sequence: Option[Long],
    time: Option[Timestamp],
    product_id: Option[String],
    price: Option[Double],
    side: Option[String],
    last_size: Option[Double],
    best_bid: Option[Double],
    best_ask: Option[Double]
)
