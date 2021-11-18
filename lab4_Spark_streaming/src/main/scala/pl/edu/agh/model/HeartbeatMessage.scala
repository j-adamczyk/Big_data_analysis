package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
case class HeartbeatMessage(
  `type`: Option[String],
  sequence: Option[Long],
  last_trade_id: Option[Long],
  product_id: Option[String],
  time: Option[String]
)
