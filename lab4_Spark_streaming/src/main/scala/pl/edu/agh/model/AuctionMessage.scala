package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
case class AuctionMessage(
    `type`: Option[String],
    product_id: Option[String],
    sequence: Option[Long],
    auction_state: Option[String],
    best_bid_price: Option[Double],
    best_bid_size: Option[Double],
    best_ask_price: Option[Double],
    best_ask_size: Option[Double],
    open_price: Option[Double],
    open_size: Option[Double],
    can_open: Option[String],
    time: Option[String]
)
