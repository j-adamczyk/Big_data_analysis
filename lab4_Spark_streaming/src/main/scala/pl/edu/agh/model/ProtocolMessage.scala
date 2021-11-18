package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
case class Channel(name: String, product_ids: List[String] = List())

@JsonCodec
case class ProtocolMessage(`type`: String, channels: List[Channel])
