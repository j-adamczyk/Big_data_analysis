package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
case class Level2Message(
    `type`: Option[String] // TODO
)
