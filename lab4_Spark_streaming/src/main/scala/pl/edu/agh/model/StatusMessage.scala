package pl.edu.agh.model

import io.circe.generic.JsonCodec

@JsonCodec
case class StatusMessage(
    `type`: Option[String] // TODO
)
