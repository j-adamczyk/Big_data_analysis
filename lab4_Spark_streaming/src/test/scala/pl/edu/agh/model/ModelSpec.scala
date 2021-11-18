package pl.edu.agh.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatest._
import flatspec._
import matchers._

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

class ModelSpec extends AnyFlatSpec with should.Matchers {
  // it should "parse sample json with data" in {
  //   val sampleJson =
  //     """
  //       |{
  //       |    "type": "ticker",
  //       |    "trade_id": 20153558,
  //       |    "sequence": 3262786978,
  //       |    "time": "2017-09-02T17:05:49.250000Z",
  //       |    "product_id": "BTC-USD",
  //       |    "price": "4388.01000000",
  //       |    "side": "buy",
  //       |    "last_size": "0.03000000",
  //       |    "best_bid": "4388",
  //       |    "best_ask": "4388.01"
  //       |}""".stripMargin

  //   val expected = OpenAQMessage(
  //     `type` = Some("ticker"),
  //     trade_id = Some(20153558),
  //     sequence = Some(3262786978L),
  //     time = Some("2017-09-02T17:05:49.250000Z"),
  //     product_id = Some("BTC-USD"),
  //     price = Some(4388.01000000),
  //     side = Some("buy"),
  //     last_size = Some(0.03000000),
  //     best_bid = Some(4388),
  //     best_ask = Some(4388.01)
  //   )
  //   val res = Parser[OpenAQMessage](sampleJson)
  //   assert(res.isRight)
  //   res.right.get should be(expected)
  // }
}
