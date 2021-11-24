package ro.neghina.bootcamp.kafka.streamapp

import org.apache.kafka.streams.scala._
import ImplicitConversions._
import org.apache.kafka.streams.kstream.JoinWindows
import serialization.Serdes._

import java.time.Duration

object KStreamJoinKStream extends KafkaApp {
  override def topology: StreamsBuilder = {
    val builder: StreamsBuilder = new StreamsBuilder
    val firstStream = builder.stream[String, String](StreamSettings.inputTopic)
    val secondStream = builder.stream[String, String](StreamSettings.inputSecondTopic)

    firstStream.outerJoin(secondStream)(
        (left, right) => s"left=$left, right=$right",
        JoinWindows.of(Duration.ofMinutes(10))
      )
      .peek((k, v) => println(s"key $k value $v"))
      .to(StreamSettings.outputTopic)

    builder
  }
}
