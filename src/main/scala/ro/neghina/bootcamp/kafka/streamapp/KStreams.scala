package ro.neghina.bootcamp.kafka.streamapp

import org.apache.kafka.streams.scala._
import ImplicitConversions._
import serialization.Serdes._

object KStreams extends KafkaApp {
  override def topology: StreamsBuilder = {
    val orderNumberStart = "orderNumber-"

    val builder: StreamsBuilder = new StreamsBuilder
    val firstStream = builder.stream[String, String](StreamSettings.inputTopic)

    firstStream
      .peek((k,v) => println(s"key $k value $v"))
      .filter((k,v) => v.startsWith(orderNumberStart))
      .mapValues(v => v.substring(v.indexOf("-") + 1))
      .filter((k,v) => v.toLong > 1000)
      .peek((k,v) => println(s"key $k value $v"))
      .to(StreamSettings.outputTopic)

    builder
  }
}
