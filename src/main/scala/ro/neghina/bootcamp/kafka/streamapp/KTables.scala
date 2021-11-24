package ro.neghina.bootcamp.kafka.streamapp

import org.apache.kafka.streams.scala._
import ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.Materialized
import serialization.Serdes._

object KTables extends KafkaApp {
  override def topology:StreamsBuilder = {
    val orderNumberStart = "orderNumber-"

    val builder: StreamsBuilder = new StreamsBuilder
    val firstKTable = builder.table[String, String](
      topic = StreamSettings.inputTopic,
      materialized = Materialized.as[String, String, ByteArrayKeyValueStore]("ktable-store")
    )

    firstKTable.filter((k,v) => v.startsWith(orderNumberStart))
      .mapValues(v => v.substring(v.indexOf("-") + 1))
      .filter((k,v) => v.toLong > 1000)
      .toStream
      .peek((k,v) => println(s"key $k value $v"))
      .to(StreamSettings.outputTopic)

    builder
  }
}
