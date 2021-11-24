package ro.neghina.bootcamp.kafka.streamapp

import org.apache.kafka.streams.scala._
import ImplicitConversions._
import org.apache.kafka.streams.scala.kstream._
import serialization.Serdes._

object KStreamJoinKTable extends KafkaApp {
  override def topology: StreamsBuilder = {
    val builder: StreamsBuilder = new StreamsBuilder

    val kStream: KStream[String, String] = builder.stream(StreamSettings.inputTopic)
    val kTable: KTable[String, String] = builder.table(StreamSettings.inputSecondTopic)

    kStream.leftJoin(kTable)((lv, rv)=>"left=" + lv + ", right=" + rv)
      .peek((k, v) => println(s"key $k value $v"))
      .to(StreamSettings.outputTopic)

    builder
  }
}
