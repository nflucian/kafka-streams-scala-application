package ro.neghina.bootcamp.kafka.streamapp

import org.apache.kafka.streams.scala._
import ImplicitConversions._
import org.apache.kafka.streams.scala.kstream._
import serialization.Serdes._

object KTableJoinKTable extends KafkaApp {
  override def topology: StreamsBuilder = {
    val builder = new StreamsBuilder()
    val firstTable: KTable[String, String] = builder.table(StreamSettings.inputTopic)
    val secondTable: KTable[String, String] = builder.table(StreamSettings.inputSecondTopic)

    firstTable.join(secondTable){
      (left, right) => s"left=$left, right=$right"
    }
      .toStream
      .peek((k,v) => println(s"key $k value $v"))
      .to(StreamSettings.outputTopic)

    builder
  }
}
