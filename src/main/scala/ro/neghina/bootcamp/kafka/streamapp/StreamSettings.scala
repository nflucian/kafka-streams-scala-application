package ro.neghina.bootcamp.kafka.streamapp

import com.typesafe.config.{ Config, ConfigFactory }

object StreamSettings {
  val conf: Config = ConfigFactory.load()

  val bootstrapServers: String = conf.getString("kafka-streams.bootstrap-servers")
  val appID: String            = conf.getString("kafka-streams.application-id")
  val autoResetConfig: String  = conf.getString("kafka-streams.auto-reset-config")
  val inputTopic: String       = conf.getString("kafka-streams.input-topic")
  val inputSecondTopic: String       = conf.getString("kafka-streams.input-second-topic")
  val outputTopic: String      = conf.getString("kafka-streams.output-topic")
  val outputSecondTopic: String      = conf.getString("kafka-streams.output-second-topic")


}
