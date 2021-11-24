package ro.neghina.bootcamp.kafka.streamapp

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder

import java.time.Duration
import java.util.Properties

trait KafkaApp extends App {
  def topology: StreamsBuilder

  val properties = new Properties()
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StreamSettings.autoResetConfig)
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, StreamSettings.bootstrapServers)
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamSettings.appID)
  properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0L: java.lang.Long)

  val kStream = new KafkaStreams(topology.build(), properties)
  kStream.start()

  // attach shutdown handler to catch control-c
  sys.ShutdownHookThread {
    kStream.close(Duration.ofSeconds(10))
  }

}
