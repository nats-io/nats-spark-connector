package org.apache.spark.sql.nats

import io.nats.client.Nats
import io.nats.client.Options
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import java.time.{Duration => JDuration}
import scala.concurrent.duration.DurationInt

class NatsStreamProvider
    extends DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider {

  override def shortName(): String = "nats"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = providerName -> NatsSource.schema

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val config = NatsSourceConfig(parameters)
    val auth = Nats.credentials(config.jetStreamConfig.credentialsFile)
    val options = new Options.Builder()
      .server(s"nats://${config.jetStreamConfig.host}:${config.jetStreamConfig.port}")
      .authHandler(auth)
      .reconnectWait(JDuration.ofSeconds(1.seconds.toSeconds))
      .verbose()
      .build()
    val connection = Nats.connect(options)

    val natsSubscriber = NatsSubscriber(
      connection,
      config.subscriptionConfig
    )
    val _ = natsSubscriber.pull

    val natsBatchManager = NatsBatchManager(
      natsSubscriber,
      config.subscriptionConfig.batcherConfig
    )

    val source = NatsSource(sqlContext, natsBatchManager)
    source.start()
    source
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val config = NatsSinkConfig(parameters)
    val auth = Nats.credentials(config.jetStreamConfig.credentialsFile)
    val connection = Nats.connect(
      s"nats://${config.jetStreamConfig.host}:${config.jetStreamConfig.port}",
      auth)
    val jetStream = connection.jetStream()

    val publisher = NatsPublisher(jetStream, config.stream)

    NatsSink(publisher)
  }
}
