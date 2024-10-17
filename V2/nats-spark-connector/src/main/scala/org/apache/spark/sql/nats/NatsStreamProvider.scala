package org.apache.spark.sql.nats

import io.nats.client.api.ConsumerConfiguration
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.nats.NatsConnection.withConnection
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import java.nio.file.Files
import java.nio.file.Path
import scala.collection.JavaConverters._

class DefaultSource 
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
    val authFileBytes = Files.readAllBytes(Path.of(config.jetStreamConfig.credentialsFile))
    val connectionConfig = NatsConnectionConfig(
      authFileBytes,
      s"nats://${config.jetStreamConfig.host}:${config.jetStreamConfig.port}")

    if (config.subscriptionConfig.createConsumer) {
      val consumerConfiguration = ConsumerConfiguration
        .builder()
        .durable(config.subscriptionConfig.consumerConfig.durableName)
        .ackWait(config.subscriptionConfig.consumerConfig.msgAckTime.toMillis)
        .maxAckPending(config.subscriptionConfig.consumerConfig.maxAckPending)
        .maxBatch(config.subscriptionConfig.consumerConfig.maxBatch.toLong)
        .filterSubjects(config.subscriptionConfig.consumerConfig.filterSubjects.asJava)
        .build()
      withConnection(connectionConfig)(
        _.jetStreamManagement()
          .addOrUpdateConsumer(config.subscriptionConfig.streamName, consumerConfiguration))

    }

    NatsSource(
      sqlContext,
      NatsSourceParams(
        connectionConfig,
        config.subscriptionConfig.streamName,
        config.subscriptionConfig.consumerConfig.durableName,
        config.batchSize,
        config.maxWait,
        config.subscriptionConfig.payloadCompression
      )
    )
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val config = NatsSinkConfig(parameters)
    val authFileBytes = Files.readAllBytes(Path.of(config.jetStreamConfig.credentialsFile))
    val publisherConfig = NatsPublisherConfig(
      NatsConnectionConfig(
        authFileBytes,
        s"nats://${config.jetStreamConfig.host}:${config.jetStreamConfig.port}"),
      config.stream)
    NatsSink(publisherConfig)
  }
}
