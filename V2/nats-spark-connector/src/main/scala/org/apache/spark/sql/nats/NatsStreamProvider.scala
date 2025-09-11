package org.apache.spark.sql.nats

import io.nats.client.JetStreamOptions
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.support.NatsJetStreamConstants
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.nats.NatsConnection.withConnection
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

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
    val authFileBytes = Files.readAllBytes(Paths.get(config.jetStreamConfig.credentialsFile))
    val jsApiPrefix = parameters.getOrElse(sourceJsAPIPrefix, NatsJetStreamConstants.DEFAULT_API_PREFIX)
    val jetStreamOptions = JetStreamOptions.builder().prefix(jsApiPrefix).build()
    val connectionConfig = NatsConnectionConfig(
      authFileBytes,
      s"nats://${config.jetStreamConfig.host}:${config.jetStreamConfig.port}", parameters, jsApiPrefix)

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
        _.jetStreamManagement(jetStreamOptions)
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
    val authFileBytes = Files.readAllBytes(Paths.get(config.jetStreamConfig.credentialsFile))
    val jsApiPrefix = parameters.getOrElse(sinkJsAPIPrefix, NatsJetStreamConstants.DEFAULT_API_PREFIX)

    val connectionConfig = NatsConnectionConfig(
      authFileBytes,
      s"nats://${config.jetStreamConfig.host}:${config.jetStreamConfig.port}", parameters, jsApiPrefix)

    val publisherConfig = NatsPublisherConfig(
      connectionConfig,
      config.stream)
    NatsSink(publisherConfig)
  }
}
