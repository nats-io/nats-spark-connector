package org.apache.spark.sql.nats

import java.util.Locale
import scala.concurrent.duration._

final case class JetStreamConfig(host: String, port: Int, credentialsFile: String)
final case class ConsumerConfig(
    msgAckTime: FiniteDuration,
    maxBatch: Int,
    maxAckPending: Int,
    filterSubjects: Seq[String],
    durableName: String
)
final case class SubscriptionConfig(
    streamName: String,
    createConsumer: Boolean,
    consumerConfig: ConsumerConfig,
    batcherConfig: BatcherConfig)
final case class BatcherConfig(
    initialDelay: FiniteDuration,
    frequencySecs: FiniteDuration,
    pullBatchSize: Int,
    pullWaitTime: FiniteDuration
)
final case class NatsSourceConfig(
    jetStreamConfig: JetStreamConfig,
    subscriptionConfig: SubscriptionConfig
)

// TODO(mrosti): cats.Validated or PureConfig pls
object NatsSourceConfig {
  private def required(config: Map[String, String])(key: String): String =
    config.getOrElse(key, throw new IllegalArgumentException(s"Please specify '$key'"))
  private def default(config: Map[String, String])(key: String, default: String): String =
    config.getOrElse(key, default)

  def apply(config: Map[String, String]): NatsSourceConfig = {
    def requiredKey(key: String): String = required(config)(key)
    def defaultKey(key: String, dflt: String): String = default(config)(key, dflt)

    // JetStream configuration
    val host = requiredKey(SourceJSHostOption)
    val port = requiredKey(SourceJSPortOption).toInt
    val credFile = requiredKey(SourceJSCredentialFileOption)

    // Subscription Config
    val streamName = requiredKey(SourcePullSubscriptionNameOption)
    val durableName = requiredKey(SourcePullSubscriptionDurableOption)

    // Consumer Config
    val createConsumer =
      defaultKey(SourceConsumerCreateOption, "true").toLowerCase(Locale.US).toBoolean
    val msgAckTime = defaultKey(SourceConsumerMsgAckTimeOption, "90").toInt.seconds
    val maxAckPending = defaultKey(SourceConsumerMaxAckPendingOption, "1000").toInt
    val maxBatch = defaultKey(SourceConsumerMaxBatchOption, "100").toInt
    val filterSubjects =
      defaultKey(SourceConsumerFilterSubjectsOption, "").split(",")

    // Read batch options
    val initialDelay = defaultKey(SourceBatcherInitialDelayOption, "60").toInt.seconds
    val frequencySecs = defaultKey(SourceBatcherFrequencySecsOption, "30").toInt.seconds
    val pullBatchSize = defaultKey(SourcePullBatchSizeOption, "100").toInt
    val pullWaitTime = defaultKey(SourcePullWaitTimeOption, "60").toInt.seconds

    NatsSourceConfig(
      JetStreamConfig(host, port, credFile),
      SubscriptionConfig(
        streamName,
        createConsumer,
        ConsumerConfig(msgAckTime, maxBatch, maxAckPending, filterSubjects, durableName),
        BatcherConfig(initialDelay, frequencySecs, pullBatchSize, pullWaitTime)
      )
    )
  }
}
