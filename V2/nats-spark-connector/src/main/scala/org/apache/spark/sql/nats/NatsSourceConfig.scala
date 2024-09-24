package org.apache.spark.sql.nats

import org.apache.spark.internal.Logging

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
    payloadCompression: String
)

final case class NatsSourceConfig(
    jetStreamConfig: JetStreamConfig,
    subscriptionConfig: SubscriptionConfig,
    batchSize: Int,
    maxWait: FiniteDuration
)

// TODO(mrosti): cats.Validated or PureConfig pls
object NatsSourceConfig extends Logging {
  private def required(config: Map[String, String])(key: String): String =
    config.getOrElse(key, throw new IllegalArgumentException(s"Please specify '$key'"))
  private def default(config: Map[String, String])(key: String, default: String): String =
    config.getOrElse(key, default)

  def apply(config: Map[String, String]): NatsSourceConfig = {
    def requiredKey(key: String): String = required(config)(key)
    def defaultKey(key: String, dflt: String): String = default(config)(key, dflt)

    // Connection configuration
    val host = requiredKey(SourceJSHostOption)
    val port = requiredKey(SourceJSPortOption).toInt
    val credFile = requiredKey(SourceJSCredentialFileOption)

    // Stream Config
    val streamName = requiredKey(SourcePullSubscriptionNameOption)
    val durableName = requiredKey(SourcePullSubscriptionDurableOption)
    val payloadCompression = defaultKey(SourcePullSubscriptionCompressionOption, "none")
    payloadCompression match {
      case "none" | "zlib" => () 
      case _ => println(s"Option passed to '$SourcePullSubscriptionCompressionOption' not recognized. Defaulting to 'none'")
    }

    // Consumer Config
    val createConsumer =
      defaultKey(SourceConsumerCreateOption, "true").toLowerCase(Locale.US).toBoolean
    val msgAckTime = defaultKey(SourceConsumerMsgAckTimeOption, "90").toInt.seconds
    val maxAckPending = defaultKey(SourceConsumerMaxAckPendingOption, "1000").toInt
    val maxBatch = defaultKey(SourceConsumerMaxBatchOption, "100").toInt
    val filterSubjects =
      defaultKey(SourceConsumerFilterSubjectsOption, "").split(",")

    // Read batch options
    val batchSize = defaultKey(SourcePullBatchSizeOption, "1000").toInt
    val maxWait = defaultKey(SourcePullWaitTimeOption, "1").toInt.seconds

    val conf = NatsSourceConfig(
      JetStreamConfig(host, port, credFile),
      SubscriptionConfig(
        streamName,
        createConsumer,
        ConsumerConfig(msgAckTime, maxBatch, maxAckPending, filterSubjects, durableName),
        payloadCompression
      ),
      batchSize,
      maxWait
    )
    logInfo(s"$conf")
    conf
  }
}
