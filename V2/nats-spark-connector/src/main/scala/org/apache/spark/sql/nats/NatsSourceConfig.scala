package org.apache.spark.sql.nats

import org.apache.spark.internal.Logging

import java.util.Locale
import scala.concurrent.duration._

final case class JetStreamConfig(host: String, port: Int, credentialsFile: String, tlsAlgorithm: Option[String], truststorePath: Option[String], truststorePassword: Option[Array[Char]], keystorePath: Option[String], keystorePassword: Option[Array[Char]], sslContextFactoryClass: Option[String], jsApiPrefix: Option[String])
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
    consumerConfig: ConsumerConfig)

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
    val tlsAlgorithm = config.get(SourceTlsAlgorithmOption)
    val truststorePath = config.get(SourceTrustStorePathOption)
    val truststorePassword = config.get(SourceTrustStorePasswordOption).map(_.toCharArray)
    val keystorePath = config.get(SourceKeyStorePathOption)
    val keystorePassword = config.get(SourceKeyStorePasswordOption).map(_.toCharArray)
    val sslContextFactoryClass = config.get(SourceSslContextFactoryOption)
    val jsApiPrefix = config.get(SourceJSAPIPrefixOption)

    // Stream Config
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
    val batchSize = defaultKey(SourcePullBatchSizeOption, "1000").toInt
    val maxWait = defaultKey(SourcePullWaitTimeOption, "1").toInt.seconds

    val conf = NatsSourceConfig(
      JetStreamConfig(host, port, credFile, tlsAlgorithm, truststorePath, truststorePassword, keystorePath, keystorePassword, sslContextFactoryClass, jsApiPrefix),
      SubscriptionConfig(
        streamName,
        createConsumer,
        ConsumerConfig(msgAckTime, maxBatch, maxAckPending, filterSubjects, durableName)
      ),
      batchSize,
      maxWait
    )
    logInfo(s"$conf")
    conf
  }
}
