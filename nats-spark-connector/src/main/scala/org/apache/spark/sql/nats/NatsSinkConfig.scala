package org.apache.spark.sql.nats

final case class PublisherJetStreamConfig(host: String, port: Int, credentialsFile: String)
final case class NatsSinkConfig(jetStreamConfig: PublisherJetStreamConfig, stream: String)

object NatsSinkConfig {
  private def required(config: Map[String, String])(key: String): String =
    config.getOrElse(key, throw new IllegalArgumentException(s"Please specify '$key'"))

  def apply(config: Map[String, String]): NatsSinkConfig = {
    def requiredKey(key: String): String = required(config)(key)

    // JetStream configuration
    val host = requiredKey(SinkJSHostOption)
    val port = requiredKey(SinkJSPortOption).toInt
    val credFile = requiredKey(SinkJSCredentialFileOption)

    // subject
    val stream = requiredKey(SinkStreamNameOption)

    NatsSinkConfig(
      PublisherJetStreamConfig(host, port, credFile),
      stream
    )
  }
}
