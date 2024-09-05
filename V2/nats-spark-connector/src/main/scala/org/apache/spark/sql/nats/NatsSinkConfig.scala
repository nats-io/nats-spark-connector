package org.apache.spark.sql.nats

final case class PublisherJetStreamConfig(host: String, port: Int, credentialsFile: String, tlsAlgorithm: Option[String], truststorePath: Option[String], truststorePassword: Option[Array[Char]], keystorePath: Option[String], keystorePassword: Option[Array[Char]], sslContextFactoryClass: Option[String], jsApiPrefix: Option[String])
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

    val tlsAlgorithm = config.get(SinkTlsAlgorithmOption)
    val truststorePath = config.get(SinkTrustStorePathOption)
    val truststorePassword = config.get(SinkTrustStorePasswordOption).map(_.toCharArray)
    val keystorePath = config.get(SinkKeyStorePathOption)
    val keystorePassword = config.get(SinkKeyStorePasswordOption).map(_.toCharArray)
    val sslContextFactoryClass = config.get(SinkSslContextFactoryOption)
    val jsApiPrefix = config.get(SinkJSAPIPrefixOption)

    // subject
    val stream = requiredKey(SinkStreamNameOption)

    NatsSinkConfig(
      PublisherJetStreamConfig(host, port, credFile, tlsAlgorithm, truststorePath, truststorePassword, keystorePath, keystorePassword, sslContextFactoryClass, jsApiPrefix),
      stream
    )
  }
}
