package org.apache.spark.sql.nats

import io.nats.client.impl.SSLContextFactory
import io.nats.client.{Connection, JetStream, JetStreamManagement, JetStreamOptions, Nats, Options}
import org.apache.spark.internal.Logging

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
final case class NatsConnectionConfig(secretBytes: Array[Byte], url: String, tlsAlgorithm: Option[String], truststorePath: Option[String], truststorePassword: Option[Array[Char]], keystorePath: Option[String], keystorePassword: Option[Array[Char]], sslContextFactoryClass: Option[String], jsAPIPrefix: Option[String])

object NatsConnection extends Logging with Serializable {

  def getConnectionOptions(natsConnectionConfig: NatsConnectionConfig): Options = {
    val auth = Nats.staticCredentials(natsConnectionConfig.secretBytes)
    val builder = new Options.Builder()
    builder.authHandler(auth)
    builder.server(natsConnectionConfig.url)

    if (System.getenv("NATS_TLS_ALGO") != null && System.getenv("NATS_TLS_ALGO") != "") {
      builder.tlsAlgorithm(System.getenv("NATS_TLS_ALGO"))
    } else natsConnectionConfig.tlsAlgorithm.foreach(builder.tlsAlgorithm)

    if (System.getenv("NATS_TLS_TRUST_STORE") != null && System.getenv("NATS_TLS_TRUST_STORE") != "") {
      builder.truststorePath(System.getenv("NATS_TLS_TRUST_STORE"))
      if (System.getenv("NATS_TLS_TRUST_STORE_PASSWORD") != null) {
        builder.truststorePassword(System.getenv("NATS_TLS_TRUST_STORE_PASSWORD").toCharArray)
      }
    } else {
        natsConnectionConfig.truststorePath.foreach(builder.truststorePath)
        natsConnectionConfig.truststorePassword.foreach(builder.truststorePassword)
    }

    if (System.getenv("NATS_TLS_KEY_STORE") != null) {
      if (System.getenv("NATS_TLS_KEY_STORE_PASSWORD") != null) {
        builder.keystorePassword(System.getenv("NATS_TLS_KEY_STORE_PASSWORD").toCharArray)
      }
      builder.keystorePath(System.getenv("NATS_TLS_KEY_STORE"))
    } else {
      natsConnectionConfig.keystorePath.foreach(builder.keystorePath)
      natsConnectionConfig.keystorePassword.foreach(builder.keystorePassword)
    }

    def setContextFactory(cn: String): Unit = {
      Class.forName(cn) match {
        case c if classOf[SSLContextFactory].isAssignableFrom(c) =>
          builder.sslContextFactory(c.asInstanceOf[Class[SSLContextFactory]].getDeclaredConstructor().newInstance())
        case _ =>
          throw new IllegalArgumentException(
            "The class provided for sslContextFactoryClass must be a subclass of SSLContextFactory"
          )
      }
    }

    natsConnectionConfig.sslContextFactoryClass.foreach(setContextFactory)
    builder.build()
  }

  def withConnection[T](natsConnectionConfig: NatsConnectionConfig)(
    func: Connection => T): T = {
    logDebug(s"Connecting to ${natsConnectionConfig.url}")

    val connection = Nats.connect(getConnectionOptions(natsConnectionConfig))
    try {
      logTrace("Running function")
      func(connection)
    } finally {
      logDebug("Closing connection")
      connection.close()
    }
  }

  def withJS[T](natsConnectionConfig: NatsConnectionConfig)(
    func: JetStream => T): T = {
    logDebug(s"Connecting to ${natsConnectionConfig.url}")

    val connection = Nats.connect(getConnectionOptions(natsConnectionConfig))

    val jsBuilder = JetStreamOptions.builder()

    natsConnectionConfig.jsAPIPrefix.foreach(jsBuilder.prefix)

    val jsOptions = jsBuilder.build()
    val jsm = connection.jetStreamManagement()
    val js = connection.jetStream(jsOptions)
    try {
      logTrace("Running function")
      func(js)
    } finally {
      logDebug("Closing connection")
      connection.close()
    }
  }

  def withJSM[T](natsConnectionConfig: NatsConnectionConfig)(
    func: JetStreamManagement => T): T = {
    logDebug(s"Connecting to ${natsConnectionConfig.url}")

    val connection = Nats.connect(getConnectionOptions(natsConnectionConfig))

    val jsBuilder = JetStreamOptions.builder()

    natsConnectionConfig.jsAPIPrefix.foreach(jsBuilder.prefix)

    val jsOptions = jsBuilder.build()
    val jsm = connection.jetStreamManagement(jsOptions)
    try {
      logTrace("Running function")
      func(jsm)
    } finally {
      logDebug("Closing connection")
      connection.close()
    }
  }
}
