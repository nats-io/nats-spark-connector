package org.apache.spark.sql.nats

import io.nats.client.{Connection, Nats, Options}
import org.apache.spark.internal.Logging

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
final case class NatsConnectionConfig(secretBytes: Array[Byte], url: String, params: Map[String, String])

object NatsConnection extends Logging with Serializable {

  def withConnection[T](natsConnectionConfig: NatsConnectionConfig)(
      func: Connection => T): T = {
    logDebug(s"Connecting to ${natsConnectionConfig.url}")
    val auth = Nats.staticCredentials(natsConnectionConfig.secretBytes)
    val optionsBuilder = Options.builder().server(natsConnectionConfig.url)
      .authHandler(auth)
    val params = natsConnectionConfig.params

    if(params.contains(trustStorePath)) {
      optionsBuilder.truststorePath(params.getOrElse(trustStorePath, null))
    }
    if(params.contains(trustStorePassword)){
      optionsBuilder.truststorePassword(params.getOrElse(trustStorePassword, "").toCharArray)
    }
    if(params.contains(keyStorePath)) {
      optionsBuilder.keystorePath(params.getOrElse(keyStorePath, null))
    }
    if(params.contains(keyStorePassword)){
      optionsBuilder.keystorePassword(params.getOrElse(keyStorePassword, "").toCharArray)
    }
    if(params.contains(tlsAlgorithm)){
      optionsBuilder.tlsAlgorithm(params.getOrElse(tlsAlgorithm, ""))
    }

    val options = optionsBuilder.build()
    val connection = Nats.connect(options)
    try {
      logTrace("Running function")
      func(connection)
    } finally {
      logDebug("Closing connection")
      connection.close()
    }
  }

}
