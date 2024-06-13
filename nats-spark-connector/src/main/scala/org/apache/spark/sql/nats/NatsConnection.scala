package org.apache.spark.sql.nats

import io.nats.client.Connection
import io.nats.client.Nats
import org.apache.spark.internal.Logging

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
final case class NatsConnectionConfig(secretBytes: Array[Byte], url: String)

object NatsConnection extends Logging with Serializable {

  def withConnection[T](natsConnectionConfig: NatsConnectionConfig)(
      func: Connection => T): T = {
    logDebug(s"Connecting to ${natsConnectionConfig.url}")
    val auth = Nats.staticCredentials(natsConnectionConfig.secretBytes)
    val connection = Nats.connect(natsConnectionConfig.url, auth)
    try {
      logTrace("Running function")
      func(connection)
    } finally {
      logDebug("Closing connection")
      connection.close()
    }
  }

}
