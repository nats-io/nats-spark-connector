package org.apache.spark.sql.nats

import io.nats.client.JetStream
import io.nats.client.Message
import io.nats.client.PublishOptions

import scala.util.Try

trait NatsPublisher {
  def push(messages: Message): Try[Unit]
}

class NatsPublisherImpl(natsClient: JetStream, stream: String) extends NatsPublisher {
  private val pushOptions = PublishOptions.builder().stream(stream).build()
  override def push(message: Message): Try[Unit] = Try(natsClient.publish(message, pushOptions))
}

object NatsPublisher {
  def apply(jetStream: JetStream, stream: String): NatsPublisher =
    new NatsPublisherImpl(jetStream, stream)
}
