package org.apache.spark.sql.nats

import io.nats.client._
import io.nats.client.api.ConsumerConfiguration
import org.apache.spark.internal.Logging

import java.time.{Duration => JDuration}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

trait NatsSubscriber {
  def pull: Try[Seq[Message]]
}

class NatsSubscriberImpl(
    subscription: JetStreamSubscription,
    batchSize: Int,
    waitTime: Duration)
    extends NatsSubscriber
    with Logging {
  override def pull: Try[Seq[Message]] =
    Try(subscription.fetch(batchSize, JDuration.ofSeconds(waitTime.toSeconds)).asScala.toSeq)
      .recover {
        case ex: InterruptedException =>
          logError("fetch() waitTime exceeded", ex)
          List.empty[Message]
        case ex: IllegalStateException =>
          logError("Disregarding NATS msg", ex)
          List.empty[Message]
        case ex: Exception =>
          logError("CATASTROPHIC FAILURE", ex)
          List.empty[Message]
      }
}

object NatsSubscriber {
  def apply(
      connection: Connection,
      config: SubscriptionConfig
  ): NatsSubscriber = {
    val jetStream = connection.jetStream()

    val consumerConfiguration = ConsumerConfiguration
      .builder()
      .durable(config.consumerConfig.durableName)
      .ackWait(config.consumerConfig.msgAckTime.toMillis)
      .maxAckPending(config.consumerConfig.maxAckPending)
      .maxBatch(config.consumerConfig.maxBatch.toLong)
      .filterSubjects(config.consumerConfig.filterSubjects.asJava)
      .build()

    if (config.createConsumer)
      connection
        .jetStreamManagement()
        .addOrUpdateConsumer(config.streamName, consumerConfiguration)

    val pullSubscribeOptions =
      PullSubscribeOptions
        .builder()
        .stream(config.streamName)
        .fastBind(true)
        .durable(config.consumerConfig.durableName)
        .configuration(consumerConfiguration)
        .build()

    val subscription = jetStream.subscribe(None.orNull, pullSubscribeOptions)
    new NatsSubscriberImpl(
      subscription,
      config.batcherConfig.pullBatchSize,
      config.batcherConfig.pullWaitTime)
  }
}
