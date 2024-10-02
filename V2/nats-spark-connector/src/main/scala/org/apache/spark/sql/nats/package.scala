package org.apache.spark.sql

package object nats {

  // JetStream configuration
  val SourceJSHostOption = "nats.host"
  val SourceJSPortOption = "nats.port"
  val SourceJSCredentialFileOption = "nats.credential.file"

  // Subscription Config
  val SourcePullSubscriptionNameOption = "nats.pull.subscription.stream.name"
  val SourcePullSubscriptionDurableOption = "nats.pull.subscription.durable.name"
  val SourcePullSubscriptionCompressionOption = "nats.storage.payload-compression"

  // Consumer Config
  val SourceConsumerCreateOption = "nats.pull.consumer.create"
  val SourceConsumerMsgAckTimeOption = "nats.pull.consumer.ack.wait"
  val SourceConsumerMaxAckPendingOption = "nats.pull.consumer.max.ack.pending"
  val SourceConsumerMaxBatchOption = "nats.pull.consumer.max.batch"
  val SourceConsumerFilterSubjectsOption = "nats.stream.subjects"

  // Pull batcher config
  val SourcePullBatchSizeOption = "nats.pull.batch.size"
  val SourcePullWaitTimeOption = "nats.pull.wait.time"

  // JetStream configuration
  val SinkJSHostOption = "nats.host"
  val SinkJSPortOption = "nats.port"
  val SinkJSCredentialFileOption = "nats.credential.file"
  val SinkStreamNameOption = "nats.stream.name"

}
