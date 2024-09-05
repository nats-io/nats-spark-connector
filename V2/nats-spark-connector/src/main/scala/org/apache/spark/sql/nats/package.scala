package org.apache.spark.sql

package object nats {

  // Source connection configuration
  val SourceJSHostOption = "nats.host"
  val SourceJSPortOption = "nats.port"
  val SourceJSCredentialFileOption = "nats.credential.file"
  val SourceTlsAlgorithmOption = "nats.tls.algorithm"
  val SourceTrustStorePathOption = "nats.trust.store.path"
  val SourceTrustStorePasswordOption = "nats.trust.store.password"
  val SourceKeyStorePathOption = "nats.key.store.path"
  val SourceKeyStorePasswordOption = "nats.key.store.password"
  val SourceSslContextFactoryOption = "nats.ssl.context.factory.class"
  val SourceJSAPIPrefixOption = "nats.js.api.prefix"

  // Subscription Config
  val SourcePullSubscriptionNameOption = "nats.pull.subscription.stream.name"
  val SourcePullSubscriptionDurableOption = "nats.pull.subscription.durable.name"

  // Consumer Config
  val SourceConsumerCreateOption = "nats.pull.consumer.create"
  val SourceConsumerMsgAckTimeOption = "nats.pull.consumer.ack.wait"
  val SourceConsumerMaxAckPendingOption = "nats.pull.consumer.max.ack.pending"
  val SourceConsumerMaxBatchOption = "nats.pull.consumer.max.batch"
  val SourceConsumerFilterSubjectsOption = "nats.stream.subjects"

  // Pull batcher config
  val SourcePullBatchSizeOption = "nats.pull.batch.size"
  val SourcePullWaitTimeOption = "nats.pull.wait.time"

  // Sink onnection configuration
  val SinkJSHostOption = "nats.host"
  val SinkJSPortOption = "nats.port"
  val SinkJSCredentialFileOption = "nats.credential.file"
  val SinkStreamNameOption = "nats.stream.name"
  val SinkTlsAlgorithmOption = "nats.tls.algorithm"
  val SinkTrustStorePathOption = "nats.trust.store.path"
  val SinkTrustStorePasswordOption = "nats.trust.store.password"
  val SinkKeyStorePathOption = "nats.key.store.path"
  val SinkKeyStorePasswordOption = "nats.key.store.password"
  val SinkSslContextFactoryOption = "nats.ssl.context.factory.class"
  val SinkJSAPIPrefixOption = "nats.js.api.prefix"

}
