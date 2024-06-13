package org.apache.spark.sql.nats

import munit.FunSuite

import scala.concurrent.duration._

class NatSourceConfigTest extends FunSuite {
  test("it parses config values") {
    val params = Map(
      "nats.host" -> "localhost",
      "nats.port" -> "4222",
      "nats.credential.file" -> "/tmp/secret.txt",
      "nats.pull.subscription.stream.name" -> "stream",
      "nats.pull.subscription.durable.name" -> "durable",
      "nats.pull.consumer.create" -> "false",
      "nats.pull.consumer.ack.wait" -> "1",
      "nats.pull.consumer.max.ack.pending" -> "7",
      "nats.pull.consumer.max.batch" -> "2",
      "nats.stream.subjects" -> "a,b",
      "nats.pull.batch.size" -> "5",
      "nats.pull.wait.time" -> "6"
    )
    val createConsumer = false
    val expectedConfig = NatsSourceConfig(
      JetStreamConfig("localhost", 4222, "/tmp/secret.txt"),
      SubscriptionConfig(
        "stream",
        createConsumer,
        ConsumerConfig(
          1.second,
          2,
          7,
          Seq("a", "b"),
          "durable"
        )),
      5,
      6.seconds
    )

    assertEquals(NatsSourceConfig(params), expectedConfig)
  }
}
