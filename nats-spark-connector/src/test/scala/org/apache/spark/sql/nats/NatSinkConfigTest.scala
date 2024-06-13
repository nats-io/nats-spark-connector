package org.apache.spark.sql.nats

import munit.FunSuite

class NatSinkConfigTest extends FunSuite {
  test("it parses config values") {
    val params = Map(
      "nats.host" -> "localhost",
      "nats.port" -> "4222",
      "nats.credential.file" -> "/tmp/secret.txt",
      "nats.stream.name" -> "subject"
    )
    val expectedConfig = NatsSinkConfig(
      PublisherJetStreamConfig(
        "localhost",
        4222,
        "/tmp/secret.txt"
      ),
      "subject"
    )
    assertEquals(NatsSinkConfig(params), expectedConfig)
  }
}
