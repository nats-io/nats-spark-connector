package org.apache.spark.sql.nats

import munit.FunSuite

class NatSinkConfigTest extends FunSuite {
  test("it parses config values") {
    val params = Map(
      "nats.host" -> "localhost",
      "nats.port" -> "4222",
      "nats.credential.file" -> "/tmp/secret.txt",
      "nats.stream.name" -> "subject",
      "nats.tls.algorithm" -> "algo",
      "nats.trust.store.path" -> "truststore",
      "nats.key.store.path" -> "keystore",
    )
    val expectedConfig = NatsSinkConfig(
      PublisherJetStreamConfig(
        "localhost",
        4222,
        "/tmp/secret.txt",
        Some("algo"),
        Some("truststore"),
        None,
        Some("keystore"),
        None,
        None,
        None
      ),
      "subject"
    )
    assertEquals(NatsSinkConfig(params), expectedConfig)
  }
}
