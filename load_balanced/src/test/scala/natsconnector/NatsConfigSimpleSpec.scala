package natsconnector

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import io.nats.client.api.StorageType
import java.time.Duration

class NatsConfigSimpleSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  var sourceConfig: NatsConfig = _
  var sinkConfig: NatsConfig = _

  override def beforeEach(): Unit = {
    sourceConfig = new NatsConfig(isSource = true)
    sinkConfig = new NatsConfig(isSource = false)
  }

  override def afterEach(): Unit = {
    if (sourceConfig != null && !sourceConfig.isClosed) {
      sourceConfig.close()
    }
    if (sinkConfig != null && !sinkConfig.isClosed) {
      sinkConfig.close()
    }
  }

  "NatsConfig" should "initialize with default values" in {
    sourceConfig.host should be("0.0.0.0")
    sourceConfig.port should be("4222")
    sourceConfig.allowReconnect should be(true)
    sourceConfig.connectionTimeout should be(Duration.ofSeconds(20))
    sourceConfig.storageType should be(StorageType.File)
    sourceConfig.numListeners should be(1)
    sourceConfig.msgFetchBatchSize should be(100)
  }

  it should "handle missing required source parameters gracefully" in {
    val missingStreamName = Map(
      "nats.host" -> "localhost",
      "nats.port" -> "4222",
      "nats.msg.ack.wait.secs" -> "60"
    )

    an[RuntimeException] should be thrownBy {
      sourceConfig.setConnection(missingStreamName)
    }
  }

  it should "handle missing required sink parameters gracefully" in {
    val missingHost = Map(
      "nats.port" -> "4222"
    )

    an[RuntimeException] should be thrownBy {
      sinkConfig.setConnection(missingHost)
    }
  }

  it should "set optional parameters correctly" in {
    val parameters = Map(
      "nats.host" -> "localhost",
      "nats.port" -> "4222",
      "nats.stream.name" -> "TestStream",
      "nats.stream.subjects" -> "test.>",
      "nats.msg.ack.wait.secs" -> "60",
      "nats.datetime.format" -> "yyyy-MM-dd HH:mm:ss",
      "nats.ack.none" -> "true",
      "nats.num.listeners" -> "5",
      "nats.allow.reconnect" -> "false",
      "nats.connection.timeout" -> "10"
    )

    // This will fail at connection time, but we can test parameter parsing
    try {
      sourceConfig.setConnection(parameters)
    } catch {
      case _: Exception => // Expected since we don't have a NATS server running
    }

    sourceConfig.dateTimeFormat should be("yyyy-MM-dd HH:mm:ss")
    sourceConfig.ackNone should be(true)
    sourceConfig.numListeners should be(5)
    sourceConfig.allowReconnect should be(false)
    sourceConfig.connectionTimeout should be(Duration.ofSeconds(10))
  }

  it should "handle storage type parameters" in {
    val parameters = Map(
      "nats.host" -> "localhost",
      "nats.port" -> "4222",
      "nats.stream.name" -> "TestStream",
      "nats.stream.subjects" -> "test.>",
      "nats.msg.ack.wait.secs" -> "60",
      "nats.storage.type" -> "memory"
    )

    try {
      sourceConfig.setConnection(parameters)
    } catch {
      case _: Exception => // Expected since we don't have a NATS server running
    }

    sourceConfig.storageType should be(StorageType.Memory)
  }

  it should "close connection properly" in {
    sinkConfig.isClosed should be(false)
    sinkConfig.close()
    sinkConfig.isClosed should be(true)
  }

  it should "handle multiple close calls safely" in {
    sinkConfig.close()
    
    noException should be thrownBy {
      sinkConfig.close()
    }
    
    sinkConfig.isClosed should be(true)
  }

  "NatsConfigSource and NatsConfigSink" should "manage configurations independently" in {
    val sourceKey = "test-source-key"
    val sinkKey = "test-sink-key"

    val sourceConfig = NatsConfigSource.getConfig(sourceKey)
    val sinkConfig = NatsConfigSink.getConfig(sinkKey)

    sourceConfig should not be theSameInstanceAs(sinkConfig)

    NatsConfigSource.removeConfig(sourceKey)
    NatsConfigSink.removeConfig(sinkKey)
  }

  it should "return same instance for same key" in {
    val key = "test-key"

    val config1 = NatsConfigSource.getConfig(key)
    val config2 = NatsConfigSource.getConfig(key)

    config1 should be theSameInstanceAs config2

    NatsConfigSource.removeConfig(key)
  }

  it should "remove configurations correctly" in {
    val key = "test-remove-key"

    val config1 = NatsConfigSource.getConfig(key)
    NatsConfigSource.removeConfig(key)
    val config2 = NatsConfigSource.getConfig(key)

    config1 should not be theSameInstanceAs(config2)

    NatsConfigSource.removeConfig(key)
  }
}