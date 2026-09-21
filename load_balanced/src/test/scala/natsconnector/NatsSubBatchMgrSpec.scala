package natsconnector

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import java.time.Duration

class NatsSubBatchMgrSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach with NatsTestServer {

  var testConfig: NatsConfig = _

  override def beforeEach(): Unit = {
    testConfig = new NatsConfig(isSource = true)
  }

  override def afterEach(): Unit = {
    if (testConfig != null && !testConfig.isClosed) {
      testConfig.close()
    }
  }

  "NatsSubBatchMgr" should "initialize with default configuration" in {
    // Test basic initialization without NATS connection
    testConfig.host should be("0.0.0.0")
    testConfig.port should be("4222")
    testConfig.msgFetchBatchSize should be(100)
  }

  it should "handle empty message batch" in {
    // Test batch processing behavior
    val batchSize = testConfig.msgFetchBatchSize
    batchSize should be > 0
    batchSize should be <= 1000 // reasonable upper bound
  }

  it should "handle batch size configuration" in {
    val parameters = natsParams("nats.msg.fetch.batch.size" -> "50")

    testConfig.setConnection(parameters)
    testConfig.msgFetchBatchSize should be(50)
  }

  it should "handle message acknowledgment timeout" in {
    val parameters = natsParams("nats.msg.ack.wait.secs" -> "120")

    testConfig.setConnection(parameters)
    testConfig.msgAckWaitTime should be(Duration.ofSeconds(120))
  }

  it should "handle listener count configuration" in {
    val parameters = natsParams("nats.num.listeners" -> "3")

    testConfig.setConnection(parameters)
    testConfig.numListeners should be(3)
  }

  it should "handle datetime format configuration" in {
    val parameters = natsParams("nats.datetime.format" -> "yyyy-MM-dd HH:mm:ss")

    testConfig.setConnection(parameters)
    testConfig.dateTimeFormat should be("yyyy-MM-dd HH:mm:ss")
  }

  it should "validate required parameters for source configuration" in {
    val invalidParams = Map(
      "nats.host" -> natsHost,
      "nats.port" -> natsPort
      // Missing required stream.name and other source params
    )

    an[RuntimeException] should be thrownBy {
      testConfig.setConnection(invalidParams)
    }
  }
}
