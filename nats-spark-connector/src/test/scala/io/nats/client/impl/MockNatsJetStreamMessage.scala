package io.nats.client.impl

import io.nats.client.Message
import munit.FunSuite

import java.util.concurrent.atomic.AtomicInteger

object MockNatsJetStreamMessage {

  private val MOCK_SID_HOLDER = new AtomicInteger(4273)

  def mockSid: String = MOCK_SID_HOLDER.incrementAndGet.toString

  def getTestJsMessage(seq: Long): String =
    s"$$JS.ACK.v2Domain.v2Hash.test-stream.test-consumer.1.$seq.$seq.1605139610113260000.4"

  def apply(currentPosition: Long, data: Array[Byte]): Message = {
    val sid = mockSid
    val subject = "fake"
    val replyTo = getTestJsMessage(currentPosition)
    val protocolLength = 0
    val utf8mode = false
    val incomingMessageFactory = new IncomingMessageFactory(
      sid,
      subject,
      replyTo,
      protocolLength,
      utf8mode
    )
    incomingMessageFactory.setData(data)
    incomingMessageFactory.getMessage
  }
}

// Maybe not the best idea to test your tests, but hey, why not
class MockMessageTest extends FunSuite {
  test("it creates a NatsJetStreamMessage message") {
    assert(MockNatsJetStreamMessage(1, Array.empty).isJetStream)
  }
  test("it creates a NatsJetStreamMessage valid metadata") {
    val long = 1066L
    assertEquals(
      MockNatsJetStreamMessage(long, Array.empty).metaData().consumerSequence(),
      long)
  }
  test("it gets data") {
    val long = 1066L
    val secretMessage = "hunter2"
    val resp = MockNatsJetStreamMessage(long, secretMessage.getBytes).getData
    assertEquals(new String(resp), secretMessage)
  }
}
