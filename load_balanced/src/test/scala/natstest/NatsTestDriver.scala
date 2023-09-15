package natstest

import io.nats.client.Message
import io.nats.client.impl.NatsMessage

import java.nio.charset.StandardCharsets
import natsconnector.{NatsPublisher, NatsSubscriber}
import natsconnector.NatsConfig
import natsconnector.NatsConfigSource
import natsconnector.NatsConfigSink


object NatsTestDriver extends App {
  var msgNum = 0
  val parameters: Map[String, String] = 
    Map(
      "nats.stream.name" -> "TestStream", 
      "nats.stream.subjects" -> "test1, test2",
      "nats.host" -> "localhost",
      "nats.port" -> "4222",
      "nats.msg.ack.wait.secs" -> "10",
      "nats.durable.name" -> "Durable",
      "nats.storage.replicas" -> "1"
    )
  NatsConfigSource.config.setConnection(parameters)
  NatsConfigSink.config.streamName = Some("TestStream")
  NatsConfigSink.config.streamSubjects = Some("test1, test2")
  NatsConfigSink.config.setConnection(parameters)
  
  val natsSink = new NatsPublisher()
  val subjects = NatsConfigSink.config.streamSubjects.get.replace(" ", "").split(",")
  val testAckAll = false // True tests Ack All Jetstream ack policy, i.e. ack'ing last msg acks all before
  val FAILURE_TEST_ON = true
  val FAILURE_TEST_OFF = false

  new Thread(new TestRunner(4, FAILURE_TEST_OFF, testAckAll), "TestRunner 4").start()
  new Thread(new TestRunner(5, FAILURE_TEST_OFF, testAckAll), "TestRunner 5").start()
  
  while(true) {
    val data = s"This is msg ${msgNum}"
    //natsSink.sendJetStreamMsg(data, subject)
    subjects.foreach(subject => {
      natsSink.sendNatsMsg(data,subject)
    })
    msgNum += 1
    Thread.sleep(100)
  }
}

class TestRunner(id:Int, emulateFailure:Boolean, testAckAll:Boolean) extends Runnable {
  override def run(): Unit = {
    val natsSource = new NatsSubscriber()
    var iterationNum = 0
    var inFailure = false

    while(true) {
      iterationNum += 1

      val msgArray:Array[Message] =  natsSource.pullNext().flatten
      msgArray.foreach(msg => {
          val data = new String(msg.getData(), StandardCharsets.UTF_8)
          println(s"Id ${id} RECEIVED: subject:${msg.getSubject()}, data:${data}")

          if((testAckAll && (iterationNum % this.id == 0)) || !emulateFailure) {
            // if in ack all policy, only acknowledge when a number of msgs divisible by id has been received, or if not in failure.
            msg.ack()
            println(s"Id ${id} SENT ACK on msg: ${new String(msg.getData(), StandardCharsets.US_ASCII)}")
          }
        }
      )

      println()
      Thread.sleep(500)
    }    
  }
 
}
