package natstest

import natsconnector.NatsSubBatchMgr
import natsconnector.NatsPublisher
import natsconnector.NatsConfig
import natsconnector.NatsConfigSource
import natsconnector.NatsConfigSink

object NatsBatchTestDriver extends App {
  val parameters: Map[String, String] = 
    Map(
      "nats.stream.name" -> "TestBatchStream", 
      "nats.stream.subjects" -> "BatchTest1, BatchTest2",
      "nats.host" -> "localhost",
      "nats.port" -> "4222",
      "nats.msg.ack.wait.secs" -> "10",
      "nats.durable.name" -> "Durable",
      "nats.js.define-stream" -> "true",
      "nats.js.define-consumer" -> "true",
    )
  NatsConfigSource.config.setConnection(parameters)
  NatsConfigSink.config.streamName = Some("TestBatchStream")
  NatsConfigSink.config.streamSubjects = Some("BatchTest1,BatchTest2")
  NatsConfigSink.config.setConnection(parameters)

  val batchMgr = new NatsSubBatchMgr()

  val batchId1 = batchMgr.startNewBatch(None)
  val batchId2 = batchMgr.startNewBatch(None)

  val natsSink = new NatsPublisher()
  val subjects = NatsConfigSink.config.streamSubjects.get.replace(" ", "").split(",")
  // Send msgs
  for(i <- 1 to 100) {
    val data = s"This is msg ${i}"
    subjects.foreach(subject => {
      natsSink.sendNatsMsg(data,subject)
    }) 
  //  Thread.sleep(50) 
  }
  // Give a moment for the subscribers to catch up
  Thread.sleep(1000)
 
  val batch1 = batchMgr.freezeAndGetBatch(batchId1)
  val batch2 = batchMgr.freezeAndGetBatch(batchId2)

  println()
  println(s"================= BATCH 1 id:${batchId1} =================")
  println(s"Number of batched msgs:${batch1.length}")
  println("--------Batch messages:")
  batch1.foreach(msg => println(msg))
  val result1 = batchMgr.commitBatch(batchId1)
  println(s"Commit success:${result1}")
  Thread.sleep(1000)

  println()
  println(s"================= BATCH 2 id:${batchId2} =================")
  println(s"Number of batched msgs:${batch2.length}")
  println("--------Batch messages:")
  batch2.foreach(msg => println(msg))
  val result2 = batchMgr.commitBatch(batchId2)
  println(s"Commit success:${result2}")

  // Confirm all messages have been committed
  val batchId3 = batchMgr.startNewBatch(None)
  Thread.sleep(1000)
  val batch3 = batchMgr.freezeAndGetBatch(batchId3)
  println()
  if(batch3.length <= 0) {
    println("All messages confirmed committed!")
  }
  else {
    println("Messages still pending, i.e. not committed:")
    batch3.foreach(msg => println(msg))
  }

}
