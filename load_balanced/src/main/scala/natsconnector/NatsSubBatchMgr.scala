package natsconnector

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import io.nats.client.Message

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.hadoop.shaded.com.google.protobuf

import java.util.zip.Inflater
//import net.razorvine.pyro
import io.nats.client.Nats
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import java.util.zip

class NatsSubBatchMgr {
  val isLocal = false
  var payloadCompression:Option[String] = None
  val batchMap:Map[String, List[Message]] = Map.empty[String, List[Message]]
  var batcherMap:Map[String, Batcher] = Map.empty[String, Batcher]
  //val natsSubscriber:NatsSubscriber = new NatsSubscriber()

  def startNewBatch(payloadCompression:Option[String]):String = {
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info("===================In NatsSubBatchMgr.startNewBatch")
    }

    this.payloadCompression = payloadCompression


    val batcher = new Batcher()
    val batcherThread = new Thread(batcher)
    batcherThread.start()
    val newId = System.currentTimeMillis() + batcherThread.getName()
    this.batcherMap+=(newId -> batcher)
    newId
  }

  def freezeAndGetBatch(batchId:String):List[NatsMsg] = {
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info("=====================In NatsSubBatchMgr.freezeAndGetBatch")
    }
    var batch = List.empty[NatsMsg]
    if(this.batcherMap.contains(batchId)) {
      val batcher = this.batcherMap(batchId)
      val b:List[Message] = batcher.stopAndGetBatch()
      batch = convertBatch(b)
      this.batcherMap -= (batchId)
      this.batchMap += (batchId -> b)
    }
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.debug(
        s"-------- Batch for ID = ${batchId} in internal NatsMsg format:\n"
        + s"${batch.foreach(r => logger.debug("  "+r))}"
      )
    }
    batch
  }

  def commitBatch(batchId:String):Boolean = {
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info("======================In NatsSubBatchMgr.commitBatch")
    }
    var committed = false
    if(this.batchMap.contains(batchId)) {
      val batch = this.batchMap(batchId)
      if(this.isLocal) {
        val logger:Logger = NatsLogger.logger
        logger.debug(
            s"-------- Committed Nats message batch for ID = ${batchId}:\n"
            + s"${batch.foreach(r => logger.debug("  "+r))}"
        )
      }
      batch.foreach(msg => msg.ack())
      committed = true
    }
    committed
  }

  def publishBatch(batch:List[NatsMsg]):Unit = {
      val natsPublisher:NatsPublisher = new NatsPublisher()
      batch.foreach(msg => natsPublisher.sendNatsMsg(msg))
  }


  def publishMsg(msg:NatsMsg):Unit = {
    new NatsPublisher().sendNatsMsg(msg)
  }

  private def decompress(inData: Array[Byte]): Array[Byte] = {
    val inflater = new Inflater()
    inflater.setInput(inData)
    val decompressedData = new Array[Byte](inData.size * 2)
    var count = inflater.inflate(decompressedData)
    var finalData = decompressedData.take(count)
    while (count > 0) {
      count = inflater.inflate(decompressedData)
      finalData = finalData ++ decompressedData.take(count)
    }
    return finalData
  }

  private def convertBatch(in:List[Message]):List[NatsMsg] = {
    var buffer:ListBuffer[NatsMsg] = ListBuffer.empty[NatsMsg]
    val df:DateTimeFormatter = DateTimeFormatter.ofPattern(NatsConfigSource.config.dateTimeFormat)

    if (this.payloadCompression.isDefined) {
      val compressionAlgo = this.payloadCompression.get
      compressionAlgo match {
        case "zlib" => {
          in.foreach(msg => buffer+=(
              NatsMsg(msg.getSubject(),
                          msg.metaData().timestamp().format(df),
                          new String(decompress(msg.getData), StandardCharsets.US_ASCII)))
            )
        }
        case "uncompressed" | "none" => {
          in.foreach(msg => buffer+=(
              NatsMsg(msg.getSubject(),
                          msg.metaData().timestamp().format(df),
                          new String(msg.getData(), StandardCharsets.US_ASCII)))
            )
        }
        case _ => throw new Exception(s"Unsupported compression algorithm:${compressionAlgo}")
      }
    } else
      in.foreach(msg => buffer+=(
          NatsMsg(msg.getSubject(),
                      msg.metaData().timestamp().format(df),
                      new String(msg.getData(), StandardCharsets.US_ASCII)))
        )
    buffer.toList
  }

}

case class NatsMsg(val subject:String, val dateTime:String, val content:String) 

class Batcher() extends Runnable {
  var buffer:ListBuffer[Message] = ListBuffer.empty[Message]
  val natsSubscriber = new NatsSubscriber()
  var doRun = true
  var semaphore = false
 
  override def run(): Unit = {
    this.doRun = true
    while(this.doRun) {
      pullAndLoadBatch()
    }
    natsSubscriber.unsubscribe()    
  }

  def stopAndGetBatch():List[Message] = {
    this.doRun = false
    while(this.semaphore) {Thread.sleep(10)}
    val batch = this.buffer.toList
    this.buffer = ListBuffer.empty[Message]
    batch
  }

  private def pullAndLoadBatch():Unit = {
    this.semaphore = true
    val msgList = this.natsSubscriber.pullNext()

    msgList.foreach(msg => {
      if(msg != null) this.buffer.+=(msg)
    })
    this.semaphore = false
  }
}