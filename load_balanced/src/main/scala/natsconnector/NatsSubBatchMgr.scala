package natsconnector

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import io.nats.client.Message
import io.nats.client.impl.NatsJetStreamMetaData

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.hadoop.shaded.com.google.protobuf
import org.apache.spark.unsafe.types.UTF8String

import java.util.zip.Inflater
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable
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

   def getHeaders (msg:Message):Option[Map[String, List[String]]] = {
     if (msg.hasHeaders) {
       val msgHeaders = mutable.Map[String, List[String]]()
       val hdrs = msg.getHeaders
       hdrs.entrySet().forEach(entry => {
         val key = entry.getKey
         val value = entry.getValue

         msgHeaders(key)= value.toList
       })
       Some(msgHeaders)
     } else {
       None
     }
    }

    def uncompressed(msg:Message):NatsMsg = {

      NatsMsg(msg.getSubject(),
        ZonedDateTime.now().format(df),
        msg.getData(), getHeaders(msg), msg.metaData())
    }

    def compressed(msg:Message):NatsMsg = {
      NatsMsg(msg.getSubject(),
        ZonedDateTime.now().format(df),
        decompress(msg.getData()), getHeaders(msg), msg.metaData())
    }

    if (this.payloadCompression.isDefined) {
      val compressionAlgo = this.payloadCompression.get
      compressionAlgo match {
        case "zlib" =>
          in.foreach(msg => {
            try {
              buffer += compressed(msg)
            } catch {
              case _: Exception =>
                // Couldn't decompress, so just add the raw message rather than failing the whole batch was requested
                if(this.isLocal) {
                  val logger:Logger = NatsLogger.logger
                  logger.error(s"Failed to decompress a message payload using compression ${compressionAlgo}, adding the message as-is to the batch")
                }
                buffer+= uncompressed(msg)
            }
          })
        case "uncompressed" | "none" =>
          in.foreach(msg => buffer+= uncompressed(msg))
        case _ => throw new Exception(s"Unsupported compression: ${compressionAlgo}")
      }
    } else {
      in.foreach(msg => buffer+= uncompressed(msg))
    }

    buffer.toList
  }

}

case class NatsMsg(val subject:String, val dateTime:String, val content:Array[Byte], val headers:Option[Map[String, List[String]]], val jsMetaData:NatsJetStreamMetaData) {
  override def toString():String = {
    s"""{"subject": "${subject}", "Datetime": "${dateTime}", "payload": "${new String(content, StandardCharsets.UTF_8)}", "headers": ${headersToJson()}, "js-metadata": ${jSMetaDataToJson()}}"""
  }

  def headersToJson():String = {
    if (headers.isDefined) {
      "{ " + headers.get.map(entry => s""""${entry._1}": "${entry._2.mkString(",")}"""").mkString(", ") + " }"
    } else {
      ""
    }
  }

  def jSMetaDataToJson():String = {
    if (jsMetaData != null) {
      s"""{"jsMetaData": {"stream" : "${jsMetaData.getStream}", "streamSeq" :  ${jsMetaData.streamSequence()}, "consumerSeq" : ${jsMetaData.consumerSequence()}, "delivered" : ${jsMetaData.deliveredCount()}, "pending" : ${jsMetaData.pendingCount()}, "timestamp" : "${jsMetaData.timestamp()}"}}"""
    } else {
      ""
    }
  }
}

class Batcher() extends Runnable {
  var buffer:ListBuffer[Message] = ListBuffer.empty[Message]
  val natsSubscriber = new NatsSubscriber()
  @volatile var doRun = true
  @volatile var semaphore = false

  override def run(): Unit = {
    this.doRun = true
    var start = System.currentTimeMillis()
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