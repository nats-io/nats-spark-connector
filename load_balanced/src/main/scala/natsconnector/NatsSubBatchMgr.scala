package natsconnector

import scala.collection.mutable.ListBuffer
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
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

class NatsSubBatchMgr(natsConfig: NatsConfig) {
  val isLocal = false
  @volatile var payloadCompression:Option[String] = None
  // Use thread-safe concurrent maps
  private val batchMap = new ConcurrentHashMap[String, List[Message]]().asScala
  private val batcherMap = new ConcurrentHashMap[String, Batcher]().asScala
  private val threadMap = new ConcurrentHashMap[String, Thread]().asScala
  // Shared publisher to avoid resource leaks
  private lazy val natsPublisher = new NatsPublisher(natsConfig)
  //val natsSubscriber:NatsSubscriber = new NatsSubscriber()

  def startNewBatch(payloadCompression:Option[String]):String = synchronized {
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info("===================In NatsSubBatchMgr.startNewBatch")
    }

    this.payloadCompression = payloadCompression

    val batcher = new Batcher(natsConfig)
    val batcherThread = new Thread(batcher)
    val timestamp = System.currentTimeMillis()
    batcherThread.setName(s"NatsBatcher-$timestamp")
    batcherThread.setDaemon(true) // Ensure JVM can exit
    batcherThread.start()
    val newId = timestamp.toString + batcherThread.getName()
    
    // Track both batcher and thread for proper lifecycle management
    batcherMap += (newId -> batcher)
    threadMap += (newId -> batcherThread)
    newId
  }

  def freezeAndGetBatch(batchId:String):List[NatsMsg] = synchronized {
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info("=====================In NatsSubBatchMgr.freezeAndGetBatch")
    }
    var batch = List.empty[NatsMsg]
    batcherMap.get(batchId) match {
      case Some(batcher) =>
        val b:List[Message] = batcher.stopAndGetBatch()
        batch = convertBatch(b)
        batcherMap.remove(batchId)
        batchMap.put(batchId, b)
        
        // Wait for thread to finish and clean up
        threadMap.get(batchId) foreach { thread =>
          try {
            thread.join(5000) // Wait up to 5 seconds for thread to finish
            if (thread.isAlive) {
              thread.interrupt() // Force interrupt if still running
            }
          } catch {
            case _: InterruptedException => // Ignore
          }
          threadMap.remove(batchId)
        }
      case None =>
        // Batch ID not found, return empty batch
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

  def commitBatch(batchId:String):Boolean = synchronized {
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info("======================In NatsSubBatchMgr.commitBatch")
    }
    
    batchMap.get(batchId) match {
      case Some(batch) =>
        if(this.isLocal) {
          val logger:Logger = NatsLogger.logger
          logger.debug(s"-------- Committed Nats message batch for ID = ${batchId}")
          batch.foreach(r => logger.debug("  "+r))
        }
        batch.foreach(msg => msg.ack())
        batchMap.remove(batchId)
        true
      case None =>
        false
    }
  }

  def publishBatch(batch:List[NatsMsg]):Unit = {
    // Use shared publisher to avoid resource leaks
    batch.foreach(msg => natsPublisher.sendNatsMsg(msg))
  }

  def publishMsg(msg:NatsMsg):Unit = {
    // Use shared publisher to avoid resource leaks  
    natsPublisher.sendNatsMsg(msg)
  }
  
  def stop(): Unit = synchronized {
    // Stop all running batchers
    batcherMap.values.foreach(_.stop())
    
    // Wait for all threads to finish gracefully
    threadMap.values.foreach { thread =>
      try {
        thread.join(3000) // Wait up to 3 seconds per thread
        if (thread.isAlive) {
          thread.interrupt() // Force interrupt if still running
        }
      } catch {
        case _: InterruptedException => // Ignore
      }
    }
    
    // Clear all maps
    batcherMap.clear()
    threadMap.clear()
    batchMap.clear()
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
    val df:DateTimeFormatter = DateTimeFormatter.ofPattern(natsConfig.dateTimeFormat)

   def getHeaders (msg:Message):Option[Map[String, List[String]]] = {
     if (msg.hasHeaders) {
       val msgHeaders = mutable.Map[String, List[String]]()
       val hdrs = msg.getHeaders
       hdrs.entrySet().forEach(entry => {
         val key = entry.getKey
         val value = entry.getValue

         msgHeaders(key)= value.toList
       })
       Some(msgHeaders.toMap)
     } else {
       None
     }
    }

    def uncompressed(msg:Message):NatsMsg = {

      NatsMsg(msg.getSubject(),
        ZonedDateTime.now().format(df),
        msg.getData(), getHeaders(msg), Some(msg.metaData()))
    }

    def compressed(msg:Message):NatsMsg = {
      NatsMsg(msg.getSubject(),
        ZonedDateTime.now().format(df),
        decompress(msg.getData()), getHeaders(msg), Some(msg.metaData()))
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

case class CoreNatsMsg(val subject:String, val dateTime:String, val content:Array[Byte], val headers:Option[Map[String, List[String]]])

case class NatsMsg(val subject:String, val dateTime:String, val content:Array[Byte], val headers:Option[Map[String, List[String]]], val jsMetaData:Option[NatsJetStreamMetaData]) {
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
    if (jsMetaData != null && jsMetaData.isDefined) {
      s"""{"jsMetaData": {"stream" : "${jsMetaData.get.getStream}", "streamSeq" :  ${jsMetaData.get.streamSequence()}, "consumerSeq" : ${jsMetaData.get.consumerSequence()}, "delivered" : ${jsMetaData.get.deliveredCount()}, "pending" : ${jsMetaData.get.pendingCount()}, "timestamp" : "${jsMetaData.get.timestamp()}"}}"""
    } else {
      ""
    }
  }
}

class Batcher(natsConfig: NatsConfig) extends Runnable {
  var buffer:ListBuffer[Message] = ListBuffer.empty[Message]
  val natsSubscriber = new NatsSubscriber(natsConfig)
  @volatile var doRun = true
  @volatile var semaphore = false

  override def run(): Unit = {
    this.doRun = true
    try {
      var start = System.currentTimeMillis()
      while(this.doRun && !Thread.currentThread().isInterrupted()) {
        pullAndLoadBatch()
      }
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt() // Restore interrupt status
    } finally {
      try {
        natsSubscriber.unsubscribe()
      } catch {
        case _: Exception => // Ignore cleanup exceptions
      }
    }
  }

  def stop(): Unit = {
    this.doRun = false
  }
  
  def stopAndGetBatch():List[Message] = {
    this.doRun = false
    // Wait for current operation to complete, with timeout
    var waitCount = 0
    while(this.semaphore && waitCount < 500) { // Max 5 seconds wait
      try {
        Thread.sleep(10)
        waitCount += 1
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          return this.buffer.toList // Return current buffer on interrupt
      }
    }
    this.buffer.toList
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
