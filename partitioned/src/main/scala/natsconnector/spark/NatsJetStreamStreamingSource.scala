package natsconnector.spark

import natsconnector.NatsConfigSource

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.{Map => MutableMap}
import scala.util.control.Breaks._

import java.nio.channels.NonWritableChannelException
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.nio.charset.StandardCharsets

import io.nats.client.api.KeyValueStatus
import io.nats.client.KeyValueManagement
import io.nats.client.api.KeyValueConfiguration
import io.nats.client.KeyValue
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.{
  Connection,
  Message,
  PullSubscribeOptions,
  PushSubscribeOptions
}

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.log4j.Logger
//import org.slf4j.Logger
//import org.slf4j.LoggerFactory


class NatsJetStreamStreamingSource(
    sqlContext: SQLContext,
    metadataPath: String,
    userDefinedSchema: Option[StructType],
    parameters: Map[String, String]
) extends Source {

  //val logger:Logger = LoggerFactory.getLogger(classOf[NatsJetStreamStreamingSource])
  val logger:Logger = Logger.getLogger("nats.connector")
  private var currentOffset: Option[NatsJetStreamOffset] = None
  private var firstTime = true

  override def stop(): Unit = {
    NatsConfigSource.config.nc.close()
  }

  override def schema: StructType = userDefinedSchema.get

  override def getOffset: Option[Offset] = {
    this.logger.debug("=====================In NatsStreamingSource.getOffset")
    
    val offsetMap: MutableMap[Int, Long] = MutableMap()

    for(partition <- 0 until NatsConfigSource.config.numPartitions) {
      val jsm = NatsConfigSource.config.jsm
      val jsi = jsm.getStreamInfo(NatsConfigSource.config.streamPrefix + "-" + partition.toString())
      val last = jsi.getStreamState.getLastSequence()
      offsetMap += (partition -> last)
    }

    if (this.currentOffset == None) {
      this.currentOffset = Some(NatsJetStreamOffset(Some(NatsPartitionInfo(offsetMap.toMap))))
      None
    } else {
      this.currentOffset = Some(NatsJetStreamOffset(Some(NatsPartitionInfo(offsetMap.toMap))))
    }

    this.currentOffset
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val js = NatsConfigSource.config.js
    // create ordered ephemeral here and get the data

    this.logger.debug("=====================In NatsStreamingSource.getBatch")
    this.logger.debug(s"NatsStreamingSource.getBatch() 'start' offset: ${start}")
    this.logger.debug(s"NatsStreamingSource.getBatch() 'end' offset: ${end}")

    val endJSOffsetMap: MutableMap[Int, Long] = MutableMap()
 
    if (end != null && NatsJetStreamOffset.convert(end).isDefined) {
      endJSOffsetMap ++= NatsJetStreamOffset.convert(end).get.offset.get.partitionInfo
    } else
      throw new Exception(
        "NatsJetStreamStreamingSource.getBatch: valid end NatsJetStreamOffset required"
      )

    if (start.isDefined && NatsJetStreamOffset.convert(start.get).isEmpty)
      throw new Exception(
        "NatsJetStreamStreamingSource.getBatch: start must be a NatsJetStreamOffset when specified"
      )
  
    val startJSOffsetMap: MutableMap[Int, Long] = {
      val map: MutableMap[Int, Long] = MutableMap()
      if (start.isEmpty || (NatsConfigSource.config.resetOnRestart && this.firstTime)) { 
        this.firstTime = false 
        for(partition <- 0 until NatsConfigSource.config.numPartitions) {
          val jsm = NatsConfigSource.config.jsm
          val jsi = jsm.getStreamInfo(NatsConfigSource.config.streamPrefix + "-" + partition.toString())
          // val first = jsi.getStreamState.getFirstSequence() - 1
          val first = 1L
          map += (partition -> first)
        }
        map
      }
      else 
        map ++= NatsJetStreamOffset.convert(start.get).get.offset.get.partitionInfo
    }

    val buffer = NatsJetStreamBatchProcessor.processNatsBatching(startJSOffsetMap, endJSOffsetMap)
    if (buffer.isEmpty)
      return this.sqlContext.sparkSession.internalCreateDataFrame(
        this.sqlContext.sparkContext.emptyRDD[InternalRow],
        this.schema,
        isStreaming = true
      )

    val msgSeq = buffer.toSeq
    // val schema = Encoders.product[NatsMsg].schema
    val rowSeq: Seq[InternalRow] = msgSeq.map(msg => {
      val gir = new GenericInternalRow(3)
      gir.update(0, UTF8String.fromString(msg.getSubject))
      gir.update(
        1,
        UTF8String.fromString(
          msg.metaData().timestamp().format(DateTimeFormatter.ISO_DATE_TIME)
//            msg.metaData().timestamp().format(DateTimeFormatter.ofPattern("MM/dd/yyyy - HH:mm:ss Z"))
        )
      )
      gir.update(2, UTF8String.fromString(new String(msg.getData(), StandardCharsets.US_ASCII)))
      gir
    })

    // println("-------- Streaming DF content:")
    // rowSeq.foreach(r => println(r))

    val df = this.sqlContext.sparkSession.internalCreateDataFrame(
      sqlContext.sparkSession.sparkContext.parallelize(rowSeq),
      this.schema,
      isStreaming = true
    )

    df
  }
  override def commit(end: Offset): Unit = {
  }

}
