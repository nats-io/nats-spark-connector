package natsconnector.spark

import natsconnector.NatsConfigSource
import natsconnector.NatsConfigSink

import scala.util.control._
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.execution.streaming.Offset
import natsconnector.NatsSubBatchMgr
import natsconnector.NatsConfig
import natsconnector.NatsMsg
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.DataType

import java.time.{Duration, ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.collection.mutable.MutableList
import natsconnector.NatsLogger
import org.apache.log4j.Logger

import scala.concurrent.TimeoutException


class NatsStreamingSource(sqlContext: SQLContext, 
                          metadataPath: String,
                          userDefinedSchema: Option[StructType], 
                          parameters: Map[String, String],
                          natsConfig: NatsConfig)
  extends Source {
    val logger:Logger = NatsLogger.logger
    private var currentOffset: NatsOffset = new NatsOffset(None)
    private var batchMgr:NatsSubBatchMgr = new NatsSubBatchMgr(natsConfig)
    private var payloadCompression:Option[String] = None
    private var lastDeliveredBatchTimestamp:ZonedDateTime = ZonedDateTime.now()
    private var idleTimeout:Option[Duration] = natsConfig.idleTimeout
    private var ackNone = natsConfig.ackNone

    try {
        val compression = parameters("nats.storage.payload-compression")
        this.payloadCompression = Some(compression)
    } catch {
        case e: NoSuchElementException =>
    }

    override def stop(): Unit = {
        try {
            batchMgr.stop()
            // Don't close the config here as it might be shared by other sources
        } catch {
            case e: Exception => this.logger.error(s"Error stopping NATS source: ${e.getMessage()}")
        }
    }

    override def schema: StructType = userDefinedSchema.get

    override def getOffset: Option[Offset] = {
        this.logger.info("=====================In NatsStreamingSource.getOffset")
        this.logger.debug(Thread.currentThread().getName())
        val numListeners = natsConfig.numListeners
        val offsetList: MutableList[String] = MutableList()
        if(currentOffset.offset == None) {
            for(listener <- 0 until numListeners) {
                val batchId = getBatchMgr().startNewBatch(this.payloadCompression)
                offsetList += batchId
            }
            this.currentOffset = NatsOffset(Some(NatsBatchInfo(offsetList.toList)))
            None
        } else {
            this.logger.debug(
                "Current offset batch list:\n"
                + s"${offsetList}"
            )
            Some(currentOffset)
        }
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        this.logger.info("=====================In NatsStreamingSource.getBatch")
        this.logger.debug(s"NatsStreamingSource.getBatch() 'start' offset: ${start}")
        this.logger.debug(s"NatsStreamingSource.getBatch() 'end' offset: ${end}")
   
        // 'start' offset will contain previous batch id. We want to use the current batch id contained in the 'end' offset
        val natsOffset:NatsOffset = (NatsOffset.convert(end)).get
        val batchInfo:NatsBatchInfo = natsOffset.offset.get
        val batchIdList:List[String] = batchInfo.batchIdList
        val natsBatch:MutableList[NatsMsg] = MutableList.empty[NatsMsg]

        for(batchId <- batchIdList) {
            natsBatch ++= getBatchMgr().freezeAndGetBatch(batchId)
        }
            
        val rowSeq:Seq[InternalRow] = convertNatsMsgListtoInternalRowSeq(natsBatch.toList)

        this.logger.debug(
            "-------- Current batch DF content:"
            + s"${rowSeq.foreach(r => this.logger.debug("  "+r))}"
        )

        val df = this.sqlContext.sparkSession.internalCreateDataFrame(
                                    sqlContext.sparkSession.sparkContext.parallelize(rowSeq),
                                    this.schema, isStreaming = true)

        if (rowSeq.length != 0) {
            this.lastDeliveredBatchTimestamp = ZonedDateTime.now()
        }

        // We have frozen the current batch and while Spark is processing it we start a new batch in the background
        // but only if the idle timeout has not been exceeded
        if (this.idleTimeout.isEmpty || (idleTimeout.isDefined && Duration.between(this.lastDeliveredBatchTimestamp, ZonedDateTime.now()).compareTo(natsConfig.idleTimeout.get) < 0)) {
            val numListeners = natsConfig.numListeners
            val offsetList: MutableList[String] = MutableList()
            for (listener <- 0 until numListeners) {
                val batchId = getBatchMgr().startNewBatch(this.payloadCompression)
                offsetList += batchId
            }
            this.currentOffset = NatsOffset(Some(NatsBatchInfo(offsetList.toList)))
        }

        df
    }
    override def commit(end: Offset):Unit = {
        if (!ackNone) {
            val natsOffset: NatsOffset = NatsOffset.convert(end).get
            val batchInfo: NatsBatchInfo = natsOffset.offset.get
            val batchIdList: List[String] = batchInfo.batchIdList
            for (batchId <- batchIdList) {
                getBatchMgr().commitBatch(batchId)
            }
        }
    }
    
    private def getBatchMgr():NatsSubBatchMgr = {
        if(this.batchMgr == null)
            this.batchMgr = new NatsSubBatchMgr(natsConfig)

        this.batchMgr
    }
    
    private def convertNatsMsgListtoInternalRowSeq(natsBatch:List[NatsMsg]):Seq[InternalRow] = {
        val msgSeq = natsBatch.toSeq
        val rowSeq:Seq[InternalRow] = msgSeq.map(msg => {
                                            val gir = new GenericInternalRow(5)
                                            gir.update(0, UTF8String.fromString(msg.subject))
                                            gir.update(1, UTF8String.fromString(msg.dateTime))
                                            gir.update(2, UTF8String.fromBytes(msg.content))
                                            if (msg.headers.isDefined) {
                                                gir.update(3, UTF8String.fromString(msg.headersToJson()))
                                            }
                                            if (msg.jsMetaData != null) {
                                                gir.update(4, UTF8String.fromString(msg.jSMetaDataToJson()))
                                            }
                                            gir })
       
        this.logger.debug(
            "-------- Streaming DF content:"
            + s"${rowSeq.foreach(r => this.logger.debug("  "+r))}"
        )
        
        rowSeq
    }

    private def parseZonedDateTime(value:ZonedDateTime):String = {
        val formatString = natsConfig.dateTimeFormat
        val df = DateTimeFormatter.ofPattern(formatString)
        value.format(df)
    }
        
}

