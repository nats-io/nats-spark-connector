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
                          parameters: Map[String, String])
  extends Source {
    val logger:Logger = NatsLogger.logger
    private var currentOffset: NatsOffset = new NatsOffset(None)
    private var batchMgr:NatsSubBatchMgr = new NatsSubBatchMgr()
    private var payloadCompression:Option[String] = None

    try {
        val compression = parameters("nats.storage.payload-compression")
        this.payloadCompression = Some(compression)
    } catch {
        case e: NoSuchElementException =>
    }
    override def stop(): Unit = {
        val nc = NatsConfigSource.config.nc
        try {
            nc.get.drain(Duration.ofSeconds(30))
        } catch {
            case e: TimeoutException => this.logger.error(s"Timeout draining NATS connection: ${e.getMessage()}")
        }
    }

    override def schema: StructType = userDefinedSchema.get

    override def getOffset: Option[Offset] = {
        this.logger.info("=====================In NatsStreamingSource.getOffset")
        this.logger.debug(Thread.currentThread().getName())
        val numListeners = NatsConfigSource.config.numListeners
        val offsetList: MutableList[String] = MutableList()
        if(currentOffset.offset == None) {
            for(listener <- 0 until numListeners) {
                val batchId = getBatchMgr().startNewBatch(this.payloadCompression)
                offsetList += batchId
            }
            this.currentOffset = NatsOffset(Some(NatsBatchInfo(offsetList.toList)))
            None
        }
        else {
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
        var natsBatch:MutableList[NatsMsg] = MutableList.empty[NatsMsg]

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
                                    
        // We have frozen the current batch and while Spark is processing it we start a new batch in the background
        val numListeners = NatsConfigSource.config.numListeners
        val offsetList: MutableList[String] = MutableList()
        for(listener <- 0 until numListeners) {
            val batchId = getBatchMgr().startNewBatch(this.payloadCompression)
            offsetList += batchId
        }
        this.currentOffset = NatsOffset(Some(NatsBatchInfo(offsetList.toList)))

        df
    }
    override def commit(end: Offset):Unit = {
        val natsOffset:NatsOffset = NatsOffset.convert(end).get
        val batchInfo:NatsBatchInfo = natsOffset.offset.get
        val batchIdList:List[String] = batchInfo.batchIdList
        for(batchId <- batchIdList) {
            getBatchMgr().commitBatch(batchId)
        }
    }
    
    private def getBatchMgr():NatsSubBatchMgr = {
        if(this.batchMgr == null)
            this.batchMgr = new NatsSubBatchMgr()

        this.batchMgr
    }
    
    private def convertNatsMsgListtoInternalRowSeq(natsBatch:List[NatsMsg]):Seq[InternalRow] = {
        val msgSeq = natsBatch.toSeq
        val rowSeq:Seq[InternalRow] = msgSeq.map(msg => {
                                            val gir = new GenericInternalRow(3)
                                            gir.update(0, UTF8String.fromString(msg.subject.toString()))
                                            gir.update(1, UTF8String.fromString(msg.dateTime))
                                            gir.update(2, UTF8String.fromString(msg.content.toString())) 
                                            gir })
       
        this.logger.debug(
            "-------- Streaming DF content:"
            + s"${rowSeq.foreach(r => this.logger.debug("  "+r))}"
        )
        
        rowSeq
    }

    private def parseZonedDateTime(value:ZonedDateTime):String = {
        val formatString = NatsConfigSource.config.dateTimeFormat
        val df = DateTimeFormatter.ofPattern(formatString)
        value.format(df)
    }
        
}

