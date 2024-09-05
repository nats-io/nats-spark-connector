package org.apache.spark.sql.nats

import io.nats.client.Message
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.nats.NatsConnection.withJS
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.util
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.convert.ImplicitConversions._
import scala.concurrent.duration._
import scala.util.Try

object MessageToSparkRow {
  def apply(message: Message): InternalRow = {
    val headers: Option[Map[String, Seq[String]]] =
      Option(message.getHeaders).map(
        _.entrySet()
          .toSet
          .map((me: util.Map.Entry[String, util.List[String]]) =>
            (me.getKey, me.getValue.asScala))
          .toMap)

    val metadata = message.metaData()

    val values: Seq[Any] = Seq(
      // "subject
      UTF8String.fromString(message.getSubject),
      // replyTo
      UTF8String.fromString(message.getReplyTo),
      // content
      message.getData,
      // headers map
      headers.orNull,
      // domain
      UTF8String.fromString(metadata.getDomain),
      // stream
      UTF8String.fromString(metadata.getStream),
      // consumer
      UTF8String.fromString(metadata.getConsumer),
      // delivered
      metadata.deliveredCount,
      // streamSeq
      metadata.streamSequence,
      // consumerSeq
      metadata.consumerSequence,
      // timestamp
      metadata.timestamp.toInstant.toEpochMilli.millis.toMicros,
      // pending
      metadata.pendingCount()
    )

    InternalRow(
      values: _*
    )
  }
}

object NatsSource {
  val schema: StructType = StructType(
    Array(
      StructField("subject", StringType, nullable = true),
      StructField("replyTo", StringType, nullable = true),
      StructField("content", BinaryType, nullable = false),
      StructField("headers", MapType(StringType, ArrayType(StringType)), nullable = true),
      StructField("domain", StringType, nullable = true),
      StructField("stream", StringType, nullable = true),
      StructField("consumer", StringType, nullable = true),
      StructField("delivered", LongType, nullable = true),
      StructField("streamSeq", LongType, nullable = true),
      StructField("consumerSeq", LongType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true),
      StructField("pending", LongType, nullable = true)
    )
  )

  def apply(sqlContext: SQLContext, natsSourceParams: NatsSourceParams): NatsSource =
    new NatsSource(sqlContext, natsSourceParams)
}

final case class NatsSourceParams(
    natsConnectionConfig: NatsConnectionConfig,
    streamName: String,
    consumerName: String,
    batchSize: Int,
    maxWait: FiniteDuration
)

class NatsSource(sqlContext: SQLContext, natsSourceParams: NatsSourceParams)
    extends Source
    with Logging {

  private val commitTracker: TrieMap[Long, DataFrame] = TrieMap.empty
  private val initialValue: Long = -1
  private val commitNumber: AtomicLong = new AtomicLong(initialValue)

  override def schema: StructType = NatsSource.schema

  override def getOffset: Option[Offset] = {
    logInfo("getOffset")
    val pending = withJS(natsSourceParams.natsConnectionConfig)(js => {
      val consumerInfo = js
        .getConsumerContext(natsSourceParams.streamName, natsSourceParams.consumerName)
        .getConsumerInfo
      consumerInfo.getNumWaiting + consumerInfo.getNumPending
    })
    // if there are ANY messages to read, try to read them all
    if (pending > 0) {
      logDebug(s"$pending messages to read ")
      Option(LongOffset(commitNumber.incrementAndGet()))
    }
    // if there are any messages to ack, idk, just increment, try to get some more
    // but more importantly, ack every message that needs acking
    else if (commitTracker.nonEmpty) {
      logWarning("In order to ack messages, incrementing offset")
      Option(LongOffset(commitNumber.incrementAndGet()))
    }
    // if you've never seen a batch before and the other conditions fail
    else if (commitNumber.get() == initialValue) {
      logDebug("On startup & nothing to read & no previous messages yet to Ack")
      Option.empty[Offset]
    }
    // finally, just respond with a "there's nothing to do" offset
    else {
      logDebug("nothing to read and nothing to ack")
      Option(LongOffset(commitNumber.get()))
    }
  }

  private def getBatchImpl(start: Option[Long], end: Long): DataFrame = {
    val rdd = new NatsRDD(sqlContext.sparkContext, natsSourceParams)
    val df = sqlContext.internalCreateDataFrame(rdd.map(_._2), NatsSource.schema).cache()
    logDebug(s"expected messages to reply to: ${df.select(col("replyTo")).count()}")
    commitTracker.put(end, df)
    sqlContext.internalCreateDataFrame(
      df.queryExecution.toRdd,
      NatsSource.schema,
      isStreaming = true)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"getBatch($start, $end)")
    logDebug(s"commitTracker is ${commitTracker.isEmpty}")
    logDebug(s"commitTracker keys ${commitTracker.keySet}")
    (start, end) match {
      case (None, LongOffset(endOff)) =>
        getBatchImpl(Option.empty[Long], endOff)
      case (Some(LongOffset(startOff)), LongOffset(endOff)) =>
        getBatchImpl(Option(startOff), endOff)
      case (Some(SerializedOffset(json)), LongOffset(endOff)) =>
        getBatchImpl(Try(json.toLong).toOption, endOff)
      case (Some(LongOffset(startOff)), endOff: SerializedOffset) =>
        getBatchImpl(Option(startOff), LongOffset(endOff).offset)
      case (Some(SerializedOffset(json)), endOff: SerializedOffset) =>
        getBatchImpl(Try(json.toLong).toOption, LongOffset(endOff).offset)
      case (maybeOffset, offset) =>
        logError(s"Invalid offsets in getBatch ($maybeOffset, $offset)")
        sqlContext.emptyDataFrame.to(schema)
    }
  }

  private def commitDF(dataFrame: DataFrame, connectionConfig: NatsConnectionConfig): Unit = {
    dataFrame
      .select(col("replyTo"))
      .foreachPartition((iterator: Iterator[Row]) => {
        Ack(connectionConfig, iterator.map(_.getString(0)).toSeq)
      })
    dataFrame.unpersist()
  }
  override def commit(end: Offset): Unit = {
    logInfo(s"commit($end)")
    logDebug(s"commitTracker is ${commitTracker.isEmpty}")
    logDebug(s"commitTracker keys ${commitTracker.keySet}")
    val connectionConfig = natsSourceParams.natsConnectionConfig
    end match {
      case LongOffset(offset) =>
        commitTracker.remove(offset).foreach(commitDF(_, connectionConfig))
      case so: SerializedOffset =>
        commitTracker.remove(LongOffset(so).offset).foreach(commitDF(_, connectionConfig))
      case offset: Offset => throw new Exception(s"Invalid Offset :: $offset")
    }
  }

  override def stop(): Unit = {
    logInfo("stop")
  }
}
