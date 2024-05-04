package org.apache.spark.sql.nats

import io.nats.client.Message
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._
import scala.concurrent.duration._
import scala.util.Try

object MessageToSparkRow {
  def apply(message: Message): InternalRow = {
    val headers: Option[Map[String, Seq[String]]] = {
      Option(message.getHeaders).map(
        _.entrySet()
          .toSet
          .map((me: util.Map.Entry[String, util.List[String]]) =>
            (me.getKey, me.getValue.asScala.toSeq))
          .toMap)
    }
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

  def apply(sqlContext: SQLContext, natsBatchManager: NatsBatchManager): NatsSource =
    new NatsSource(sqlContext, natsBatchManager)
}

class NatsSource(sqlContext: SQLContext, natsBatchManager: NatsBatchManager)
    extends Source
    with Logging {

  override def schema: StructType = NatsSource.schema

  private def natsBatchToDF(batches: Seq[Message]): DataFrame =
    sqlContext.internalCreateDataFrame(
      sqlContext.sparkContext.parallelize[InternalRow](batches.map(MessageToSparkRow(_))),
      NatsSource.schema,
      isStreaming = true)

  override def getOffset: Option[Offset] = {
    logInfo("getOffset")
    natsBatchManager.getOffset.map(LongOffset(_))
  }

  private def getBatchImpl(start: Option[Long], end: Long): DataFrame =
    natsBatchToDF(natsBatchManager.getBatch(start, end))

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"getBatch($start, $end)")
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
        natsBatchToDF(Seq.empty[Message])
    }
  }

  override def commit(end: Offset): Unit = {
    logInfo(s"commit($end)")
    end match {
      case LongOffset(offset) => natsBatchManager.commit(offset)
      case so: SerializedOffset => natsBatchManager.commit(LongOffset(so).offset)
      case offset: Offset => throw new Exception(s"Invalid Offset :: $offset")
    }
  }

  override def stop(): Unit = natsBatchManager.shutdown()

  def start(): Unit = natsBatchManager.start()
}
