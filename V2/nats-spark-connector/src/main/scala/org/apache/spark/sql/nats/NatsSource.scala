package org.apache.spark.sql.nats

import io.nats.client.{JetStreamOptions, Message}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, MapData}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.nats.NatsConnection.withConnection
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.hadoop.fs.Path

import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.Inflater
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions._
import scala.concurrent.duration._

object MessageToSparkRow {

  // wartremover friendly zlib decompression
  //  forked from: https://github.com/nats-io/nats-spark-connector/commit/2f6314d17fab56ba4887509a76fc3b03154d61d7#diff-78057eda4f520613b3bd6358120d2be7b24b6bb55859e5fd597ea2bcacde2dfaR98
  def decompress(inData: Array[Byte]): Array[Byte] = {
    val inflater = new Inflater()
    inflater.setInput(inData)
    val decompressedData = new Array[Byte](inData.length * 2)
    val firstCount = inflater.inflate(decompressedData)
    val firstData = decompressedData.take(firstCount)

    val inflateIter = Iterator.iterate(firstData) { 
      // iterate calls to inflate until no more data to unpack
      (data) => 
        val currCount = inflater.inflate(decompressedData)
        val currData = decompressedData.take(currCount)
        if (currCount > 0) currData else null
    }
    
    // run iterator, merge segments and return
    inflateIter.takeWhile(_ != null).flatten.toArray
  }

  def apply(message: Message, payloadCompression: String): InternalRow = {

    val headers: Option[Map[UTF8String, Seq[UTF8String]]] =
      Option(message.getHeaders).map(
        _.entrySet()
          .toSet
          .map((me: util.Map.Entry[String, util.List[String]]) =>
            (UTF8String.fromString(me.getKey), me.getValue.asScala.map(UTF8String.fromString).toSeq))
          .toMap)

    val metadata = message.metaData()

    val messageContent = payloadCompression match {
      case "zlib" => decompress(message.getData) 
      case "none" => message.getData 
    }

    val headersMapData: Option[MapData] = {
      if (headers.nonEmpty) {
        Some(
          headers.map { h =>
            val keys = ArrayData.toArrayData(h.keys.toArray )
            val values = ArrayData.toArrayData(h.values.map(v => ArrayData.toArrayData(v.toArray)).toArray)
            new ArrayBasedMapData(keys, values)
          }.orNull
        )
      } else {
        None
      }
    }

    val values: Seq[Any] = Seq(
      // "subject
      UTF8String.fromString(message.getSubject),
      // replyTo
      UTF8String.fromString(message.getReplyTo),
      // content
      messageContent,
      // headers map
      headersMapData.orNull,
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

/** Even offsets identify persisted batches. The following odd offset allows Spark to commit
 * the data batch when the publisher goes idle. */
final case class NatsOffset(offset: Long) extends Offset {
  override def json: String = s"""{"natsBatchV1":$offset}"""
}

object NatsOffset {
  private val versioned = """\{"natsBatchV1":([0-9]+)\}""".r

  def index(offset: Offset): Long = offset.json match {
    case versioned(number) => number.toLong
    case other => throw new IllegalArgumentException(
      s"Unsupported NATS checkpoint offset $other. Drain the old consumer and start a new Spark checkpoint.")
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
}

final case class NatsSourceParams(
    natsConnectionConfig: NatsConnectionConfig,
    streamName: String,
    consumerName: String,
    batchSize: Int,
    maxWait: FiniteDuration,
    payloadCompression: String
)

class NatsSource(sqlContext: SQLContext, params: NatsSourceParams, metadataPath: String)
    extends Source with Logging {

  private val root = new Path(metadataPath, "nats-batches-v1")
  private val fs = root.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
  fs.mkdirs(root)
  private val batchName = """batch-([0-9]+)""".r
  private val latest = new AtomicLong(fs.listStatus(root).foldLeft(-2L) { (previous, status) =>
    status.getPath.getName match {
      case batchName(id) if fs.exists(new Path(status.getPath, "ready")) => math.max(previous, id.toLong)
      case _ => previous
    }
  })

  override def schema: StructType = NatsSource.schema

  private def directory(id: Long): Path = new Path(root, s"batch-$id")
  private def path(id: Long, name: String): Path = new Path(directory(id), name)
  private def marked(id: Long, name: String): Boolean = fs.exists(path(id, name))
  private def mark(id: Long, name: String): Unit = {
    fs.createNewFile(path(id, name))
    ()
  }

  private def records(id: Long): DataFrame =
    sqlContext.read.schema(schema)
              .option("ignoreMissingFiles", "false")
              .option("ignoreCorruptFiles", "false")
              .parquet(path(id, "rows").toString)

  private def hasDeliveries: Boolean = withConnection(params.natsConnectionConfig) { connection =>
    val options = JetStreamOptions.builder().prefix(params.natsConnectionConfig.jsAPIPrefix).build()
    val info = connection.jetStream(options).getConsumerContext(params.streamName, params.consumerName).getConsumerInfo
    info.getNumPending > 0 || info.getNumAckPending > 0
  }

  /** Only a single active Spark query may own a checkpoint. An interrupted capture has no
   * ready marker and no offset or acknowledgment can reference it. */
  private def capture(id: Long): Boolean = {
    val batch = directory(id)
    if (fs.exists(batch)) {
      require(!marked(id, "ready"), s"Refusing to replace published NATS batch $id")
      require(fs.delete(batch, true), s"Cannot remove incomplete NATS batch $id")
    }
    val deliveries = new NatsRDD(sqlContext.sparkContext, params)
    val data = SparkShim.internalCreateDataFrame(sqlContext, deliveries.map(_._2), schema, isStreaming = false)
    data.write.parquet(path(id, "rows").toString)
    if (records(id).isEmpty) {
      fs.delete(batch, true)
      false
    } else {
      mark(id, "ready")
      latest.set(id)
      true
    }
  }

  // Keep offering the data offset until Spark requests its batch. Only then offer the
  // trailing offset: Spark needs a later trigger to call commit on the data offset.
  // Persist these transitions so restarting the driver cannot skip either step.
  override def getOffset: Option[Offset] = synchronized {
    val id = latest.get()
    if (id >= 0 && !marked(id, "read")) Some(NatsOffset(id))
    else if (id >= 0 && !marked(id, "acked")) Some(NatsOffset(id + 1))
    else if (hasDeliveries && capture(id + 2)) Some(NatsOffset(id + 2))
    else if (id >= 0) Some(NatsOffset(id + 1))
    else None
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val first = start.map(NatsOffset.index).getOrElse(-1L) + 1
    val last = NatsOffset.index(end)
    val batches = (first to last).filter(_ % 2 == 0).map { id =>
      require(marked(id, "ready"), s"Missing NATS checkpoint batch $id")
      mark(id, "read")
      records(id).queryExecution.toRdd
    }
    val rows = sqlContext.sparkContext.union(batches)
    SparkShim.internalCreateDataFrame(sqlContext, rows, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = synchronized {
    val offset = NatsOffset.index(end)
    val id = offset - offset % 2
    require(marked(id, "ready"), s"Cannot acknowledge missing NATS checkpoint batch $id")
    if (!marked(id, "acked")) {
      val connectionConfig = params.natsConnectionConfig
      records(id).select(col("replyTo")).foreachPartition((rows: Iterator[Row]) =>
        Ack(connectionConfig, rows.map(_.getString(0))))
      mark(id, "acked")
    }
    // Keep the latest batch for recovery and retire its predecessor after Spark commits.
    if (id >= 2 && marked(id - 2, "acked")) {
      fs.delete(directory(id - 2), true)
      ()
    }
  }

  override def stop(): Unit = ()
}
