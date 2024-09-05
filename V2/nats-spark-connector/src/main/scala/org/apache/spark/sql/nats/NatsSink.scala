package org.apache.spark.sql.nats

import io.nats.client.{Message, PublishOptions}
import io.nats.client.impl.{Headers, NatsMessage}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.nats.NatsConnection.withJS
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
final case class NatsPublisherConfig(natsConnectionConfig: NatsConnectionConfig, stream: String)

object NatsSink {
  val schema: StructType = StructType(
    Array(
      StructField("subject", StringType, nullable = false),
      StructField("data", BinaryType, nullable = false),
      StructField("headers", MapType(StringType, ArrayType(StringType)), nullable = true)
    )
  )

  def apply(natsPublisher: NatsPublisherConfig): NatsSink = new NatsSink(natsPublisher)
}

@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
final case class NatsMessageRow(
    subject: String,
    data: Array[Byte],
    headers: Option[Map[String, List[String]]])
object MessageBuilder {
  def apply(natsMessageRow: NatsMessageRow): Message = {
    val natsHeaders = new Headers()
    natsHeaders.put(
      natsMessageRow
        .headers
        .getOrElse(Map.empty[String, List[String]])
        .map { case (k, v) => k -> v.asJava }
        .asJava)
    NatsMessage
      .builder()
      .subject(natsMessageRow.subject)
      .headers(natsHeaders)
      .data(natsMessageRow.data)
      .build()
  }
}

class NatsSink(natsPublisherConfig: NatsPublisherConfig) extends Sink with Logging {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    import data.sparkSession.implicits._
    logInfo(s"addBatch $batchId")
    val connectionConfig = natsPublisherConfig.natsConnectionConfig
    val stream = natsPublisherConfig.stream
    data
      .sparkSession
      .internalCreateDataFrame(data.queryExecution.toRdd, data.schema)
      .to(NatsSink.schema)
      .as[NatsMessageRow]
      .foreachPartition((iterator: Iterator[NatsMessageRow]) => {
        withJS(connectionConfig)(js => {
          val publishOptions =
            PublishOptions.builder().stream(stream).build()
          iterator.map(MessageBuilder(_)).foreach(js.publish(_, publishOptions))
        })
      })
  }
}
