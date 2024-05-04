package org.apache.spark.sql.nats

import io.nats.client.Message
import io.nats.client.impl.Headers
import io.nats.client.impl.NatsMessage
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object NatsSink {
  val schema: StructType = StructType(
    Array(
      StructField("subject", StringType, nullable = false),
      StructField("data", BinaryType, nullable = false),
      StructField("headers", MapType(StringType, ArrayType(StringType)), nullable = true)
    )
  )

  def apply(natsPublisher: NatsPublisher): NatsSink = new NatsSink(natsPublisher)
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

class NatsSink(natsPublisher: NatsPublisher) extends Sink with Logging {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    import data.sparkSession.implicits._
    logInfo(s"addBatch $batchId")
    data
      .sparkSession
      .internalCreateDataFrame(data.queryExecution.toRdd, data.schema)
      .to(NatsSink.schema)
      .as[NatsMessageRow]
      // TODO(mrosti): there has to be a way around collect but idk how to serialize the JS client
      .collect()
      .map(MessageBuilder(_))
      .foreach(natsPublisher.push)
  }
}
