package natsconnector.spark

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.Serialization.read

import java.util.function.LongFunction

case class NatsOffset(offset:Option[NatsBatchInfo]) extends Offset {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  // Unfortunately we cannot just serialize the NatsBatchInfo object due to a conversion issue in Spark
  override val json: String = write(this)

  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[Offset])  this.offset == obj.asInstanceOf[NatsOffset].offset
    else { if (obj.isInstanceOf[String]) {
      val other = read[NatsOffset](obj.asInstanceOf[String])
      this.offset == other.offset
      } else false
    }
  }
}

object NatsOffset {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)
  def apply(offset: SerializedOffset): NatsOffset = {
  val batchInfoJson = offset.json.replace("{\"offset\":{","{").replace("}}}", "}}")
  val batchInfo = read[Option[NatsBatchInfo]](batchInfoJson)
    new NatsOffset(batchInfo)
  }

  def convert(offset: Offset): Option[NatsOffset] = offset match {
    case lo: NatsOffset => Some(lo)
    case so: SerializedOffset => Some(NatsOffset(so))
    case _ => None
  }
}

// NatsPartitionInfo contains a map of (partition, offset) key/value pairs
case class NatsBatchInfo(batchIdList:List[String])