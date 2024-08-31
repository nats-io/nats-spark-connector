package natsconnector.spark

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.Serialization.read

import java.util.function.LongFunction

case class NatsJetStreamOffset(offset:Option[NatsPartitionInfo]) extends Offset {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  //  override val json: String = "{\"offset\":" + s"{$offset}" + "}"
  override val json: String = write(this)

  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[Offset])  this.offset == obj.asInstanceOf[NatsJetStreamOffset].offset
    else { if (obj.isInstanceOf[String]) {
      read[NatsJetStreamOffset](obj.asInstanceOf[String]) == this
    } else false
    }
  }
}

object NatsJetStreamOffset {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)
  def apply(offset: SerializedOffset): NatsJetStreamOffset = {
    val partitionInfoJson = offset.json.replace("{\"offset\":{","{").replace("}}}", "}}")
    val partitionInfo = read[Option[NatsPartitionInfo]](partitionInfoJson)
    new NatsJetStreamOffset(partitionInfo)
  }

  def convert(offset: Offset): Option[NatsJetStreamOffset] = offset match {
    case lo: NatsJetStreamOffset => Some(lo)
    case so: SerializedOffset => Some(NatsJetStreamOffset(so))
    case _ => None
  }
}

// NatsPartitionInfo contains a map of (partition, offset) key/value pairs
case class NatsPartitionInfo(partitionInfo:Map[Int, Long])

