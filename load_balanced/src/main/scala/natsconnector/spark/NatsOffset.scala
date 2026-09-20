package natsconnector.spark

import org.apache.spark.sql.execution.streaming.Offset
import org.json4s.{Formats, JNothing, JNull, JObject, NoTypeHints}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import natsconnector.NatsLogger
import org.apache.log4j.Logger

case class NatsOffset(offset:Option[NatsBatchInfo]) extends Offset {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  // Unfortunately we cannot just serialize the NatsBatchInfo object due to a conversion issue in Spark
  override val json: String = write(this)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: NatsOffset => this.offset == other.offset
      case other: Offset =>
        // Try to convert other offset types to NatsOffset for comparison
        NatsOffset.convert(other).exists(_.offset == this.offset)
      case jsonString: String =>
        NatsOffset.fromJson(jsonString).exists(_.offset == this.offset)
      case _ => false
    }
  }
}

object NatsOffset {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)
  val logger:Logger = NatsLogger.logger

  /**
   * Rebuild a NatsOffset from its JSON form, i.e. what Spark keeps in the checkpoint's offset
   * log and hands back after a restart (wrapped in its `SerializedOffset`).
   *
   * Accepted shapes are exactly what `NatsOffset.json` produces: `{}` (no batch yet),
   * `{"offset":null}` and `{"offset":{"batchIdList":[...]}}`. Anything else yields None.
   */
  def fromJson(json: String): Option[NatsOffset] = {
    try {
      parse(json) match {
        case JObject(Nil) => Some(NatsOffset(None))
        case JObject(fields) =>
          fields.collectFirst { case ("offset", value) => value } match {
            case Some(JNull) | Some(JNothing) => Some(NatsOffset(None))
            case Some(value) => Some(NatsOffset(Some(value.extract[NatsBatchInfo])))
            case None => None
          }
        case _ => None
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to parse offset JSON as a NatsOffset: $json", e)
        None
    }
  }

  /**
   * Recover a NatsOffset from any offset Spark hands us: either one we produced earlier, or
   * an opaque offset restored from the checkpoint log. Matching on the JSON rather than on
   * Spark's `SerializedOffset` class keeps this independent of where that class lives
   * (it moved between Spark 4.0 and 4.1).
   */
  def convert(offset: Offset): Option[NatsOffset] = offset match {
    case lo: NatsOffset => Some(lo)
    case other => fromJson(other.json)
  }
}

// NatsPartitionInfo contains a map of (partition, offset) key/value pairs
case class NatsBatchInfo(batchIdList:List[String])
