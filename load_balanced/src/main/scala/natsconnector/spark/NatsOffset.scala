package natsconnector.spark

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.Serialization.read
import natsconnector.NatsLogger
import org.apache.log4j.Logger

import java.util.function.LongFunction

case class NatsOffset(offset:Option[NatsBatchInfo]) extends Offset {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  // Unfortunately we cannot just serialize the NatsBatchInfo object due to a conversion issue in Spark
  override val json: String = write(this)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: NatsOffset => this.offset == other.offset
      case other: Offset => 
        // Try to convert other offset types to NatsOffset for comparison
        NatsOffset.convert(other) match {
          case Some(natsOffset) => this.offset == natsOffset.offset
          case None => false
        }
      case jsonString: String =>
        try {
          val other = read[NatsOffset](jsonString)
          this.offset == other.offset
        } catch {
          case _: Exception => false
        }
      case _ => false
    }
  }
}

object NatsOffset {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)
  val logger:Logger = NatsLogger.logger

  def apply(offset: SerializedOffset): NatsOffset = {
    import org.json4s.jackson.JsonMethods._
    try {
      // Parse the JSON to extract the nested offset structure
      val json = parse(offset.json)
      val offsetJson = (json \ "offset").extractOpt[Option[NatsBatchInfo]]
      new NatsOffset(offsetJson.getOrElse(None))
    } catch {
      case e: Exception =>
        logger.warn("Failed to parse nested offset structure. Attempting direct deserialization as NatsOffset.", e)
        // Fallback: try to parse directly as NatsOffset
        try {
          read[NatsOffset](offset.json)
        } catch {
          case e: Exception =>
            logger.error("Failed to parse JSON directly as NatsOffset. Returning empty offset as fallback.", e)
            // Final fallback: return empty offset
            new NatsOffset(None)
        }
    }
  }

  def convert(offset: Offset): Option[NatsOffset] = offset match {
    case lo: NatsOffset => Some(lo)
    case so: SerializedOffset => Some(NatsOffset(so))
    case _ => None
  }
}

// NatsPartitionInfo contains a map of (partition, offset) key/value pairs
case class NatsBatchInfo(batchIdList:List[String])