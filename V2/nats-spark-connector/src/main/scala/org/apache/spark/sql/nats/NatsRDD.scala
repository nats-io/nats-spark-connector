package org.apache.spark.sql.nats

import io.nats.client.PullSubscribeOptions
import io.nats.client.impl.AckType
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.nats.NatsConnection.{withConnection, withJS}
import org.apache.spark.sql.nats.NatsRDD._

import scala.collection.JavaConverters._

object NatsRDD {
  type MessageAck = String
  type NatsMessageMeta = (MessageAck, InternalRow)
}

object Ack extends Serializable with Logging {

  def apply(natsConnectionConfig: NatsConnectionConfig, replyTos: Seq[MessageAck]): Unit =
    withConnection(natsConnectionConfig)(conn => {
      logDebug(s"Acking ${replyTos.size} messages")
      replyTos.foreach(replyTo => {
        logDebug(s"Acking: $replyTo")
        conn.publish(replyTo, AckType.AckAck.bodyBytes(-1))
      })
    })
}

/**
 * A Nats RDD that for every batch, tries to read every message into itself. The batch is
 * partitioned up to `"nats.pull.consumer.max.batch"`
 */
class NatsRDD(sc: SparkContext, natsSourceParams: NatsSourceParams)
    extends RDD[NatsMessageMeta](sc, Nil)
    with Logging {

  /**
   * Fetch every nats batch -- since we have to use an ephemeral nats connection, we fetch and
   * then return an iterator instead of using {@link io.nats.client.JetStreamSubscription#iterate(int, java.time.Duration)}
   */
  override def compute(split: Partition, context: TaskContext): Iterator[NatsMessageMeta] = {
    logTrace("compute")
    split match {
      case NatsPartition(idx) => logDebug(s"Fetching $idx")
    }
    withJS(natsSourceParams.natsConnectionConfig)(js => {
      logDebug("Starting iterator")
      val pullSubscriptionConf = PullSubscribeOptions
        .fastBind(natsSourceParams.streamName, natsSourceParams.consumerName)
      js
        .subscribe(None.orNull, pullSubscriptionConf)
        .fetch(natsSourceParams.batchSize, natsSourceParams.maxWait.toMillis)
        .iterator()
        .asScala
        .map(message => (message.getReplyTo, MessageToSparkRow(message)))
    })
  }

  // returns a list of ints, that tries to capture every outstanding nats message
  override protected def getPartitions: Array[Partition] = {
    logInfo("getPartitions")
    withJS(natsSourceParams.natsConnectionConfig)(js => {
      val numPending = js
        .getConsumerContext(natsSourceParams.streamName, natsSourceParams.consumerName)
        .getConsumerInfo
        .getNumPending
      val parts = Math.ceil(numPending.toDouble / natsSourceParams.batchSize).toInt
      Range.Int(0, parts, 1).map(NatsPartition).toArray
    })
  }

}

final case class NatsPartition(idx: Int) extends Partition {
  override def index: Int = idx
}
