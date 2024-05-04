package org.apache.spark.sql.nats

import cats.data._
import cats.implicits._
import io.nats.client.Message
import org.apache.spark.internal.Logging
import org.apache.spark.util.ThreadUtils

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.util.Try

final case class NatsBatch(id: Long, messages: Seq[Message])

trait NatsBatchManager extends NatsSubscriber {
  def commit(end: Long): Try[Unit]
  def getBatch(start: Option[Long], end: Long): Seq[Message]
  def getOffset: Option[Long]
  def shutdown(): Unit
  def start(): Unit
}

/**
 * *
 *
 * @param commitTracker
 *   The concurrent map used to track message's consumerSeq
 * @param subscriber
 *   a minimal wrapper for the JetStream client
 * @param initialDelay
 *   How long to wait for the initial nats request
 * @param batchWaitTime
 *   How long to wait between nats requests
 * @param threadPool
 *   A [[java.util.concurrent.ScheduledExecutorService]] to run our requests to nats in
 */
class NatsBatchManagerImpl(
    commitTracker: TrieMap[Long, Message],
    subscriber: NatsSubscriber,
    initialDelay: Duration,
    batchWaitTime: Duration,
    threadPool: ScheduledExecutorService)
    extends NatsBatchManager
    with Logging {

  private def periodicPuller(): Unit = {
    logInfo("Pulling")
    val pulled = pull
    logInfo(s"Pulled: $pulled")
    logDebug(s"Pulled: ${pulled.map(_.size)}")
  }

  override def start(): Unit = {
    logInfo("Test pull")
    logInfo(s"Started polling NATS every $batchWaitTime")
    val thread = threadPool.scheduleWithFixedDelay(
      () => periodicPuller(),
      initialDelay.toSeconds,
      batchWaitTime.toSeconds,
      TimeUnit.SECONDS
    )
    logInfo(s"$threadPool")
    logInfo(s"$thread")
  }

  private val beginningOfTime: Long = -1
  private val counter: AtomicLong = new AtomicLong(beginningOfTime)

  private def commitOne(id: Long): Try[Unit] = for {
    message <- commitTracker
      .remove(id)
      .toRight(new NoSuchElementException(s"No tracked message for $id"))
      .toTry
    _ = logTrace(s"Committing: $id")
    _ <- Try(message.ack())
    _ = logTrace(s"Acked: $id")
  } yield logTrace(s"Commited: $id")

  private def addToMap(maybeMessages: Seq[Message]): Try[NatsBatch] = {
    for {
      messages <- NonEmptyList
        .fromList(maybeMessages.toList)
        .toRight(new Exception("empty batch"))
        .toTry
      consumerSequence <- Try(messages.map(_.metaData().consumerSequence()))
      consumerCommitMin = consumerSequence.minimum

      consumerCommitMax = consumerSequence.maximum
      _ = logDebug(
        s"Upserting from $consumerCommitMin to $consumerCommitMax with ${messages.size} messages"
      )
      _ <- messages.foldLeft(Try(()))((acc, msg) => {
        acc.flatMap(_ => Try(commitTracker.update(msg.metaData().consumerSequence(), msg)))

      })
      _ <- Try(counter.set(consumerCommitMax))
    } yield NatsBatch(consumerCommitMax, messages.toList)

  }

  override def pull: Try[Seq[Message]] = {
    logInfo("pull")
    for {
      messages <- subscriber.pull
      _ <- addToMap(messages)
      _ = logDebug(s"Pulled ${messages.size} messages")
      _ = logTrace(s"Tracker now has ${commitTracker.values.size} messages")
    } yield messages
  }

  override def commit(end: Long): Try[Unit] = Try {
    logInfo(s"commit($end)")
    val toCommit = commitTracker.keySet.filter(_ <= end)
    for {
      t <- Try(toCommit.foreach(commitOne))
      _ = logDebug(s"Commited ${toCommit.size} messages")
    } yield t
  }

  override def getBatch(start: Option[Long], end: Long): Seq[Message] = {
    logInfo(s"getBatch($start, $end)")
    val startOrAll = start.getOrElse(beginningOfTime)
    logDebug(s"Getting batches from $startOrAll to $end")
    val messages = commitTracker
      .keySet
      .toSeq
      .filter(t => startOrAll <= t && t <= end)
      .flatMap(t => commitTracker.get(t).toList)
    logTrace(s"Returning ${messages.size} messages")
    messages
  }

  override def getOffset: Option[Long] = {
    logInfo(s"getOffset => ${counter.get()}")
    counter.get() match {
      case idx if idx === beginningOfTime => Option.empty[Long]
      case idx =>
        logDebug(s"Current offset: $idx")
        Option(idx)
    }
  }

  override def shutdown(): Unit = {
    logWarning("Shutting down nats source")
    // Stop querying
    threadPool.shutdown()

    // nak incomplete batches
    val batches = commitTracker.values.toSeq
    commitTracker.clear()
    logTrace(s"Naking ${batches.size} messages")
    Try(batches.foreach(_.nak()))
  }
}

object NatsBatchManager {
  def apply(
      subscriber: NatsSubscriber,
      batcherConfig: BatcherConfig
  ): NatsBatchManager = {
    val threadPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("pulling-thread")
    val commitTracker: TrieMap[Long, Message] = TrieMap.empty
    new NatsBatchManagerImpl(
      commitTracker,
      subscriber,
      batcherConfig.initialDelay,
      batcherConfig.pullWaitTime,
      threadPool)
  }
}
