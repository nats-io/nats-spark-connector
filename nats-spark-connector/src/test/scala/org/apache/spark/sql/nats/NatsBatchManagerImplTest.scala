package org.apache.spark.sql.nats

import io.nats.client.Message
import io.nats.client.impl._
import munit.FunSuite
import org.apache.spark.util.ThreadUtils

import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.util._

class NatsBatchManagerImplTest extends FunSuite {

  private def mockSubscriber: NatsSubscriber = new NatsSubscriber {
    private val counter = new AtomicLong(-1)
    override def pull: Try[Seq[Message]] = {
      val data = counter.incrementAndGet()
      val mockMessage = MockNatsJetStreamMessage(data, s"${data + 1}".getBytes)
      if (data <= 4L) {
        Try(Seq(mockMessage))
      } else {
        Failure(new NoSuchElementException("exhausted"))
      }
    }
  }

  val subscriber: FunFixture[NatsBatchManager] = FunFixture[NatsBatchManager](
    setup = { _ =>
      new NatsBatchManagerImpl(
        TrieMap.empty,
        mockSubscriber,
        1.millisecond,
        1.millisecond,
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("thread")
      )
    },
    teardown = _ => {}
  )

  subscriber.test("its first batch is None") { natsSub => assert(natsSub.getOffset.isEmpty) }
  subscriber.test("it increments the batch counter on pull") { natsSub =>
    natsSub.pull
    assertEquals(natsSub.getOffset, Option(0L))
    natsSub.pull
    assertEquals(natsSub.getOffset, Option(1L))
  }
  subscriber.test("it returns the appropriate batches") { natsSub =>
    natsSub.pull
    natsSub.pull

    val noBatches = natsSub.getBatch(Option.empty, -1)
    assert(noBatches.isEmpty, "Should return no batches for (empty, -1)")
    assertEquals(
      natsSub.getBatch(Option.empty, 0).size,
      1,
      "Should return 1 batch for (empty, 0)")
    assertEquals(
      natsSub.getBatch(Option.empty, 1).size,
      2,
      "Should return 2 batches for (empty, 1)")
    assertEquals(natsSub.getBatch(Option(0), 1).size, 2, "Should return 2 batches for (0, 1)")

    assertEquals(natsSub.getBatch(Option(1), 2).size, 1, "Should return 2 batches for (1, 2)")
  }
  subscriber.test("it removes batches on commit") { natsSub =>
    natsSub.pull
    natsSub.pull
    natsSub.pull

    val batches = natsSub.getBatch(Option(0L), 2L).map(t => new String(t.getData).toInt)
    assertEquals(batches, Vector(1, 2, 3))
    natsSub.commit(1L)
    val nextBatches =
      natsSub.getBatch(Option(0L), 2L).map(t => new String(t.getData).toInt)
    assertEquals(nextBatches, Vector(3))
    natsSub.commit(2L)
    val exhausted = natsSub.getBatch(Option(0L), 2L).map(t => new String(t.getData).toInt)
    assertEquals(exhausted, Vector.empty)
  }

  subscriber.test("it removes commits in batch") { natsSub =>
    natsSub.pull
    natsSub.pull
    natsSub.pull

    assertEquals(natsSub.getBatch(Option(0L), 2L).size, 3)
    natsSub.commit(1L)
    assertEquals(natsSub.getBatch(Option(0), 2L).size, 1)
  }

}
