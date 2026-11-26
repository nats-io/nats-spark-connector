package org.apache.spark.sql.nats

import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import io.nats.client.Nats
import io.nats.client.api.{RetentionPolicy, StreamConfiguration}
import munit.FunSuite
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}

class NatsRecoveryTest extends FunSuite {
  test("checkpoint restart replays the original batch and acknowledges only written rows") {
    assume(sys.env.contains("NATS_TEST_PORT"), "Start JetStream and set NATS_TEST_PORT")
    val port = sys.env("NATS_TEST_PORT")
    val stream = "RECOVERY_" + UUID.randomUUID().toString.replace("-", "")
    val credentials = Files.createTempDirectory("nats-recovery")
    val checkpoint = credentials.resolve("checkpoint").toString
    val spark = SparkSession.builder().master("local[2]").appName("NatsRecoveryTest")
      .config("spark.driver.host", "127.0.0.1").config("spark.ui.enabled", "false").getOrCreate()
    val connection = Nats.connect(s"nats://127.0.0.1:$port")
    val management = connection.jetStreamManagement()
    try {
      management.addStream(StreamConfiguration.builder().name(stream).subjects(stream)
        .retentionPolicy(RetentionPolicy.WorkQueue).build())
      val source = spark.readStream.format(classOf[NatsStreamProvider].getName)
        .option("nats.host", "127.0.0.1").option("nats.port", port)
        .option("nats.credential.file", Files.createFile(credentials.resolve("anonymous.creds")).toString)
        .option("nats.pull.subscription.stream.name", stream)
        .option("nats.pull.subscription.durable.name", "reader")
        .option("nats.stream.subjects", stream).option("nats.pull.batch.size", 1).load()

      val first = connection.jetStream().publish(stream, Array[Byte](1)).getSeqno
      def failBeforeWrite(rows: DataFrame, batchId: Long): Unit = {
        assertEquals(batchId, 0L)
        assertEquals(rows.select("streamSeq").collect().toList.map(_.getLong(0)), List(first))
        throw new IllegalStateException("injected sink failure")
      }
      val failed = source.writeStream.option("checkpointLocation", checkpoint)
        .foreachBatch(failBeforeWrite _).start()
      try {
        val error = intercept[StreamingQueryException](failed.awaitTermination(60000))
        assert(error.getMessage.contains("injected sink failure"))
      } finally failed.stop()

      val second = connection.jetStream().publish(stream, Array[Byte](2)).getSeqno
      val sink = new AtomicReference(List.empty[(Long, Long)])
      val recovered = source.writeStream.option("checkpointLocation", checkpoint)
        .trigger(Trigger.ProcessingTime("500 milliseconds"))
        .foreachBatch { (rows: DataFrame, batchId: Long) =>
          val sequences = rows.select("streamSeq").collect().toList.map(_.getLong(0))
          sink.set(sink.get() ++ sequences.map(sequence => (batchId, sequence)))
          // Force Spark to reconstruct anything the acknowledgment path recomputes.
          spark.sparkContext.getPersistentRDDs.values.foreach(_.unpersist(blocking = true))
        }.start()
      try {
        val deadline = System.nanoTime() + 60000000000L
        while (management.getStreamInfo(stream).getStreamState.getMsgCount > 0 &&
               System.nanoTime() < deadline) {
          assert(!recovered.awaitTermination(200))
        }
        assertEquals(management.getStreamInfo(stream).getStreamState.getMsgCount, 0L)
        assertEquals(sink.get().filter(_._1 == 0L), List((0L, first)))
        assertEquals(sink.get().map(_._2), List(first, second))
      } finally recovered.stop()
    } finally {
      try management.deleteStream(stream)
      finally {
        connection.close()
        val path = new Path(credentials.toUri)
        path.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(path, true)
        spark.stop()
      }
    }
  }
}
