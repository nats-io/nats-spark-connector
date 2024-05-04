import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.streaming.OutputMode.Append

import scala.concurrent.duration._

object SparkTest extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("marcus")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
//  spark.sparkContext.setLogLevel("TRACE")

  spark
    .readStream
    .format("nats")
    .option("nats.host", "localhost")
    .option("nats.port", "4222")
    .option("nats.credential.file", "CHANGE_ME")
    .option("nats.pull.subscription.stream.name", "CHANGE_ME")
    .option("nats.pull.subscription.durable.name", "CHANGE_ME")
    .option("nats.pull.consumer.ack.wait", "CHANGE_ME")
    .option("nats.pull.consumer.max.batch", "CHANGE_ME")
    .option("nats.stream.subjects", "CHANGE_ME")
    .option("nats.pull.batcher.initial.delay", "CHANGE_ME")
    .option("nats.pull.batcher.frequency.secs", "CHANGE_ME")
    .option("nats.pull.batch.size", "CHANGE_ME")
    .option("nats.pull.wait.time", "CHANGE_ME")
    .load()
    .writeStream
    .format("console")
    .outputMode(Append())
    .trigger(ProcessingTimeTrigger(10.second))
    .start()
    .awaitTermination()
}
