import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger
import org.apache.spark.sql.streaming.OutputMode.Append

import scala.concurrent.duration._

object SparkTest extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("changeme")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  spark.sparkContext.setLogLevel("DEBUG")

  spark
    .readStream
    .format("nats")
    .option("nats.host", "localhost")
    .option("nats.port", "4222")
    .option("nats.credential.file", "changeme")
    .option("nats.pull.subscription.stream.name", "changeme")
    .option("nats.pull.subscription.durable.name", "changeme")
    .option("nats.pull.consumer.create", "false")
    .option("nats.pull.batch.size", "changeme")
    .option("nats.pull.wait.time", "changeme")
    .option("nats.storage.payload-compression", "zlib")
    .load()
    .withWatermark("timestamp", "1 minute")
    .writeStream
    .format("console")
    .outputMode(Append())
    .trigger(ProcessingTimeTrigger(30.seconds))
    .start()
    .awaitTermination()
}
