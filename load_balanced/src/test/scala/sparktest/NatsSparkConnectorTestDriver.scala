package sparktest

import natsconnector.NatsConfigSource
import natsconnector.NatsConfigSink
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import io.nats.client.Subscription
import java.time.Duration
import io.nats.client.Message
import java.nio.charset.StandardCharsets
//import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.hadoop.yarn.util.RackResolver
import java.util.logging.LogManager
import io.nats.client.impl.NatsMessage
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
//import org.slf4j.Logger
//import org.slf4j.LoggerFactory
import java.util.Properties
import java.io.FileInputStream
import org.apache.log4j.PropertyConfigurator
import natsconnector.NatsLogger
import org.apache.logging.log4j.core.LoggerContext
import collection.JavaConverters._
import natsconnector.NatsPublisher
import org.apache.spark.sql.streaming.Trigger



object NatsSparkConnectorTestDriver extends App {
  val logger:Logger = NatsLogger.logger
  val doConsole = false
  val doFile = false

  new Thread(new MsgProducer(), "MsgProducer-Thread").start()
  new Thread(new MsgConsumer(), "MsgConsumer-Thread").start()
  System.setProperty("hadoop.home.dir", "/")

  // Create Spark Session
  val spark = SparkSession
    .builder()
    //.master("local[2]")
    .master("spark://localhost:7077")
    .appName("NatsReaderTest")
    .config("spark.streaming.stopGracefullyOnShutDown",true)
    .config("spark.logConf", "false")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.jars", 
    "/Users/sergiosennder/stuff/work/synadia/spark-connector/load_balanced/nats-spark-connector/target/scala-2.12/nats-spark-connector_2.12-0.1.jar,"
    + "/Users/sergiosennder/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/nats/jnats/2.17.4/jnats-2.17.4.jar"
    )
    .config("spark.executor.instances", "2")
    .config("spark.cores.max", "4")
    .config("spark.executor.memory", "2g")
    .getOrCreate()

  //  spark.sparkContext.setLogLevel("ERROR")

  // Create Streaming DataFrame by reading data from socket.
  val host = "0.0.0.0"
  val port = "4222"
    
  val initDF = spark
    .readStream
    .format("nats")
    .option("nats.host", host)
    .option("nats.port", port)
    .option("nats.stream.name", "TestStream")
    .option("nats.stream.subjects", "subject1, subject2") 
    //.option("nats.stream.subjects", "subject1") 
    .option("nats.msg.ack.wait.secs", 90) // wait 90 seconds for an ack before resending a message
    .option("nats.num.listeners", 2)
    .option("nats.durable.name", "test_durable")
    .option("nats.msg.fetch.batch.size", 10)
    .load()

  //initDF.explain()
    
  val newDF = initDF.withColumn("subject", 
                                  regexp_replace(col("subject"), "(.+)", "done.$1"))

  // newDF.show()
  // newDF.explain()

  // Check if DataFrame is streaming or Not.
  //logger.debug("Streaming DataFrame : " + initDF.isStreaming)
    
  if(doConsole) {
    newDF
    .writeStream
    .outputMode("append") // only send new messages "complete" mode.
    //.option("truncate", false)
    .format("console")
    .option("checkpointLocation", "Users/sergiosennder/spark_checkpoint") 
    .start()
    .awaitTermination()
  } 
  // else if(doFile) {
  //   newDF
  //   .writeStream
  //   .outputMode("append") // only send new messages "complete" mode.
  //   //.option("truncate", false)
  //   .option("checkpointLocation", "Users/sergiosennder/spark_checkpoint") 
  //   .format("csv")
  //   .trigger(Trigger.ProcessingTime("30 seconds"))
  //   .option("path", "/Users/sergiosennder/stuff/work/synadia/spark-connector/load_balanced/nats-spark-connector/output")
  //   .start()
  //   .awaitTermination()    
  // }
    
  newDF
  .writeStream
  .outputMode("append") // only send new messages "complete" mode.
  //.option("truncate", false)
  .format("nats")
  .option("checkpointLocation", "Users/sergiosennder/spark_checkpoint") 
  .option("nats.stream.name", "TestStream")
  .option("nats.host", host)
  .option("nats.port", port)
  .start()
  .awaitTermination()
}

class MsgProducer extends Runnable {
  val logger:Logger = NatsLogger.logger
  override def run(): Unit = {
    // Let Spark initialize
    Thread.sleep(20000L)

    var msgNum = 0
    val subjects = NatsConfigSource.config.streamSubjects.get.split(",")

    while(true) {
      msgNum += 1
      subjects.foreach(subject => {
        val data = s"Msg ${msgNum}-${System.currentTimeMillis()}"
        val msg = NatsMessage.builder()
          .data(data.getBytes(StandardCharsets.US_ASCII))
          .subject(subject)
          .build()
        NatsConfigSource.config.nc.get.publish(msg)
        println(s"Sent msg:${data} on subject:${subject}")
      })
      Thread.sleep(1000)
    }
  }
}

class MsgConsumer extends Runnable {
  val logger:Logger = NatsLogger.logger
  override def run(): Unit = {
    // Let Spark initialize
    Thread.sleep(20000L)

    val subject = "done.>"
    val sub:Subscription = NatsConfigSink.config.nc.get.subscribe(subject)
    val msgWaitTime:Duration = NatsConfigSink.config.messageReceiveWaitTime

    while(true) {
      var msg:Message = null
      while(msg == null) {
        try {
          msg = sub.nextMessage(msgWaitTime)
        } catch {
          case ex: InterruptedException => println("nextMessage() waitTime exceeded.") // just try again
        }
      }
      val data = new String(msg.getData(), StandardCharsets.UTF_8)
      println(s"Received msg:${data} on subject:${msg.getSubject()}")
    }
  }
}
