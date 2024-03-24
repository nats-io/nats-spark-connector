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
import natsconnector.spark.NatsJetStreamBatchProcessor
import java.util.Properties
import java.io.FileInputStream
import org.apache.log4j.PropertyConfigurator
import natsconnector.NatsLogger
import org.apache.logging.log4j.core.LoggerContext
import collection.JavaConverters._

object NatsSparkConnectorTestDriver extends App {
  //  val logger:Logger = LoggerFactory.getLogger(this.getClass())

    val doConsole = false
    val natsTestSubjectPrefix = "subject."
    new Thread(new MsgProducer(), "MsgProducer-Thread").start()
    new Thread(new MsgConsumer(), "MsgConsumer-Thread").start()
    System.setProperty("hadoop.home.dir", "/")

    // Create Spark Session
    val spark = SparkSession
    .builder()
    //.master("local[2]")
    .master("spark://localhost:7077")
    .appName("NatsReaderTest")
    .config("spark.logConf", "false")
    //.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.jars", 
    "/Users/sergiosennder/stuff/work/synadia/spark-connector/partitioned/nats-spark-connector/target/scala-2.12/nats-spark-connector_2.12-0.1.jar,"
    + "/Users/sergiosennder/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/io/nats/jnats/2.17.4/jnats-2.17.4.jar"
    )
    .config("spark.executor.instances", "2")
    .config("spark.cores.max", "4")
    .config("spark.executor.memory", "2g")
    .getOrCreate()

   // sc.parallelize(Seq(""))
    //   .foreachPartition(x => { 
    //       import org.apache.log4j.{LogManager, Level}   
    //       import org.apache.commons.logging.LogFactory
    //       LogManager.getRootLogger().setLevel(Level.ERROR)   
    //       //val log = LogFactory.getLog("EXECUTOR-LOG:")   
    //       //log.error("START EXECUTOR ERROR LOG LEVEL") 
    //     })

    // Create Streaming DataFrame by reading data from socket.
    val host = "localhost"
    val port = "4222"
    val streamPrefix  = "benchstream"
    
    val initDF = spark
    .readStream
    .format("natsJS")
    .option("nats.host", host)
    .option("nats.port", port)
    .option("nats.stream.prefix", streamPrefix)
    .option("nats.num.partitions", 2)
    .option("nats.reset.on.restart", false)
    .load()
    

    //initDF.explain()
    // // TODO: convert pub.<route> into <route>.myRegistration
    val newDF = initDF.withColumn("subject", 
                                   regexp_replace(col("subject"), "subject.(.+)", "done.$1"))

    // newDF.show()
    // newDF.explain()

 
    // Check if DataFrame is streaming or Not.
    println("Streaming DataFrame : " + initDF.isStreaming)
    
    if(doConsole) {
      newDF
      .writeStream
      .outputMode("append") // only send new messages "complete" mode.
      //.option("truncate", false)
      .format("console")
      .start()
      .awaitTermination()
    }
    
    newDF
    .writeStream
    .outputMode("append") // only send new messages "complete" mode.
    //.option("truncate", false)
    .format("natsJS")
    .option("checkpointLocation", "Users/spark_checkpoint") 
    .option("nats.host", host)
    .option("nats.port", port)
    .start()
    .awaitTermination()


}

class MsgProducer extends Runnable {

  override def run(): Unit = {
    var msgNum = 0
    val subjectPrefix = NatsSparkConnectorTestDriver.natsTestSubjectPrefix
    
    // Let Spark initialize
    Thread.sleep(11000L)

    val numPartitions = NatsConfigSource.config.numPartitions

    while(true) {
      msgNum += 1
      for(partition <- 0 until numPartitions) {
        val subject = subjectPrefix + partition.toString()
        val data = s"Msg ${msgNum}_$partition"
        val msg = NatsMessage.builder()
                  .data(data.getBytes(StandardCharsets.US_ASCII))
                  .subject(subject)
                  .build()
        //println(s"Publishing Nats msg:${data}")
        NatsConfigSource.config.nc.publish(msg)
        //println(s"Sent msg:${data} on subject:${subject}")
      }
      Thread.sleep(10L)
    }
  }
}

class MsgConsumer extends Runnable {
  override def run(): Unit = {
    val subject = "done.>"
    val msgWaitTime:Duration = Duration.ZERO
    
    // Let Spark initialize
    Thread.sleep(11000L)

    val sub:Subscription = NatsConfigSink.config.nc.subscribe(subject)


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
      val msgSubject = msg.getSubject()
      println(s"Received msg:${data} on subject:${msgSubject} at time ${System.currentTimeMillis()}")
    }
  }
}
