package natsconnector

import scala.collection.JavaConverters._

import java.time.Duration
import java.nio.charset.StandardCharsets
import java.io.IOException

import org.apache.spark.sql.SparkSession

//import org.slf4j.Logger
//import org.slf4j.LoggerFactory
import org.apache.log4j.Logger
import org.apache.log4j.Level

import io.nats.client.JetStream
import io.nats.client.JetStreamManagement
import io.nats.client.api.StreamInfo
import io.nats.client.JetStreamApiException
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.ErrorListener
import io.nats.client.Connection
import io.nats.client.Consumer
import io.nats.client.ConnectionListener
import io.nats.client.ConnectionListener
import io.nats.client.ConnectionListener.Events
import io.nats.client.AuthHandler
import io.nats.client.api.StreamConfiguration
import io.nats.client.api.StorageType
import io.nats.client.{PushSubscribeOptions, PullSubscribeOptions}
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.AckPolicy
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.DeliverPolicy
import io.nats.client.KeyValueManagement
import io.nats.client.api.KeyValueConfiguration
import io.nats.client.api.KeyValueStatus
import io.nats.client.KeyValue
import io.nats.client.impl.Headers
import io.nats.client.impl.NatsMessage
import java.util.Properties
import org.apache.log4j.PropertyConfigurator
import java.io.FileInputStream


object NatsConfigSource {
  val config = new NatsConfig(true)
}

object NatsConfigSink {
  val config = new NatsConfig(false)
}

class NatsConfig(isSource:Boolean) {
  // Note on security:
  // Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
  // Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
  // Use the URL in the -s server connection parameter for user/pass/token authentication.

  // ====================================================== CONFIG VARIABLES =================================
  // Following are shared variables across all JVM connections. They should match all JVMs in a cluster
  // ============== NATS Connection Config Values
  var host = "0.0.0.0"
  var port = "4222"
  var server:String = null
  var allowReconnect = true
  var connectionTimeout = Duration.ofSeconds(20)
  var pingInterval = Duration.ofSeconds(10) 
  var reconnectWait = Duration.ofSeconds(20)
  var resetOnRestart = false

  // ============== JetStream stream Config Values
  var streamPrefix = "None set"

  // ============== Application Config Values
  val dateTimeFormat = "MM/dd/yyyy - HH:mm:ss Z"

  var options:Options = null
  // Nats connection
  var nc: Connection = Nats.connect()

  var numPartitions = 1

  // Clear the KV stored partitions before resetting affinity 
  var kv:KeyValue = null
 // =========================================================================================================
  def setConnection(parameters: Map[String, String]): Unit = {
    // Obligatory parameters
    var param:String = ""
    try {
      if(isSource) {
        param = "nats.stream.prefix"
        this.streamPrefix = parameters(param)
      }
      param = "nats.host"
      this.host = parameters(param)
      param = "nats.port"
      this.port = parameters(param)
    } catch {
      case e: NoSuchElementException => throw new RuntimeException(missingParamMsg(param))
    }

    // Optional parameters
    try {
      this.numPartitions = parameters("nats.num.partitions").toInt
    } catch {
      case e: NoSuchElementException => 
    }

    try {
      this.allowReconnect = parameters("nats.allow.reconnect").toBoolean
    } catch {
      case e: NoSuchElementException => 
    }

    try {
      this.connectionTimeout = Duration.ofSeconds(parameters("nats.connection.timeout").toLong)
    } catch {
      case e: NoSuchElementException => 
    }

    try {
      this.pingInterval = Duration.ofSeconds(parameters("nats.ping.interval").toLong)
    } catch {
      case e: NoSuchElementException => 
    }

    try {
      this.reconnectWait = Duration.ofSeconds(parameters("nats.reconnect.wait").toLong)
    } catch {
      case e: NoSuchElementException => 
    }

    try {
      this.resetOnRestart = parameters("nats.reset.on.restart").toBoolean
    } catch {
      case e: NoSuchElementException => 
    }

    this.nc = {
      this.server = s"nats://${this.host}:${this.port}"
      this.options = createConnectionOptions(this.server, this.allowReconnect)
      Nats.connect(options)
    }

    this.kv = {
      val kvm:KeyValueManagement  = this.nc.keyValueManagement();
      val kvc:KeyValueConfiguration = KeyValueConfiguration.builder()
                                 .name("partitions")
                                 .build();
      val keyValueStatus:KeyValueStatus = kvm.create(kvc);
      val kval = this.nc.keyValue("partitions"); 
      var worker = 0
      while(worker < this.numPartitions) {
        kval.delete(worker.toString())
        worker+=1
      }
      kval.purgeDeletes()
      kval
    }
  }

  private def missingParamMsg(param:String): String = {
    val s = s"""Missing parameter '$param'.\n
      Please add '.option(\"$param\", <param_value>)' to the 'spark.readStream'\n
      and spark.writeStream declarations, where <param_value> is a proper value \n
      for parameter $param""".stripMargin 

    return s
  }


  private def createConnectionOptions(
      server: String,
      allowReconnect: Boolean
  ): Options = {
    val el = new ErrorListener() {
      override def exceptionOccurred(conn: Connection, exp: Exception): Unit = {
        System.out.println("Exception " + exp.getMessage());
      }

      override def errorOccurred(conn: Connection, errorType: String): Unit = {
        System.out.println("Error " + errorType);
      }

      override def slowConsumerDetected(conn: Connection, consumer: Consumer): Unit = {
        System.out.println("Slow consumer");
      }
    }

    val cl = new ConnectionListener() {
      def connectionEvent(conn: Connection, eventType: Events): Unit = {
        System.out.println(s"Status change ${eventType}")
      }
    }

    var builder = new Options.Builder()
      .server(this.server)
      .connectionTimeout(this.connectionTimeout)
      .pingInterval(this.pingInterval)
      .reconnectWait(this.reconnectWait)
      .errorListener(el)
      .connectionListener(cl)

    if (!this.allowReconnect) {
      builder = builder.noReconnect()
    } else {
      builder = builder.maxReconnects(-1)
    }

    if (
      System.getenv("NATS_NKEY") != null && System.getenv("NATS_NKEY") != ""
    ) {
      val handler: AuthHandler = new SampleAuthHandler(
        System.getenv("NATS_NKEY")
      )
      builder.authHandler(handler)
    } else if (
      System.getenv("NATS_CREDS") != null && System.getenv("NATS_CREDS") != ""
    ) {
      builder.authHandler(Nats.credentials(System.getenv("NATS_CREDS")));
    }
    return builder.build()
  }

}

case class NatsMsg(val subject:String, val dateTime:String, val content:String) 

object NatsLogger {
  val logger = {
    val logger:Logger = Logger.getLogger("NATSCON =>")
    val log4JPropertyFile = "src/test/resources/log4j.properties"
    val p = new Properties()
    p.load(new FileInputStream(log4JPropertyFile))
    PropertyConfigurator.configure(p)  
    logger
  }
}

// object NatsBatchPublisher {
//   var publisher:NatsBatchPublisher = null
//   val createPublisher = (parameters:Map[String, String]) => {
//     // println("========== In createPublisher ========")
//     publisher = new NatsBatchPublisher(parameters)
//   }
// }

// class NatsBatchPublisher(parameters:Map[String, String]) extends Serializable{
//   val options = NatsConfig.config.options
//   val sendNatsMsg = (subject:String, dateTime:String, data:String) => {
//     // println("=========== In sendNatsMsg =============")
//     val headers:Headers = new Headers()
//     assert(headers != null)
//     headers.add("originTimestamp", dateTime)
//     val natsMsg = NatsMessage.builder()
//           .data(data.getBytes(StandardCharsets.US_ASCII))
//           .subject(subject)
//           .headers(headers)
//           .build()
//     assert(natsMsg != null)
//     val con = Nats.connect(options)
//     assert(con != null)
//     con.publish(natsMsg)
//   }
// }