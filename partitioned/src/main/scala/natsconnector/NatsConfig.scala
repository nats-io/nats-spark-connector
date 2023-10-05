package natsconnector

import java.io.BufferedInputStream
import java.time.Duration

//import org.slf4j.Logger
//import org.slf4j.LoggerFactory
import io.nats.client.ConnectionListener.Events
import io.nats.client._
import io.nats.client.api.{KeyValueConfiguration, KeyValueStatus}

import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import java.security.{KeyStore, SecureRandom}
import java.util.Properties
import javax.net.ssl.{KeyManagerFactory, TrustManagerFactory}

import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.Logger


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
  var jsAPIPrefix: Option[String] = None // configurable
  var userName: Option[String] = None // configurable
  var userPassword: Option[String] = None // configurable

  // ============== JetStream stream Config Values
  var streamPrefix = "None set"

  // ============== Application Config Values
  val dateTimeFormat = "MM/dd/yyyy - HH:mm:ss Z"

  var options:Options = null
  // Nats connection
  var nc: Connection = Nats.connect()

  var numPartitions = 1

  var jsm: JetStreamManagement = null
  var js: JetStream = null

  // Clear the KV stored partitions before resetting affinity 
  var kvm:KeyValue = null
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

    try {
      this.userName = Some(parameters("nats.connection.user.name"))
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      this.userPassword = Some(parameters("nats.connection.user.password"))
    } catch {
      case e: NoSuchElementException =>
    }


    try {
      val param = parameters("nats.js.api-prefix")
      if (param != null && param != "") {
        this.jsAPIPrefix = Some(param)
      }
    } catch {
      case e: NoSuchElementException =>
    }

    this.server = s"nats://${this.host}:${this.port}"
    this.options = createConnectionOptions(this.server, this.allowReconnect)

    this.nc = Nats.connect(options)

    this.jsm = if (this.jsAPIPrefix.isEmpty) {
      this.nc.jetStreamManagement()
    } else {
      val options = JetStreamOptions.builder().prefix(this.jsAPIPrefix.get).build()
      this.nc.jetStreamManagement(options)
    }

    this.js = if (this.jsAPIPrefix.isEmpty) {
      this.nc.jetStream()
    } else {
      val options = JetStreamOptions.builder().prefix(this.jsAPIPrefix.get).build()
      this.nc.jetStream(options)
    }

    this.kvm = {
      val kvm:KeyValueManagement  = if (this.jsAPIPrefix.isEmpty) {
        this.nc.keyValueManagement()
      } else {
        val options = KeyValueOptions.builder().jetStreamOptions(JetStreamOptions.builder().prefix(this.jsAPIPrefix.get).build()).build()
        this.nc.keyValueManagement(options)
      }

      val kvc:KeyValueConfiguration = KeyValueConfiguration.builder()
                                 .name("partitions")
                                 .build();
      val keyValueStatus:KeyValueStatus = kvm.create(kvc);
      val kval = if (this.jsAPIPrefix.isEmpty) {
        this.nc.keyValue("partitions")
      } else {
        val options = KeyValueOptions.builder().jetStreamOptions(JetStreamOptions.builder().prefix(this.jsAPIPrefix.get).build()).build()
        this.nc.keyValue("partitions", options)
      }

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

    if (System.getenv("NATS_NKEY") != null && System.getenv("NATS_NKEY") != "") {
      val handler: AuthHandler = new SampleAuthHandler(
        System.getenv("NATS_NKEY")
      )
      builder.authHandler(handler)
    } else if (System.getenv("NATS_CREDS") != null && System.getenv("NATS_CREDS") != "") {
      builder.authHandler(Nats.credentials(System.getenv("NATS_CREDS")));
    }

    if (System.getenv("NATS_TLS_ALGO") != null && System.getenv("NATS_TLS_ALGO") != "") {
      builder.tlsAlgorithm(System.getenv("NATS_TLS_ALGO"))
    }

    if (System.getenv("NATS_TLS_TRUST_STORE") != null && System.getenv("NATS_TLS_TRUST_STORE") != "") {

      builder.truststorePath(System.getenv("NATS_TLS_TRUST_STORE"));
      if (System.getenv("NATS_TLS_TRUST_STORE_PASSWORD") != null) {
        builder.truststorePassword(System.getenv("NATS_TLS_TRUST_STORE_PASSWORD").toCharArray)
      }
    }

    if (System.getenv("NATS_TLS_KEY_STORE") != null) {
      if (System.getenv("NATS_TLS_KEY_STORE_PASSWORD") != null) {
        builder.keystorePassword(System.getenv("NATS_TLS_KEY_STORE_PASSWORD").toCharArray)
      }
      builder.keystorePath(System.getenv("NATS_TLS_KEY_STORE"));
    }

    if (this.userName.isDefined) {
      builder.userInfo(this.userName.get, this.userPassword.getOrElse(""))
    }

    builder.build()
  }

}

case class NatsMsg(val subject:String, val dateTime:String, val content:String)

object NatsLogger {
  val logger = {
    val logger: Logger = Logger.getLogger("NATSCON =>")
    val p = new Properties()

    if (System.getenv("LOG_PROP_PATH") != null) {
      p.load(new FileInputStream(System.getenv("LOG_PROP_PATH")))
    }

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