package natsconnector

import java.time.Duration

import io.nats.client.JetStream
import io.nats.client.JetStreamManagement
import io.nats.client.api.StreamInfo
import java.io.IOException
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
import scala.collection.JavaConverters._
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.sql.SparkSession

object NatsConfigSource {
  val config = new NatsConfig(true)
}

object NatsConfigSink {
  val config = new NatsConfig(false)
}

class NatsConfig(isSource: Boolean) { 
  val isLocal = false
  // Note on security:
  // Set the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.
  // Set the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.
  // Use the URL in the -s server connection parameter for user/pass/token authentication.

  // ====================================================== CONFIG VARIABLES =================================
  // Following are shared variables across all JVM connections. They should match all JVMs in a cluster
  // ============== NATS Connection Config Values
  var host = "0.0.0.0"
  var port = "4222"
  var server: Option[String] = None
  var allowReconnect = true 
  var connectionTimeout = Duration.ofSeconds(20) 
  var pingInterval = Duration.ofSeconds(10) 
  var reconnectWait = Duration.ofSeconds(20) 
  var messageReceiveWaitTime = Duration.ofMillis(50) 
  var flushWaitTime = Duration.ofMillis(
    0
  ) // how long to wait for a connection to flush all msgs; '0' waits forever
  var msgFetchBatchSize = 100 // how many messages to get at once. Will get any messages where 1<bach_size<=100.
                              // If zero messages then subsciber will wait messageReceiveWaitTime before giving up.

  // ============== JetStream stream Config Values
  // TODO: add replication configuration
  var streamName: Option[String] = None // configurable
  var storageType: StorageType = StorageType.Memory // configurable
  var streamSubjects: Option[String] = None // configurable
  var durable: Option[String] = None // configurable
  val ackPolicy: AckPolicy =
    AckPolicy.Explicit // ack all msgs by acknowledging last msg - do not change
  val retentionPolicy: RetentionPolicy =
    RetentionPolicy.Limits // configure for retention limits - do not change
  val deliverPolicy: DeliverPolicy =
    DeliverPolicy.All // used with 'durable', starts new subscribers with earliest non-ack msgs - do not change
  var msgAckWaitTime =
    Duration.ofSeconds(60) // time server expects ack back after sending msg,
  // should be longer than max time expected to process msg. - configurable

  // ============== Application Config Values
  val dateTimeFormat = "MM/dd/yyyy - HH:mm:ss Z"

  // Nats connection
  var options: Option[Options] = None
  // Nats connection
  var nc: Option[Connection] = None

  var numListeners = 1

  // JetStream context
  var js: Option[JetStream] = None

  // =========================================================================================================
  def setConnection(parameters: Map[String, String]): Any = {
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info("=====================In NatsConfig.setConnection")
    }
    // Obligatory parameters
    var param: String = ""
    try {
      if (isSource) {
        param = "nats.stream.name"
        this.streamName = Some(parameters(param))

        param = "nats.stream.subjects"
        this.streamSubjects = Some(parameters(param).replace(" ", ""))

        param = "nats.msg.ack.wait.secs"
        this.msgAckWaitTime = Duration.ofSeconds(parameters(param).toLong)
      }
      param = "nats.host"
      this.host = parameters(param)
      param = "nats.port"
      this.port = parameters(param)

    } catch {
      case e: NoSuchElementException =>
        throw new RuntimeException(missingParamMsg(param))
    }

    // Optional parameters
    try {
      this.numListeners = parameters("nats.num.listeners").toInt
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      this.allowReconnect = parameters("nats.allow.reconnect").toBoolean
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      this.connectionTimeout =
        Duration.ofSeconds(parameters("nats.connection.timeout").toLong)
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      this.pingInterval =
        Duration.ofSeconds(parameters("nats.ping.interval.secs").toLong)
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      this.reconnectWait =
        Duration.ofSeconds(parameters("nats.reconnect.wait.secs").toLong)
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      this.messageReceiveWaitTime =
        Duration.ofMillis(parameters("nats.msg.receive.wait.millis").toLong)
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      this.flushWaitTime =
        Duration.ofMillis(parameters("nats.flush.wait.millis").toLong)
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      this.durable = Some(parameters("nats.durable.name"))
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      this.msgFetchBatchSize = parameters("nats.msg.fetch.batch.size").toInt
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      val typeOfStorage: String = parameters("nats.storage.type")
      // storage type default is set to 'memory'
      if (typeOfStorage.trim().toLowerCase.equals("file"))
        this.storageType = StorageType.File
    } catch {
      case e: NoSuchElementException =>
    }

    this.nc = {
      this.server = Some(s"nats://${this.host}:${this.port}")
      this.options = Some(createConnectionOptions(this.server.get, this.allowReconnect))
      Some(Nats.connect(options.get))
    }
    if (isSource) {
      this.js = Some(getJetStreamContext())
    }

    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.debug(
        "Current internal config state:\n"
        + s"host = ${this.host}\n"
        + s"port = ${this.port}\n"
        + s"server = ${this.server}\n"
        + s"allowReconnect = ${this.allowReconnect}\n"
        + s"connectionTimeout = ${this.connectionTimeout}\n"
        + s"pingInterval = ${this.pingInterval}\n"
        + s"reconnectWait = ${this.reconnectWait}\n"
        + s"messageReceiveWaitTime = ${this.messageReceiveWaitTime}\n"
        + s"flushWaitTime = ${this.flushWaitTime}\n"
        + s"msgFetchBatchSize = ${this.msgFetchBatchSize}\n"
        + s"streamName = ${this.streamName}\n"
        + s"storageType = ${this.storageType}\n"
        + s"streamSubjects = ${this.streamSubjects}\n"
        + s"durable = ${this.durable}\n"
        + s"ackPolicy = ${this.ackPolicy}\n"
        + s"retentionPolicy = ${this.retentionPolicy}\n"
        + s"deliverPolicy = ${this.deliverPolicy}\n"
        + s"msgAckWaitTime = ${this.msgAckWaitTime}\n"
        + s"dateTimeFormat = ${this.dateTimeFormat}\n"
        + s"numListeners = ${this.numListeners}\n"
        + s"[Connection] options = ${this.options}\n"
        + s"[Nats connection] nc = ${this.nc}\n"
        + s"[JetStream context] js= ${this.js}\n"
      )
    }
  }

  private def missingParamMsg(param: String): String = {
    val s = s"""Missing parameter '$param'.\n
    Please add '.option(\"$param\", <param_value>)' to the 'spark.readStream'\n
    and spark.writeStream declarations, where <param_value> is a proper value \n
    for parameter $param""".stripMargin

    return s
  }

  private def getJetStreamContext(): JetStream = {
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info("=====================In NatsConfig.getJetStreamContext")
    }
    val jsm: JetStreamManagement = this.nc.get.jetStreamManagement()

    val subjects: List[String] = null // List("bs", this.streamSubject)

    val sc: StreamConfiguration = StreamConfiguration
      .builder()
      .name(this.streamName.get)
      .storageType(this.storageType)
      // .subjects(subjects.asJava)
      .subjects(this.streamSubjects.get.replace(" ", "").split(",").toList.asJava)
      .retentionPolicy(this.retentionPolicy)
      .build()

    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info(sc.toJson())
    }
    // Add or use an existing stream.
    val si: StreamInfo = jsm.addStream(sc)
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.info(si.getConfiguration())
      logger.info(
        s"Created stream ${jsm.getStreamInfo(this.streamName.get)} with consumers ${jsm
          .getConsumers(this.streamName.get)}."
      )
    }
    val subjectArray = this.streamSubjects.get.replace(" ", "").split(",")
    subjectArray.zipWithIndex.foreach {
        case (subject, idx) => {
          val configBuilder = ConsumerConfiguration
                        .builder()
                        .ackWait(this.msgAckWaitTime)
                        .ackPolicy(this.ackPolicy)
                        .filterSubject(subject)
                        .deliverPolicy(this.deliverPolicy)
          if(this.durable != None) 
            configBuilder.durable(s"${this.durable.get}-${idx}")  
          else {
            // TODO: Add configBuilder.InactiveThreshold() 
          }  
          jsm.addOrUpdateConsumer(this.streamName.get, configBuilder.build())
        }
      }
    this.nc.get.jetStream()
  }

  private def getStreamInfoOrNullIfNonExistent(
      jsm: JetStreamManagement,
      streamName: String
  ): StreamInfo = {
    try {
      return jsm.getStreamInfo(streamName)
    } catch {
      case ex: JetStreamApiException => return null
      // Although IOException during startup will stop the system, the compiler still needs a return value, i.e. null
      case ex: IOException => {
        println(
          s"Got IOException when trying to connect to Jetstream: ${ex.getMessage()}"
        ); System.exit(-1); return null
      }
      case _: Throwable => {
        println("Got some other kind of Throwable exception"); return null
      }
    }
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

      override def slowConsumerDetected(
          conn: Connection,
          consumer: Consumer
      ): Unit = {
        System.out.println("Slow consumer");
      }
    }

    val cl = new ConnectionListener() {
      def connectionEvent(conn: Connection, eventType: Events): Unit = {
        System.out.println(s"Status change ${eventType}")
      }
    }

    var builder = new Options.Builder()
      .server(this.server.get)
      .connectionTimeout(this.connectionTimeout)
      .pingInterval(this.pingInterval)
      .reconnectWait(this.reconnectWait)
      .errorListener(el)
      .connectionListener(cl)

    if (!allowReconnect) {
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

object NatsLogger {
  val logger = {
    val logger: Logger = Logger.getLogger("NATSCON =>")
    val log4JPropertyFile = "src/test/resources/log4j.properties"
    val p = new Properties()
    p.load(new FileInputStream(log4JPropertyFile))
    PropertyConfigurator.configure(p)
    logger
  }
}
