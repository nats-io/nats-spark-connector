package natsconnector

import java.time.Duration
import io.nats.client.{AuthHandler, Connection, ConnectionListener, Consumer, ErrorListener, JetStream, JetStreamApiException, JetStreamManagement, JetStreamOptions, Nats, Options, PullSubscribeOptions, PushSubscribeOptions}
import io.nats.client.api.StreamInfo

import java.io.{BufferedInputStream, FileInputStream, IOException}
import io.nats.client.ConnectionListener.Events
import io.nats.client.api.StreamConfiguration
import io.nats.client.api.StorageType
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.AckPolicy
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.DeliverPolicy

import scala.collection.JavaConverters._
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.Logger
import org.apache.log4j.Level

import java.util.Properties
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

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
  var defineStream = false // configurable
  var replicationCount = 1 // configurable
  var streamName: Option[String] = None // configurable
  var storageType: StorageType = StorageType.File // configurable
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
  var jsAPIPrefix: Option[String] = None // configurable
  var userName: Option[String] = None // configurable
  var userPassword: Option[String] = None // configurable

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
      // storage type default is set to 'file'
      if (typeOfStorage.trim().toLowerCase.equals("memory"))
        this.storageType = StorageType.Memory
    } catch {
      case e: NoSuchElementException =>
    }

    try {
      val numReplicas = parameters("nats.storage.replicas").toInt
      // num of replicas is set to '1' by default
      this.replicationCount = numReplicas
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

    try {
      val param = parameters("nats.js.define-stream").toBoolean
      this.defineStream = param
    } catch {
      case e: NoSuchElementException =>
    }

    this.server = Some(s"nats://${this.host}:${this.port}")
    this.options = Some(createConnectionOptions(this.server.get, this.allowReconnect))

    if (this.isLocal) {
      val logger: Logger = NatsLogger.logger
      logger.debug(
        "Current internal config state:\n"
          + s"host = ${this.host}\n"
          + s"port = ${this.port}\n"
          + s"server = ${this.server}\n"
          + s"userName = ${this.userName}\n"
          + s"userPassword = ${this.userPassword.getOrElse("")}\n"
          + s"allowReconnect = ${this.allowReconnect}\n"
          + s"connectionTimeout = ${this.connectionTimeout}\n"
          + s"pingInterval = ${this.pingInterval}\n"
          + s"reconnectWait = ${this.reconnectWait}\n"
          + s"messageReceiveWaitTime = ${this.messageReceiveWaitTime}\n"
          + s"flushWaitTime = ${this.flushWaitTime}\n"
          + s"msgFetchBatchSize = ${this.msgFetchBatchSize}\n"
          + s"jsAPIPrefix = ${this.jsAPIPrefix}\n"
          + s"defineStream = ${this.defineStream}\n"
          + s"replicationCount = ${this.replicationCount}\n"
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

    this.nc = Some(Nats.connect(options.get))

    if (isSource) {
      this.js = Some(getJetStreamContext())
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
    if (this.isLocal) {
      val logger: Logger = NatsLogger.logger
      logger.info("=====================In NatsConfig.getJetStreamContext")
    }

    val jsm: JetStreamManagement = if (this.jsAPIPrefix.isEmpty) {
      this.nc.get.jetStreamManagement()
    } else {
      val options = JetStreamOptions.builder().prefix(this.jsAPIPrefix.get).build()
      this.nc.get.jetStreamManagement(options)
    }

    // define the stream if so configured
    if (this.defineStream) {
      val sc: StreamConfiguration = StreamConfiguration
        .builder()
        .name(this.streamName.get)
        .storageType(this.storageType)
        .replicas(this.replicationCount)
        // .subjects(subjects.asJava)
        .subjects(this.streamSubjects.get.replace(" ", "").split(",").toList.asJava)
        .retentionPolicy(this.retentionPolicy)
        .build()

      if (this.isLocal) {
        val logger: Logger = NatsLogger.logger
        logger.info(sc.toJson())
      }

      // Add or use an existing stream.
      val si: StreamInfo = try {
        jsm.getStreamInfo(this.streamName.get)
        // if the stream is already defined, don't try to define it
      } catch {
        case e: JetStreamApiException =>
          if (e.getApiErrorCode == 10059) {
            // stream not found, define it
            jsm.addStream(sc)
          } else throw e
      }

      if (this.isLocal) {
        val logger: Logger = NatsLogger.logger
        logger.info(si.getConfiguration())
        logger.info(
          s"Created stream ${jsm.getStreamInfo(this.streamName.get)} with consumers ${
            jsm
              .getConsumers(this.streamName.get)
          }."
        )
      }
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
        if(this.durable.isDefined)
          configBuilder.durable(s"${this.durable.get}-${idx}")
        else {
          // TODO: Add configBuilder.InactiveThreshold()
        }
        jsm.addOrUpdateConsumer(this.streamName.get, configBuilder.build())
      }
    }

    if (this.jsAPIPrefix.isEmpty) {
      this.nc.get.jetStream()
    } else {
      val options = JetStreamOptions.builder().prefix(this.jsAPIPrefix.get).build()
      this.nc.get.jetStream(options)
    }
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

    if (System.getenv("NATS_NKEY") != null && System.getenv("NATS_NKEY") != "") {
      val handler: AuthHandler = new SampleAuthHandler(
        System.getenv("NATS_NKEY")
      )
      builder.authHandler(handler)
    } else if (System.getenv("NATS_CREDS") != null && System.getenv("NATS_CREDS") != "") {
      builder.authHandler(Nats.credentials(System.getenv("NATS_CREDS")));
    }

    val tlsAlgo = if (System.getenv("NATS_TLS_ALGO") != null && System.getenv("NATS_TLS_ALGO") != "") {
      System.getenv("NATS_TLS_ALGO")
    } else "SunX509"

    val instanceType = if (System.getenv("NATS_TLS_STORE_TYPE") != null && System.getenv("NATS_TLS_STORE_TYPE") != "") {
      System.getenv("NATS_TLS_STORE_TYPE")
    } else "JKS"

    if (System.getenv("NATS_TLS_TRUST_STORE") != null && System.getenv("NATS_TLS_TRUST_STORE") != "") {
      //      val trustStorePassword = if (System.getenv("NATS_TLS_TRUST_STORE_PASSWORD") != null) {
      //        System.getenv("NATS_TLS_TRUST_STORE_PASSWORD").toCharArray
      //      } else "".toCharArray

      //      val ctx = javax.net.ssl.SSLContext.getInstance(Options.DEFAULT_SSL_PROTOCOL)
      //
      //      val trustStore = KeyStore.getInstance(instanceType)
      //      val inputTrustF = new BufferedInputStream(Files.newInputStream(Paths.get(System.getenv("NATS_TLS_TRUST_STORE"))))
      //      try {
      //        trustStore.load(inputTrustF, trustStorePassword)
      //      } catch {
      //        case e: Exception => System.out.println("Exception " + e.getMessage)
      //      } finally {
      //        if (inputTrustF != null) inputTrustF.close()
      //      }
      //
      //      val tmsFactory = TrustManagerFactory.getInstance(tlsAlgo)
      //      tmsFactory.init(trustStore)
      //      val tms = tmsFactory.getTrustManagers

      builder.truststorePath(System.getenv("NATS_TLS_TRUST_STORE"));
      if (System.getenv("NATS_TLS_TRUST_STORE_PASSWORD") != null) {
        builder.truststorePassword(System.getenv("NATS_TLS_TRUST_STORE_PASSWORD").toCharArray)
      }

      if (System.getenv("NATS_TLS_KEY_STORE") != null) {
        if (System.getenv("NATS_TLS_KEY_STORE_PASSWORD") != null) {
          builder.keystorePassword(System.getenv("NATS_TLS_KEY_STORE_PASSWORD").toCharArray)
        }


        /* val keyStore = KeyStore.getInstance(instanceType)

         val inputKeyF = new BufferedInputStream(Files.newInputStream(Paths.get(System.getenv("NATS_TLS_KEY_STORE"))))
         try {
           keyStore.load(inputKeyF, keyStorePassword)
         } catch {
           case e: Exception => System.out.println("Exception " + e.getMessage)
         } finally {
           if (inputKeyF != null) {
             inputKeyF.close()
           }
         }

         val kmsFactory = KeyManagerFactory.getInstance(tlsAlgo)
         kmsFactory.init(keyStore, keyStorePassword)
         val kms = kmsFactory.getKeyManagers

         ctx.init(kms, tms, new SecureRandom())
         builder.sslContext(ctx)*/
        builder.keystorePath(System.getenv("NATS_TLS_KEY_STORE"));

      } else {
        //        ctx.init(null, tms, new SecureRandom())
        //        builder.sslContext(ctx)
      }
    }

    if (this.userName.isDefined) {
      builder.userInfo(this.userName.get, this.userPassword.getOrElse(""))
    }

    builder.build()
  }
}

object NatsLogger {
  val logger = {
    val logger: Logger = Logger.getLogger("NATSCON =>")
    val log4JPropertyFile = if (System.getenv("LOG_PROP_PATH") != null) System.getenv("LOG_PROP_PATH") else "src/test/resources/log4j.properties"
    val p = new Properties()
    p.load(new FileInputStream(log4JPropertyFile))
    PropertyConfigurator.configure(p)
    logger
  }
}
