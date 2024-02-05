package natsconnector

import java.util.concurrent.TimeUnit
import io.nats.client.{Connection, JetStream, JetStreamApiException, JetStreamSubscription, Message, Nats, Options, PullSubscribeOptions, PushSubscribeOptions}
import io.nats.client.api.{ConsumerConfiguration, PublishAck}
import natsconnector.NatsLogger.logger
import org.apache.log4j.Logger

import java.time.Duration
import java.util
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import java.util.ArrayList
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}

class NatsSubscriber() {
  val subjects:String = NatsConfigSource.config.streamSubjects.get
  //val deliverySubject:String = NatsConfigSource.config.queueDeliverySubject
  //val queue:String = NatsConfigSource.config.queue
  val js:JetStream = NatsConfigSource.config.js.get
  val nc:Connection = NatsConfigSource.config.nc.get
  val messageReceiveWaitTime:Duration = NatsConfigSource.config.messageReceiveWaitTime
  val durable:Option[String] = NatsConfigSource.config.durable
  val streamName = NatsConfigSource.config.streamName.get
  val fetchBatchSize = NatsConfigSource.config.msgFetchBatchSize

  val jSub: JetStreamSubscription = {
    val subjectArray = this.subjects.replace(" ", "").split(",")

    val pso = {
      val config = PullSubscribeOptions.builder()
        .stream(this.streamName)
      if(this.durable.isDefined) {
        config.durable(this.durable.get)
      } else {
        val cco = ConsumerConfiguration.builder()
          .filterSubjects(subjectArray.toList.asJava)
          .build()
        config.configuration(cco)
      }
      config.fastBind(true)
      config.build()
    }
    try {
      js.subscribe(null, pso)
    } catch {
      case ex: IllegalStateException =>
        if (NatsConfigSource.config.isLocal) {
          val logger: Logger = NatsLogger.logger
          logger.error(s"Error subscribing to NATS stream ${this.streamName} ${this.durable} : ${ex.getMessage()}\n ${ex.printStackTrace()}")
        }
        null
    }
  }

  def pullNext():List[Message] = {
    //var msgArray:Array[Message] = null
    // println(s"Subscription is active:${jSub.isActive()}")

    if (this.jSub == null) {
      List.empty[Message]
    } else
      try {
        this.jSub.fetch(this.fetchBatchSize, this.messageReceiveWaitTime).asScala.toList
      } catch {
        case ex: InterruptedException => println(s"nextMessage() waitTime exceeded: ${ex.getMessage()}."); List.empty[Message]
        case ex: IllegalStateException => println(s"Disregarding NATS msg: ${ex.getMessage()}"); new util.ArrayList[Message](); List.empty[Message]
      }
  }

  def unsubscribe():Unit = {
    if (this.jSub != null) {
      jSub.drain(Duration.ofSeconds(30))
    }
  }
}