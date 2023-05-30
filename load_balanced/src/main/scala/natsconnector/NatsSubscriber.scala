package natsconnector

import java.util.concurrent.TimeUnit
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.Message
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.api.PublishAck
import io.nats.client.{PushSubscribeOptions, PullSubscribeOptions}
import io.nats.client.JetStreamSubscription
import java.time.Duration
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import java.util.ArrayList

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

  val jSub:Array[JetStreamSubscription] = {
    val subjectArray = this.subjects.replace(" ", "").split(",")
    subjectArray.zipWithIndex.map {
      case (subject, idx) => {
      val pso = {
        val config = PullSubscribeOptions.builder()
          .stream(this.streamName)
        if(this.durable != None) 
          config.durable(s"${this.durable.get}-${idx}")
        config.build()
      }
      js.subscribe(subject, pso)}
    }
  }

  def pullNext():Array[List[Message]] = {
    //var msgArray:Array[Message] = null
    // println(s"Subscription is active:${jSub.isActive()}")
    this.jSub.map(sub => {
      var msgs: java.util.List[Message] = new ArrayList[Message]()
      try {
       msgs = sub.fetch(this.fetchBatchSize, this.messageReceiveWaitTime)
      } catch {
        case ex: InterruptedException => println(s"nextMessage() waitTime exceeded: ${ex.getMessage()}.") // just try again
        case ex: IllegalStateException => println(s"Disregarding NATS msg: ${ex.getMessage()}") // do nothing, i.e. disregard NATS messages, only aquire Jetstreamed msgs
      }
      msgs.toList
    })
  }

  def unsubscribe():Unit = {
    jSub.foreach(sub => sub.drain(Duration.ofSeconds(1)))
  }
}



