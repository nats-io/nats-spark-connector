package natsconnector

import java.util.concurrent.TimeUnit
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.Message
import io.nats.client.{PushSubscribeOptions, PullSubscribeOptions}
import io.nats.client.JetStreamSubscription
import java.time.Duration
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
  val  defineConsumer = NatsConfigSource.config.defineConsumer;

  val jSub:JetStreamSubscription = {
    val trimmedSubjects = this.subjects.replace(" ", "")
    //subjectArray.zipWithIndex.map {
      //case (subject, idx) => {
      val pso = {
        val config = PullSubscribeOptions.builder()
          .stream(this.streamName)
        if(this.durable != None) {
          config.durable(s"${this.durable.get}")
          config.bind(!defineConsumer)
        }
        config.build()
      }
      if(defineConsumer)
        js.subscribe(null, pso)
      else
        js.subscribe(trimmedSubjects, pso)
      //}
    //}
  }

  def pullNext():java.util.List[Message] = {
    //var msgArray:Array[Message] = null
    // println(s"Subscription is active:${jSub.isActive()}")
    //this.jSub.map(sub => {
      var msgs: java.util.List[Message] = new ArrayList[Message]()
      try {
       msgs = this.jSub.fetch(this.fetchBatchSize, this.messageReceiveWaitTime)
      } catch {
        case ex: InterruptedException => println(s"nextMessage() waitTime exceeded: ${ex.getMessage()}.") // just try again
        case ex: IllegalStateException => println(s"Disregarding NATS msg: ${ex.getMessage()}") // do nothing, i.e. disregard NATS messages, only aquire Jetstreamed msgs
      }
      msgs
    //})
  }

  def unsubscribe():Unit = {
    jSub.drain(Duration.ofSeconds(1))
  }
}



