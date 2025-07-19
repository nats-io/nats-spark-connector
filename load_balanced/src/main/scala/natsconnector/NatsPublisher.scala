package natsconnector

import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.Message
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.api.PublishAck
import java.nio.charset.StandardCharsets
import io.nats.client.impl.NatsMessage
import io.nats.client.PublishOptions
import java.time.Duration
import io.nats.client.impl.Headers
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.Logger


class NatsPublisher(natsConfig: NatsConfig) {
  val isLocal = false
  val nc:Connection = natsConfig.nc.get
  // val js:JetStream = NatsConfigSink.config.js.get
  // val stream = NatsConfigSink.config.streamName.get
  
  // Will we need this functionality?
  def sendJetStreamMsg(data:String, subject:String):Unit = {
    //nc.publish(subject, data)
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.debug(s"publishing JetStream msg:${data}")
    }
    // val po:PublishOptions = PublishOptions.builder().stream(this.stream).build()
    // val pa:PublishAck = js.publish(subject, data.getBytes(StandardCharsets.US_ASCII), po)
  }

  def sendNatsMsg(data:String, subject:String):Unit = {
    val headers:Headers = new Headers()

    val msg = NatsMessage.builder()
              .data(data.getBytes(StandardCharsets.UTF_8))
              .subject(subject)
      .headers(headers)
              .build()
    if(this.isLocal) {
      val logger:Logger = NatsLogger.logger
      logger.debug(s"Publishing Nats msg:${data}")
    }
    nc.publish(msg)
    flush()
  }

  def sendNatsMsg(natsMsg:NatsMsg): Unit = {
    val timestamp:String = natsMsg.dateTime
    val headers:Headers = new Headers()
    headers.add("originTimestamp", timestamp)
    val msg = NatsMessage.builder()
              .data(natsMsg.content)
              .subject(natsMsg.subject)
              .headers(headers)
              .build()
    nc.publish(msg)
  }

  def flush():Unit = {
    val duration:Duration = natsConfig.flushWaitTime
    
    nc.flush(duration)
  }
}

