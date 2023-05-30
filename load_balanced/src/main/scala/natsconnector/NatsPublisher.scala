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


class NatsPublisher {
  val isLocal = false
  val nc:Connection = NatsConfigSink.config.nc.get
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
    val msg = NatsMessage.builder()
              .data(data.getBytes(StandardCharsets.US_ASCII))
              .subject(subject)
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
              .data(natsMsg.content.getBytes(StandardCharsets.US_ASCII))
              .subject(natsMsg.subject)
              .headers(headers)
              .build()
    nc.publish(msg)
  }

  def flush():Unit = {
    val nc:Connection = NatsConfigSink.config.nc.get
    val duration:Duration = NatsConfigSink.config.flushWaitTime
    
    nc.flush(duration)
  }
}

