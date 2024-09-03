package natsconnector.spark

import natsconnector.NatsMsg
//import natsconnector.NatsBatchPublisher
import natsconnector.NatsConfigSink
import natsconnector.NatsConfig

import java.nio.charset.StandardCharsets

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

import io.nats.client.impl.Headers
import io.nats.client.impl.NatsMessage
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Message



class NatsJetStreamStreamingSink(sqlContext: SQLContext,
                                 parameters: Map[String, String],
                                 partitionColumns: Seq[String],
                                 outputMode: OutputMode)
  extends Sink {
  val options = NatsConfigSink.config.options      
  val con = Nats.connect(options)
  override def addBatch(batchId: Long, data: DataFrame):Unit = {
    // println("=====================In NatsStreamingSink.addBatch")
    // conver data frame into a list of NatsMsg
    val sparkSession:SparkSession = sqlContext.sparkSession
    import sparkSession.implicits._

    val sendNatsMsg = (subject:String, dateTime:String, data:String) => {
      //val options = NatsConfig.config.options
      // println(s"Options: $options")
      val headers:Headers = new Headers()
      headers.add("originTimestamp", dateTime)
      val natsMsg = NatsMessage.builder()
              .data(data.getBytes(StandardCharsets.US_ASCII))
              .subject(subject)
              .headers(headers)
              .build()
      //val con = Nats.connect(options)
      con.publish(natsMsg)
    }

    val rdd: RDD[Row] = data.sparkSession.sparkContext.parallelize(data.collect())
    val df = data.sparkSession.createDataFrame(rdd, data.schema)

    val natsMsgDataset = df.map(row => 
                     new NatsMsg(row.getString(0), row.getString(1), row.getString(2)))

    val natsMsgs: Seq[NatsMsg] = natsMsgDataset.collect().toSeq

    // var msg:Message = null
    // for(msg <- natsMsgs) {
    //   sendNatsMsg(msg.subject, msg.dateTime, msg.content)
    // }
    natsMsgs.foreach(msg => sendNatsMsg(msg.subject, msg.dateTime, msg.content))
  
  }
}