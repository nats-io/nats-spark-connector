package natsconnector.spark

import natsconnector.{CoreNatsMsg, NatsMsg}
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



class NatsStreamingSink(sqlContext: SQLContext,
                                 parameters: Map[String, String],
                                 partitionColumns: Seq[String],
                                 outputMode: OutputMode,
                                 natsConfig: NatsConfig)
  extends Sink {
  val options = natsConfig.options      
  val con = natsConfig.nc.get
  override def addBatch(batchId: Long, data: DataFrame):Unit = {
    // println("=====================In NatsStreamingSink.addBatch")
    // conver data frame into a list of NatsMsg
    val sparkSession:SparkSession = sqlContext.sparkSession
    import sparkSession.implicits._

    val sendNatsMsg = (subject:String, dateTime:String, data:Array[Byte]) => {
      //val options = NatsConfig.config.options
      // println(s"Options: $options")
      val headers:Headers = new Headers()
      headers.add("originTimestamp", dateTime)
      val natsMsg = NatsMessage.builder()
              .data(data)
              .subject(subject)
              .headers(headers)
              .build()
      con.publish(natsMsg)
    }

    val rdd: RDD[Row] = data.sparkSession.sparkContext.parallelize(data.collect())
    val df = data.sparkSession.createDataFrame(rdd, data.schema)

    val natsMsgDataset = df.map(row =>
                     new CoreNatsMsg(row.getString(0), row.getString(1), row.getString(2).getBytes(), None))

    val natsMsgs: Seq[CoreNatsMsg] = natsMsgDataset.collect().toSeq

    natsMsgs.foreach(msg => sendNatsMsg(msg.subject, msg.dateTime, msg.content))

  }
}
