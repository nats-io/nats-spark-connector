package natsconnector.spark

import natsconnector.NatsConfigSource
import natsconnector.NatsConfigSink
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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
import io.nats.client.{PullSubscribeOptions, PushSubscribeOptions}
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.AckPolicy
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.DeliverPolicy
import natsconnector.SampleAuthHandler

import java.time.Duration
import io.nats.client.KeyValueManagement
import io.nats.client.api.KeyValueConfiguration
import io.nats.client.api.KeyValueStatus
import io.nats.client.KeyValue
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

class NatsJetStreamProvider extends DataSourceRegister
  with StreamSourceProvider with StreamSinkProvider {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String,
                            parameters: Map[String,String]): (String, StructType) = {
    val schema = StructType (
      StructField("subject", StringType, true) ::
        StructField("dateTime", StringType, true) ::
        StructField("content", StringType, true) :: Nil
    )

    ("NatsStruct", schema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String,String]): Source = {
    // Put developer set configurations into NatsConfig singleton                           
    NatsConfigSource.config.setConnection(parameters) 
    // permit time to connect to Nats server 
    val (_, ss) = sourceSchema(sqlContext, schema, providerName, parameters)
    NatsJetStreamBatchProcessor.startBatchingThreads(NatsConfigSource.config.numPartitions) 
    val source = new NatsJetStreamStreamingSource(sqlContext, metadataPath, Some(ss), parameters)
   source
  }

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    NatsConfigSink.config.setConnection(parameters) 
    val sink = new NatsJetStreamStreamingSink(sqlContext, parameters, partitionColumns, outputMode)
    sink
  }

  override def shortName(): String = "natsJS"

}
