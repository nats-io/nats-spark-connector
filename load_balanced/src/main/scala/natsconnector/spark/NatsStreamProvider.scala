package natsconnector.spark

import natsconnector.NatsConfigSource
import natsconnector.NatsConfigSink
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider, StreamSinkProvider}
import org.apache.spark.sql.types.{StringType, TimestampType, StructField, StructType}

import org.apache.spark.sql.execution.streaming.{Source, Sink}
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.log4j.Logger
import natsconnector.NatsLogger



class NatsStreamProvider extends DataSourceRegister 
  with StreamSourceProvider with StreamSinkProvider {
  val logger:Logger = NatsLogger.logger
  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, 
                            parameters: Map[String,String]): (String, StructType) = {
    val schema = StructType (
                    StructField("subject", StringType, true) ::
                    StructField("dateTime", StringType, true) :: 
                    StructField("content", StringType, true) :: Nil
                  )
    this.logger.debug(
      "Nats message schema:\n"
      + s"${schema}"
    )
    ("NatsStruct", schema)
  }


  override def createSource(sqlContext: SQLContext, 
                            metadataPath: String, 
                            schema: Option[StructType], 
                            providerName: String, 
                            parameters: Map[String,String]): Source = {                           
    this.logger.debug(
      "Source config parameters:\n"
      + s"${parameters}"
    )
    NatsConfigSource.config.setConnection(parameters) 
    val (_, ss) = sourceSchema(sqlContext, schema, providerName, parameters)
    val source = new NatsStreamingSource(sqlContext, metadataPath, Some(ss), parameters)
    source
  }

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): NatsStreamingSink = {
    this.logger.debug(
      "Sink config parameters:\n"
      + s"${parameters}"
    )
    NatsConfigSink.config.setConnection(parameters)
    val sink = new NatsStreamingSink(sqlContext, parameters, partitionColumns, outputMode)
    sink
  }

  override def shortName(): String = "nats"  

}
