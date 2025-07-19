package natsconnector.spark

import natsconnector.NatsConfigSource
import natsconnector.NatsConfigSink
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
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
        StructField("content", StringType, true) ::
        StructField("headers", StringType, true) ::
        StructField("jsMetadata", StringType, true) :: Nil
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
    // Create a unique config key based on stream name and other identifying parameters
    val configKey = createConfigKey(parameters, "source")
    val config = NatsConfigSource.getConfig(configKey)
    config.setConnection(parameters)
    val (_, ss) = sourceSchema(sqlContext, schema, providerName, parameters)
    val source = new NatsStreamingSource(sqlContext, metadataPath, Some(ss), parameters, config)
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
    // Create a unique config key based on parameters
    val configKey = createConfigKey(parameters, "sink")
    val config = NatsConfigSink.getConfig(configKey)
    config.setConnection(parameters)
    val sink = new NatsStreamingSink(sqlContext, parameters, partitionColumns, outputMode, config)
    sink
  }

  override def shortName(): String = "nats"
  
  private def createConfigKey(parameters: Map[String, String], sourceType: String): String = {
    val host = parameters.getOrElse("nats.host", "localhost")
    val port = parameters.getOrElse("nats.port", "4222")
    val streamName = parameters.getOrElse("nats.stream.name", "default")
    val subjects = parameters.getOrElse("nats.stream.subjects", "default")
    val durable = parameters.getOrElse("nats.durable.name", "")
    
    s"${sourceType}-${host}-${port}-${streamName}-${subjects}-${durable}-${Thread.currentThread().getId}"
  }

}
