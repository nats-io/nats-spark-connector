# NATS-SPARK CONNECTOR

## Overview
In this flavor, NATS utilizes a single JetStream partition, using a durable
and queued configuration to load balance messages across Spark threads, each
thread contributing to single streaming micro-batch Dataframe at each Spark "pull".

Current message offset is kept in the NATS queue for the purpose of fault tolerance
(FT). Spark simply acknowledges each message during a micro-batch 'commit', and
NATS resends a message if an ack is not received within a pre-set, configurable
timeframe.

There is no need to pre-create the stream, queue, and durable.
The connector creates the stream based on user configuration, and sets the
queue and durable configuration transparently. The user also informs the connector
of the stream subjects the connector should utilize to acquire messages.
Multiple subjects may be configured using wildcards if so desired. Please refer
to option configurations in the 'Spark Streaming Source Options' section.

If so desired, one may publish to NATS messages out of Spark, i.e. stream sink,
via options in the Spark Session 'writeStream' configuration. The output
Dataframe should have the same format as the input from NATS, i.e. 'subject' as
the first column, 'dateTime' as the second column, and 'content' as the third
column. The connector will add the dateTime to the message header, then send
the content to the specified subject. Please refer to option configurations in
the 'Spark Streaming Sink Options' section.

## Spark and NATS Documentation
For general Spark development tips, including info on development using an IDE,
see ["Useful Developer Tools"](https://spark.apache.org/developer-tools.html).

For general NATS development tips, see ["NATS Docs"](https://docs.nats.io).

## Setting Up For Connector Utilization
### Sample Spark Configuration
A Spark configuration will consist of a driver and workers. The number of workers
**does not** have to match the number of NATS listeners. The connector utilizes
listeners, one per thread, to pull NATS messages. Once a NATS-sent batch
of messages gets converted and aggregated into a single Dataframe Spark will
partition work on the Dataframe across all its workers.

A sample Spark Docker configuration for a master and two workers can be found in
this repository at src/test/resources/docker-compose.yml.

### Sample NATS Configuration
As previously described, a NATS configuration for the connector will basically
consist of setting up a JetStream stream name, and a list of comma-separated
subjects that may use wildcards. The user should also setup an ack wait time,
which is the maximum time NATS will wait for a message acknowledgement before
resending a message. These and other configurations are described in the
'Spark Streaming Source Options' section.

## Setting Up to Run the Connector
### Tying the Connector to Spark
You may build the code using `sbt assembly` which will create the jar file.
After placing the jar in a directory of your choice, point Spark to said
directory by setting the Spark Session builder option "spark.jars".

A Scala sample test Spark Session builder configuration follows:
```
val spark = SparkSession
  .builder()
  .master("spark://localhost:7077")
  .appName("NatsReaderTest")
  .config("spark.logConf", "false")
  .config("spark.jars",
  "/some_path_to_the_connector_jar/nats-spark-connector_2.12-0.1.jar,"
  + "/some_path_to_the_Nats_jar/jnats-2.17.4.jar"
  )
  .config("spark.executor.instances", "2")
  .config("spark.cores.max", "4")
  .config("spark.executor.memory", "2g")
  .getOrCreate()
```
  For more information, please refer to the Spark documentation.


### Spark Streaming Source Options
An example Scala source configuration for the NATS connector follows:
```
val initDF = spark
    .readStream
    .format("nats")
    .option("nats.host", host)
    .option("nats.port", port)
    .option("nats.stream.name", "TestStream")
    .option("nats.stream.subjects", "subject1, subject2")
    // wait 90 seconds for an ack before resending a message
    .option("nats.msg.ack.wait.secs", 90)
    .option("nats.num.listeners", 2)
    // Each listener will fetch 10 messages at a time
    .option("nats.msg.fetch.batch.size", 10)
    .load()
```
where 'initDF' is the Dataframe obtained at each micro-batch acquisition
iteration, and NATS is configured for localhost access.
The 'format'  configuration should be **always set** to "nats", which will tie
Spark to this connector implementation.

Possible options are:
- "nats.host"
  The IP or DNS alias where NATS is installed. Obligatory configuration.

- "nats.port"
  The port to which the connector should listen. Obligatory configuration.

- "nats.stream.name"
  The JetStream string name from which the connector will read. The connector
  will attempt to create the stream. If the stream has been created externally
  make sure it matches the connector configuration or the connector will complain.
  Obligatory configuration.

- "nats.stream.subjects"
  The list of subjects to which the connector will listen. The connector will
  create a consumer per subject times the number of listeners. For example,
  for two listeners the connector will create two consumers per subject, each
  consumer will acquire interleaved, non-overlapping, messages concurrently.
  Obligatory configuration.

- "nats.num.listeners"
  The number of concurrent threads listening for messages. Each subject will
  contain a number of consumers equal to the number of listeners.
  (see "nats.stream.subjects"). Default is 1 listener.

- "nats.msg.ack.wait.secs"
  The amount of time the NATS server will wait for acknowledgement before resending
  a message. Spark commits a micro-batch at the end of the batch processing.
  At that point the connector sends an ack per message to the NATS server to
  indicate the messages have been processed. Make sure the ack wait time is
  greater than the maximum expected time for Spark batch processing. Repeated
  messages may be a good indication that the ack wait time is too short, and
  NATS is resending messages for which it has not gotten acks. Default is 60
  seconds.

- "nats.msg.fetch.batch.size"
  The maximum number of messages each consumer will attempt to acquire at once.
  The consumer will get as many messages as possible up to the configured batch
  size. If no messages are present, the consumer will wait
  "nats.msg.receive.wait.millis" before giving up. Default is 100.

- "nats.msg.receive.wait.millis"
  The maximum amount of time a consumer will wait if no messages to consume.
  See "nats.msg.fetch.batch.size". Default is 50 milliseconds.

- "nats.durable.name"
  Durable subscriptions allow clients to assign a durable name to a subscription
  when it is created. Doing this causes the NATS Streaming server to track the
  last acknowledged message for that clientID + durable name, so that only
  messages since the last acknowledged message will be delivered to the client.
  Obligatory configuration.

- "nats.storage.type"
  Defines the resilience of the message storage, memory or file. Default is
  StorageType.Memory.

- "nats.allow.reconnect"
  Default is 'true'. A 'true' setting will allow for infinite reconnects. Default
  is 'true'.

-  "nats.connection.timeout"
  Set the timeout for connection attempts. Each server in the options is allowed
  this timeout so if 3 servers are tried with a timeout of 5s the total time
  could be 15s. Default is 20 seconds.

- "nats.ping.interval"
  Set the interval between attempts to pings the server. These pings are
  automated, and capped by maxPingsOut(). As of 2.4.4 the library may wait up to
  2 * time to send a ping. Incoming traffic from the server can postpone the next
  ping to avoid pings taking up bandwidth during busy messaging.

  Keep in mind that a ping requires a round trip to the server. Setting this
  value to a small number can result in quick failures due to maxPingsOut being
  reached, these failures will force a disconnect/reconnect which can result in
  messages being held back or failed. In general, the ping interval should be set
  in seconds but this value is not enforced as it would result in an API change
  from the 2.0 release. Default is 10 seconds.

- "nats.reconnect.wait"
  Set the time to wait between reconnect attempts to the same server. This
  setting is only used by the client when the same server appears twice in the
  reconnect attempts, either because it is the only known server or by random
  chance. Note, the randomization of the server list doesn't occur per attempt,
  it is performed once at the start, so if there are 2 servers in the list you
  will never encounter the reconnect wait. Default is 20 seconds.

- "nats.storage.payload-compression"
  Specifies the compression type to be used to decompress the payload of the message. 
  Currently supported values are: `"zlib"`,`"uncompressed"` and `"none"`.
  If the payload of a particular message can not be decompressed, it is then passed on as-is.

- "nats.datetime.format"
  Specifies the format of the date time in the second column of the row, which is the time at
  which the message was received from NATS (the time the message was recorded into the stream
  is included in the JS Metadata column JSON).

- "nats.ssl.context.factory.class"
  Can be used to specify the name of your own SSL context factory class to use for the NATS connection.

### Spark Streaming Sink Options
An example Scala sink configuration for the NATS connector follows:
```
outboundDF.writeStream
  .outputMode("append") // only send new messages
  .format("nats")
  .option("checkpointLocation", "Users/spark_checkpoint")
  .option("nats.host", "0.0.0.0")
  .option("nats.port", "4222")
  .start()
  .awaitTermination()
```
  where 'outboundDF' is the Dataframe containing the output messages in the format
  captured in the 'Overview' section, and NATS is configured for localhost access.
  The 'format'  configuration should be **always set** to "nats", which will tie
  Spark to this connector implementation. The option "checkpointLocation" is a
  Spark setting indicating where checkpoints should be stored.

  Possible options are:
  - "nats.host"
    The IP or DNS alias where NATS is installed.

  - "nats.port"
    The port to which the connector should listen.


## Simple Connector Test
A simple connector test that reads messages and converts the inbound subjects
into outbound ones can be found in the repository at
'src/test/scala/sparktest/NatsSparkConnectorTestDriver.scala'.
The test first starts one publisher and one
subscriber thread. The publisher sends messages to the connector and Spark converts
inbound subject names into outbound ones, then sends the messages to the
subscriber thread.
