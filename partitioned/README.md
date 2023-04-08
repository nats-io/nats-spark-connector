# NATS-SPARK CONNECTOR

## Overview
This flavor of Nats-Spark connector maps JetStream partitions to a Spark
micro-batch streaming job.

On the Nats side, JetStream partitions should consist of a string prefix followed
by an appended hyphen and a partition number starting from zero (0). For example,
if "JSPartition" is chosen as the string prefix, and three (3) partitions are
desired, then partitions should be named "JSPartition-0", "JSPartition-1", and
"JSPartition-2". Appropriate subjects should be assigned to each partition as
needed.

For the purpose of input into Spark, i.e. stream sourcing, the connector will
obtain the chosen JetStream string prefix via an 'option' setting in the Spark
Session 'readStream' configuration. Please refer to the "nats.stream.prefix"
option and other option configurations in the 'Spark Streaming Source Options'
section.

Messages from **all** JetStream partitions will be aggregated into a single
micro-batch input Dataframe, one message per row, that will have as columns the
'subject' from which the message was obtained, the message 'dateTime' obtained
from the message metadata, and the message 'content'.

If so desired, one may publish to Nats messages out of Spark, i.e. stream sink,
via options in the Spark Session 'writeStream' configuration. The output
Dataframe should have the same format as the input from Nats, i.e. 'subject' as
the first column, 'dateTime' as the second column, and 'content' as the third
column. The connector will add the dateTime to the message header, then send
the content to the specified subject. Please refer to option configurations in
the 'Spark Streaming Sink Options' section.

## Spark and Nats Documentation
For general Spark development tips, including info on development using an IDE,
see ["Useful Developer Tools"](https://spark.apache.org/developer-tools.html).

For general Nats development tips, see ["Nats Docs"](https://docs.nats.io).

## Setting Up For Connector Utilization
### Sample Spark Configuration
A Spark configuration will consist of a driver and workers. The number of workers
**does not** have to match the number of Nats partitions. Once a Nats-sent batch
of messages gets converted and aggregated into a single Dataframe Spark will
partition work on the Dataframe across all its workers.

A sample Spark Docker configuration for a master and two workers can be found in
this repository at src/test/resources/docker-compose.yml.

### Sample Nats Configuration
As previously described, a Nats configuration for the connector will consist
of JetStream partitions in the form <partition string prefix>-<partition number>.
For example, if "JSPartition" is chosen as the string prefix, and three (3)
partitions are desired, then partitions should be named "JSPartition-0",
"JSPartition-1", and "JSPartition-2". Appropriate subjects should be assigned to
each partition as needed.

Sample Nats configuration scripts for Linux and MacOS can be found in this
repository at src/test/resources/create_partition_streams-linux.bash and at
src/test/resources/create_partition_streams-macos.bash.

## Setting Up to Run the Connector
### Tying the Connector to Spark
You may build the code in the IDE of your choice or you may utilize the jar
located in the repository at 'target/scala-2.12/nats-spark-connector_2.12-0.1.jar'
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
  + "/some_path_to_the_Nats_jar/jnats-2.13.2.jar"
  )
  .config("spark.executor.instances", "2")
  .config("spark.cores.max", "4")
  .config("spark.executor.memory", "2g")
  .getOrCreate()
```
  For more information, please refer to the Spark documentation.


### Spark Streaming Source Options
An example Scala source configuration for the Nats connector follows:
```
val inboundDF = spark
    .readStream
    .format("natsJS")
    .option("nats.host", "0.0.0.0")
    .option("nats.port", "4222")
    .option("nats.stream.prefix", "SomeStreamPrefix")
    .option("nats.num.partitions", 2)
    .option("nats.reset.on.restart", false)
    .load()
```
where 'inboundDF' is the Dataframe obtained at each micro-batch acquisition
iteration, and Nats is configured for localhost access.
The 'format'  configuration should be **always set** to "natsJS", which will tie
Spark to this connector implementation.

Possible options are:
- **"nats.host"**
The IP or DNS alias where Nats is installed.

- **"nats.port"**
The port to which the connector should listen.

- **"nats.stream.prefix"**
The JetStream string prefix to which Nats will append partion numbers starting
from zero (0).

- **"nats.num.partitions"**
The number of JetStream partitions. Nats will append partition numbers to the
JetStream string prefix, from zero (0) to 'nats.num.partitions - 1'.
Default is '1'.

- **"nats.reset.on.restart"**
If set to 'true' the connector will start reading from the top of each partition
at every Spark restart. Otherwise (set to 'false') the connector will
continue from the next message after each partition's last-read offset, e.g.
if '30' was the last message offset read before Spark went down, then after
restart the connector will start reading from message offset '31'. This is the
usual Fault Tolerance (FT) recovery mode.

- **"nats.allow.reconnect"**
Default is 'true'. A 'true' setting will allow for infinite reconnects.

-  **"nats.connection.timeout"**
Set the timeout for connection attempts. Each server in the options is allowed
this timeout so if 3 servers are tried with a timeout of 5s the total time
could be 15s. Default is 20 seconds.

- **"nats.ping.interval"**
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

- **"nats.reconnect.wait"**
Set the time to wait between reconnect attempts to the same server. This
setting is only used by the client when the same server appears twice in the
reconnect attempts, either because it is the only known server or by random
chance. Note, the randomization of the server list doesn't occur per attempt,
it is performed once at the start, so if there are 2 servers in the list you
will never encounter the reconnect wait. Default is 20 seconds.

### Spark Streaming Sink Options
An example Scala sink configuration for the Nats connector follows:
```
outboundDF.writeStream
  .outputMode("append") // only send new messages
  .format("natsJS")
  .option("checkpointLocation", "Users/spark_checkpoint")
  .option("nats.host", "0.0.0.0")
  .option("nats.port", "4222")
  .start()
  .awaitTermination()
```
where 'outboundDF' is the Dataframe containing the output messages in the format
captured in the 'Overview' section, and Nats is configured for localhost access.
The 'format'  configuration should be **always set** to "natsJS", which will tie
Spark to this connector implementation. The option "checkpointLocation" is a
Spark setting indicating where checkpoints should be stored.

Possible options are:
  - **"nats.host"**
The IP or DNS alias where Nats is installed.

  - **"nats.port"**
The port to which the connector should listen.

## Simple Connector Test
A simple connector test that reads messages and converts the inbound subjects
into outbound ones can be found in the repository at 'src/test/scala/sparktest/NatsSparkConnectorTestDriver.scala'. The test first starts one publisher and one
subscriber thread. The publisher sends messages to the connector and Spark converts
inbound subject names into outbound ones, then sends the messages to the
subscriber thread.
