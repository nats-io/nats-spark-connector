# NATS-SPARK-CONNECTOR

## Overview
A single threaded JetStream client that turns Messages into an unadulterated
spark streaming dataframe.

### Credits and Influence
Originally forked from [Nats Spark](https://github.com/nats-io/nats-spark-connector)

Influenced and informed by [Spark Redis](https://www.google.com/search?q=spark+redis&sourceid=chrome&ie=UTF-8)

## Spark and NATS Documentation
For general Spark development tips, including info on development using an IDE,
see ["Useful Developer Tools"](https://spark.apache.org/developer-tools.html).

For general NATS development tips, see ["NATS Docs"](https://docs.nats.io).

## Building

The connector is built with sbt. By default it targets Spark 3.5.x and cross-builds
for Scala 2.12 and 2.13:

```shell
sbt +test +package +assembly
# -> nats-spark-connector/target/scala-2.1{2,3}/nats-spark-connector[-assembly]_2.1x-<version>.jar
```

### Spark 4

To build for Spark 4.x, pass the Spark version as a system property:

```shell
sbt -Dspark.version=4.1.2 test package assembly
# -> nats-spark-connector/target/scala-2.13/nats-spark-connector-spark4-assembly-<version>.jar
```

Spark 4 requires Java 17+ and only supports Scala 2.13, so the cross-build collapses to
2.13 and the artifact gets a `-spark4` suffix so it can't be confused with the Spark 3.5
Scala 2.13 jar. Both the 4.0.x and 4.1.x lines are supported.

The connector uses a handful of Spark-internal APIs (`Source`/`Sink`,
`internalCreateDataFrame`) whose home changed in Spark 4; the version-specific glue is
isolated in `src/main/scala-spark3/` and `src/main/scala-spark4/` (`SparkShim`) and the
build picks the right one based on `spark.version`. The streaming offsets written to the
checkpoint are the same plain numbers in both, so checkpoints are portable across builds.

## Setting Up to Run the Connector
```scala
val spark = SparkSession
  .builder()
  .master("local[*]")
  .getOrCreate()
```
For more information, please refer to the Spark documentation.


### Spark Streaming Source Options
An example Scala source configuration for the NATS connector follows:
```scala
val initDF = spark
  .format("nats") // use "org.apache.spark.sql.nats" if you see DATA_SOURCE_NOT_FOUND
  .option("nats.host", "localhost")
  .option("nats.port", "4222")
  .option("nats.credential.file", "/Users/mrosti/shh/...")
  .option("nats.pull.subscription.stream.name", "my-stream")
  .option("nats.pull.subscription.durable.name", "my-stream")
  .option("nats.storage.payload-compression", "zlib") // default is "none"
  .option("nats.pull.consumer.ack.wait", "90")
  .option("nats.pull.consumer.max.batch", "10")
  .option("nats.stream.subjects", "my.stream.data")
  .option("nats.pull.batcher.initial.delay", "1")
  .option("nats.pull.batcher.frequency.secs", "30")
  .option("nats.pull.batch.size", "100")
  .option("nats.pull.wait.time", "10")
  .option("nats.trust-store.path", "/path/to/trust-store")
  .option("nats.trust-store.password", "trust_store_password")
  .option("nats.key-store.path", "/path/to/key-store")
  .option("nats.key-store.password", "key_store_password")
  .option("nats.tls.algorithm", "alg")
  .option("nats.source.js.api-prefix", "source-js-api-prefix")
  .option("nats.sink.js.api-prefix", "sink-js-api-prefix")
```

JetStream schema
```scala
StructType(
  Array(
    StructField("subject", StringType, nullable = true),
    StructField("replyTo", StringType, nullable = true),
    StructField("content", BinaryType, nullable = false),
    StructField("headers", MapType(StringType, ArrayType(StringType)), nullable = true),
    StructField("domain", StringType, nullable = true),
    StructField("stream", StringType, nullable = true),
    StructField("consumer", StringType, nullable = true),
    StructField("delivered", LongType, nullable = true),
    StructField("streamSeq", LongType, nullable = true),
    StructField("consumerSeq", LongType, nullable = true),
    StructField("timestamp", TimestampType, nullable = true),
    StructField("pending", LongType, nullable = true)
  )
)

```

### Checkpointing
I presume there to be some issues with checkpointing; however, to our best efforts
we have used `consumerSeq` as the offset for spark commits

### Watermarking
You can use the timestamp field from nats to watermark your data

```scala
myDF
  .withWatermark("timestamp", "1 minute")
```

### Spark Streaming Sink Options
The sink is quite simple, on addBatch.
```scala
initDF
  .withColumn("subject", lit("my-output"))
  .withColumn("data", lit(Array.empty[Byte]))
  .writeStream
  .format("console")
  .option("nats.host", "localhost")
  .option("nats.port", "4222")
  .option("nats.credential.file", "/Users/mrosti/shh/...")
  .option("nats.stream.name", "my-output-stream")
  .option("nats.stream.subject", "my.output.stream.data")
```

Required output schema
```scala
  StructType(
    Array(
      StructField("subject", StringType, nullable = false),
      StructField("data", BinaryType, nullable = false),
      StructField("headers", MapType(StringType, ArrayType(StringType)), nullable = true)
    )
  )
```
