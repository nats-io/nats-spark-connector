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
  .format("nats")
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
