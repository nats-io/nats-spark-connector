package org.apache.spark.sql.nats

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Spark 3.x flavour of the version-specific glue. `internalCreateDataFrame` is a
 * `private[sql]` member directly on `SQLContext` / `SparkSession`.
 *
 * Selected by the build via `src/main/scala-spark3` (see `build.sbt`).
 */
private[nats] object SparkShim {

  def internalCreateDataFrame(
      sqlContext: SQLContext,
      rdd: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean): DataFrame =
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming)

  def internalCreateDataFrame(
      session: SparkSession,
      rdd: RDD[InternalRow],
      schema: StructType): DataFrame =
    session.internalCreateDataFrame(rdd, schema)
}
