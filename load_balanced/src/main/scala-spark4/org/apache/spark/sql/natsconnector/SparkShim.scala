package org.apache.spark.sql.natsconnector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.types.StructType

/**
 * Spark 4.x flavour of the version-specific glue. In Spark 4 `SQLContext`,
 * `SparkSession` and `Dataset` are abstract API classes shared with Spark Connect;
 * the internals (`internalCreateDataFrame`, ...) live on the `classic`
 * implementations, reached through `ClassicConversions`.
 *
 * Selected by the build via `src/main/scala-spark4` (see `build.sbt`).
 */
object SparkShim {

  def internalCreateDataFrame(
      sqlContext: SQLContext,
      rdd: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean): DataFrame =
    castToImpl(sqlContext).internalCreateDataFrame(rdd, schema, isStreaming)
}
