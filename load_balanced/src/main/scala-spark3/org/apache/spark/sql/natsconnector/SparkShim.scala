package org.apache.spark.sql.natsconnector

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * Spark 3.x flavour of the version-specific glue. `internalCreateDataFrame` is a
 * `private[sql]` member of `SQLContext`, which this object can reach because it lives
 * under the `org.apache.spark.sql` package.
 *
 * Selected by the build via `src/main/scala-spark3` (see `build.sbt`).
 */
object SparkShim {

  def internalCreateDataFrame(
      sqlContext: SQLContext,
      rdd: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean): DataFrame =
    sqlContext.internalCreateDataFrame(rdd, schema, isStreaming)
}
