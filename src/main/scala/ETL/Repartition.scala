package org.example
package ETL

import org.apache.spark.sql.{DataFrame, SparkSession}

object Repartition {
	def execute(spark: SparkSession, df: DataFrame): DataFrame = {
		// repartition the final df based on row count
		val rowsPerPartition = 200
		val partitions = math.ceil(df.count().toFloat / rowsPerPartition.toFloat).toInt
		val resultDf = df.coalesce(partitions)  // use coalesce instead of repartition for reducing partitions
		println(s"No. rows: ${resultDf.count()}")
		println(s"No. partitions: ${resultDf.rdd.getNumPartitions}")

		resultDf
	}
}
