package org.example
package ETL

import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.{DataFrame, SparkSession}

object EpochToTimestamp {
	def execute(spark: SparkSession, df: DataFrame): DataFrame = {
		spark.conf.set("spark.sql.session.timeZone", "GMT")

		var newDf = df
		for(column <- df.schema.fields) {
			if(column.name.endsWith("dt") || column.name.endsWith("rise") || column.name.endsWith("set")) {
				newDf = newDf.withColumn(column.name, from_unixtime(col(column.name), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
			}
		}

		spark.conf.unset("spark.sql.session.timeZone")

		newDf
	}
}
