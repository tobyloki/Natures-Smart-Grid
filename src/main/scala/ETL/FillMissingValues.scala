package org.example
package ETL

import org.apache.spark.sql.{DataFrame, SparkSession}

object FillMissingValues {
	def execute(spark: SparkSession, df: DataFrame): DataFrame = {
		var columnNames: Array[String] = Array()
		for(column <- df.schema.fields) {
			if(column.name.contains("_rain")) {
				columnNames :+= column.name
			}
		}

		// for local testing, use "0" instead of 0 b/c program can't infer types for data - it assumes all are strings
		val filledDf = df.na.fill("0"/*0*/, columnNames)

		filledDf
	}
}
