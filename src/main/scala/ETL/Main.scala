package org.example
package ETL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.ERROR)
		println("Starting Spark session")
		val spark = SparkSession.builder
			.appName("SparkSQL")
			.master("local[*]")
			.getOrCreate()

		// get a single input df
		val inputDf = spark.read.json("src/main/data/weatherData.json")
		var df = spark.emptyDataFrame
		df = df.unionByName(inputDf, allowMissingColumns=true)

		// flatten df
		df = FlattenJson.execute(spark, df)

		// enrich data with locale
		df = EnrichData.execute(spark, df)

		// convert epoch to timestamp
		df = EpochToTimestamp.execute(spark, df)

		// fill missing values in rain column with 0
		df = FillMissingValues.execute(spark, df)

		df.show(false)

		// region Hourly
		println("Hourly")

		// extract hourly columns
		var hourlyDf = ExtractHourly.execute(spark, df)
		// repartition
		hourlyDf = Repartition.execute(spark, hourlyDf)

		hourlyDf.show(50, truncate = false)
		// endregion

		// region Daily
		println("Daily")

		// extract daily columns
		var dailyDf = ExtractDaily.execute(spark, df)
		// repartition
		dailyDf = Repartition.execute(spark, dailyDf)

		dailyDf.show(false)
		// endregion
	}
}