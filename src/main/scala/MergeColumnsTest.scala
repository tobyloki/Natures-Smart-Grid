package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object MergeColumnsTest {
	def main(args: Array[String]): Unit = {
		Logger.getLogger("org").setLevel(Level.ERROR)
		println("Starting Spark session")
		val spark = SparkSession.builder
			.appName("SparkSQL")
			.master("local[*]")
			.getOrCreate()

		import spark.implicits._

		val data = Seq(("Java", null, null), ("Python", null, "1"), ("Scala", "2", "2.5"), ("Scala", "3", null))
		val df = spark.sparkContext.parallelize(data).toDF()
		df.show()

		// find columns to do a coalesce
		val cols = df.columns.filter(x => x == "_2" || x == "_3").map(col)

		// do the actual coalesce
		val newDf = df.select($"_1", coalesce(cols: _*).as("merged"))
		newDf.show()
	}
}
