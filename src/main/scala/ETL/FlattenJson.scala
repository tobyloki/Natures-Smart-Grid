package org.example
package ETL

import com.github.wnameless.json.flattener.JsonFlattener
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlattenJson {
	def execute(spark: SparkSession, df: DataFrame): DataFrame = {
		implicit class DFExtensions(df: DataFrame) {
			def flatten: DataFrame = {
				var flattenedDf = spark.emptyDataFrame
				for(row <- df.collect()) {
					val flattenedRowJson = JsonFlattener.flatten(row.json)
					val flattenedRowRdd = spark.sparkContext.parallelize(flattenedRowJson::Nil)
					val flattenedRowDf = spark.read.json(flattenedRowRdd)

					flattenedDf = flattenedDf.unionByName(flattenedRowDf, allowMissingColumns=true)
				}
				flattenedDf
			}
		}

		// remove duplicates before processing
		val uniqueDf = df.dropDuplicates

		// flatten each row of the df to a single line
		val flattenedDf = uniqueDf.flatten

		// rename columns with ., [, ] to _
		val renamedDf = spark.createDataFrame(
			flattenedDf.rdd,
			StructType(flattenedDf.schema.map(s =>
				StructField(
					s.name
						.replaceAllLiterally(".", "_")
						.replaceAllLiterally("[", "_")
						.replaceAllLiterally("]", ""),
					s.dataType,
					s.nullable
				)
			))
		)

		// remove columns that end with _int & rename _double to nothing
		var renamedDf1 = renamedDf
		for(column <- renamedDf.schema.fields) {
			if(column.name.endsWith("_int")) {
				renamedDf1 = renamedDf1.drop(column.name)
			} else if(column.name.endsWith("_double")) {
				renamedDf1 = renamedDf1.withColumnRenamed(column.name, column.name.replaceAllLiterally("_double", ""))
			}
		}

		// only used for local, remove when using in Glue Studio
		// rename _1h to nothing
		var renamedDf2 = renamedDf1
		for(column <- renamedDf1.schema.fields) {
			if(column.name.endsWith("_1h")) {
				renamedDf2 = renamedDf2.withColumnRenamed(column.name, column.name.replaceAllLiterally("_1h", ""))
			}
		}

		renamedDf2
	}
}
