package org.example
package ETL

import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql.toSparkSessionFunctions
import org.apache.spark.sql.{DataFrame, SparkSession}

object EnrichData {
	def execute(spark: SparkSession, df: DataFrame): DataFrame = {
		val username = "<mongodb user username>"
		val password = "<mongodb user password>"
		val endpoint = "<mongodb cluster endpoint>"
		val db = "mydb"
		val collection = "places"

		val localeDf = spark.loadFromMongoDB(ReadConfig(
			Map("uri" -> s"mongodb+srv://$username:$password@$endpoint/$db.$collection")
		))

		// join the df with the localeDf to get the locale
		val joinedDf = df
			.join(
				localeDf,
				df("lat") === localeDf("lat") && df("lng") === localeDf("lng"),
				"left")
			.select(localeDf("city"), localeDf("state"), df("*"))

		joinedDf
	}
}
