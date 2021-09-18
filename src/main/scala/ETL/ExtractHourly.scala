package org.example
package ETL

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractHourly {
	def execute(spark: SparkSession, df: DataFrame): DataFrame = {
		// in Glue Studio, need to reference rain_1h columns instead of _rain

		// select relevant columns
		val hourlyDf = df.select(df.columns.filter(f =>
			f == "city" ||
				f == "state" ||
				f == "lat" ||
				f == "lng" ||
				f == "forecast_timeZone" ||
				(f.startsWith("forecast_hourly") && (
					f.endsWith("dt") ||
						f.endsWith("uvi") ||
						f.endsWith("wind_speed") ||
						f.endsWith("wind_gust") ||
						f.endsWith("humidity") ||
						f.endsWith("dew_point") ||
						f.endsWith("clouds") ||
						f.endsWith("pop") ||
						f.endsWith("rain") ||
						f.endsWith("temp")
					))
		).map(m => col(m)):_*)

		// merge columns
		var hourlyIterator = 1
		var mergedHourlyDf = hourlyDf
			.withColumnRenamed("forecast_timeZone", "timezone")
			.withColumnRenamed("forecast_hourly_0_dt", "dt")
			.withColumnRenamed("forecast_hourly_0_uvi", "uvi")
			.withColumnRenamed("forecast_hourly_0_wind_speed", "wind_speed")
			.withColumnRenamed("forecast_hourly_0_wind_gust", "wind_gust")
			.withColumnRenamed("forecast_hourly_0_humidity", "humidity")
			.withColumnRenamed("forecast_hourly_0_dew_point", "dew_point")
			.withColumnRenamed("forecast_hourly_0_clouds", "clouds")
			.withColumnRenamed("forecast_hourly_0_pop", "pop")
			.withColumnRenamed("forecast_hourly_0_rain", "rain")
			.withColumnRenamed("forecast_hourly_0_temp", "temp")

		for(column <- hourlyDf.schema.fields) {
			if(column.name.startsWith("forecast_hourly") && column.name.endsWith("dt")) {
				if(hourlyIterator < 24) {
					mergedHourlyDf = mergedHourlyDf.select(
						"city",
						"state",
						"lat",
						"lng",
						"timezone",
						"dt",
						"uvi",
						"wind_speed",
						"wind_gust",
						"humidity",
						"dew_point",
						"clouds",
						"pop",
						"rain",
						"temp"
					)
						.union(hourlyDf.select(
							"city",
							"state",
							"lat",
							"lng",
							"forecast_timeZone",
							s"forecast_hourly_${hourlyIterator}_dt",
							s"forecast_hourly_${hourlyIterator}_uvi",
							s"forecast_hourly_${hourlyIterator}_wind_speed",
							s"forecast_hourly_${hourlyIterator}_wind_gust",
							s"forecast_hourly_${hourlyIterator}_humidity",
							s"forecast_hourly_${hourlyIterator}_dew_point",
							s"forecast_hourly_${hourlyIterator}_clouds",
							s"forecast_hourly_${hourlyIterator}_pop",
							s"forecast_hourly_${hourlyIterator}_rain",
							s"forecast_hourly_${hourlyIterator}_temp"
						))
				}
				hourlyIterator += 1
			}
		}

		mergedHourlyDf
	}
}
