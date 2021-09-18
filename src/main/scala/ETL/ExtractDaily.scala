package org.example
package ETL

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExtractDaily {
	def execute(spark: SparkSession, df: DataFrame): DataFrame = {
		// select relevant columns
		val dailyDf = df.select(df.columns.filter(f =>
			f == "city" ||
				f == "state" ||
				f == "lat" ||
				f == "lng" ||
				f == "forecast_timeZone" ||
				(f.startsWith("forecast_daily") && (
					f.endsWith("dt") ||
						f.endsWith("uvi") ||
						f.endsWith("wind_speed") ||
						f.endsWith("wind_gust") ||
						f.endsWith("humidity") ||
						f.endsWith("dew_point") ||
						f.endsWith("clouds") ||
						f.endsWith("pop") ||
						f.endsWith("rain") ||
						f.endsWith("snow") ||
						f.endsWith("temp_day") ||
						f.endsWith("temp_eve") ||
						f.endsWith("temp_max") ||
						f.endsWith("temp_min") ||
						f.endsWith("temp_morn") ||
						f.endsWith("temp_night") ||
						f.endsWith("sunrise") ||
						f.endsWith("sunset")
					))
		).map(m => col(m)):_*)

		// merge columns
		var dailyIterator = 1
		var mergedDailyDf = dailyDf
			.withColumnRenamed("forecast_timeZone", "timezone")
			.withColumnRenamed("forecast_daily_0_dt", "dt")
			.withColumnRenamed("forecast_daily_0_uvi", "uvi")
			.withColumnRenamed("forecast_daily_0_wind_speed", "wind_speed")
			.withColumnRenamed("forecast_daily_0_wind_gust", "wind_gust")
			.withColumnRenamed("forecast_daily_0_humidity", "humidity")
			.withColumnRenamed("forecast_daily_0_dew_point", "dew_point")
			.withColumnRenamed("forecast_daily_0_clouds", "clouds")
			.withColumnRenamed("forecast_daily_0_pop", "pop")
			.withColumnRenamed("forecast_daily_0_rain", "rain")
			.withColumnRenamed("forecast_daily_0_temp_day", "temp_day")
			.withColumnRenamed("forecast_daily_0_temp_eve", "temp_eve")
			.withColumnRenamed("forecast_daily_0_temp_max", "temp_max")
			.withColumnRenamed("forecast_daily_0_temp_min", "temp_min")
			.withColumnRenamed("forecast_daily_0_temp_morn", "temp_morn")
			.withColumnRenamed("forecast_daily_0_temp_night", "temp_night")
			.withColumnRenamed("forecast_daily_0_sunrise", "sunrise")
			.withColumnRenamed("forecast_daily_0_sunset", "sunset")

		for(column <- dailyDf.schema.fields) {
			if(column.name.startsWith("forecast_daily") && column.name.endsWith("dt")) {
				if(dailyIterator < 2) {
					mergedDailyDf = mergedDailyDf.select(
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
						"temp_day",
						"temp_eve",
						"temp_max",
						"temp_min",
						"temp_morn",
						"temp_night",
						"sunrise",
						"sunset"
					)
						.union(dailyDf.select(
							"city",
							"state",
							"lat",
							"lng",
							"forecast_timeZone",
							s"forecast_daily_${dailyIterator}_dt",
							s"forecast_daily_${dailyIterator}_uvi",
							s"forecast_daily_${dailyIterator}_wind_speed",
							s"forecast_daily_${dailyIterator}_wind_gust",
							s"forecast_daily_${dailyIterator}_humidity",
							s"forecast_daily_${dailyIterator}_dew_point",
							s"forecast_daily_${dailyIterator}_clouds",
							s"forecast_daily_${dailyIterator}_pop",
							s"forecast_daily_${dailyIterator}_rain",
							s"forecast_daily_${dailyIterator}_temp_day",
							s"forecast_daily_${dailyIterator}_temp_eve",
							s"forecast_daily_${dailyIterator}_temp_max",
							s"forecast_daily_${dailyIterator}_temp_min",
							s"forecast_daily_${dailyIterator}_temp_morn",
							s"forecast_daily_${dailyIterator}_temp_night",
							s"forecast_daily_${dailyIterator}_sunrise",
							s"forecast_daily_${dailyIterator}_sunset"
						))
				}
				dailyIterator += 1
			}
		}

		mergedDailyDf
	}
}
