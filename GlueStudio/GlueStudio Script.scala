import com.amazonaws.services.glue.DynamicFrame
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._

object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    // @type: DataSource
    // @args: [format_options = JsonOptions("""{"jsonPath":"","multiline":false}"""), connection_type = "s3", format = "json", connection_options = {"paths": ["s3://myplayground-bucket1/log/"], "recurse":true}, transformation_ctx = "DataSource0"]
    // @return: DataSource0
    // @inputs: []
    val DataSource0 = glueContext.getSourceWithFormat(connectionType = "s3", options = JsonOptions("""{"paths": ["s3://myplayground-bucket1/log/"], "recurse":true}"""), transformationContext = "DataSource0", format = "json", formatOptions = JsonOptions("""{"jsonPath":"","multiline":false}""")).getDynamicFrame()
    // @type: ApplyMapping
    // @args: [mappings = [("lat", "double", "lat", "double"), ("lng", "double", "lng", "double"), ("future.timeZone", "string", "forecast.timeZone", "string"), ("future.dailyFuture", "array", "forecast.daily", "array"), ("future.hourlyFuture", "array", "forecast.hourly", "array")], transformation_ctx = "Transform6"]
    // @return: Transform6
    // @inputs: [frame = DataSource0]
    val Transform6 = DataSource0.applyMapping(mappings = Seq(("lat", "double", "lat", "double"), ("lng", "double", "lng", "double"), ("future.timeZone", "string", "forecast.timeZone", "string"), ("future.dailyFuture", "array", "forecast.daily", "array"), ("future.hourlyFuture", "array", "forecast.hourly", "array")), caseSensitive = false, transformationContext = "Transform6")
    // @type: CustomCode
    // @args: [dynamicFrameConstruction = Seq(Transform6), className = FlattenJson, transformation_ctx = "Transform9"]
    // @return: Transform9
    // @inputs: [dfc = Transform6]
    val Transform9 = FlattenJson.execute(glueContext, Seq(Transform6))
    // @type: CustomCode
    // @args: [className = EnrichData, transformation_ctx = "Transform4"]
    // @return: Transform4
    // @inputs: [dfc = Transform9]
    val Transform4 = EnrichData.execute(glueContext, Transform9)
    // @type: CustomCode
    // @args: [className = EpochToTimestamp, transformation_ctx = "Transform5"]
    // @return: Transform5
    // @inputs: [dfc = Transform4]
    val Transform5 = EpochToTimestamp.execute(glueContext, Transform4)
    // @type: CustomCode
    // @args: [className = FillMissingValues, transformation_ctx = "Transform3"]
    // @return: Transform3
    // @inputs: [dfc = Transform5]
    val Transform3 = FillMissingValues.execute(glueContext, Transform5)
    // @type: CustomCode
    // @args: [className = ExtractHourly, transformation_ctx = "Transform7"]
    // @return: Transform7
    // @inputs: [dfc = Transform3]
    val Transform7 = ExtractHourly.execute(glueContext, Transform3)
    // @type: CustomCode
    // @args: [className = RepartitionHourly, transformation_ctx = "Transform1"]
    // @return: Transform1
    // @inputs: [dfc = Transform7]
    val Transform1 = RepartitionHourly.execute(glueContext, Transform7)
    // @type: SelectFromCollection
    // @args: [index = 0, transformation_ctx = "Transform0"]
    // @return: Transform0
    // @inputs: [dfc = Transform1]
    val Transform0 = Transform1(0)
    // @type: DataSink
    // @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://myplayground-bucket1/output/hourly/", "compression": "gzip", "partitionKeys": []}, transformation_ctx = "DataSink1"]
    // @return: DataSink1
    // @inputs: [frame = Transform0]
    val DataSink1 = glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions("""{"path": "s3://myplayground-bucket1/output/hourly/", "compression": "gzip", "partitionKeys": []}"""), transformationContext = "DataSink1", format = "csv").writeDynamicFrame(Transform0)
    // @type: CustomCode
    // @args: [className = ExtractDaily, transformation_ctx = "Transform2"]
    // @return: Transform2
    // @inputs: [dfc = Transform3]
    val Transform2 = ExtractDaily.execute(glueContext, Transform3)
    // @type: CustomCode
    // @args: [className = RepartitionDaily, transformation_ctx = "Transform10"]
    // @return: Transform10
    // @inputs: [dfc = Transform2]
    val Transform10 = RepartitionDaily.execute(glueContext, Transform2)
    // @type: SelectFromCollection
    // @args: [index = 0, transformation_ctx = "Transform8"]
    // @return: Transform8
    // @inputs: [dfc = Transform10]
    val Transform8 = Transform10(0)
    // @type: DataSink
    // @args: [connection_type = "s3", format = "csv", connection_options = {"path": "s3://myplayground-bucket1/output/daily/", "compression": "gzip", "partitionKeys": []}, transformation_ctx = "DataSink0"]
    // @return: DataSink0
    // @inputs: [frame = Transform8]
    val DataSink0 = glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions("""{"path": "s3://myplayground-bucket1/output/daily/", "compression": "gzip", "partitionKeys": []}"""), transformationContext = "DataSink0", format = "csv").writeDynamicFrame(Transform8)
    Job.commit()
  }
}
object FlattenJson {
  def execute(glueContext : GlueContext, input : Seq[DynamicFrame]) : Seq[DynamicFrame] = {
    val spark = glueContext.getSparkSession
    // get input df
    val df = input(0).toDF()
    
    import com.github.wnameless.json.flattener.JsonFlattener
    import org.apache.spark.sql.{DataFrame, SparkSession}
    import org.apache.spark.sql.types.{StructField, StructType}
    
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
    
    Seq(DynamicFrame(renamedDf1, glueContext))
  }
}
object EnrichData {
  def execute(glueContext : GlueContext, input : Seq[DynamicFrame]) : Seq[DynamicFrame] = {
    val spark = glueContext.getSparkSession
    // get input df
    val df = input(0).toDF()
    
    import com.mongodb.spark.config.ReadConfig
    import com.mongodb.spark.sql.toSparkSessionFunctions
    import org.apache.spark.sql.{DataFrame, SparkSession}
    
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
    
    Seq(DynamicFrame(joinedDf, glueContext))
  }
}
object EpochToTimestamp {
  def execute(glueContext : GlueContext, input : Seq[DynamicFrame]) : Seq[DynamicFrame] = {
    val spark = glueContext.getSparkSession
    // get input df
    val df = input(0).toDF()
    
    import org.apache.spark.sql.functions.{col, from_unixtime}
    import org.apache.spark.sql.{DataFrame, SparkSession}
    
		spark.conf.set("spark.sql.session.timeZone", "GMT")

		var newDf = df
		for(column <- df.schema.fields) {
			if(column.name.endsWith("dt") || column.name.endsWith("rise") || column.name.endsWith("set")) {
				newDf = newDf.withColumn(column.name, from_unixtime(col(column.name), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
			}
		}

		spark.conf.unset("spark.sql.session.timeZone")
    		
    Seq(DynamicFrame(newDf, glueContext))
  }
}
object FillMissingValues {
  def execute(glueContext : GlueContext, input : Seq[DynamicFrame]) : Seq[DynamicFrame] = {
    val spark = glueContext.getSparkSession
    // get input df
    val df = input(0).toDF()
    
  	var columnNames: Array[String] = Array()
  	for(column <- df.schema.fields) {
  		if(column.name.contains("_rain")) {
  			columnNames :+= column.name
  		}
  	}
  	
  	val filledDf = df.na.fill(0, columnNames)
    
    Seq(DynamicFrame(filledDf, glueContext))
  }
}
object ExtractHourly {
  def execute(glueContext : GlueContext, input : Seq[DynamicFrame]) : Seq[DynamicFrame] = {
    val spark = glueContext.getSparkSession
    // get input df
    val df = input(0).toDF()
    
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.{DataFrame, SparkSession}
    
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
						f.endsWith("rain_1h") ||
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
			.withColumnRenamed("forecast_hourly_0_rain_1h", "rain")
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
							s"forecast_hourly_${hourlyIterator}_rain_1h",
							s"forecast_hourly_${hourlyIterator}_temp"
						))
				}
				hourlyIterator += 1
			}
		}
    
    Seq(DynamicFrame(mergedHourlyDf, glueContext))
  }
}
object RepartitionHourly {
  def execute(glueContext : GlueContext, input : Seq[DynamicFrame]) : Seq[DynamicFrame] = {
    val spark = glueContext.getSparkSession
    // get input df
    val df = input(0).toDF()
    
    // repartition the final df based on row count
  	val rowsPerPartition = 2000
  	val partitions = math.ceil(df.count().toFloat / rowsPerPartition.toFloat).toInt
  	val resultDf = df.coalesce(partitions)  // use coalesce instead of repartition for reducing partitions
  	println(s"Hourly no. rows: ${resultDf.count()}")
  	println(s"Hourly no partitions: ${resultDf.rdd.getNumPartitions}")
    	
    Seq(DynamicFrame(resultDf, glueContext))
  }
}
object ExtractDaily {
  def execute(glueContext : GlueContext, input : Seq[DynamicFrame]) : Seq[DynamicFrame] = {
    val spark = glueContext.getSparkSession
    // get input df
    val df = input(0).toDF()
    
    import org.apache.spark.sql.functions.col
    import org.apache.spark.sql.{DataFrame, SparkSession}
    
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
    
    Seq(DynamicFrame(mergedDailyDf, glueContext))
  }
}
object RepartitionDaily {
  def execute(glueContext : GlueContext, input : Seq[DynamicFrame]) : Seq[DynamicFrame] = {
    val spark = glueContext.getSparkSession
    // get input df
    val df = input(0).toDF()
    
    // repartition the final df based on row count
  	val rowsPerPartition = 2000
  	val partitions = math.ceil(df.count().toFloat / rowsPerPartition.toFloat).toInt
  	val resultDf = df.coalesce(partitions)  // use coalesce instead of repartition for reducing partitions
  	println(s"Daily no. rows: ${resultDf.count()}")
  	println(s"Daily no. partitions: ${resultDf.rdd.getNumPartitions}")
    	
    Seq(DynamicFrame(resultDf, glueContext))
  }
}