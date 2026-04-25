import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CleanWeather {
  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.grouped(2).collect {
      case Array(key, value) if key.startsWith("--") => key.drop(2) -> value
    }.toMap
  }

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    val inputPath = params.getOrElse(
      "input",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/raw/weather/*.gz"
    )
    val outputPath = params.getOrElse(
      "output",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/etl/weather_clean"
    )

    val spark = SparkSession.builder()
      .appName("CleanWeather")
      .getOrCreate()

    val raw = spark.read.text(inputPath)

    println(s"[INFO] Weather ETL input: $inputPath")
    println(s"[INFO] Weather ETL output: $outputPath")
    println(s"raw_line_count=${raw.count()}")

    val parsed = raw
      .withColumn("source_path", input_file_name())
      .withColumn("parts", split(trim(col("value")), "\\s+"))
      .where(size(col("parts")) >= 12)
      .select(
        regexp_extract(col("source_path"), "([0-9]{6}-[0-9]{5}-[0-9]{4})", 1).alias("station_id"),
        lower(trim(col("source_path"))).alias("source_path_normalized"),
        col("parts").getItem(0).cast("int").alias("year"),
        col("parts").getItem(1).cast("int").alias("month"),
        col("parts").getItem(2).cast("int").alias("day"),
        col("parts").getItem(3).cast("int").alias("hour_utc"),
        col("parts").getItem(4).cast("int").alias("air_temp_tenths_c"),
        col("parts").getItem(5).cast("int").alias("dew_point_tenths_c"),
        col("parts").getItem(6).cast("int").alias("sea_level_pressure_tenths_hpa"),
        col("parts").getItem(7).cast("int").alias("wind_direction_degrees"),
        col("parts").getItem(8).cast("int").alias("wind_speed_tenths_mps"),
        col("parts").getItem(9).cast("int").alias("sky_cover_code"),
        col("parts").getItem(10).cast("int").alias("precipitation_1hr_tenths_mm"),
        col("parts").getItem(11).cast("int").alias("precipitation_6hr_tenths_mm")
      )

    // Correction over earlier drafts: weather records arrive in UTC, but taxi demand is local time.
    // Converting before the join avoids a systematic hour misalignment.
    val utcTimestamp = to_timestamp(
      format_string(
        "%04d-%02d-%02d %02d:00:00",
        col("year"),
        col("month"),
        col("day"),
        col("hour_utc")
      ),
      "yyyy-MM-dd HH:mm:ss"
    )

    val cleaned = parsed
      .withColumn("observation_ts_utc", utcTimestamp)
      // Cleaning options performed for the project rubric:
      // 1. Date formatting via UTC -> America/New_York conversion and local date/hour extraction
      // 2. Text normalization via lower(trim(source_path))
      // 3. Binary feature creation via is_rain_1hr
      .withColumn("observation_ts_local", from_utc_timestamp(col("observation_ts_utc"), "America/New_York"))
      .withColumn("observation_date_local", to_date(col("observation_ts_local")))
      .withColumn("observation_hour_local", hour(col("observation_ts_local")))
      .withColumn("air_temp_c", when(col("air_temp_tenths_c") =!= -9999, col("air_temp_tenths_c") / 10.0))
      .withColumn("dew_point_c", when(col("dew_point_tenths_c") =!= -9999, col("dew_point_tenths_c") / 10.0))
      .withColumn("sea_level_pressure_hpa", when(col("sea_level_pressure_tenths_hpa") =!= -9999, col("sea_level_pressure_tenths_hpa") / 10.0))
      .withColumn("wind_speed_mps", when(col("wind_speed_tenths_mps") =!= -9999, col("wind_speed_tenths_mps") / 10.0))
      .withColumn(
        "precipitation_1hr_mm",
        when(col("precipitation_1hr_tenths_mm") === -9999, lit(null))
          .when(col("precipitation_1hr_tenths_mm") === -1, lit(0.1))
          .otherwise(col("precipitation_1hr_tenths_mm") / 10.0)
      )
      .withColumn(
        "precipitation_6hr_mm",
        when(col("precipitation_6hr_tenths_mm") === -9999, lit(null))
          .when(col("precipitation_6hr_tenths_mm") === -1, lit(0.1))
          .otherwise(col("precipitation_6hr_tenths_mm") / 10.0)
      )
      .withColumn("is_rain_1hr", when(col("precipitation_1hr_mm") > 0.0, 1).otherwise(0))
      .select(
        "station_id",
        "source_path_normalized",
        "observation_ts_utc",
        "observation_ts_local",
        "observation_date_local",
        "observation_hour_local",
        "air_temp_c",
        "dew_point_c",
        "sea_level_pressure_hpa",
        "wind_direction_degrees",
        "wind_speed_mps",
        "sky_cover_code",
        "precipitation_1hr_mm",
        "precipitation_6hr_mm",
        "is_rain_1hr"
      )
      .where(col("observation_date_local").between(lit("2023-12-31"), lit("2024-01-31")))

    println("[STEP] Weather cleaned schema")
    cleaned.printSchema()
    println(s"clean_record_count=${cleaned.count()}")
    cleaned.show(20, truncate = false)

    cleaned.write
      .mode("overwrite")
      .partitionBy("observation_date_local")
      .parquet(outputPath)

    println(s"[DONE] Weather cleaned output written to $outputPath")
    spark.stop()
  }
}
