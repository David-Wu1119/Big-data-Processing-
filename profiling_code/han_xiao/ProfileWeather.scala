import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProfileWeather {
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

    val spark = SparkSession.builder()
      .appName("ProfileWeather")
      .getOrCreate()

    val raw = spark.read.text(inputPath)

    println(s"[INFO] Weather profiling input: $inputPath")
    println("[STEP] Raw line count")
    println(s"raw_line_count=${raw.count()}")

    val parsed = raw
      .withColumn("source_path", input_file_name())
      .withColumn("parts", split(trim(col("value")), "\\s+"))
      .where(size(col("parts")) >= 12)
      .select(
        regexp_extract(col("source_path"), "([0-9]{6}-[0-9]{5}-[0-9]{4})", 1).alias("station_id"),
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
      .withColumn("air_temp_c", when(col("air_temp_tenths_c") =!= -9999, col("air_temp_tenths_c") / 10.0))
      .withColumn("wind_speed_mps", when(col("wind_speed_tenths_mps") =!= -9999, col("wind_speed_tenths_mps") / 10.0))
      .withColumn(
        "precipitation_1hr_mm",
        when(col("precipitation_1hr_tenths_mm") === -9999, lit(null))
          .when(col("precipitation_1hr_tenths_mm") === -1, lit(0.1))
          .otherwise(col("precipitation_1hr_tenths_mm") / 10.0)
      )

    println("[STEP] Parsed schema")
    parsed.printSchema()

    println("[STEP] Distinct stations")
    parsed.select("station_id").distinct().orderBy("station_id").show(50, truncate = false)

    println("[STEP] Distinct wind directions")
    parsed.select("wind_direction_degrees")
      .where(col("wind_direction_degrees").isNotNull && col("wind_direction_degrees") =!= -9999)
      .distinct()
      .orderBy("wind_direction_degrees")
      .show(50, truncate = false)

    println("[STEP] Distinct sky cover codes")
    parsed.select("sky_cover_code")
      .where(col("sky_cover_code").isNotNull && col("sky_cover_code") =!= -9999)
      .distinct()
      .orderBy("sky_cover_code")
      .show(50, truncate = false)

    Seq("air_temp_c", "wind_speed_mps", "precipitation_1hr_mm").foreach { columnName =>
      println(s"[STEP] Numeric stats for $columnName")
      val cleaned = parsed.where(col(columnName).isNotNull)
      cleaned.agg(
        count(col(columnName)).alias("non_null_count"),
        avg(col(columnName)).alias("mean_value"),
        expr(s"percentile_approx($columnName, 0.5)").alias("median_value"),
        stddev(col(columnName)).alias("stddev_value"),
        min(col(columnName)).alias("min_value"),
        max(col(columnName)).alias("max_value")
      ).show(false)

      println(s"[STEP] Mode for $columnName")
      cleaned.groupBy(col(columnName))
        .count()
        .orderBy(desc("count"), asc(columnName))
        .show(10, truncate = false)
    }

    println("[STEP] Sample parsed rows")
    parsed.show(20, truncate = false)

    spark.stop()
  }
}
