import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object CleanTaxi {
  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.grouped(2).collect {
      case Array(key, value) if key.startsWith("--") => key.drop(2) -> value
    }.toMap
  }

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    val inputPath = params.getOrElse(
      "input",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/raw/taxi/yellow_tripdata_2024-01.parquet"
    )
    val outputPath = params.getOrElse(
      "output",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/etl/taxi_clean"
    )

    val spark = SparkSession.builder()
      .appName("CleanTaxi")
      .getOrCreate()

    val rawDf = spark.read.parquet(inputPath)

    println(s"[INFO] Taxi ETL input: $inputPath")
    println(s"[INFO] Taxi ETL output: $outputPath")
    println("[STEP] Raw schema")
    rawDf.printSchema()

    // Correction over earlier drafts: restrict to the study window before downstream aggregation
    // so stale records do not leak into January 2024 demand features.
    val cleanedDf = rawDf
      .select(
        col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        col("PULocationID").cast(IntegerType).alias("PULocationID"),
        col("DOLocationID").cast(IntegerType).alias("DOLocationID"),
        col("passenger_count").cast(IntegerType).alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        col("RatecodeID").cast(IntegerType).alias("RatecodeID"),
        col("payment_type").cast(IntegerType).alias("payment_type"),
        lower(trim(col("store_and_fwd_flag"))).alias("store_and_fwd_flag_normalized"),
        col("fare_amount").cast("double").alias("fare_amount"),
        col("total_amount").cast("double").alias("total_amount")
      )
      .where(col("pickup_ts").isNotNull)
      .where(col("PULocationID").isNotNull && col("PULocationID") > 0)
      .where(col("DOLocationID").isNotNull && col("DOLocationID") > 0)
      .where(col("passenger_count").isNotNull && col("passenger_count") >= 0)
      .where(col("trip_distance").isNotNull && col("trip_distance") > 0.0)
      .where(col("fare_amount").isNotNull && col("fare_amount") >= 0.0)
      .where(col("total_amount").isNotNull && col("total_amount") >= 0.0)
      .where(to_date(col("pickup_ts")).between(lit("2024-01-01"), lit("2024-01-31")))
      // Cleaning options performed for the project rubric:
      // 1. Date formatting via pickup_date and pickup_hour
      // 2. Text normalization via lower(trim(store_and_fwd_flag))
      // 3. Binary feature creation via is_long_trip and is_high_fare
      .withColumn("pickup_date", to_date(col("pickup_ts")))
      .withColumn("pickup_hour", hour(col("pickup_ts")))
      .withColumn("pickup_day_of_week", dayofweek(col("pickup_ts")))
      .withColumn("is_long_trip", when(col("trip_distance") >= 5.0, 1).otherwise(0))
      .withColumn("is_high_fare", when(col("fare_amount") >= 20.0, 1).otherwise(0))
      .drop("pickup_ts")
      .select(
        "pickup_date",
        "pickup_hour",
        "pickup_day_of_week",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "payment_type",
        "store_and_fwd_flag_normalized",
        "fare_amount",
        "total_amount",
        "is_long_trip",
        "is_high_fare"
      )

    println("[STEP] Cleaned schema")
    cleanedDf.printSchema()
    println(s"raw_record_count=${rawDf.count()}")
    println(s"clean_record_count=${cleanedDf.count()}")
    cleanedDf.show(20, truncate = false)

    cleanedDf.write
      .mode("overwrite")
      .partitionBy("pickup_date")
      .parquet(outputPath)

    println(s"[DONE] Taxi cleaned output written to $outputPath")
    spark.stop()
  }
}
