import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProfileTaxi {
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

    val spark = SparkSession.builder()
      .appName("ProfileTaxi")
      .getOrCreate()

    val df = spark.read.parquet(inputPath)

    println(s"[INFO] Taxi profiling input: $inputPath")
    println("[STEP] Schema")
    df.printSchema()

    println("[STEP] Record count")
    println(s"raw_record_count=${df.count()}")

    println("[STEP] Record count via map/reduce")
    df.rdd.map(_ => ("record_count", 1L))
      .reduceByKey(_ + _)
      .collect()
      .foreach { case (k, v) => println(s"$k=$v") }

    Seq("VendorID", "RatecodeID", "payment_type", "store_and_fwd_flag").foreach { columnName =>
      println(s"[STEP] Distinct values for $columnName")
      df.select(col(columnName))
        .where(col(columnName).isNotNull)
        .distinct()
        .orderBy(col(columnName))
        .show(30, truncate = false)
    }

    println("[STEP] Null counts for analytic columns")
    df.select(
      sum(when(col("tpep_pickup_datetime").isNull, 1).otherwise(0)).alias("null_pickup_ts"),
      sum(when(col("PULocationID").isNull, 1).otherwise(0)).alias("null_pu_location"),
      sum(when(col("DOLocationID").isNull, 1).otherwise(0)).alias("null_do_location"),
      sum(when(col("trip_distance").isNull, 1).otherwise(0)).alias("null_trip_distance"),
      sum(when(col("fare_amount").isNull, 1).otherwise(0)).alias("null_fare_amount"),
      sum(when(col("total_amount").isNull, 1).otherwise(0)).alias("null_total_amount")
    ).show(false)

    println("[STEP] Numeric profiling")
    df.select(
      min(col("trip_distance")).alias("min_trip_distance"),
      max(col("trip_distance")).alias("max_trip_distance"),
      avg(col("trip_distance")).alias("avg_trip_distance"),
      min(col("fare_amount")).alias("min_fare_amount"),
      max(col("fare_amount")).alias("max_fare_amount"),
      avg(col("fare_amount")).alias("avg_fare_amount"),
      min(col("total_amount")).alias("min_total_amount"),
      max(col("total_amount")).alias("max_total_amount"),
      avg(col("total_amount")).alias("avg_total_amount")
    ).show(false)

    println("[STEP] Sample rows")
    df.select(
      "tpep_pickup_datetime",
      "PULocationID",
      "DOLocationID",
      "passenger_count",
      "trip_distance",
      "RatecodeID",
      "payment_type",
      "fare_amount",
      "total_amount"
    ).show(20, truncate = false)

    spark.stop()
  }
}
