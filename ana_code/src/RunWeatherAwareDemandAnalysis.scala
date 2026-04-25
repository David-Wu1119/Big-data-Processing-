import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class MetricRow(
  model_name: String,
  mae: Double,
  rmse: Double,
  mape: Double,
  test_rows: Long,
  train_end_date: String
)

object RunWeatherAwareDemandAnalysis {
  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.grouped(2).collect {
      case Array(key, value) if key.startsWith("--") => key.drop(2) -> value
    }.toMap
  }

  private def requireColumns(df: DataFrame, required: Seq[String], dfName: String): Unit = {
    val missing = required.filterNot(df.columns.contains)
    require(missing.isEmpty, s"$dfName is missing required columns: ${missing.mkString(", ")}")
  }

  private def metricValues(predictions: DataFrame, predictionCol: String, labelCol: String): (Double, Double, Double, Long) = {
    val usable = predictions
      .where(col(predictionCol).isNotNull && col(labelCol).isNotNull)
      .cache()

    val rowCount = usable.count()
    require(rowCount > 0, s"No usable rows found for metrics on prediction=$predictionCol label=$labelCol")

    val metricRow = usable
      .agg(
        avg(abs(col(predictionCol) - col(labelCol))).alias("mae"),
        sqrt(avg(pow(col(predictionCol) - col(labelCol), 2))).alias("rmse"),
        avg(
          when(col(labelCol) > 0.0, abs((col(predictionCol) - col(labelCol)) / col(labelCol)))
        ).alias("mape")
      )
      .first()

    val mae = metricRow.getAs[Double]("mae")
    val rmse = metricRow.getAs[Double]("rmse")
    val mapeIndex = metricRow.fieldIndex("mape")
    val mape =
      if (metricRow.isNullAt(mapeIndex)) Double.NaN
      else metricRow.getDouble(mapeIndex)

    usable.unpersist()
    (mae, rmse, mape, rowCount)
  }

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)

    val taxiInput = params.getOrElse(
      "taxi-input",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/etl/taxi_clean"
    )
    val weatherInput = params.getOrElse(
      "weather-input",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/etl/weather_clean"
    )
    val featuresOutput = params.getOrElse(
      "features-output",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/analytics/features"
    )
    val baselineOutput = params.getOrElse(
      "baseline-output",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/analytics/baseline_predictions"
    )
    val modelOutput = params.getOrElse(
      "model-output",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/analytics/weather_aware_predictions"
    )
    val metricsOutput = params.getOrElse(
      "metrics-output",
      "hdfs:///user/yw7566_nyu_edu/final_code_drop/analytics/metrics"
    )
    val trainEndDate = params.getOrElse("train-end-date", "2024-01-24")

    val spark = SparkSession.builder()
      .appName("RunWeatherAwareDemandAnalysis")
      .getOrCreate()

    import spark.implicits._

    println(s"[INFO] taxiInput=$taxiInput")
    println(s"[INFO] weatherInput=$weatherInput")
    println(s"[INFO] featuresOutput=$featuresOutput")
    println(s"[INFO] baselineOutput=$baselineOutput")
    println(s"[INFO] modelOutput=$modelOutput")
    println(s"[INFO] metricsOutput=$metricsOutput")
    println(s"[INFO] trainEndDate=$trainEndDate")

    val taxi = spark.read.parquet(taxiInput)
    val weather = spark.read.parquet(weatherInput)

    requireColumns(
      taxi,
      Seq("pickup_date", "pickup_hour", "PULocationID", "trip_distance", "fare_amount"),
      "taxi"
    )
    requireColumns(
      weather,
      Seq("observation_date_local", "observation_hour_local", "station_id", "air_temp_c", "wind_speed_mps", "precipitation_1hr_mm", "is_rain_1hr"),
      "weather"
    )

    val taxiHourly = taxi
      .withColumn("pickup_date", col("pickup_date").cast("date"))
      .groupBy("pickup_date", "pickup_hour", "PULocationID")
      .agg(
        count(lit(1)).alias("trip_count"),
        avg("trip_distance").alias("avg_trip_distance"),
        avg("fare_amount").alias("avg_fare_amount")
      )

    val weatherHourly = weather
      .withColumn("observation_date_local", col("observation_date_local").cast("date"))
      .groupBy("observation_date_local", "observation_hour_local")
      .agg(
        avg("air_temp_c").alias("avg_temp_c"),
        avg("wind_speed_mps").alias("avg_wind_speed_mps"),
        avg("precipitation_1hr_mm").alias("avg_precipitation_1hr_mm"),
        max("is_rain_1hr").alias("is_rain_hour"),
        countDistinct("station_id").alias("station_count")
      )

    // Keep a strong historical baseline in the same pipeline so the weather-aware model
    // is judged against a real benchmark instead of against intuition.
    val features = taxiHourly
      .join(
        weatherHourly,
        taxiHourly("pickup_date") === weatherHourly("observation_date_local") &&
          taxiHourly("pickup_hour") === weatherHourly("observation_hour_local"),
        "left"
      )
      .drop("observation_date_local", "observation_hour_local")
      .withColumn("pickup_date", col("pickup_date").cast("date"))
      .withColumn("day_of_week", dayofweek(col("pickup_date")))
      .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0))
      .na.fill(
        Map(
          "avg_temp_c" -> 0.0,
          "avg_wind_speed_mps" -> 0.0,
          "avg_precipitation_1hr_mm" -> 0.0,
          "is_rain_hour" -> 0,
          "station_count" -> 0L,
          "avg_trip_distance" -> 0.0,
          "avg_fare_amount" -> 0.0
        )
      )
      .cache()

    println(s"[INFO] joined_feature_rows=${features.count()}")
    features.show(20, truncate = false)

    features.write.mode("overwrite").parquet(featuresOutput)

    val train = features.where(col("pickup_date") <= lit(trainEndDate).cast("date")).cache()
    val test = features.where(col("pickup_date") > lit(trainEndDate).cast("date")).cache()

    val trainCount = train.count()
    val testCount = test.count()
    require(trainCount > 0, s"No training rows found on or before $trainEndDate")
    require(testCount > 0, s"No test rows found after $trainEndDate")

    println(s"[INFO] trainCount=$trainCount")
    println(s"[INFO] testCount=$testCount")

    val trainGlobalMean = train.agg(avg("trip_count")).first().getDouble(0)

    val baselineLookup = train
      .groupBy("PULocationID", "pickup_hour")
      .agg(avg("trip_count").alias("baseline_prediction"))

    val baselinePredictions = test
      .join(baselineLookup, Seq("PULocationID", "pickup_hour"), "left")
      .withColumn("prediction", coalesce(col("baseline_prediction"), lit(trainGlobalMean)))
      .select(
        col("pickup_date"),
        col("pickup_hour"),
        col("PULocationID"),
        col("trip_count").alias("label"),
        col("prediction")
      )

    val (baselineMae, baselineRmse, baselineMape, baselineRows) =
      metricValues(baselinePredictions, "prediction", "label")

    baselinePredictions.write.mode("overwrite").parquet(baselineOutput)

    val zoneIndexer = new StringIndexer()
      .setInputCol("PULocationID")
      .setOutputCol("PULocationID_indexed")
      .setHandleInvalid("keep")

    val zoneEncoder = new OneHotEncoder()
      .setInputCol("PULocationID_indexed")
      .setOutputCol("PULocationID_ohe")
      .setHandleInvalid("keep")

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "PULocationID_ohe",
        "pickup_hour",
        "day_of_week",
        "is_weekend",
        "avg_temp_c",
        "avg_wind_speed_mps",
        "avg_precipitation_1hr_mm",
        "is_rain_hour",
        "station_count",
        "avg_trip_distance",
        "avg_fare_amount"
      ))
      .setOutputCol("features")

    val regression = new LinearRegression()
      .setLabelCol("trip_count")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setMaxIter(50)
      .setRegParam(0.1)
      .setElasticNetParam(0.1)

    val pipeline = new Pipeline().setStages(Array(zoneIndexer, zoneEncoder, assembler, regression))
    val model = pipeline.fit(train)

    val modelPredictions = model.transform(test)
      .select(
        col("pickup_date"),
        col("pickup_hour"),
        col("PULocationID"),
        col("trip_count").alias("label"),
        col("prediction"),
        col("avg_temp_c"),
        col("avg_wind_speed_mps"),
        col("avg_precipitation_1hr_mm"),
        col("is_rain_hour")
      )

    val (modelMae, modelRmse, modelMape, modelRows) =
      metricValues(modelPredictions, "prediction", "label")

    modelPredictions.write.mode("overwrite").parquet(modelOutput)

    Seq(
      MetricRow("baseline_zone_hour_mean", baselineMae, baselineRmse, baselineMape, baselineRows, trainEndDate),
      MetricRow("weather_aware_linear_regression", modelMae, modelRmse, modelMape, modelRows, trainEndDate)
    ).toDS()
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(metricsOutput)

    println(s"[RESULT] baseline_mae=$baselineMae baseline_rmse=$baselineRmse baseline_mape=$baselineMape")
    println(s"[RESULT] weather_model_mae=$modelMae weather_model_rmse=$modelRmse weather_model_mape=$modelMape")

    train.unpersist()
    test.unpersist()
    features.unpersist()
    spark.stop()
  }
}
