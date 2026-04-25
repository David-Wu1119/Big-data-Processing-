#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
HDFS_USER="${HDFS_USER:-$(whoami)}"
HDFS_BASE="/user/${HDFS_USER}/final_code_drop"
JAR_PATH="${ROOT_DIR}/ana_code/dist/weather_taxi_final_code_drop.jar"

bash "${ROOT_DIR}/data_ingest/upload_local_data_to_hdfs.sh"
bash "${ROOT_DIR}/ana_code/build_and_package.sh"

spark-submit --class ProfileTaxi "$JAR_PATH" \
  --input "hdfs://${HDFS_BASE}/raw/taxi/yellow_tripdata_2024-01.parquet"

spark-submit --class ProfileWeather "$JAR_PATH" \
  --input "hdfs://${HDFS_BASE}/raw/weather/*.gz"

spark-submit --class CleanTaxi "$JAR_PATH" \
  --input "hdfs://${HDFS_BASE}/raw/taxi/yellow_tripdata_2024-01.parquet" \
  --output "hdfs://${HDFS_BASE}/etl/taxi_clean"

spark-submit --class CleanWeather "$JAR_PATH" \
  --input "hdfs://${HDFS_BASE}/raw/weather/*.gz" \
  --output "hdfs://${HDFS_BASE}/etl/weather_clean"

bash "${ROOT_DIR}/ana_code/run_analytics.sh"

echo "[INFO] Final HDFS outputs"
hdfs dfs -ls "${HDFS_BASE}/etl/taxi_clean"
hdfs dfs -ls "${HDFS_BASE}/etl/weather_clean"
hdfs dfs -ls "${HDFS_BASE}/analytics/features"
hdfs dfs -ls "${HDFS_BASE}/analytics/baseline_predictions"
hdfs dfs -ls "${HDFS_BASE}/analytics/weather_aware_predictions"
hdfs dfs -ls "${HDFS_BASE}/analytics/metrics"
