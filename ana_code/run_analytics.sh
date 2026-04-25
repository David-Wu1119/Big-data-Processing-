#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

HDFS_USER="${HDFS_USER:-$(whoami)}"
HDFS_BASE="/user/${HDFS_USER}/final_code_drop"

JAR_PATH="${SCRIPT_DIR}/dist/weather_taxi_final_code_drop.jar"

if [ ! -f "$JAR_PATH" ]; then
  echo "[ERROR] Missing analytics jar: $JAR_PATH" >&2
  echo "[ERROR] Run ana_code/build_and_package.sh first." >&2
  exit 1
fi

spark-submit \
  --class RunWeatherAwareDemandAnalysis \
  "$JAR_PATH" \
  --taxi-input "hdfs://${HDFS_BASE}/etl/taxi_clean" \
  --weather-input "hdfs://${HDFS_BASE}/etl/weather_clean" \
  --features-output "hdfs://${HDFS_BASE}/analytics/features" \
  --baseline-output "hdfs://${HDFS_BASE}/analytics/baseline_predictions" \
  --model-output "hdfs://${HDFS_BASE}/analytics/weather_aware_predictions" \
  --metrics-output "hdfs://${HDFS_BASE}/analytics/metrics" \
  --train-end-date "2024-01-24"
