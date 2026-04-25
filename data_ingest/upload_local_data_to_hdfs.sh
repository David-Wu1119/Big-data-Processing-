#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

HDFS_USER="${HDFS_USER:-$(whoami)}"
HDFS_BASE="/user/${HDFS_USER}/final_code_drop"
RAW_TAXI_DIR="${HDFS_BASE}/raw/taxi"
RAW_WEATHER_DIR="${HDFS_BASE}/raw/weather"

LOCAL_TAXI_FILE="${ROOT_DIR}/data_ingest/local_data/taxi/yellow_tripdata_2024-01.parquet"
LOCAL_WEATHER_GLOB="${ROOT_DIR}/data_ingest/local_data/weather"/*.gz

if [ ! -f "$LOCAL_TAXI_FILE" ]; then
  echo "[ERROR] Missing local taxi file: $LOCAL_TAXI_FILE" >&2
  exit 1
fi

if ! ls $LOCAL_WEATHER_GLOB >/dev/null 2>&1; then
  echo "[ERROR] Missing local weather .gz files under ${ROOT_DIR}/data_ingest/local_data/weather" >&2
  exit 1
fi

echo "[INFO] Creating HDFS raw directories"
hdfs dfs -mkdir -p "$RAW_TAXI_DIR"
hdfs dfs -mkdir -p "$RAW_WEATHER_DIR"

echo "[INFO] Uploading taxi parquet to ${RAW_TAXI_DIR}"
hdfs dfs -put -f "$LOCAL_TAXI_FILE" "$RAW_TAXI_DIR/"

echo "[INFO] Uploading NOAA weather files to ${RAW_WEATHER_DIR}"
hdfs dfs -put -f ${LOCAL_WEATHER_GLOB} "$RAW_WEATHER_DIR/"

echo "[INFO] Raw taxi HDFS listing"
hdfs dfs -ls "$RAW_TAXI_DIR"

echo "[INFO] Raw weather HDFS listing"
hdfs dfs -ls "$RAW_WEATHER_DIR"

echo "[DONE] Taxi raw HDFS path: hdfs://${RAW_TAXI_DIR}/yellow_tripdata_2024-01.parquet"
echo "[DONE] Weather raw HDFS path: hdfs://${RAW_WEATHER_DIR}"
