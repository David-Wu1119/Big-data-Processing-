#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build/classes"
DIST_DIR="${SCRIPT_DIR}/dist"
JAR_PATH="${DIST_DIR}/weather_taxi_final_code_drop.jar"

SPARK_HOME="${SPARK_HOME:-/usr/lib/spark}"
SCALA_BIN="${SCALA_BIN:-scala}"

mkdir -p "$BUILD_DIR" "$DIST_DIR"
rm -rf "${BUILD_DIR:?}"/*
rm -f "$JAR_PATH"

SPARK_CP=$(printf "%s:" "$SPARK_HOME"/jars/*.jar)
SPARK_CP="${SPARK_CP%:}"

SOURCES=(
  "${ROOT_DIR}/profiling_code/yanze_wu/ProfileTaxi.scala"
  "${ROOT_DIR}/profiling_code/han_xiao/ProfileWeather.scala"
  "${ROOT_DIR}/etl_code/yanze_wu/CleanTaxi.scala"
  "${ROOT_DIR}/etl_code/han_xiao/CleanWeather.scala"
  "${SCRIPT_DIR}/src/RunWeatherAwareDemandAnalysis.scala"
)

for source_file in "${SOURCES[@]}"; do
  if [ ! -f "$source_file" ]; then
    echo "[ERROR] Missing source file: $source_file" >&2
    exit 1
  fi
done

echo "[INFO] Compiling Scala sources with Spark classpath"
if command -v scalac >/dev/null 2>&1; then
  scalac -classpath "$SPARK_CP" -d "$BUILD_DIR" "${SOURCES[@]}"
else
  "$SCALA_BIN" scala.tools.nsc.Main \
    -usejavacp \
    -classpath "$SPARK_CP" \
    -d "$BUILD_DIR" \
    "${SOURCES[@]}"
fi

echo "[INFO] Packaging jar at $JAR_PATH"
jar cf "$JAR_PATH" -C "$BUILD_DIR" .

echo "[DONE] Built analytics jar: $JAR_PATH"
