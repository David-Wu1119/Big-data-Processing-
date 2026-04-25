# Scalability And Stress Testing

## Distributed execution model

The project is designed to run on NYU Dataproc using Spark and HDFS. The pipeline is split into stages:

1. raw data ingestion into HDFS
2. raw profiling on distributed inputs
3. ETL and cleaning to partitioned parquet outputs
4. final feature synthesis and model evaluation

That separation matters for scalability. Each stage can be rerun independently, and expensive parsing work is not repeated unnecessarily once cleaned parquet exists.

## Scalability choices already used

### Column pruning and early filtering

Both ETL jobs aggressively select only the columns needed for downstream analytics and drop invalid rows before writing the cleaned outputs. That reduces downstream scan width and avoids carrying unusable rows into feature synthesis.

### Parquet outputs

The ETL outputs and analytics outputs use parquet. That is the correct storage choice here because:

- it reduces storage footprint relative to plain text
- Spark can read it efficiently
- schema is preserved
- downstream scans can use columnar reads

### Partitioning by date

The cleaned taxi and weather datasets are written partitioned by date:

- taxi: `pickup_date`
- weather: `observation_date_local`

That supports bounded date-range scans and keeps the directory structure aligned with the forecasting window.

### Explicit feature materialization

The joined taxi-weather feature table is written to HDFS before final result interpretation. This avoids treating feature synthesis as an opaque transient step and makes debugging and reuse easier.

### Caching where it matters

The analytics stage caches:

- the joined feature table
- the train split
- the test split

That is a reasonable tradeoff because those datasets are reused in multiple downstream computations:

- baseline generation
- model fitting
- metric computation

## What was stress-tested

The pipeline was validated on the full January 2024 taxi parquet input and the selected NOAA weather files on Dataproc without the job crashing. The outputs were written successfully to:

- `raw/`
- `etl/`
- `analytics/features`
- `analytics/baseline_predictions`
- `analytics/weather_aware_predictions`
- `analytics/metrics`

That demonstrates the pipeline is operational on the assigned infrastructure.

## Current scalability limits

The current implementation is functional, but not maximally optimized.

### Join granularity

Weather is aggregated at hour level and then joined to taxi demand aggregated by zone-hour. That is manageable for this project, but the taxi side still creates a large number of rows when zone cardinality is high.

### Model choice

The weather-aware model uses a single Spark ML linear regression with one-hot encoded pickup zone. This is computationally acceptable for the current project, but wider feature sets or longer time windows would increase memory pressure and feature dimensionality.

### File counts

The analytics outputs currently generate many parquet part files. That is acceptable for course work, but in production a post-write compaction step would likely be added for easier downstream consumption.

## Optimizations that would be used next

If the dataset or time window grows, the next engineering improvements would be:

- repartition the feature table explicitly by date or zone before heavy writes
- compact small parquet files after ETL and analytics writes
- test broadcast behavior only where one side is demonstrably small
- add lagged weather features in a way that avoids repeated self-joins
- persist selected intermediate tables with a storage level chosen from observed executor memory pressure

## Bottom line

This project meets the scalability bar for the course because it runs end-to-end on Dataproc, uses HDFS and Spark correctly, writes partitioned parquet outputs, and completes without crashing on the project data. It does not claim full production hardening, but the current implementation demonstrates the expected distributed-computing competence.
