# Weather-Aware Forecasting of NYC Yellow Taxi Demand

Author: Yanze Wu  
Contributor: Han Xiao

This repository contains the final code and supporting artifacts for a Spark Scala project that tests whether hourly weather observations improve short-horizon forecasting of NYC yellow taxi demand.

## Project summary

I built a distributed pipeline on NYU Dataproc that:

1. ingests NYC TLC yellow taxi parquet data and NOAA ISD-Lite weather files into HDFS
2. profiles both datasets before downstream modeling
3. cleans and normalizes taxi and weather records
4. converts NOAA timestamps from UTC to New York local time
5. aggregates taxi demand by pickup zone and hour
6. joins hourly weather features to the taxi demand target
7. compares a historical baseline against a weather-aware linear regression model

The main result is that the historical zone-hour baseline outperformed the weather-aware linear regression model on the held-out test window. That negative result is meaningful: it shows that adding external weather features does not automatically improve forecasting when the external signal is spatially coarse and the baseline is already strong.

## Repository structure

- `ana_code/`
  Final analytics source code, build script, run script, and output directories for class files and jar builds.
- `data_ingest/`
  Commands and local source files used to load taxi and weather data into HDFS.
- `etl_code/`
  Cleaning and ETL code, split by team member.
- `profiling_code/`
  Profiling code, split by team member.
- `screenshots/`
  Evidence of the pipeline running on Dataproc.
- `ANALYTICAL_FINDINGS.md`
  Interpretation of the final result.
- `ETHICS_AND_GOVERNANCE.md`
  Bias risks, governance limits, and alternative processing choices.
- `SCALABILITY_AND_STRESS_TESTING.md`
  Scalability notes for the Spark and HDFS pipeline.
- `TECHNICAL_AUDIT_AND_CORRECTIONS.md`
  Concrete corrections that improved correctness over time.

## Data sources

### 1. NYC TLC Yellow Taxi Trip Records

- Description: trip-level NYC taxi data used to construct zone-hour demand targets
- Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Local bundled file:
  `data_ingest/local_data/taxi/yellow_tripdata_2024-01.parquet`

### 2. NOAA ISD-Lite Hourly Weather Data

- Description: hourly weather observations used as external features
- Source directory: https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/2024/
- Local bundled files:
  - `data_ingest/local_data/weather/jfk_744860-94789-2024.gz`
  - `data_ingest/local_data/weather/lga_725030-14732-2024.gz`

## Final metrics

The Dataproc run used in the final submission produced these metrics on the held-out period:

| Model | MAE | RMSE | MAPE | Test Rows |
| --- | ---: | ---: | ---: | ---: |
| baseline_zone_hour_mean | 12.3361 | 28.5303 | 0.9193 | 15784 |
| weather_aware_linear_regression | 27.6084 | 46.9819 | 5.7551 | 15784 |

## How to build on Dataproc

```bash
cd ~/FINAL_CODE_DROP
chmod +x data_ingest/upload_local_data_to_hdfs.sh \
  ana_code/build_and_package.sh \
  ana_code/run_analytics.sh \
  run_all_on_dataproc.sh

bash ana_code/build_and_package.sh
```

Expected outputs:

- `ana_code/build/classes/`
- `ana_code/dist/weather_taxi_final_code_drop.jar`

## How to run on Dataproc

```bash
cd ~/FINAL_CODE_DROP
bash run_all_on_dataproc.sh | tee final_code_drop_run.log
```

## Key engineering challenge

The hardest correctness problem was aligning raw NOAA weather records with hourly NYC taxi demand. The weather files arrive as raw text with sentinel values and UTC timestamps. Taxi demand is analyzed by local date and local hour. If those timestamps are not converted into the same time zone before aggregation and joining, the analytic silently learns from mismatched hours.

## Notes

- This repo is designed for transparency and reproducibility, not for hiding a negative result.
- The public artifact for the writeup is the Medium post authored by Yanze Wu.
- The animation asset used in the Medium post compares the baseline and weather-aware model metrics.
