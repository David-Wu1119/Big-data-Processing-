# HDFS Paths

These are the default HDFS locations used by the final code drop scripts.

Base path:

- `/user/<your_netid>/final_code_drop`

Raw inputs:

- Taxi raw:
  `/user/<your_netid>/final_code_drop/raw/taxi/yellow_tripdata_2024-01.parquet`
- Weather raw:
  `/user/<your_netid>/final_code_drop/raw/weather/*.gz`

ETL outputs:

- Taxi cleaned:
  `/user/<your_netid>/final_code_drop/etl/taxi_clean`
- Weather cleaned:
  `/user/<your_netid>/final_code_drop/etl/weather_clean`

Analytics outputs:

- Joined feature table:
  `/user/<your_netid>/final_code_drop/analytics/features`
- Baseline predictions:
  `/user/<your_netid>/final_code_drop/analytics/baseline_predictions`
- Weather-aware predictions:
  `/user/<your_netid>/final_code_drop/analytics/weather_aware_predictions`
- Metrics:
  `/user/<your_netid>/final_code_drop/analytics/metrics`
