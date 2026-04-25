# Test Code Notes

No separate automated test suite is bundled in this package.

Recommended validation checks on Dataproc:

- verify the raw HDFS upload listings before running profiling
- verify the cleaned taxi and weather parquet outputs exist after ETL
- verify the analytics outputs exist after the final run
- inspect the metrics CSV and confirm both baseline and weather-aware rows are present
- confirm the joined feature table contains both taxi and weather columns

Manual smoke-check commands are included in the root `README.md`.
