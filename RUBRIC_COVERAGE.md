# Rubric Coverage

This file maps the submission directly to the grading rubric.

## 1. Pipeline and Data Synthesis

Evidence:

- raw data ingest:
  - `data_ingest/upload_local_data_to_hdfs.sh`
  - `data_ingest/HDFS_PATHS.md`
- ETL:
  - `etl_code/yanze_wu/CleanTaxi.scala`
  - `etl_code/han_xiao/CleanWeather.scala`
- feature synthesis and final outputs:
  - `ana_code/src/RunWeatherAwareDemandAnalysis.scala`

Why this satisfies the rubric:

- ingests two large structured datasets
- performs cleaning and schema mapping
- writes partitioned parquet outputs to HDFS
- joins the cleaned datasets into an analytics feature table

## 2. Technical Audit

Evidence:

- package overview and run instructions:
  - `README.md`
  - `RUN_ON_HPC_COMMANDS.md`
- corrections over time:
  - `TECHNICAL_AUDIT_AND_CORRECTIONS.md`
- source comments calling out specific corrections:
  - `etl_code/yanze_wu/CleanTaxi.scala`
  - `etl_code/han_xiao/CleanWeather.scala`
  - `ana_code/src/RunWeatherAwareDemandAnalysis.scala`

## 3. Scalability and Stress Testing

Evidence:

- distributed run architecture:
  - `run_all_on_dataproc.sh`
  - `ana_code/build_and_package.sh`
  - `ana_code/run_analytics.sh`
- design rationale:
  - `SCALABILITY_AND_STRESS_TESTING.md`
- HDFS outputs and screenshots:
  - `screenshots/`

## 4. Analytical Depth and Insight

Evidence:

- final model comparison:
  - `ana_code/src/RunWeatherAwareDemandAnalysis.scala`
- interpretation of the non-obvious anomaly:
  - `ANALYTICAL_FINDINGS.md`

Why this matters:

- the project does not stop at descriptive stats
- it explains why the simpler baseline outperformed the weather-aware model and why that is still a meaningful result

## 5. Ethical Guardrails and Governance

Evidence:

- governance and bias discussion:
  - `ETHICS_AND_GOVERNANCE.md`
- processing documentation:
  - `README.md`
  - `HDFS_ACCESS_REQUEST.txt`

Why this matters:

- the package explicitly documents representation risk, aggregation bias, limited station coverage, and proper scope of use
