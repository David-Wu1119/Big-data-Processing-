# Commands To Run On HPC

## 1. Copy the package to Dataproc

From your local machine:

```bash
scp -r /Users/yanzewu/Desktop/CS_BigData_472/Hws/FINAL_CODE_DROP yw7566_nyu_edu@nyu-dataproc-m:~/
```

## 2. Build and run everything on Dataproc

From the Dataproc VM:

```bash
cd ~/FINAL_CODE_DROP

chmod +x data_ingest/upload_local_data_to_hdfs.sh \
  ana_code/build_and_package.sh \
  ana_code/run_analytics.sh \
  run_all_on_dataproc.sh

bash run_all_on_dataproc.sh | tee final_code_drop_run.log
```

## 3. Verify the important HDFS outputs

```bash
HDFS_BASE=/user/$(whoami)/final_code_drop

hdfs dfs -ls "${HDFS_BASE}/raw/taxi"
hdfs dfs -ls "${HDFS_BASE}/raw/weather"
hdfs dfs -ls "${HDFS_BASE}/etl/taxi_clean"
hdfs dfs -ls "${HDFS_BASE}/etl/weather_clean"
hdfs dfs -ls "${HDFS_BASE}/analytics/features"
hdfs dfs -ls "${HDFS_BASE}/analytics/baseline_predictions"
hdfs dfs -ls "${HDFS_BASE}/analytics/weather_aware_predictions"
hdfs dfs -ls "${HDFS_BASE}/analytics/metrics"
```

## 4. Inspect the metrics output

```bash
HDFS_BASE=/user/$(whoami)/final_code_drop

hdfs dfs -cat "${HDFS_BASE}/analytics/metrics/part-"* | head -n 20
```

## 5. Capture screenshots

Take screenshots of:

- raw HDFS upload listings
- taxi profiling run
- weather profiling run
- taxi ETL run
- weather ETL run
- analytics run
- analytics metrics output
- final HDFS output listings

## 6. Send the access request

```bash
cat HDFS_ACCESS_REQUEST.txt
```

Then email the content to `HPC@nyu.edu`.
