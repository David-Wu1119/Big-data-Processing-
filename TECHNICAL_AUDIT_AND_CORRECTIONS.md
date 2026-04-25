# Technical Audit And Corrections

## Audit summary

The final package is not just a dump of working code. Several correctness and reliability issues were identified and corrected during development. This document records the specific improvements so the evolution of the code is explicit.

## Correction 1: Taxi study-window filtering

**Issue in earlier drafts**

Earlier taxi cleaning drafts did not strictly constrain the study window. That allowed rows outside the intended January 2024 analysis horizon to remain in downstream data.

**Correction**

The taxi ETL now enforces:

```scala
.where(to_date(col("pickup_ts")).between(lit("2024-01-01"), lit("2024-01-31")))
```

**Why this matters**

- prevents stale rows from contaminating training and evaluation
- keeps the feature table aligned to the project scope
- reduces the risk of misleading demand patterns from out-of-window records

## Correction 2: Weather UTC to local-time conversion

**Issue in earlier drafts**

Raw NOAA ISD-Lite records are in UTC. Joining them directly to local taxi pickup hours creates a temporal alignment bug.

**Correction**

The weather ETL now converts UTC observations to `America/New_York`:

```scala
.withColumn("observation_ts_local", from_utc_timestamp(col("observation_ts_utc"), "America/New_York"))
.withColumn("observation_date_local", to_date(col("observation_ts_local")))
.withColumn("observation_hour_local", hour(col("observation_ts_local")))
```

**Why this matters**

- aligns weather and taxi observations on the same local clock
- avoids a systematic hour-shift error in the join
- materially improves logical correctness of the feature synthesis

## Correction 3: Weather sentinel normalization

**Issue in earlier drafts**

NOAA weather files encode missing or special cases with sentinel values such as `-9999` and `-1`. Treating those as ordinary numbers corrupts statistics and features.

**Correction**

The ETL and profiling code normalize those values before analysis:

- `-9999` becomes null for fields such as temperature and precipitation
- `-1` precipitation is converted to `0.1` mm as defined in ISD-Lite conventions used here

**Why this matters**

- prevents extreme fake values from polluting summaries and model inputs
- improves data validity
- makes the weather feature table interpretable

## Correction 4: Build script classpath handling on Dataproc

**Issue in earlier drafts**

The original build flow did not pass the Spark jars correctly to the Scala compiler on Dataproc. That caused compile failures such as unresolved `org.apache.spark.*` imports and no jar output.

**Correction**

The build script was revised to compile with the Spark classpath explicitly and package the jar only after successful compilation.

**Why this matters**

- produces the required class files and jar for submission
- avoids a silent failure path where downstream jobs try to run a missing jar
- makes the build step deterministic on Dataproc

## Correction 5: Metric computation null safety

**Issue in earlier drafts**

The `mape` metric can be null when labels are zero or filtered rows do not produce a valid average. Earlier logic did not guard this case explicitly.

**Correction**

The analytics code now checks whether the aggregated `mape` field is null before reading it:

```scala
val mapeIndex = metricRow.fieldIndex("mape")
val mape =
  if (metricRow.isNullAt(mapeIndex)) Double.NaN
  else metricRow.getDouble(mapeIndex)
```

**Why this matters**

- prevents runtime failures in metric extraction
- preserves the rest of the evaluation even when MAPE is undefined
- makes failure behavior explicit instead of accidental

## Correction 6: Baseline before model complexity

**Issue in earlier drafts**

It is easy to overstate the value of the weather-aware model without a strong benchmark.

**Correction**

The final analytics stage always computes:

- a zone-hour historical mean baseline
- the weather-aware linear regression model

and writes metrics for both.

**Why this matters**

- forces the evaluation to compare against a defensible benchmark
- avoids misleading claims that a more complex model is automatically better
- improved the analysis quality when the final run showed the baseline actually outperformed the weather-aware model

## Remaining limitations

Not every issue is solved:

- only a limited number of weather stations are used
- the model family is intentionally simple
- the run window is short and seasonally narrow

These are project-scope limits, not hidden code defects. They are documented explicitly rather than ignored.
