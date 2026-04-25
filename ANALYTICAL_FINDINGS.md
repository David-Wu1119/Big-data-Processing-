# Analytical Findings

## Final result summary

The final Dataproc run produced two evaluation rows in the metrics output:

- `baseline_zone_hour_mean`
  - `mae=12.336126410845173`
  - `rmse=28.53028296263549`
  - `mape=0.919258434643404`
  - `test_rows=15784`
  - `train_end_date=2024-01-24`
- `weather_aware_linear_regression`
  - `mae=27.60838471797054`
  - `rmse=46.98186458499783`
  - `mape=5.7551026304736785`
  - `test_rows=15784`
  - `train_end_date=2024-01-24`

The strongest practical conclusion is not that weather improved demand forecasting. It did not. The historical zone-hour baseline materially outperformed the weather-aware linear regression model on the held-out data.

## Non-obvious anomaly

The non-obvious anomaly is that adding weather features made the model worse, even though weather is widely believed to affect taxi demand.

At first glance this looks wrong. In real city operations, rain, wind, and temperature shifts do influence travel demand. The anomaly becomes more plausible after looking at the structure of the data and the aggregation level of the model:

- The target is aggregated by `pickup_date`, `pickup_hour`, and `PULocationID`. At that level, demand is already strongly explained by persistent zone-hour behavior.
- The weather signal is city-level or station-level and much coarser than the spatial resolution of taxi pickup zones.
- Only two airport-adjacent weather stations were used. That is reasonable for a class project, but it is not a high-resolution proxy for street-level weather across all NYC taxi zones.
- The modeling window is short. January 2024 gives only a narrow seasonal slice, which makes it harder for a linear model to learn robust weather effects without overfitting noise.

## Domain interpretation

This result is consistent with a transportation-operations interpretation: short-horizon taxi demand can be dominated by habitual spatial-temporal structure rather than weather, especially when the weather representation is coarse.

In practical terms:

- Airport and commuter flows create stable zone-hour demand patterns.
- Demand spikes from weather may be highly localized, nonlinear, and lagged.
- A simple linear regression with limited weather inputs is a weak approximation for those effects.

The negative result is still analytically useful. It shows that blindly adding external data does not guarantee better forecasting. For operational planning, a strong benchmark baseline is necessary, and richer weather-aware modeling would likely require one or more of the following:

- more weather stations or gridded weather coverage
- lagged weather features
- interaction terms between zone, hour, and weather
- longer training windows spanning multiple months or seasons
- nonlinear models rather than a single linear regression

## Trends and correlations observed

The project pipeline still captures meaningful relationships:

- taxi demand varies systematically by zone and hour
- weather features can be joined successfully to hourly taxi aggregates
- precipitation and wind are present as usable exogenous variables
- the joined feature table is suitable for model comparison and ablation

That comparison is the key insight: under this data design, the baseline is more reliable than the current weather-aware linear model.

## Why this matters

This is a stronger analytic conclusion than a superficial statement such as "weather affects taxi demand." The actual evidence from the run shows that:

- the baseline is hard to beat
- feature granularity matters
- the operational value of external data depends on measurement quality and model specification

That is the result this project defends.
