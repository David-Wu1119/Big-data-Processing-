# Ethics And Governance

## Scope of the analytic

This project predicts aggregate taxi demand by `pickup_date`, `pickup_hour`, and `PULocationID`. It is not designed for individual-level decision making, customer scoring, policing, or driver evaluation. The intended use is aggregate operational planning and exploratory forecasting.

## Bias and representation risks

Several representation risks exist in the data and pipeline:

### Spatial representation bias

The weather features come from a small number of airport-area stations. Those measurements are not equally representative of all taxi pickup zones in New York City. A Manhattan street canyon, an outer-borough residential area, and an airport corridor can experience materially different conditions even within the same hour.

### Modal coverage bias

Taxi demand reflects the behavior of people who use yellow taxis. It does not directly represent all travelers. Riders with different income levels, transit access, disability status, or neighborhood access patterns may be underrepresented or differently represented in the dataset.

### Aggregation bias

The model aggregates to zone-hour level. Aggregation is necessary for scalability and project scope, but it can hide localized effects and mask subgroup differences between neighborhoods.

### Temporal bias

The current run is trained on a narrow January 2024 window. That means the result is vulnerable to unusual holiday patterns, winter-specific behavior, and short-term operational disruptions.

## Guardrails used in this project

The implementation uses several practical guardrails:

- invalid and null records are filtered explicitly rather than silently passing through the ETL
- weather sentinel values such as `-9999` are normalized before analysis
- taxi timestamps are constrained to the study window so stale rows do not leak into the target period
- weather timestamps are converted from UTC to `America/New_York` before joining, which reduces temporal misalignment risk
- the analytics compare a benchmark baseline with a weather-aware model instead of assuming the more complex model is better

## Bias testing and model governance position

This project does not claim a fairness-certified forecasting system. The correct governance position is narrower:

- use the model only for aggregate exploratory planning
- do not use it for allocating service in a way that could systematically disadvantage neighborhoods without additional fairness checks
- do not interpret aggregate demand forecasts as evidence about rider value or neighborhood desirability

The baseline outperforming the weather-aware model is itself a governance signal. It suggests the additional model complexity is not yet justified for decision support.

## Alternative approaches considered

Several alternative processing choices are defensible:

- use gridded weather or more station coverage instead of relying on two airport stations
- create neighborhood-cluster weather mappings instead of city-wide hourly averages
- test lagged weather features because taxi demand may respond after the weather event rather than within the same hour
- stratify evaluation by borough or zone cluster to test whether one geography is systematically worse predicted than another
- include seasonal windows beyond January to reduce temporal instability

## Recommended next governance step

If this project were extended beyond course scope, the next required step would be a disparity audit. At minimum, forecast error should be compared across:

- airport zones vs non-airport zones
- Manhattan vs outer-borough zones
- high-demand vs low-demand zones

If error is systematically worse for specific geographies, the pipeline should be adjusted before any operational use.
