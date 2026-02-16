This file contains solutions for Module 04 Homework: Analytics Engineering from the Data Engineering Zoomcamp 2026, focusing on transforming NYC Taxi data using dbt with BigQuery, building staging, intermediate, and fact models, and applying data quality tests and lineage-based execution.

# Question 1. dbt Lineage and Execution
Given a dbt project with the following structure:

```postgresql
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?


**Solution**: This builds the upstream models `stg_green_tripdata` and `stg_yellow_tripdata`, followed by the selected model `int_trips_unioned`.



# Question 2. dbt Tests
You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

**Solution**: The **accepted_values** test query will find rows that violate the rule (`payment_type = 6`). **dbt will fail the test, returning a non-zero exit code**.

| Command | Runs models | Runs tests | Exit code |
|------------|------------|------------|---------------------------|
| `dbt run`  | Yes | No | `0` |
| `dbt test` | No | Yes| non-zero |
| `dbt build`| Yes | Yes | non-zero |