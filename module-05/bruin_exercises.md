# Question 1. Bruin Pipeline Structure
In a Bruin project, what are the required files/directories?

**Solution**:
- `.bruin.yml` stores project-level configuration (connections, profiles).
- `pipeline/pipeline.yml` defines the pipeline (variables, defaults)
- `assets/` contains the pipeline assets.

<br>

# Question 2. Materialization Strategies
You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

**Solution**: `time_interval` – incremental based on a time column. It enables to delete and reprocess data for a specific time range (e.g., by month).

<br>

# Question 3. Pipeline Variables
You have the following variable defined in `pipeline.yml`:
``` yml
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

**Solution**: `bruin run --var 'taxi_types=["yellow"]'`. `taxi_types` is defined as an **array**, which should be passed as valid JSON.

<br>

# Question 4. Running with Dependencies
You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

**Solution**: `bruin run ingestion/trips.py --downstream`. Runs the selected asset plus all assets that depend on it.

<br>

# Question 5. Quality Checks
You want to ensure the ``pickup_datetime`` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

**Solution**: `name: not_null`.

<br>

# Question 6. Lineage and Dependencies
After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

**Solution**: `bruin lineage`.

<br>

# Question 7. First-Time Run
You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

**Solution**: `--full-refresh` rebuilds tables from scratch and and it is necessary when running a pipeline for the first time on a new database.