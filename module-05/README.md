# Module 05 – Data Pipelines with Bruin

## Overview

This module covers building a structured, production-style data pipeline using **Bruin**, an asset-based data workflow tool.

The goal is to model NYC Taxi data in DuckDB using a layered ingestion ➜ staging ➜ reporting flow while practicing modern analytics engineering patterns.

---

## What is Bruin?

Bruin brings together multiple data engineering concepts in one framework - orchestration, transformation, incremental processing, dependency management, and data quality validation - through a simple CLI-based workflow.

Key ideas behind Bruin:

- Pipelines are defined in `pipeline.yml`
- Transformations live inside an `assets/` directory
- Dependencies are declared explicitly
- Incremental strategies are built-in
- Data quality checks run as part of execution
- Lineage can be visualized from the CLI


## Project Structure

```yml
.
├── bruin_exercises.md
└── pipeline
    ├── assets
    │   ├── ingestion
    │   │   ├── __pycache__
    │   │   ├── payment_lookup.asset.yml
    │   │   ├── payment_lookup.csv
    │   │   ├── requirements.txt
    │   │   └── trips.py
    │   ├── reports
    │   │   └── trips_report.sql
    │   └── staging
    │       └── trips.sql
    └── pipeline.yml
```

- **Ingestion** (raw tables)
- **Staging** (cleaned & incremental models)
- **Reports** (aggregated outputs)

## Pipeline Configuration
Defined in `pipeline/pipeline.yml`

This file acts as the control layer of the project. It defines:

- Pipeline name: `nyc_taxi`
- Start date (`2022-01-01`)
- Default DuckDB connection
- A configurable variable `taxi_types` (default: `["yellow"]`)

It centralizes execution settings and allows parameterization without modifying asset logic.

---

## Assets Overview

The project follows a layered structure: ingestion → staging → reporting.

### Ingestion Layer

**`ingestion.trips` (Python asset)**  
- Materialized as a table using **append** strategy  
- Loads raw trip data into DuckDB  
- Defines schema explicitly  

This represents the raw fact table ingestion step.

**`ingestion.payment_lookup` (Seed asset)**  
- Loads a static CSV lookup table  
- Adds built-in quality checks (`not_null`, `unique`)  

This demonstrates how reference/dimension data can be version-controlled inside the pipeline.

---

### Staging Layer

**`staging.trips` (SQL asset)**  
Depends on both ingestion assets.

Responsibilities:

- Enrich trip data with payment type name (JOIN)
- Deduplicate using `ROW_NUMBER()`
- Apply **time-window incremental strategy**
  - `incremental_key: pickup_datetime`
  - Filters using `start_datetime` / `end_datetime`
- Run quality checks during execution

This layer handles cleaning, enrichment, and controlled incremental recomputation of time slices - a practical approach to scalable and idempotent processing.

---

### Reporting Layer

**`reports.trips_report` (SQL asset)**  
Depends on `staging.trips`.

- Materialized using `create+replace`
- Produces daily aggregates:
  - `trip_count`
  - `total_fare`
  - `avg_fare`
- Includes validation checks on computed metrics

This serves as the final analytical output layer.

---

## Incremental Strategy

Two materialization approaches are demonstrated:

- **Append** for raw ingestion  
- **Time-window incremental** for staging  

The staging model deletes and rebuilds only the relevant time interval on each run. This keeps execution efficient while maintaining predictable, idempotent behavior.

---


## How to Run

```bash
bruin run
bruin run --select staging.trips --downstream
bruin run --full-refresh
bruin lineage
```

## Key Points
In this project, core data engineering concepts - orchestration, transformation, incremental processing, and data quality testing - are brought together in a single, structured workflow using **Bruin**.

- Asset-based pipeline structure  
- Declarative configuration via `pipeline.yml`  
- Built-in incremental materialization strategies  
- Integrated data quality validation  
- CLI-driven orchestration and lineage
