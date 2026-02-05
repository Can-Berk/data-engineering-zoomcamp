# Module 02 — Workflow Orchestration

## Overview
This module focuses on **workflow orchestration** using **Kestra** to automate, schedule, and monitor data pipelines.

## Workflow Orchestration
Workflow orchestration is the practice of coordinating multiple data tasks into reliable pipelines.

## Topics Covered
- Define multi-step workflows (extract, load, transform)
- Schedule pipelines using time-based triggers
- Handle retries, failures, and backfills
- Parameterize workflows using variables and dynamic inputs

## Kestra
Kestra is an open-source workflow orchestrator designed for data pipelines and event-driven automation.

### Kestra Concepts
- **Flows** — declarative workflow definitions
- **Tasks** — individual execution steps
- **Triggers** — scheduling and event-based execution
- **Variables & Inputs** — dynamic pipeline configuration
- **Executions & Logs** — pipeline monitoring and debugging
- **Storage & Outputs** — managing intermediate files

## Exercises Overview

| Exercise | Main topic | Description |
|----------|-------|-------------|
| [Workflow Orchestration & Local ETL](./kestra-local-etl) | Workflow orchestration, Local ETL, Docker, PostgreSQL | Build and orchestrate ETL pipelines with Kestra that extract NYC taxi data and load it into a local PostgreSQL database running in Docker. |
| [Workflow Orchestration & Cloud ELT](./kestra-cloud-elt) | Workflow orchestration, Cloud ELT, Data Lake, Data Warehouse (GCP) | Build and orchestrate ELT pipelines with Kestra that load raw data into Google Cloud Storage as a data lake and transform and analyze it in BigQuery as a data warehouse. |