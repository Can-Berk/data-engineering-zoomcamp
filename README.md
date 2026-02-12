# Data Engineering Zoomcamp
This repository contains my work and notes from [Data Engineering Zoomcamp](https://datatalks.club/blog/data-engineering-zoomcamp.html) (2026 edition), exploring modern data engineering tools and workflows.

## Technologies Used

| Tool | Category | Purpose |
|----------|-------|-------------|
| [Docker](https://www.docker.com/) | Containerization | Reproducible environments for data pipelines and services |
| [Terraform](https://www.terraform.io/) | Infrastructure as Code | Provision and manage cloud infrastructure resources on GCP |
| [Python](https://www.python.org/) | Programming Language | Data ingestion, orchestration, and pipeline logic |
| [PostgreSQL](https://www.postgresql.org/) | Relational Database | Source system for ingestion and transactional data |
| [Google Cloud Storage (GCS)](https://cloud.google.com/storage) | Data Lake | Object storage for raw and processed datasets |
| [BigQuery](https://cloud.google.com/bigquery) | Data Warehouse | Serverless analytics at scale |
| [SQL](https://en.wikipedia.org/wiki/SQL) | Query Language | Querying and transforming data in PostgreSQL and BigQuery |
| [dbt](https://www.getdbt.com/) | Analytics Engineering | Data modeling, testing, and transformations in BigQuery |
| [Apache Spark](https://spark.apache.org/) | Distributed Processing Engine | Large-scale batch data processing |
| [Apache Kafka](https://kafka.apache.org/) | Event Streaming Platform | Real-time event ingestion and stream processing |

<br>

![Data Engineering Zoomcamp Architecture](./images/de-technologies-architecture.jpg)
*Architecture adapted from the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).*

## Modules

- [**`Module 1`**](module-01/): Docker, PostgreSQL, Terraform, GCP
- [**`Module 2`**](module-02/): Workflow Orchestration with Kestra
- [**`Module 3`**](module-03/): Data Warehousing with BigQuery
- [**`Module 4`**](module-04/): Analytics engineering with dbt
