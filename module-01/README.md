# Module 01 — Containerized Data Pipelines and Infrastructure Management

## Overview
This module covers the following tools: Docker, Postgres, Terraform, Google Cloud Platform (GCP).


## Topics Covered
### Docker
- **Docker** is a containerization platform that allows applications to run in **isolated environments** called containers.
- Containers package application code together with its dependencies, enabling consistent execution across different machines.

#### Postgres in Docker
- Running Postgres as a Docker container.
- Persisting database data using Docker volumes.
- Connecting to Postgres from other containers (e.g., ingestion jobs, pgAdmin).

---
### Terraform
- **Terraform** is an **Infrastructure as Code (IaC)** tool that allows infrastructure to be defined using configuration files instead of manual steps.
- Infrastructure definitions are version-controlled, reproducible, and auditable.
- Cloud access and resource management are handled via Terraform **providers**

#### Terraform Provider for GCP (Google Cloud Platform)
- Using the Terraform provider for GCP to manage Google Cloud resources.
- Defining cloud infrastructure declaratively.
- Provisioning storage, compute, and networking components required for data engineering pipelines.

---

## Exercises Overview

| Exercise | Main topic | Description |
|----------|-------|-------------|
| [Exercise 01](./exercise-01) | Docker image & container | Build and run a Docker image for a simple Python pipeline |
| [Exercise 02](./exercise-02) | Multi-container orchestration | Dockerized data ingestion into a Dockerized Postgres database, inspected via pgAdmin. |
| [Exercise 03](./exercise-03) | Container, SQL & Terraform | Dockerized data ingestion, querying data with SQL, and managing GCP infrastructure using Terraform. |
