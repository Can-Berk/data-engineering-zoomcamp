# Module 01 — Docker, Infrastructure, and Data Engineering Basics

## Overview
This module covers the following tools: Docker, Postgres, Terraform, GCP


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

#### GCP (Google Cloud Platform)
- Terraform is later used to provision and manage cloud resources on GCP.
- This includes storage, compute, and networking components required for data engineering pipelines.

---

## Exercises Overview

| Exercise | Main topic | Description |
|----------|-------|-------------|
| Exercise 01 | Docker image & container | Build and run a Docker image for a simple Python pipeline |
| Exercise 02 | Multi-container orchestration | Dockerized data ingestion into a Dockerized Postgres database, inspected via pgAdmin. |
