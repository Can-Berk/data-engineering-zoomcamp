# Exercise 02 — Multi-container Data Ingestion with Docker, Postgres, and pgAdmin

## Description

In this exercise, we run a data ingestion script inside a Docker container, load data into a Postgres database running in another Docker container, and inspect the results using pgAdmin running in a third container.
All containers communicate over the same Docker network, simulating a reproducible data-engineering setup.

The goal is to:
- Run a **Postgres database** inside a Docker container
- Run a **data ingestion pipeline** inside another Docker container
- Inspect the ingested data using **pgAdmin**, also running in Docker
- Orchestrate all services using **Docker Compose**

---

## Architecture Overview

This exercise consists of three main components:

1. **Postgres container**
   - Stores NYC Taxi trip data
   - Uses a Docker volume for persistent storage

2. **Ingestion container**
   - Runs a Python script that downloads NYC Taxi data
   - Loads the data into Postgres in chunks
   - Connects to Postgres using SQLAlchemy

3. **pgAdmin container**
   - Provides a web-based UI to inspect the Postgres database
   - Connects to the Postgres container over the Docker network

All containers run on the same Docker network created by Docker Compose.



## Local Python Setup (for development)

Before containerizing the ingestion pipeline, we initialize the Python project using **uv**.

```bash
uv init --python=3.13
```

Install required dependencies:
```bash
uv add pandas pyarrow
uv add sqlalchemy psycopg2-binary
uv add tqdm
uv add 
```

## Running PostgreSQL with Docker

```bash
docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  postgres:18
  ```
  
### Explanation of parameters:

-e sets environment variables (user, password, database name)
-v ny_taxi_postgres_data:/var/lib/postgresql creates a named volume
    - Docker manages this volume automatically
    - Data persists even after container is removed
    - Volume is stored in Docker's internal storage
-p 5432:5432 maps port 5432 from container to host
postgres:18 uses PostgreSQL version 18 (latest as of Dec 2025)



## Running pgAdmin - Database Management Tool
```bash
  docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  dpage/pgadmin4
  ```

At this point, pgAdmin cannot see Postgres yet — both containers must be on the same Docker network.

## Connecting Postgres and pgAdmin with a Docker Network
### Create a virtual Docker network called pg-network
```bash
docker network create pg-network
```

Run PostgreSQL on the network:
```bash
  docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18
  ```

In another terminal, run pgAdmin on the same network:
```bash
  docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
  ```

Now connect to Postgres from pgAdmin using:
- Host: pgdatabase
- Port: 5432
- User / Password: root

## Build the Docker Image
```bash
docker build -t taxi_ingest:v001 .
```

## Containerized Ingestion
```bash
  docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips_2021_2 \
    --year=2021 \
    --month=2 \
    --chunksize=100000
  ```

After ingestion completes, the data will be available in Postgres and visible in pgAdmin.

## Docker Compose
Docker Compose allows us to define and run Postgres and pgAdmin together using a single configuration file.


### Start Services with Docker Compose
```bash
docker-compose up
```
Docker Compose automatically:
- Docker Compose automatically:
- Creates a dedicated network
- Starts Postgres and pgAdmin


### Run data ingestion with docker-compose
First, find the network name:
```bash docker network ls```
In this case: pipeline_default

Run the ingestion container on that network:

```bash
  docker run -it \
  --network=exercise-02_default \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips_2021_2 \
    --year=2021 \
    --month=2 \
    --chunksize=100000
```

## Summary
- PostgreSQL, pgAdmin, and the ingestion pipeline run in separate containers
- Docker networking enables container-to-container communication
- Data is persisted using Docker volumes
- Docker Compose simplifies multi-service orchestration
- The same pipeline runs reproducibly across environments
