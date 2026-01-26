# Exercise 01 — Build and Run a Dockerized Python Pipeline

## Description

In this exercise, we develop a simple Python pipeline using a virtual environment, then package it into a Docker image using a Dockerfile and run it as a container.

The goal is to understand:

- local development with an isolated Python environment

- reproducible dependency management

- Docker image vs Docker container

- basic Docker build and run workflow

---

## Local development with a virtual environment

We use a **virtual environment** to isolate dependencies for this project from other projects and from the system Python.

### Install `uv`
```bash
pip install uv
```

### Initialize a Python Project

```bash
uv init --python=3.13
```

This creates:

- `pyproject.toml` — project metadata and dependency definitions
- `uv.lock` — locked dependency versions for reproducibility
- `.python-version` — Python version used by the project
- A virtual environment managed by `uv`

### Verify the Virtual Environment

```bash
uv run which python
uv run python -V
```

### Install Dependencies

Install the dependencies required to run the pipeline:

```bash
uv add pandas pyarrow
```
Dependencies are recorded in pyproject.toml and locked in uv.lock.

### Run the Pipeline Locally

```bash
uv run python pipeline.py 10
```

This script produces a binary (parquet) file, so let's make sure we don't accidentally commit it to git by adding parquet extensions to .gitignore:

```
*.parquet
```


## Dockerizing the Pipeline
Once the pipeline works locally, we package it into a Docker image.

### Key Dockerfile Instructions

- **FROM** — defines the base image (Python 3.13 slim)
- **RUN** — executes commands during the image build
- **WORKDIR** — sets the working directory inside the container
- **COPY** — copies files into the image
- **ENTRYPOINT** — defines the default command when the container starts

### Core Docker Concepts
**Dockerfile** is the recipe that describes how an image is built
**Docker image** is a read-only build artifact created from a Dockerfile using docker build.
**Docker container** is a running instance of a Docker image, created and started using docker run.


### Build the Docker Image

```bash
docker build -t test:v1 .
```

This command:

- Uses the `Dockerfile` in the current directory
- Builds a Docker image named `test` with tag `v1`


### Run the Docker Container
We can now run the container and pass an argument to it, so that our pipeline will receive it.

```bash
docker run -it --rm test:v1 10
```

- `-it` runs the container interactively

- `--rm` removes the container after it exits

- Arguments after the image name are passed to the pipeline script via ENTRYPOINT


## Summary

- The pipeline is first developed and tested locally in an isolated virtual environment

- Dependencies are locked and reproduced inside the Docker image

- Docker provides isolation, portability, and reproducible execution

- The same pipeline runs consistently on the host machine and inside a Docker container