# Every Docker build stage starts with FROM: choose a base image (e.g. Python 3.13 slim).
FROM python:3.13.11-slim

# Copy the uv binary from the uv image into /bin so it’s on PATH.
# The registry&owner of the image must be specified because uv is not a Docker Hub "official" image.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin

# Set working directory.
WORKDIR /code

# Add /code/.venv/bin in front of the existing PATH. This allows all Python commands automatically use venv instead of the system Python.
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency metadata first to maximize Docker layer caching.
COPY pyproject.toml uv.lock .python-version ./

# Install deps exactly from the lockfile (ensures reproducibility).
RUN uv sync --locked

# Copy the script file to the container.
COPY pipeline.py .

# Set an entry point that will be executed when docker run.
ENTRYPOINT ["python", "pipeline.py"]