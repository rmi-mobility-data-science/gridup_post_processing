# Stage 1: Builder
FROM python:3.11-slim AS builder

# Install basic build tools only (no geospatial libraries)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /install

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --prefix=/install/python-env --no-cache-dir -r requirements.txt

# Stage 2: Final runtime image
FROM python:3.11-slim AS final

# Create a non-root user
RUN useradd --create-home appuser

WORKDIR /app

# Copy installed Python environment
COPY --from=builder /install/python-env /usr/local

# Copy only required source files
COPY aggregation_cli.py .
COPY src/ src/

# Give appuser ownership of the workdir
RUN chown -R appuser:appuser /app

# Use non-root user
USER appuser

# Default command to run the pipeline
ENTRYPOINT ["python", "aggregation_cli.py"]