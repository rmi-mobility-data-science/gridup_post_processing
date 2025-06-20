# Stage 1: Builder
FROM python:3.11-slim AS builder

# Install system dependencies required for geospatial libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    libgeos-dev \
    libgdal-dev \
    libproj-dev \
    libspatialindex-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set work directory and copy only what's needed to install packages
WORKDIR /install
COPY environment.txt .

# Install packages into a custom location (not global)
RUN pip install --prefix=/install/python-env --no-cache-dir -r environment.txt

# Stage 2: Final minimal runtime image
FROM python:3.11-slim AS final

# Set work directory
WORKDIR /app

# Copy only the installed Python packages from builder
COPY --from=builder /install/python-env /usr/local

# Copy your main script (and optionally others if needed)
COPY aggregation_cli.py .

# Optional: if you need spatial extensions installed dynamically by DuckDB
# they'll be downloaded at runtime if not included here

# Default command
ENTRYPOINT ["python", "aggregation_cli.py"]