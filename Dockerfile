# Use a lightweight Python base image
FROM python:3.11-slim

# Install system packages needed for geospatial libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    libgeos-dev \
    libgdal-dev \
    libproj-dev \
    libspatialindex-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy environment.txt and install Python dependencies
COPY environment.txt .
RUN pip install --no-cache-dir -r environment.txt

# Copy the full application code into the container
COPY . .

# Run the script by default (you can override this with docker run args)
CMD ["python", "aggregation_cli.py"]