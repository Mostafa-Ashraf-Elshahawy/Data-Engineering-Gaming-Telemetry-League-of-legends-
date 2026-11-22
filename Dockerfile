# Use official lightweight Python 3.11 runtime image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffer stdout/stderr so logs appear immediately
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy dependency list first to leverage Docker layer caching
COPY requirements.txt /app/requirements.txt

# Install Python dependencies without storing pip cache
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy pipeline scripts and SQL files into the container
COPY scripts/ /app/scripts/
COPY sql/ /app/sql/

# Create raw data landing directory (mounted via volume in docker-compose.yml)
RUN mkdir -p /app/data/raw

# Execute Phase 2 raw ingestion followed by Phase 3 cleaning, loading & quality checks
CMD ["sh", "-c", "python scripts/ingest_data.py && python scripts/clean_and_load.py"]
