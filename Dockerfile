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

# Copy pipeline scripts into the container
COPY scripts/ /app/scripts/

# Create raw data landing directory (mounted via volume in docker-compose.yml)
RUN mkdir -p /app/data/raw

# Execute the modular ingestion script when the container starts
CMD ["python", "scripts/ingest_data.py"]
