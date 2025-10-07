# Use Python 3.10 slim image
FROM python:3.10-slim

# Prevent Python from writing .pyc files and enable stdout flush
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (needed for Postgres/MySQL/ODBC drivers)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    unixodbc-dev \
 && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

# Copy your application code
COPY . .

# Expose the port your app runs on
EXPOSE 8081

# Default command
CMD ["python", "firestone_to_rds_pipeline.py"]
