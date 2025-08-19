FROM python:3.11-slim

# Install system dependencies for better performance
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app.py .
COPY hometech.py .

# Create a non-root user for security
RUN useradd --create-home --shell /bin/bash loadtester
USER loadtester

# Set environment variables for better performance
ENV PYTHONUNBUFFERED=1
ENV PYTHONOPTIMIZE=1

# Default command
CMD ["python", "app.py", "--help"]
