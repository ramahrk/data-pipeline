FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/output data/quarantine data/reference logs metrics

# Set environment variables
ENV PYTHONPATH=/app
ENV LOG_LEVEL=INFO
ENV METRICS_ENABLED=true


# Default command
CMD ["python", "-m", "src.pipeline"]


