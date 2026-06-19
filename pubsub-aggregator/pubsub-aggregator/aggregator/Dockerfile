FROM python:3.11-slim AS base

# Non-root user untuk keamanan
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Install dependencies sistem minimal
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY app/ ./app/

# Buat direktori log dan set permission
RUN mkdir -p /var/log/aggregator && chown appuser:appuser /var/log/aggregator

USER appuser

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--loop", "asyncio"]
