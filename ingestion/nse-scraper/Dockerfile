FROM python:3.11-slim

LABEL maintainer="Champion Team <team@champion.com>"
LABEL description="NSE data scraper for Champion platform"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m -u 1000 scraper && \
    mkdir -p /data/nse && \
    chown -R scraper:scraper /data

WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry==1.7.1

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Install dependencies (without dev dependencies)
RUN poetry config virtualenvs.create false && \
    poetry install --without dev --no-interaction --no-ansi --no-root

# Copy application code and entrypoint
COPY src/ ./src/
COPY run_scraper.py ./
COPY entrypoint.sh ./
RUN chmod +x entrypoint.sh

# Switch to non-root user
USER scraper

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9090/metrics || exit 1

# Expose metrics port
EXPOSE 9090
