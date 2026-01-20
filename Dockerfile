# =============================================================================
# Single stage: Install everything needed
# =============================================================================
FROM python:3.11-slim

LABEL maintainer="Champion Team <team@champion.com>"
LABEL description="Champion data platform for market analytics"
LABEL version="1.0.0"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    ca-certificates \
    build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user with fixed UID for better security
RUN useradd -m -u 1000 -s /bin/bash champion && \
    mkdir -p /data /app && \
    chown -R champion:champion /data /app

WORKDIR /app

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Install poetry and dependencies
RUN pip install --no-cache-dir poetry==1.7.1

RUN poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi 2>&1

# Ensure critical packages are installed
RUN pip install --no-cache-dir uvicorn fastapi typer

# Copy application code
COPY --chown=champion:champion src/ ./src/
COPY --chown=champion:champion scripts/ ./scripts/

# Switch to non-root user
USER champion

# Expose ports
EXPOSE 8000 9090

# Health check - use wget instead of curl
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8000/health || exit 1

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

# Default command - start API server
CMD ["python", "-m", "champion", "api", "serve", "--host", "0.0.0.0", "--port", "8000"]
