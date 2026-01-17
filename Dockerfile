# =============================================================================
# Stage 1: Builder - Install dependencies
# =============================================================================
FROM python:3.11-slim AS builder

LABEL maintainer="Champion Team <team@champion.com>"
LABEL description="Champion data platform for market analytics - Builder Stage"

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry==1.7.1

# Copy dependency files
COPY pyproject.toml poetry.lock poetry.toml ./

# Configure Poetry to not create virtual environment (install to system)
# Install dependencies only (without dev dependencies and root package)
RUN poetry config virtualenvs.create false && \
    poetry install --without dev --no-interaction --no-ansi --no-root

# =============================================================================
# Stage 2: Runtime - Minimal production image
# =============================================================================
FROM python:3.11-slim AS runtime

LABEL maintainer="Champion Team <team@champion.com>"
LABEL description="Champion data platform for market analytics"
LABEL version="1.0.0"

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user with fixed UID for better security
RUN useradd -m -u 1000 -s /bin/bash champion && \
    mkdir -p /data /app && \
    chown -R champion:champion /data /app

WORKDIR /app

# Copy Python dependencies from builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY --chown=champion:champion src/ ./src/
COPY --chown=champion:champion scripts/ ./scripts/

# Switch to non-root user
USER champion

# Expose ports
# 8080: Health check endpoint
# 9090: Metrics port
EXPOSE 8080 9090

# Health check configuration
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Environment variables for production
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

# Default command - runs the CLI with signal handling
CMD ["python", "-m", "champion"]
