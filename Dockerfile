FROM python:3.11-slim

LABEL maintainer="Champion Team <team@champion.com>"
LABEL description="Champion data platform for market analytics"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -m -u 1000 champion && \
    mkdir -p /data && \
    chown -R champion:champion /data

WORKDIR /app

# Install Poetry
RUN pip install --no-cache-dir poetry==1.7.1

# Copy dependency files
COPY pyproject.toml poetry.lock poetry.toml ./

# Install dependencies (without dev dependencies)
RUN poetry config virtualenvs.create false && \
    poetry install --without dev --no-interaction --no-ansi --no-root

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/

# Switch to non-root user
USER champion

# Expose metrics port
EXPOSE 9090

# Default command
CMD ["python", "-m", "champion"]
