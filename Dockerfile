FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT="/usr/local/"

WORKDIR /app

# Minimal packages to fetch uv
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Copy only manifests first to use Docker layer cache for deps
COPY pyproject.toml ./

# Install uv and sync deps (no dev), then clean caches
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    /root/.local/bin/uv --version && \
    /root/.local/bin/uv lock && \
    /root/.local/bin/uv sync --no-dev --locked && \
    rm -rf /root/.cache/uv /root/.cache/pip

# Copy source
COPY . .

STOPSIGNAL SIGTERM
EXPOSE 8080 80 9092 9093 9094
