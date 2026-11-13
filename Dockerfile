FROM python:3.15-slim AS dependencies

ARG PYTHON_VERSION=3.15

ENV PYTHONDONTWRITEBYTECODE=1 \
    UV_PYTHON_INSTALL_DIR=/opt/python \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /app

# Some free-threaded dependencies need a source build. Keep build tools out of
# the runtime image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# Copy only manifests first to use Docker layer cache for deps
COPY pyproject.toml uv.lock ./

# Install uv and sync deps (no dev), then clean caches
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    /root/.local/bin/uv --version && \
    /root/.local/bin/uv sync --python "$PYTHON_VERSION" --no-dev --locked && \
    rm -rf /root/.cache/uv /root/.cache/pip

FROM python:3.15-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

COPY --from=dependencies /opt /opt

# Copy source
COPY . .

STOPSIGNAL SIGTERM

CMD ["python3", "main.py"]
