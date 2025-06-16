FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y build-essential default-jre-headless && \
    rm -rf /var/lib/apt/lists/*

COPY . .

ENV UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

RUN pip3 install uv
RUN uv pip install --system -r pyproject.toml

RUN rm -rf /root/.cache/pip
RUN rm -rf algorithms/checkpoints/*.ckpt

STOPSIGNAL SIGTERM
EXPOSE 8080 80 9092 9093 9094
