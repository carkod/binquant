FROM python:3.11
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
RUN apt update && apt install --no-install-recommends -y build-essential default-jre
COPY . .

ENV UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

RUN pip3 install uv
RUN uv pip install --system -r pyproject.toml
RUN apt clean && apt autoremove --purge -y && rm -rf /var/lib/apt/lists/* && rm -rf /etc/apt/sources.list.d/*.list
STOPSIGNAL SIGTERM
EXPOSE 8080 80 9092 9093 9094
