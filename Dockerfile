FROM python:3.10-slim
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
RUN apt update && apt install --no-install-recommends -y build-essential default-jre
COPY . .
RUN pip install -r requirements.txt
RUN apt clean && apt autoremove --purge -y && rm -rf /var/lib/apt/lists/* && rm -rf /etc/apt/sources.list.d/*.list
STOPSIGNAL SIGTERM
EXPOSE 8080 80 9092 9093 9094
