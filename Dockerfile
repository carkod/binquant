FROM ubuntu:latest
RUN apt-get update && apt-get install -y --no-install-recommends build-essential python3-dev python3-setuptools python3 python3-pip
COPY . .
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN apt autoremove --purge -y && rm -rf /var/lib/apt/lists/* /etc/apt/sources.list.d/*.list
STOPSIGNAL SIGTERM
EXPOSE 9092 9094
