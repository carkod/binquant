FROM ubuntu:latest
RUN apt update && apt install -y --no-install-recommends build-essential python3-dev python3-setuptools python3 python3-pip default-jre
COPY . .
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN apt autoremove --purge -y && rm -rf /var/lib/apt/lists/* /etc/apt/sources.list.d/*.list
STOPSIGNAL SIGTERM
EXPOSE 9092 9094
