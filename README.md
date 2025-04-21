# Binquant (beta)

The [Binbot](https://github.com/carkod/binbot) quantitative analyses tool.

Performs technical, statistical, AI analysis and feeds it to the signal and telegram systems to eventually create autotrades for Binbot.

Potential replacement of binbot-research, but using Kafka, which hopefully will solve memory and EOL issues that currently are present using websockets polling. It also stores klines (candlestick chart data into Kafka topics).

## Architecture

![image](https://github.com/carkod/binbot/assets/14939793/fbfde06b-1dba-4183-9c4e-26e68a48fa10)

### TODO:

- Support for Spark structured streaming using Spark streaming dataframes
- Integration with TimeGPT and Uni

## Development

1. Use the [docker-compose.yml](https://github.com/carkod/binbot/blob/master/docker-compose.yml) from [Binbot project](https://github.com/carkod/binbot).
2. Use the settings provided in the .vscode folder and run the debugger for Binbot: Api
3. Now you can use the settings provided in the .vscode folder and run the debugger for Binquant: Producer and Binquant: Consumer

## Debugging Kafka

### Recreate Kafka cluster

1. Go into the container and empty data

```
docker exec binquant_kafka rm -rf /opt/bitnami/kafka/data/*
```

2. Restart container. This will also recreate the topics

```
docker start binquant_kafka
```

### Consumers not receiving data

Inside of the docker container i.e. `docker exec -ti binquant_kafka bash`

1. Run the following command, replacing topic with the one you want to inspect.

```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic klines-store-topic
```

This will tell you if the producer is emitting data.

2. In another terminal run

```
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group data-process-group
```

One of the common problems is that consumers are inactive. With that command check that Consumer-IDs are filled up.
