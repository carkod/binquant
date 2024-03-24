import os
from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaRouter
from shared.enums import KafkaTopics
from producers.klines_connector import KlinesConnector


broker = KafkaBroker(f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}')
app = FastStream(broker)
klines_topic = KafkaTopics.candlestick_data_topic.value
publisher = broker.publisher(klines_topic)


@broker.subscriber(klines_topic)
async def consume_klines(message):
    print("Consumer: ", message)


@broker.subscriber(KafkaTopics.candlestick_data_topic.value)
def process_kline_stream(self, result):
    """
    Updates market data in DB for research
    """

    symbol = result["k"]["s"]
    if (
        symbol
        and "k" in result
        and "s" in result["k"]
    ):

        candlestick_data = KlinesProducer(symbol).produce(result["k"])
        broker.publisher(candlestick_data, topic=KafkaTopics.candlestick_data_topic.value)

    pass


KlinesConnector().start_stream()

