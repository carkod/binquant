import logging
import os

from producers.base import BaseProducer
from producers.klines_connector import KlinesConnector

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    filename=None,
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def main():
    logging.info("Starting Binquant Producer with confluent-kafka...")
    base_producer = BaseProducer()
    producer = base_producer.start_producer()
    connector = KlinesConnector(producer)
    connector.start_stream()
    logging.info("Stream started. Waiting for messages... (Press Ctrl+C to exit)")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logging.error(f"Error in Binquant Producer: {error}", exc_info=True)
