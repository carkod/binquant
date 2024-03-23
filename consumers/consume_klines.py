import json
import os
from kafka import KafkaConsumer
from shared.enums import KafkaTopics

class KlinesConsumer:
    def __init__(self):
        # Create a consumer instance
        print("Started Kafka consumer")
        self.consumer = KafkaConsumer(
            "candlestick_data_topic",
            bootstrap_servers=f'{os.environ["KAFKA_HOST"]}:{os.environ["KAFKA_PORT"]}',
        )
    
    def consume(self):
        for message in self.consumer:
            print(f"Received message: {message}")
            topic = message.topic
            value = message.value
            key = message.key
            timestamp = message.timestamp / 1000.0  # Convert epoch milliseconds to seconds

            # Round the value to the nearest dollar
            rounded_value = round(value)

            print(f"Topic: {topic}, Key: {key}, Value: {rounded_value}, Timestamp: {timestamp}")
