import json
from kafka import KafkaConsumer

class SignalsOutbound:
    def __init__(self):
        # Create a consumer instance
        self.consumer = KafkaConsumer(
            'bitcoin', 'ethereum',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
