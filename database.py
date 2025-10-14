import os

from dotenv import load_dotenv
from pymongo import MongoClient

from shared.apis.binance_api import BinanceApi

load_dotenv()


class KafkaDB(BinanceApi):
    def __init__(self):
        super().__init__()
        client: MongoClient = MongoClient(
            host=os.getenv("MONGO_HOSTNAME"),
            port=int(os.getenv("MONGO_PORT", 27017)),
            authSource="admin",
            username=os.getenv("MONGO_AUTH_USERNAME"),
            password=os.getenv("MONGO_AUTH_PASSWORD"),
        )
        self.db = client[os.environ["MONGO_KAFKA_DATABASE"]]
