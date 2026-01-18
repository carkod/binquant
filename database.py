from os import getenv
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


class KafkaDB:
    def __init__(self):
        super().__init__()
        client: MongoClient = MongoClient(
            host=getenv("MONGO_HOSTNAME"),
            port=int(getenv("MONGO_PORT", 27017)),
            authSource="admin",
            username=getenv("MONGO_AUTH_USERNAME"),
            password=getenv("MONGO_AUTH_PASSWORD"),
        )
        self.db = client[getenv("MONGO_KAFKA_DATABASE", "kafka_db")]
