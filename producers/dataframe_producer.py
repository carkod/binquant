import json

class DataframeProducer:
    def __init__(self):
        pass

    def on_send_success(self, record_metadata):
        print(f"Message sent successfully: {record_metadata}")
    
    def on_send_error(self, excp):
        print(f"Message failed to send: {excp}")

    def store(self, data):
        candle = self.map_websocket_klines(data)

        # Produce message with asset name as key and candlestick data as value
        self.producer.send(self.topic, partition=0, value=json.dumps(candle)).add_callback(self.on_send_success).add_errback(self.on_send_error)

        # Flush self.producer buffer
        self.producer.flush()