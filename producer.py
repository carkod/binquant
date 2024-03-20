import logging

from inbound_data.signals_inbound import SignalsInbound
from websocket import (
    WebSocketException,
)

logging.basicConfig(
    filename="./binbot-research.log",
    filemode="a",
    format="%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

if __name__ == "__main__":
    rs = SignalsInbound()
    rs.start_stream()
