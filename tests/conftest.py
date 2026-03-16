import pytest
from unittest.mock import patch


@pytest.fixture(autouse=True)
def mock_binbot_api():
    # Patch BinbotApi in all relevant modules to prevent real HTTP requests
    patchers = [
        patch("consumers.klines_provider.BinbotApi", autospec=True),
        patch("consumers.autotrade_consumer.BinbotApi", autospec=True),
        patch("producers.klines_connector.BinbotApi", autospec=True),
        patch("shared.streaming.websocket_factory.BinbotApi", autospec=True),
    ]
    mocks = [p.start() for p in patchers]
    for mock in mocks:
        mock.return_value._login_service_account.return_value = None
    yield
    for p in patchers:
        p.stop()
