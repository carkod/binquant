import pytest
from importlib import import_module
from unittest.mock import patch


@pytest.fixture(autouse=True)
def mock_binbot_api():
    # Patch BinbotApi in all relevant modules to prevent real HTTP requests
    modules = [
        import_module("consumers.klines_provider"),
        import_module("consumers.autotrade_consumer"),
        import_module("producers.klines_connector"),
        import_module("shared.streaming.websocket_factory"),
    ]
    patchers = [patch.object(module, "BinbotApi", autospec=True) for module in modules]
    mocks = [p.start() for p in patchers]
    for mock in mocks:
        mock.return_value._login_service_account.return_value = None
    yield
    for p in patchers:
        p.stop()
