import pytest
from importlib import import_module
from unittest.mock import patch


@pytest.fixture(autouse=True)
def disable_quiet_hours():
    """
    The time-of-day autotrade filter consults real London-local time, which
    would otherwise make tests pass or fail based on the wall clock. Pin
    suppression off across the suite; tests that exercise the filter
    explicitly override it.
    """
    targets = []
    for module_name in (
        "shared.time_of_day_filter",
        "strategies.coinrule.buy_the_dip",
        "strategies.coinrule.price_tracker",
    ):
        try:
            module = import_module(module_name)
        except Exception:
            continue
        if hasattr(module, "is_autotrade_suppressed"):
            targets.append(
                patch.object(module, "is_autotrade_suppressed", return_value=False)
            )
    started = [t.start() for t in targets]
    yield started
    for t in targets:
        t.stop()


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
