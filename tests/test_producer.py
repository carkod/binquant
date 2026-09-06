from inspect import getsource
from re import findall
from asyncio import Event, Queue
from datetime import UTC, datetime
from os import environ
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame
from producers.context_evaluator import ContextEvaluator
from producers.klines_connector import KlinesConnector
from pybinbot import (
    AutotradeSettingsSchema,
    BotBase,
    ExchangeId,
    GridDeploymentRequest,
    HABollinguerSpread,
    MarketType,
    Position,
    SignalsConsumer,
    SymbolModel,
)
from market_regime.grid_only_policy import GridOnlyPolicy
from market_regime.models import DerivativesPositioningFeatures


@pytest.fixture
def klines_connector(monkeypatch):
    """
    Patch the KlinesConnector for testing
    """

    class Client:
        def klines(self):
            return None

    def new_init(self, producer, interval="1m"):
        self.interval = interval
        self.last_processed_kline = {}
        self.client = Client()

        self.symbol_partitions = []
        self.partition_count = 0
        self.queue = producer
        self.blacklist_data = []
        self.autotrade_settings = AutotradeSettingsSchema(fiat="USDC")
        self.exchange_info = {"symbols": []}

    async def async_noop(*args, **kwargs):  # noqa: ARG001
        return None

    monkeypatch.setattr(KlinesConnector, "__init__", new_init)
    monkeypatch.setattr(KlinesConnector, "start_stream", async_noop)
    monkeypatch.setattr(KlinesConnector, "process_kline_stream", async_noop)

    return KlinesConnector


@pytest.mark.asyncio
async def test_producer(klines_connector: KlinesConnector):
    res = {
        "e": "kline",
        "E": 1631598140000,
        "s": "BTCUSDC",
        "k": {
            "t": 1631598120000,
            "T": 1631598179999,
            "s": "BTCUSDC",
            "i": "1m",
            "f": 1,
            "L": 1,
            "o": "0.00000000",
            "c": "0.00000000",
            "h": "0.00000000",
            "l": "0.00000000",
            "v": "0.00000000",
            "n": 1,
            "x": False,
            "q": "0.00000000",
            "V": "0.00000000",
            "Q": "0.00000000",
            "B": "0",
        },
    }
    await klines_connector.start_stream()
    await klines_connector.process_kline_stream(res)


@pytest.mark.asyncio
async def test_producer_error(klines_connector: KlinesConnector):
    res = {
        "e": "kline",
        "E": 1631598140000,
        "s": "BTCUSDC",
    }
    # Arrange
    try:
        await klines_connector.start_stream()
        await klines_connector.process_kline_stream(res)
        assert AssertionError()
    except KeyError:
        assert True


@pytest.mark.asyncio
async def test_usdt_filtering():
    """Test that only USDT markets are subscribed to"""
    from unittest.mock import AsyncMock, MagicMock

    # Mock symbols with mixed quote assets
    mock_symbols = [
        SymbolModel(
            id="BTCUSDT", exchange_id="binance", base_asset="BTC", quote_asset="USDT"
        ),
        SymbolModel(
            id="ETHUSDT", exchange_id="binance", base_asset="ETH", quote_asset="USDT"
        ),
        SymbolModel(
            id="BTCUSDC", exchange_id="binance", base_asset="BTC", quote_asset="USDC"
        ),
        SymbolModel(
            id="ETHBTC", exchange_id="binance", base_asset="ETH", quote_asset="BTC"
        ),
        SymbolModel(
            id="BNBUSDT", exchange_id="binance", base_asset="BNB", quote_asset="USDT"
        ),
    ]

    # Set fake BACKEND_DOMAIN before instantiation
    environ["BACKEND_DOMAIN"] = "http://test-url"

    from unittest.mock import patch
    from pybinbot.apis.binbot.base import BinbotApi

    mock_client = MagicMock()
    mock_client.send_message_to_server = AsyncMock()
    mock_queue: Queue[dict[str, Any]] = Queue()

    with (
        patch.object(BinbotApi, "get_symbols", return_value=mock_symbols),
        patch.object(
            BinbotApi,
            "get_autotrade_settings",
            return_value=AutotradeSettingsSchema(fiat="USDT"),
        ),
        patch.object(
            KlinesConnector, "connect_client", AsyncMock(return_value=mock_client)
        ),
    ):
        connector = KlinesConnector(queue=mock_queue)
        await connector.start_stream()
        # Manually add the mock client if not already present (simulate connect_client)
        if not connector.clients:
            connector.clients.append(mock_client)
        assert len(connector.clients) > 0
        if mock_client.send_message_to_server.called:
            call_args = mock_client.send_message_to_server.call_args
            markets = call_args[0][0] if call_args else []
            for market in markets:
                symbol = market.split("@")[0].upper()
                assert symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]


@pytest.mark.asyncio
async def test_dispatch_signal_record_uses_json_mode_payloads_and_links_bot():
    evaluator = object.__new__(ContextEvaluator)
    evaluator.symbol = "MOVEUSDTM"
    evaluator.latest_market_context = None
    evaluator.binbot_api = Mock()
    evaluator.binbot_api.create_signal = AsyncMock(return_value=SimpleNamespace(id=42))

    value = SignalsConsumer(
        autotrade=True,
        current_price=0.01785,
        score=0.91,
        bb_spreads=HABollinguerSpread(
            bb_high=0.019,
            bb_mid=0.018,
            bb_low=0.017,
        ),
        bot_params=BotBase(
            pair="MOVEUSDTM",
            fiat="USDT",
            name="coinrule_price_tracker",
            position=Position.long,
            market_type=MarketType.FUTURES,
        ),
    )

    evaluator.finalize_signal_bot_params(value)

    assert value.bot_params is not None
    assert value.bot_params.fiat_order_size == 4.0
    assert "fiat_order_size" in value.bot_params.model_fields_set
    assert value.open_interest_sizing is None
    await evaluator.dispatch_signal_record(value=value)

    evaluator.binbot_api.create_signal.assert_awaited_once()
    payload = evaluator.binbot_api.create_signal.call_args.kwargs
    assert payload["algorithm_name"] == "coinrule_price_tracker"
    assert payload["symbol"] == "MOVEUSDTM"
    assert payload["direction"] == "long"
    assert payload["bot_params"]["market_type"] == "FUTURES"
    assert payload["bot_params"]["position"] == "long"
    assert payload["bot_params"]["quote_asset"] == "USDC"
    assert payload["bot_params"]["fiat_order_size"] == 4.0
    assert payload["indicators"]["bb_spreads"] == {
        "bb_high": 0.019,
        "bb_mid": 0.018,
        "bb_low": 0.017,
    }
    assert value.bot_params.signal_id == 42


@pytest.mark.asyncio
async def test_dispatch_signal_record_links_grid_ladder():
    evaluator = object.__new__(ContextEvaluator)
    evaluator.symbol = "BTCUSDTM"
    evaluator.latest_market_context = None
    evaluator.binbot_api = Mock()
    evaluator.binbot_api.create_signal = AsyncMock(return_value=SimpleNamespace(id=43))
    grid_params = GridDeploymentRequest(
        symbol="BTCUSDTM",
        fiat="USDT",
        exchange=ExchangeId.KUCOIN,
        market_type=MarketType.FUTURES,
        algorithm_name="grid_ladder",
        generated_at=datetime.now(UTC),
        range_low=95,
        range_high=105,
        level_count=3,
        total_margin=10,
        breakout_low=94,
        breakout_high=106,
        current_price=100,
        allocation_pct=50,
        cash_reserve_pct=25,
    )
    value = SignalsConsumer(
        signal_kind="grid_deploy",
        direction="grid",
        grid_params=grid_params,
    )

    await evaluator.dispatch_signal_record(value=value)

    assert grid_params.signal_id == 43
    payload = evaluator.binbot_api.create_signal.call_args.kwargs
    assert payload["signal_kind"] == "grid_deploy"
    assert payload["grid_params"]["symbol"] == "BTCUSDTM"


@pytest.mark.asyncio
async def test_dispatch_signal_record_timeout_does_not_block_trade_path(caplog):
    evaluator = object.__new__(ContextEvaluator)
    evaluator.symbol = "MOVEUSDTM"
    evaluator.latest_market_context = None
    evaluator.SIGNAL_PERSISTENCE_TIMEOUT_SECONDS = 0.01
    evaluator.binbot_api = Mock()
    request_started = Event()
    request_cancelled = Event()

    async def stalled_create_signal(**_):
        request_started.set()
        try:
            await Event().wait()
        finally:
            request_cancelled.set()

    evaluator.binbot_api.create_signal = AsyncMock(side_effect=stalled_create_signal)
    value = SignalsConsumer(
        autotrade=True,
        bot_params=BotBase(pair="MOVEUSDTM", name="coinrule_price_tracker"),
    )

    with caplog.at_level("WARNING"):
        await evaluator.dispatch_signal_record(value=value)

    assert request_started.is_set()
    assert request_cancelled.is_set()
    assert value.bot_params is not None
    assert value.bot_params.signal_id is None
    assert "trade path continues without signal_id" in caplog.text


@pytest.mark.asyncio
async def test_dispatch_signal_record_snapshots_derivatives_in_indicators():
    evaluator = object.__new__(ContextEvaluator)
    evaluator.symbol = "MOVEUSDTM"
    snapshot_timestamp = int(datetime.now(UTC).timestamp() * 1000)
    derivatives = DerivativesPositioningFeatures(
        timestamp=snapshot_timestamp,
        open_interest=1_000.0,
        open_interest_notional=100_000.0,
        oi_change_15m=-0.02,
        short_liquidation_notional=25_000.0,
        positioning_state="SHORT_SQUEEZE",
    )
    context = Mock()
    context.timestamp = snapshot_timestamp
    context.market_regime = "TREND_UP"
    context.get_symbol_features.return_value = SimpleNamespace(derivatives=derivatives)
    context.model_dump.return_value = {}
    evaluator.latest_market_context = context
    evaluator.binbot_api = Mock()
    evaluator.binbot_api.create_signal = AsyncMock(return_value=SimpleNamespace(id=43))
    value = SignalsConsumer(
        direction="LONG",
        bot_params=BotBase(
            pair="MOVEUSDTM",
            name="liquidation_sweep_pump",
            position=Position.long,
            market_type=MarketType.FUTURES,
        ),
    )

    evaluator.finalize_signal_bot_params(value)

    assert value.bot_params is not None
    assert value.bot_params.fiat_order_size == 8.0
    assert "fiat_order_size" in value.bot_params.model_fields_set
    assert value.open_interest_sizing is not None
    assert value.open_interest_sizing.evidence == "STRONGLY_SUPPORTIVE"
    await evaluator.dispatch_signal_record(value=value)

    payload = evaluator.binbot_api.create_signal.call_args.kwargs
    assert payload["indicators"]["derivatives_positioning"] == derivatives.model_dump(
        mode="json"
    )
    assert payload["bot_params"]["fiat_order_size"] == 8.0
    assert payload["bot_params"]["logs"] == []
    assert payload["indicators"]["estimated_initial_margin"] == 8.0
    assert payload["indicators"]["open_interest_sizing"]["evidence"] == (
        "STRONGLY_SUPPORTIVE"
    )


def test_finalize_signal_bot_params_rejects_snapshot_stale_at_signal_time():
    evaluator = object.__new__(ContextEvaluator)
    evaluator.symbol = "MOVEUSDTM"
    stale_timestamp = int(datetime.now(UTC).timestamp() * 1000) - 16 * 60 * 1000
    derivatives = DerivativesPositioningFeatures(
        timestamp=stale_timestamp,
        open_interest=1_000.0,
        open_interest_notional=100_000.0,
        oi_change_15m=-0.02,
        short_liquidation_notional=25_000.0,
        positioning_state="SHORT_SQUEEZE",
    )
    context = Mock()
    context.timestamp = stale_timestamp
    context.get_symbol_features.return_value = SimpleNamespace(derivatives=derivatives)
    evaluator.latest_market_context = context
    value = SignalsConsumer(
        direction="LONG",
        bot_params=BotBase(
            pair="MOVEUSDTM",
            name="liquidation_sweep_pump",
            position=Position.long,
            market_type=MarketType.FUTURES,
            fiat_order_size=2.0,
        ),
    )

    evaluator.finalize_signal_bot_params(value)

    assert value.bot_params is not None
    assert value.bot_params.fiat_order_size == 4.0
    assert value.open_interest_sizing is None


def test_process_data_does_not_run_disabled_price_tracker():
    source = getsource(ContextEvaluator.process_data)
    safe_signal_names = findall(
        r"_safe_signal\(\s*\n?\s*[\"']([^\"']+)[\"']",
        source,
    )

    assert safe_signal_names == [
        "ActivityBurstPump",
        "RelativeStrengthImpulseRider",
        "TopGainerEarlyMomentum",
        "TopGainerMomentumRecovery",
        "FailedSpikeFade",
        "MarketRegimeNotifier",
        "LiquidationSweepPump",
        "LadderDeployer",
        "TopLoserEarlyMomentum",
    ]


@pytest.mark.asyncio
async def test_process_data_keeps_price_tracker_disabled_when_15m_history_is_empty(
    monkeypatch,
):
    rows = 100
    df_5m = DataFrame(
        {
            "close": [100.0] * rows,
            "ma_7": [100.0] * rows,
            "ma_25": [100.0] * rows,
            "ma_100": [100.0] * rows,
        }
    )

    class FakeCandles:
        def __init__(self, exchange, candles):  # noqa: ARG002
            self.candles = candles

        def pre_process(self):
            return df_5m.copy() if self.candles == "5m" else DataFrame()

        def post_process(self, df):
            return df

        def resample(self, df, interval):  # noqa: ARG002
            return DataFrame()

    activity_signal = AsyncMock()
    evaluator: Any = object.__new__(ContextEvaluator)
    evaluator.exchange = Mock()
    evaluator.symbol = "TESTUSDT"
    evaluator.latest_market_context = None
    evaluator.market_breadth_data = None
    evaluator.at_consumer = SimpleNamespace(
        autotrade_settings=AutotradeSettingsSchema(enable_grid_ladders=False)
    )
    evaluator.symbol_dependent_data = Mock()
    evaluator.indicators_enrichment = lambda df: df
    evaluator.bb_spreads = lambda df: HABollinguerSpread(
        bb_high=101.0,
        bb_mid=100.0,
        bb_low=99.0,
    )

    def load_5m_algorithms():
        evaluator.abp = SimpleNamespace(signal=activity_signal)

    evaluator.load_5m_algorithms = load_5m_algorithms
    monkeypatch.setattr("producers.context_evaluator.Candles", FakeCandles)

    await evaluator.process_data(candles="5m", candles_15m="15m")

    activity_signal.assert_awaited_once()


def test_grid_only_policy_is_disabled_with_grid_ladder_switch() -> None:
    evaluator: Any = object.__new__(ContextEvaluator)
    evaluator.at_consumer = SimpleNamespace(
        autotrade_settings=AutotradeSettingsSchema(enable_grid_ladders=False)
    )
    evaluator.latest_market_context = SimpleNamespace(market_regime="RANGE")
    evaluator.market_breadth_data = None

    policy = evaluator.refresh_grid_only_policy()

    assert policy.allow_grid_ladder is False
    assert policy.block_standard_bots is False
    assert policy.reason == "grid_ladders_disabled"
    assert evaluator.at_consumer.grid_only_policy is policy


def test_grid_only_policy_is_resolved_with_grid_ladder_switch(monkeypatch) -> None:
    evaluator: Any = object.__new__(ContextEvaluator)
    evaluator.at_consumer = SimpleNamespace(
        autotrade_settings=AutotradeSettingsSchema(enable_grid_ladders=True)
    )
    evaluator.latest_market_context = SimpleNamespace(market_regime="RANGE")
    evaluator.market_breadth_data = None
    resolved = GridOnlyPolicy.active(
        direction="toward_range",
        source="market_breadth_ma",
        latest=0.10,
        previous=0.12,
    )
    resolve = Mock(return_value=resolved)
    monkeypatch.setattr(GridOnlyPolicy, "resolve", resolve)

    policy = evaluator.refresh_grid_only_policy()

    assert policy is resolved
    resolve.assert_called_once_with(
        evaluator.latest_market_context,
        evaluator.market_breadth_data,
    )
    assert evaluator.at_consumer.grid_only_policy is resolved
