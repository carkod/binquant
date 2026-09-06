from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from consumers.klines_provider import KlinesProvider
from market_regime.models import LiveMarketContext
from pybinbot import (
    AutotradeSettingsSchema,
    KucoinKlineIntervals,
    GainerLoserEntry,
    GainersLosersSnapshot,
    MarketBreadthSeries,
    MarketType,
    Status,
    TestAutotradeSettingsSchema,
)
from strategies.top_gainer_momentum_recovery import TopGainerMomentumRecovery


class TestKlinesProvider:
    @patch(
        "consumers.klines_provider.BinbotApi.get_autotrade_settings",
        return_value=AutotradeSettingsSchema(exchange_id="binance"),
    )
    @patch("consumers.klines_provider.BinbotApi.get_symbols", return_value=[])
    @patch("consumers.klines_provider.BinbotApi.get_active_pairs", return_value=set())
    @patch(
        "consumers.klines_provider.BinbotApi.get_test_autotrade_settings",
        return_value=TestAutotradeSettingsSchema(),
    )
    def test_init(
        self,
        mock_get_test_autotrade,
        mock_get_active_pairs,
        mock_get_symbols,
        mock_get_settings,
    ):
        provider = KlinesProvider()
        assert hasattr(provider, "binbot_api")
        assert hasattr(provider, "api")
        assert hasattr(provider, "market_state_store")

    @pytest.mark.asyncio
    @patch("consumers.klines_provider.BinbotApi")
    @patch("consumers.klines_provider.BinanceApi")
    async def test_load_data_on_start(self, MockBinanceApi, MockBinbotApi):
        provider = KlinesProvider()
        api_instance = MagicMock()
        api_instance.get_symbols.return_value = []
        api_instance.get_active_pairs.return_value = set()
        api_instance.get_market_breadth = AsyncMock(return_value={})
        api_instance.get_gainers_losers_series = AsyncMock(return_value=[])
        api_instance.get_autotrade_settings.return_value = AutotradeSettingsSchema(
            exchange_id="kucoin"
        )
        api_instance.get_test_autotrade_settings.return_value = (
            TestAutotradeSettingsSchema()
        )
        provider.binbot_api = api_instance
        await provider.load_data_on_start()
        assert hasattr(provider, "ac_api")

    @pytest.mark.asyncio
    @patch("consumers.klines_provider.BinbotApi")
    @patch("consumers.klines_provider.BinanceApi")
    async def test_load_data_on_start_survives_gainers_losers_failure(
        self, MockBinanceApi, MockBinbotApi
    ):
        provider = KlinesProvider()
        api_instance = MagicMock()
        api_instance.get_active_pairs.return_value = set()
        api_instance.get_market_breadth = AsyncMock(return_value={})
        api_instance.get_gainers_losers_series = AsyncMock(
            side_effect=RuntimeError("backend unavailable")
        )
        provider.binbot_api = api_instance

        await provider.load_data_on_start()

        assert provider.gainers_losers_series == []
        assert provider._last_market_tape_bucket is not None

    @patch(
        "consumers.klines_provider.BinbotApi.get_autotrade_settings",
        return_value=AutotradeSettingsSchema(exchange_id="binance"),
    )
    @patch("consumers.klines_provider.BinbotApi.get_symbols", return_value=[])
    @patch("consumers.klines_provider.BinbotApi.get_active_pairs", return_value=set())
    @patch(
        "consumers.klines_provider.BinbotApi.get_test_autotrade_settings",
        return_value=TestAutotradeSettingsSchema(),
    )
    def test_sync_market_state_from_ui_klines(
        self,
        mock_get_test_autotrade,
        mock_get_active_pairs,
        mock_get_symbols,
        mock_get_settings,
    ):
        provider = KlinesProvider()
        provider.candles_15m = [
            [1000, "1.0", "1.2", "0.9", "1.1", "100", 1999],
            [2000, "1.1", "1.3", "1.0", "1.2", "120", 2999],
        ]

        provider._sync_market_state_from_ui_klines("eth-usdt", provider.candles_15m)
        history = provider.market_state_store.get_symbol_history("eth-usdt")

        assert len(history) == 2
        assert int(history.iloc[-1]["timestamp"]) == 2999
        assert float(history.iloc[-1]["close"]) == 1.2

    @patch("consumers.klines_provider.time", return_value=3.0)
    @patch(
        "consumers.klines_provider.BinbotApi.get_autotrade_settings",
        return_value=AutotradeSettingsSchema(exchange_id="binance"),
    )
    @patch("consumers.klines_provider.BinbotApi.get_symbols", return_value=[])
    @patch("consumers.klines_provider.BinbotApi.get_active_pairs", return_value=set())
    @patch(
        "consumers.klines_provider.BinbotApi.get_test_autotrade_settings",
        return_value=TestAutotradeSettingsSchema(),
    )
    def test_sync_market_state_ignores_unclosed_ui_kline(
        self,
        mock_get_test_autotrade,
        mock_get_active_pairs,
        mock_get_symbols,
        mock_get_settings,
        mock_time,
    ):
        provider = KlinesProvider()
        provider.candles_15m = [
            [1000, "1.0", "1.2", "0.9", "1.1", "100", 2999],
            [3000, "1.1", "1.3", "1.0", "1.2", "120", 3999],
        ]

        closed_candles = provider._sync_market_state_from_ui_klines(
            "eth-usdt", provider.candles_15m
        )
        history = provider.market_state_store.get_symbol_history("eth-usdt")

        assert len(closed_candles) == 1
        assert len(history) == 1
        assert int(history.iloc[-1]["timestamp"]) == 2999
        assert float(history.iloc[-1]["close"]) == 1.1


def test_recovery_bot_snapshot_refreshes_once_per_bucket() -> None:
    provider = cast(Any, object.__new__(KlinesProvider))
    source_bots = [SimpleNamespace(id="source-bot")]
    provider.binbot_api = SimpleNamespace(
        get_bots_by_status=Mock(return_value=source_bots)
    )
    provider.top_gainer_recovery_bots = []
    provider._last_top_gainer_recovery_bucket = None
    current_time = datetime(2026, 9, 3, 12, 0, 0)
    now_ms = int(current_time.timestamp() * 1000)

    provider._refresh_top_gainer_recovery_bots_for_bucket(
        current_time=current_time,
        bucket=100,
    )
    provider._refresh_top_gainer_recovery_bots_for_bucket(
        current_time=current_time,
        bucket=100,
    )

    assert provider.top_gainer_recovery_bots == source_bots
    provider.binbot_api.get_bots_by_status.assert_called_once_with(
        start_date=now_ms - TopGainerMomentumRecovery.WATCH_WINDOW_MS,
        end_date=now_ms,
        status=Status.all,
    )


def test_recovery_bot_snapshot_keeps_previous_data_when_refresh_fails() -> None:
    provider = cast(Any, object.__new__(KlinesProvider))
    previous_bots = [SimpleNamespace(id="previous-source")]
    provider.binbot_api = SimpleNamespace(
        get_bots_by_status=Mock(side_effect=RuntimeError("backend unavailable"))
    )
    provider.top_gainer_recovery_bots = previous_bots
    provider._last_top_gainer_recovery_bucket = None

    provider._refresh_top_gainer_recovery_bots_for_bucket(
        current_time=datetime(2026, 9, 3, 12, 0, 0),
        bucket=100,
    )

    assert provider.top_gainer_recovery_bots == previous_bots


def make_market_context(timestamp: int = 1_000) -> LiveMarketContext:
    return LiveMarketContext(
        timestamp=timestamp,
        fresh_count=50,
        total_tracked_symbols=50,
        coverage_ratio=1.0,
        btc_symbol="XBTUSDTM",
        btc_present=True,
        confidence=1.0,
        is_provisional=False,
        advancers=32,
        decliners=18,
        advancers_ratio=0.64,
        decliners_ratio=0.36,
        advancers_decliners_ratio=32 / 18,
        average_return=0.013,
        average_relative_strength_vs_btc=0.01,
        pct_above_ema20=0.68,
        pct_above_ema50=0.64,
        average_trend_score=0.05,
        average_atr_pct=0.02,
        average_bb_width=0.04,
        btc_return=0.01,
        btc_trend_score=0.03,
        btc_regime_score=0.16,
        market_stress_score=0.1,
        long_tailwind=0.36,
        short_tailwind=0.04,
        market_regime="TREND_UP",
        long_regime_score=0.71,
        short_regime_score=0.18,
        range_regime_score=0.24,
        stress_regime_score=0.1,
    )


def make_provider(
    *,
    existing_context: LiveMarketContext | None = None,
    refreshed_context: LiveMarketContext | None = None,
    latest_context: LiveMarketContext | None = None,
) -> Any:
    provider = cast(Any, object.__new__(KlinesProvider))
    provider.latest_market_context = existing_context
    provider._get_benchmark_symbol = Mock(return_value="XBTUSDTM")
    provider.market_context_accumulator = SimpleNamespace(
        btc_symbol="OLD",
        refresh_context_for_timestamp=Mock(return_value=refreshed_context),
        get_latest_context=Mock(return_value=latest_context),
    )
    return provider


def make_market_tape_refresh_provider() -> Any:
    provider = cast(Any, object.__new__(KlinesProvider))
    provider.binbot_api = SimpleNamespace(
        get_market_breadth=AsyncMock(),
        get_gainers_losers_series=AsyncMock(return_value=[]),
    )
    provider.market_breadth_data = make_market_breadth_series(0.1, 0.09)
    provider.gainers_losers_series = []
    provider._last_market_tape_bucket = None
    provider.interval_15m = KucoinKlineIntervals.FIFTEEN_MINUTES
    return provider


def make_gainers_losers_snapshot(
    symbol: str, price_change_percent: float
) -> GainersLosersSnapshot:
    return GainersLosersSnapshot(
        source="kucoin_futures",
        recorded_at="2026-07-04T12:00:00+00:00",
        top_gainers=[
            GainerLoserEntry(symbol=symbol, price_change_percent=price_change_percent)
        ],
        top_losers=[],
    )


def make_market_breadth_series(latest: float, previous: float) -> MarketBreadthSeries:
    return MarketBreadthSeries(
        timestamp=[
            "2026-07-04T00:15:00+00:00",
            "2026-07-04T00:00:00+00:00",
        ],
        advancers=[32, 30],
        decliners=[18, 20],
        market_breadth=[latest, previous],
        market_breadth_ma=[latest, previous],
        avg_gain=[0.02, 0.01],
        avg_loss=[-0.01, -0.02],
        total_volume=[1000, 900],
        strength_index=[0.2, 0.1],
    )


@pytest.mark.asyncio
async def test_refresh_market_tape_once_per_15m_bucket():
    provider = make_market_tape_refresh_provider()
    refreshed = make_market_breadth_series(0.12, 0.1)
    provider.binbot_api.get_market_breadth.return_value = refreshed
    snapshots = [make_gainers_losers_snapshot("BTRUSDTM", 181.88)]
    provider.binbot_api.get_gainers_losers_series.return_value = snapshots

    await provider._refresh_market_tape_for_bucket(datetime(2026, 7, 4, 12, 15, 1))
    await provider._refresh_market_tape_for_bucket(datetime(2026, 7, 4, 12, 29, 59))

    provider.binbot_api.get_market_breadth.assert_awaited_once()
    provider.binbot_api.get_gainers_losers_series.assert_awaited_once()
    assert provider.market_breadth_data == refreshed
    assert provider.gainers_losers_series == snapshots


@pytest.mark.asyncio
async def test_refresh_market_tape_again_on_next_15m_bucket():
    provider = make_market_tape_refresh_provider()
    first_refresh = make_market_breadth_series(0.12, 0.1)
    second_refresh = make_market_breadth_series(0.09, 0.12)
    provider.binbot_api.get_market_breadth.side_effect = [
        first_refresh,
        second_refresh,
    ]

    await provider._refresh_market_tape_for_bucket(datetime(2026, 7, 4, 12, 15))
    await provider._refresh_market_tape_for_bucket(datetime(2026, 7, 4, 12, 30))

    assert provider.binbot_api.get_market_breadth.await_count == 2
    assert provider.binbot_api.get_gainers_losers_series.await_count == 2
    assert provider.market_breadth_data == second_refresh


@pytest.mark.asyncio
async def test_refresh_market_tape_retains_previous_series_after_failure():
    provider = make_market_tape_refresh_provider()
    previous = [make_gainers_losers_snapshot("OLDUSDTM", 8.0)]
    refreshed_breadth = make_market_breadth_series(0.12, 0.1)
    provider.gainers_losers_series = previous
    provider.binbot_api.get_market_breadth.return_value = refreshed_breadth
    provider.binbot_api.get_gainers_losers_series.side_effect = RuntimeError(
        "deployment skew"
    )
    current_time = datetime(2026, 7, 4, 12, 15, 1)

    await provider._refresh_market_tape_for_bucket(current_time)
    await provider._refresh_market_tape_for_bucket(current_time)

    assert provider.market_breadth_data == refreshed_breadth
    assert provider.gainers_losers_series == previous
    provider.binbot_api.get_gainers_losers_series.assert_awaited_once()


def test_refresh_latest_market_context_keeps_existing_context_when_refresh_is_none():
    existing_context = make_market_context(timestamp=1_000)
    provider = make_provider(existing_context=existing_context)

    result = provider._refresh_latest_market_context(
        timestamp=2_000,
        market_type=MarketType.FUTURES,
    )

    assert result is existing_context
    assert provider.latest_market_context is existing_context
    provider.market_context_accumulator.get_latest_context.assert_not_called()
    assert provider.market_context_accumulator.btc_symbol == "XBTUSDTM"


def test_refresh_latest_market_context_uses_refreshed_context_when_available():
    existing_context = make_market_context(timestamp=1_000)
    refreshed_context = make_market_context(timestamp=2_000)
    provider = make_provider(
        existing_context=existing_context,
        refreshed_context=refreshed_context,
    )

    result = provider._refresh_latest_market_context(
        timestamp=2_000,
        market_type=MarketType.FUTURES,
    )

    assert result is refreshed_context
    assert provider.latest_market_context is refreshed_context
    provider.market_context_accumulator.get_latest_context.assert_not_called()


def test_refresh_latest_market_context_recovers_cached_context_on_cold_provider():
    latest_context = make_market_context(timestamp=1_000)
    provider = make_provider(latest_context=latest_context)

    result = provider._refresh_latest_market_context(
        timestamp=2_000,
        market_type=MarketType.FUTURES,
    )

    assert result is latest_context
    assert provider.latest_market_context is latest_context
    provider.market_context_accumulator.get_latest_context.assert_called_once_with()
