import json
from typing import Any, cast

import pytest
from pybinbot import (
    BinanceFundingRate,
    FundingRateHistoryPoint,
    FuturesContractMarketData,
    OpenInterestHistoryPoint,
)

from market_regime.derivatives_positioning import DerivativesPositioningProvider
from market_regime.liquidation_state_store import LiquidationStateStore
from shared.streaming.binance_liquidation_stream import BinanceLiquidationStream


class FakeKucoinFuturesApi:
    def __init__(
        self,
        contracts: list[FuturesContractMarketData],
        oi_history: list[OpenInterestHistoryPoint],
        funding_history: list[FundingRateHistoryPoint],
    ) -> None:
        self.contracts = contracts
        self.oi_history = oi_history
        self.funding_history = funding_history
        self.calls: list[str] = []

    def get_active_contracts(self) -> list[FuturesContractMarketData]:
        self.calls.append("contracts")
        return self.contracts

    def get_open_interest_history(
        self,
        symbol: str,
        interval: str,
        page_size: int,
    ) -> list[OpenInterestHistoryPoint]:
        assert (symbol, interval, page_size) == ("XBTUSDTM", "5min", 100)
        self.calls.append("open_interest")
        return self.oi_history

    def get_public_funding_history(
        self,
        symbol: str,
        start_at: int,
        end_at: int,
    ) -> list[FundingRateHistoryPoint]:
        assert symbol == "XBTUSDTM"
        assert start_at < end_at
        self.calls.append("funding_history")
        return self.funding_history


class FakeBinanceApi:
    def __init__(self, rates: list[BinanceFundingRate]) -> None:
        self.rates = rates
        self.calls = 0

    def get_futures_funding_rates(self) -> list[BinanceFundingRate]:
        self.calls += 1
        return self.rates


def test_positioning_builds_short_window_features_and_caches_before_network() -> None:
    now_ms = 10_000_000
    contracts = [
        FuturesContractMarketData(
            symbol="XBTUSDTM",
            settle_currency="USDT",
            is_inverse=False,
            expire_date=None,
            multiplier=0.001,
            open_interest=120,
            mark_price=100.0,
            index_price=99.0,
            funding_fee_rate=0.0001,
            funding_rate_granularity=28_800_000,
            turnover_24h=1_000.0,
        ),
        FuturesContractMarketData(
            symbol="XBTUSDM",
            settle_currency="XBT",
            is_inverse=True,
            expire_date=None,
            multiplier=-1.0,
            open_interest=50,
            mark_price=100.0,
        ),
    ]
    oi_history = [
        OpenInterestHistoryPoint(
            timestamp=now_ms - 60 * 60 * 1000,
            open_interest=80,
        ),
        OpenInterestHistoryPoint(
            timestamp=now_ms - 15 * 60 * 1000,
            open_interest=100,
        ),
        OpenInterestHistoryPoint(
            timestamp=now_ms - 5 * 60 * 1000,
            open_interest=110,
        ),
        # Defensive regression: future observations must not become the
        # current point when the exchange returns descending/misaligned data.
        OpenInterestHistoryPoint(
            timestamp=now_ms + 5 * 60 * 1000,
            open_interest=999,
        ),
    ]
    kucoin_api = FakeKucoinFuturesApi(
        contracts=contracts,
        oi_history=oi_history,
        funding_history=[
            FundingRateHistoryPoint(funding_rate=-0.0001),
            FundingRateHistoryPoint(funding_rate=0.0001),
            FundingRateHistoryPoint(funding_rate=0.0003),
        ],
    )
    binance_api = FakeBinanceApi(
        [BinanceFundingRate(symbol="BTCUSDT", funding_rate=0.00005)]
    )
    liquidations = LiquidationStateStore()
    liquidations.add(
        timestamp=now_ms - 1_000,
        symbol="BTCUSDT",
        order_side="SELL",
        price=100.0,
        quantity=0.1,
    )
    liquidations.add(
        timestamp=now_ms - 500,
        symbol="BTCUSDT",
        order_side="BUY",
        price=100.0,
        quantity=0.2,
    )
    provider = DerivativesPositioningProvider(
        kucoin_futures_api=cast(Any, kucoin_api),
        binance_api=cast(Any, binance_api),
        liquidation_store=liquidations,
    )
    candles: list[list[int | float]] = [[0, 99, 101, 98, 100, 100, now_ms - 1]]

    first = provider.get_positioning("XBTUSDTM", candles, now_ms=now_ms)
    second = provider.get_positioning("XBTUSDTM", candles, now_ms=now_ms + 1_000)

    assert first is not None
    assert second is first
    assert kucoin_api.calls == ["contracts", "open_interest", "funding_history"]
    assert binance_api.calls == 1
    assert first.timestamp == now_ms
    assert first.open_interest == 120.0
    assert first.open_interest_notional == 12.0
    assert first.oi_change_5m == pytest.approx(120 / 110 - 1)
    assert first.oi_change_15m == pytest.approx(0.2)
    assert first.oi_change_1h == pytest.approx(0.5)
    assert first.oi_zscore is not None
    assert first.current_funding_rate == 0.0001
    assert first.annualized_funding_rate == pytest.approx(0.1095)
    assert first.funding_percentile == 0.5
    assert first.funding_dispersion == pytest.approx(0.00005)
    assert first.funding_rates_by_exchange == {
        "kucoin": 0.0001,
        "binance": 0.00005,
    }
    assert first.mark_index_basis_bps == pytest.approx(101.010101)
    assert first.long_liquidation_notional == 10.0
    assert first.short_liquidation_notional == 20.0
    assert first.liquidation_intensity == 3.0
    assert first.stablecoin_margined_oi == 12.0
    assert first.coin_margined_oi == 50.0


def test_oi_change_is_unavailable_without_a_prior_observation() -> None:
    assert DerivativesPositioningProvider._oi_change([(1_000, 42.0)], 500) is None


def test_oi_change_is_unavailable_when_baseline_is_too_old() -> None:
    assert (
        DerivativesPositioningProvider._oi_change(
            [(1_000, 40.0), (1_000_000, 42.0)],
            500_000,
        )
        is None
    )


def test_binance_force_order_maps_buy_to_short_liquidation() -> None:
    store = LiquidationStateStore()
    stream = BinanceLiquidationStream(store)
    stream.process_message(
        json.dumps(
            {
                "e": "forceOrder",
                "E": 2_000,
                "o": {
                    "s": "BTCUSDT",
                    "S": "BUY",
                    "ap": "100",
                    "l": "2",
                    "z": "7",
                    "T": 2_000,
                },
            }
        )
    )

    window = store.window("XBTUSDTM", window_ms=1_000, as_of_ms=2_500)

    assert window.long_notional == 0.0
    assert window.short_notional == 200.0
