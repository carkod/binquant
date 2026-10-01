from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from pandas import DataFrame
from pybinbot import ExchangeId, MarketType, Position

from strategies.coinrule.coinrule import Coinrule


def make_coinrule() -> Coinrule:
    ti = SimpleNamespace(
        config=SimpleNamespace(env="test"),
        exchange=ExchangeId.KUCOIN,
        market_type=MarketType.SPOT,
        market_breadth_data=None,
        symbol="TESTUSDT",
        telegram_consumer=SimpleNamespace(dispatch_signal=Mock()),
        at_consumer=SimpleNamespace(process_autotrade_restrictions=AsyncMock()),
        bot_strategy=Position.long,
        current_market_dominance=None,
        market_domination_reversal=True,
        _breadth_cross_tolerance=0.0,
        latest_market_context=None,
        dispatch_signal_record=AsyncMock(),
        df_5m=DataFrame({"close": [1.0] * 12}),
        df_1h=DataFrame({"close": [1.0, 1.0], "twap": [2.0, 2.0]}),
    )
    return Coinrule(ti)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_buy_low_sell_high_never_requests_autotrade():
    algo = make_coinrule()

    await algo.buy_low_sell_high(
        close_price=101.0, rsi=30, ma_25=100.0, bb_high=110.0, bb_mid=100.0, bb_low=90.0
    )

    value = algo.ti.dispatch_signal_record.call_args.kwargs["value"]  # type: ignore[attr-defined]
    assert value.autotrade is False
    msg = algo.telegram_consumer.dispatch_signal.call_args.args[0]  # type: ignore[attr-defined]
    assert "Autotrade is disabled; notification only" in msg


@pytest.mark.asyncio
async def test_twap_momentum_sniper_never_requests_autotrade():
    algo = make_coinrule()

    await algo.twap_momentum_sniper(
        close_price=1.0, bb_high=1.1, bb_low=0.9, bb_mid=1.0
    )

    value = algo.ti.dispatch_signal_record.call_args.kwargs["value"]  # type: ignore[attr-defined]
    assert value.autotrade is False
