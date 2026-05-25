from types import SimpleNamespace
from typing import cast

import pytest
from pybinbot import AutotradeSettingsSchema, MarketType, SignalsConsumer
from producers.context_evaluator import ContextEvaluator
from strategies.grid.ladder_deployer import LadderDeployer


class FakeAutotradeConsumer:
    def __init__(self) -> None:
        self.autotrade_settings = AutotradeSettingsSchema(
            fiat="USDT",
            grid_total_margin=150.0,
            grid_level_count=3,
            grid_allocation_pct=1.0,
            grid_cash_reserve_pct=0.0,
        )
        self.values: list[SignalsConsumer] = []

    async def process_autotrade_restrictions(self, value) -> None:
        self.values.append(value)


class FakeContextEvaluator:
    def __init__(self) -> None:
        self.symbol = "ADAUSDTM"
        self.telegram_consumer = None
        self.at_consumer = FakeAutotradeConsumer()
        self.exchange = "kucoin"
        self.market_type = MarketType.FUTURES
        self.latest_market_context = SimpleNamespace(
            market_regime="RANGE",
            regime_is_transitioning=False,
            model_dump=lambda mode: {"market_regime": "RANGE"},
        )
        self.dispatched_values: list[SignalsConsumer] = []

    def dispatch_signal_record(self, value) -> None:
        self.dispatched_values.append(value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("bb_low", "bb_high"),
    [
        (99.0, 101.0),
        (98.0, 102.0),
        (97.0, 103.0),
    ],
)
async def test_ladder_deployer_uses_three_total_levels(
    monkeypatch,
    bb_low: float,
    bb_high: float,
) -> None:
    evaluator = FakeContextEvaluator()
    deployer = LadderDeployer(cast(ContextEvaluator, evaluator))
    monkeypatch.setattr(deployer, "_bb_stable", lambda n, max_change_pct: True)
    monkeypatch.setattr(
        "strategies.grid.ladder_deployer.resolve_symbol_features",
        lambda context, symbol: SimpleNamespace(
            micro_regime="RANGE",
            micro_regime_transition=None,
        ),
    )

    await deployer.signal(
        current_price=100.0,
        bb_high=bb_high,
        bb_mid=100.0,
        bb_low=bb_low,
    )

    assert len(evaluator.dispatched_values) == 1
    value = evaluator.dispatched_values[0]
    assert value.grid_params is not None
    assert value.grid_params.level_count == 3
    assert value.grid_params.allocation_pct == 1.0
    assert value.grid_params.cash_reserve_pct == 0.0
