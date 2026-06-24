from unittest.mock import MagicMock

import pytest
from pybinbot import ExchangeId, SymbolModel

from calibrators.leverage_calibrator import LeverageCalibrator
from market_regime.models import LiveMarketContext, SymbolMarketFeatures


def _make_features(
    symbol: str, *, close: float, atr_pct: float
) -> SymbolMarketFeatures:
    return SymbolMarketFeatures(
        symbol=symbol,
        timestamp=1_700_000_000_000,
        close=close,
        return_pct=0.0,
        ema20=close,
        ema50=close,
        above_ema20=True,
        above_ema50=True,
        trend_score=0.5,
        relative_strength_vs_btc=0.0,
        atr_pct=atr_pct,
        bb_width=0.02,
    )


def _make_context(
    *,
    market_regime: str | None = "RANGE",
    market_stress_score: float = 0.2,
    confidence: float = 0.8,
    symbol_features: dict[str, SymbolMarketFeatures] | None = None,
) -> LiveMarketContext:
    return LiveMarketContext(
        timestamp=1_700_000_000_000,
        fresh_count=1,
        total_tracked_symbols=1,
        coverage_ratio=1.0,
        btc_symbol="XBTUSDTM",
        btc_present=True,
        confidence=confidence,
        is_provisional=False,
        advancers=1,
        decliners=0,
        advancers_ratio=1.0,
        decliners_ratio=0.0,
        advancers_decliners_ratio=1.0,
        average_return=0.0,
        average_relative_strength_vs_btc=0.0,
        pct_above_ema20=1.0,
        pct_above_ema50=1.0,
        average_trend_score=0.5,
        average_atr_pct=0.02,
        average_bb_width=0.02,
        btc_return=0.0,
        btc_trend_score=0.0,
        btc_regime_score=0.0,
        market_stress_score=market_stress_score,
        long_tailwind=0.0,
        short_tailwind=0.0,
        market_regime=market_regime,
        symbol_features=symbol_features or {},
    )


def _make_symbol(
    symbol: str,
    *,
    base_asset: str,
    futures_leverage: int = 1,
    price_precision: int = 2,
) -> SymbolModel:
    return SymbolModel(
        id=symbol,
        exchange_id=ExchangeId.KUCOIN,
        active=True,
        futures_leverage=futures_leverage,
        quote_asset="USDT",
        base_asset=base_asset,
        price_precision=price_precision,
        qty_precision=0,
        min_notional=0,
        is_margin_trading_allowed=False,
    )


class TestTargetLeverage:
    def setup_method(self) -> None:
        self.calibrator = LeverageCalibrator(
            binbot_api=MagicMock(), exchange=ExchangeId.KUCOIN
        )

    @pytest.mark.parametrize(
        "regime,expected",
        [
            ("RANGE", 2),
            ("TREND_UP", 3),
            ("TREND_DOWN", 3),
            ("HIGH_STRESS", 1),
            ("TRANSITIONAL", 1),
            (None, 1),
        ],
    )
    def test_regime_ladder(self, regime: str | None, expected: int) -> None:
        features = _make_features("SOLUSDTM", close=89.0, atr_pct=0.01)
        context = _make_context(
            market_regime=regime, symbol_features={"SOLUSDTM": features}
        )
        assert self.calibrator.target_leverage("SOLUSDTM", 89.0, context) == expected

    def test_expensive_coin_stays_at_1x_even_in_trend(self) -> None:
        features = _make_features("ETHUSDTM", close=2200.0, atr_pct=0.01)
        context = _make_context(
            market_regime="TREND_UP", symbol_features={"ETHUSDTM": features}
        )
        assert self.calibrator.target_leverage("ETHUSDTM", 2200.0, context) == 1

    def test_high_stress_score_forces_1x(self) -> None:
        features = _make_features("SOLUSDTM", close=89.0, atr_pct=0.01)
        context = _make_context(
            market_regime="TREND_UP",
            market_stress_score=0.85,
            symbol_features={"SOLUSDTM": features},
        )
        assert self.calibrator.target_leverage("SOLUSDTM", 89.0, context) == 1

    def test_low_confidence_forces_1x(self) -> None:
        features = _make_features("SOLUSDTM", close=89.0, atr_pct=0.01)
        context = _make_context(
            market_regime="TREND_UP",
            confidence=0.3,
            symbol_features={"SOLUSDTM": features},
        )
        assert self.calibrator.target_leverage("SOLUSDTM", 89.0, context) == 1

    def test_spiky_atr_forces_1x(self) -> None:
        features = _make_features("SOLUSDTM", close=89.0, atr_pct=0.08)
        context = _make_context(
            market_regime="TREND_UP", symbol_features={"SOLUSDTM": features}
        )
        assert self.calibrator.target_leverage("SOLUSDTM", 89.0, context) == 1


class TestCalibrateAll:
    def setup_method(self) -> None:
        self.binbot_api = MagicMock()
        self.calibrator = LeverageCalibrator(
            binbot_api=self.binbot_api, exchange=ExchangeId.KUCOIN
        )
        self.symbols: list[SymbolModel] = [
            _make_symbol("SOLUSDTM", base_asset="SOL"),
            _make_symbol("ETHUSDTM", base_asset="ETH"),
            _make_symbol(
                "TRUMPUSDTM",
                base_asset="TRUMP",
                futures_leverage=2,
                price_precision=4,
            ),
        ]

    def test_writes_only_on_diff(self) -> None:
        # SOL: 1 -> 3 (trend, cheap)         => applied
        # ETH: 1 -> 1 (expensive stays 1x)   => no_change
        # TRUMP: 2 -> 3 (trend, cheap)       => applied
        context = _make_context(
            market_regime="TREND_UP",
            symbol_features={
                "SOLUSDTM": _make_features("SOLUSDTM", close=89.0, atr_pct=0.01),
                "ETHUSDTM": _make_features("ETHUSDTM", close=2200.0, atr_pct=0.01),
                "TRUMPUSDTM": _make_features("TRUMPUSDTM", close=2.2, atr_pct=0.01),
            },
        )

        result = self.calibrator.calibrate_all(context, self.symbols)

        assert result == {"applied": 2, "no_change": 1, "skipped": 0}
        assert self.binbot_api.edit_symbol.call_count == 2
        called_symbols = {
            call.kwargs["symbol"] for call in self.binbot_api.edit_symbol.call_args_list
        }
        called_leverages = {
            call.kwargs["symbol"]: call.kwargs["futures_leverage"]
            for call in self.binbot_api.edit_symbol.call_args_list
        }
        called_exchanges = {
            call.kwargs["symbol"]: call.kwargs["exchange_id"]
            for call in self.binbot_api.edit_symbol.call_args_list
        }
        assert called_symbols == {"SOLUSDTM", "TRUMPUSDTM"}
        assert called_leverages == {"SOLUSDTM": 3, "TRUMPUSDTM": 3}
        assert called_exchanges == {
            "SOLUSDTM": ExchangeId.KUCOIN,
            "TRUMPUSDTM": ExchangeId.KUCOIN,
        }

    def test_payload_echoes_existing_row_fields(self) -> None:
        context = _make_context(
            market_regime="TREND_UP",
            symbol_features={
                "SOLUSDTM": _make_features("SOLUSDTM", close=89.0, atr_pct=0.01),
            },
        )

        self.calibrator.calibrate_all(context, self.symbols)

        payload = self.binbot_api.edit_symbol.call_args.kwargs
        assert payload["symbol"] == "SOLUSDTM"
        assert payload["exchange_id"] == ExchangeId.KUCOIN
        assert payload["futures_leverage"] == 3
        assert "active" not in payload
        assert "quote_asset" not in payload
        assert "base_asset" not in payload

    def test_skips_symbol_not_in_table(self) -> None:
        context = _make_context(
            market_regime="TREND_UP",
            symbol_features={
                "GHOSTUSDTM": _make_features("GHOSTUSDTM", close=1.0, atr_pct=0.01),
            },
        )

        result = self.calibrator.calibrate_all(context, self.symbols)

        assert result == {"applied": 0, "no_change": 0, "skipped": 1}
        self.binbot_api.edit_symbol.assert_not_called()

    def test_logs_and_continues_on_edit_failure(self) -> None:
        self.binbot_api.edit_symbol.side_effect = RuntimeError("boom")
        context = _make_context(
            market_regime="TREND_UP",
            symbol_features={
                "SOLUSDTM": _make_features("SOLUSDTM", close=89.0, atr_pct=0.01),
                "TRUMPUSDTM": _make_features("TRUMPUSDTM", close=2.2, atr_pct=0.01),
            },
        )

        result = self.calibrator.calibrate_all(context, self.symbols)

        # Both attempts fail but the loop completes.
        assert result == {"applied": 0, "no_change": 0, "skipped": 2}
        assert self.binbot_api.edit_symbol.call_count == 2
