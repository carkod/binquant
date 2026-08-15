import logging
from math import sqrt
from statistics import fmean
from time import time

from pybinbot import (
    BinanceApi,
    FuturesContractMarketData,
    KucoinFutures,
    OpenInterestHistoryPoint,
)

from market_regime.liquidation_state_store import (
    LiquidationStateStore,
    canonical_derivatives_symbol,
)
from market_regime.models import DerivativesPositioningFeatures
from shared.utils import clamp


class DerivativesPositioningProvider:
    """Build cached KuCoin positioning snapshots with Binance confirmation."""

    SNAPSHOT_TTL_MS = 5 * 60 * 1000
    MAX_STALE_SNAPSHOT_MS = 15 * 60 * 1000
    REFERENCE_DATA_TTL_MS = 5 * 60 * 1000
    FUNDING_HISTORY_TTL_MS = 6 * 60 * 60 * 1000
    LIQUIDATION_WINDOW_MS = 15 * 60 * 1000
    OI_HISTORY_POINTS = 100
    OI_HISTORY_INTERVAL_MS = 5 * 60 * 1000
    FUNDING_HISTORY_DAYS = 30

    def __init__(
        self,
        kucoin_futures_api: KucoinFutures,
        binance_api: BinanceApi,
        liquidation_store: LiquidationStateStore | None = None,
    ) -> None:
        self.kucoin_futures_api = kucoin_futures_api
        self.binance_api = binance_api
        self.liquidation_store = liquidation_store
        self._snapshots: dict[str, tuple[int, DerivativesPositioningFeatures]] = {}
        self._contracts_cache: (
            tuple[int, dict[str, FuturesContractMarketData]] | None
        ) = None
        self._binance_funding_cache: tuple[int, dict[str, float]] | None = None
        self._funding_history_cache: dict[str, tuple[int, list[float]]] = {}

    def get_positioning(
        self,
        symbol: str,
        candles_15m: list[list[int | float]],
        *,
        now_ms: int | None = None,
    ) -> DerivativesPositioningFeatures | None:
        """Return a five-minute snapshot without moving TTLs on cache hits."""
        timestamp = now_ms if now_ms is not None else int(time() * 1000)
        normalized_symbol = symbol.upper().strip()
        cached = self._snapshots.get(normalized_symbol)
        if cached is not None and timestamp - cached[0] < self.SNAPSHOT_TTL_MS:
            return cached[1]

        try:
            contracts_observed_at, contracts = self._get_contracts(timestamp)
            contract = contracts.get(normalized_symbol)
            if contract is None:
                return (
                    cached[1]
                    if cached is not None
                    and timestamp - cached[0] <= self.MAX_STALE_SNAPSHOT_MS
                    else None
                )
            snapshot = self._build_snapshot(
                symbol=normalized_symbol,
                contract=contract,
                contracts=contracts,
                candles_15m=candles_15m,
                now_ms=timestamp,
                observed_at_ms=contracts_observed_at,
            )
        except Exception:
            logging.exception(
                "Derivatives positioning refresh failed for %s", normalized_symbol
            )
            return (
                cached[1]
                if cached is not None
                and timestamp - cached[0] <= self.MAX_STALE_SNAPSHOT_MS
                else None
            )

        self._snapshots[normalized_symbol] = (timestamp, snapshot)
        return snapshot

    def _build_snapshot(
        self,
        *,
        symbol: str,
        contract: FuturesContractMarketData,
        contracts: dict[str, FuturesContractMarketData],
        candles_15m: list[list[int | float]],
        now_ms: int,
        observed_at_ms: int,
    ) -> DerivativesPositioningFeatures:
        open_interest = contract.open_interest
        open_interest_notional = self._contract_oi_notional(contract)
        oi_history = self._get_oi_history(symbol)
        oi_by_timestamp = {
            point.timestamp: point.open_interest
            for point in oi_history
            if point.timestamp <= observed_at_ms
        }
        oi_by_timestamp[observed_at_ms] = open_interest
        oi_series = sorted(
            oi_by_timestamp.items(),
            key=lambda item: item[0],
        )

        funding_rate = contract.funding_fee_rate
        funding_granularity = contract.funding_rate_granularity
        annualized_funding_rate = (
            funding_rate * (365 * 24 * 60 * 60 * 1000 / funding_granularity)
            if funding_rate is not None and funding_granularity
            else None
        )
        funding_history = self._get_funding_history(symbol, now_ms)
        funding_percentile = self._percentile_rank(funding_rate, funding_history)

        funding_rates_by_exchange: dict[str, float] = {}
        if funding_rate is not None:
            funding_rates_by_exchange["kucoin"] = funding_rate
        binance_symbol = f"{canonical_derivatives_symbol(symbol)}USDT"
        binance_funding = self._get_binance_funding(now_ms).get(binance_symbol)
        if binance_funding is not None:
            funding_rates_by_exchange["binance"] = binance_funding
        funding_dispersion = (
            max(funding_rates_by_exchange.values())
            - min(funding_rates_by_exchange.values())
            if len(funding_rates_by_exchange) >= 2
            else None
        )

        mark_price = contract.mark_price
        index_price = contract.index_price
        mark_index_basis_bps = (
            (mark_price - index_price) / index_price * 10_000
            if mark_price is not None and index_price
            else None
        )

        stablecoin_oi, coin_oi = self._margin_type_oi(
            contract=contract,
            contracts=contracts,
        )
        normal_volume_notional = self._normal_volume_notional(
            candles_15m=candles_15m,
            contract=contract,
            now_ms=now_ms,
        )
        long_liquidation_notional = 0.0
        short_liquidation_notional = 0.0
        liquidation_intensity = None
        if self.liquidation_store is not None:
            liquidations = self.liquidation_store.window(
                symbol,
                window_ms=self.LIQUIDATION_WINDOW_MS,
                as_of_ms=now_ms,
            )
            long_liquidation_notional = liquidations.long_notional
            short_liquidation_notional = liquidations.short_notional
            if normal_volume_notional > 0:
                liquidation_intensity = (
                    liquidations.total_notional / normal_volume_notional
                )

        turnover_24h = contract.turnover_24h
        oi_to_volume_ratio = (
            open_interest_notional / turnover_24h
            if turnover_24h is not None and turnover_24h > 0
            else None
        )
        oi_zscore = self._zscore([value for _, value in oi_series])
        derivatives_stress_score = self._stress_score(
            annualized_funding_rate=annualized_funding_rate,
            funding_dispersion=funding_dispersion,
            mark_index_basis_bps=mark_index_basis_bps,
            oi_zscore=oi_zscore,
            liquidation_intensity=liquidation_intensity,
            oi_to_volume_ratio=oi_to_volume_ratio,
        )

        return DerivativesPositioningFeatures(
            timestamp=observed_at_ms,
            open_interest=open_interest,
            open_interest_notional=open_interest_notional,
            oi_change_5m=self._oi_change(oi_series, observed_at_ms - 5 * 60 * 1000),
            oi_change_15m=self._oi_change(oi_series, observed_at_ms - 15 * 60 * 1000),
            oi_change_1h=self._oi_change(oi_series, observed_at_ms - 60 * 60 * 1000),
            oi_zscore=oi_zscore,
            current_funding_rate=funding_rate,
            annualized_funding_rate=annualized_funding_rate,
            funding_percentile=funding_percentile,
            funding_dispersion=funding_dispersion,
            funding_rates_by_exchange=funding_rates_by_exchange,
            mark_index_basis_bps=mark_index_basis_bps,
            long_liquidation_notional=long_liquidation_notional,
            short_liquidation_notional=short_liquidation_notional,
            liquidation_intensity=liquidation_intensity,
            stablecoin_margined_oi=stablecoin_oi,
            coin_margined_oi=coin_oi,
            oi_to_volume_ratio=oi_to_volume_ratio,
            derivatives_stress_score=derivatives_stress_score,
            liquidation_source=(
                "binance" if self.liquidation_store is not None else None
            ),
        )

    def _get_contracts(
        self, now_ms: int
    ) -> tuple[int, dict[str, FuturesContractMarketData]]:
        cached = self._contracts_cache
        if cached is not None and now_ms - cached[0] < self.REFERENCE_DATA_TTL_MS:
            return cached
        rows = self.kucoin_futures_api.get_active_contracts()
        contracts = {row.symbol: row for row in rows}
        self._contracts_cache = (now_ms, contracts)
        return self._contracts_cache

    def _get_binance_funding(self, now_ms: int) -> dict[str, float]:
        cached = self._binance_funding_cache
        if cached is not None and now_ms - cached[0] < self.REFERENCE_DATA_TTL_MS:
            return cached[1]
        try:
            rows = self.binance_api.get_futures_funding_rates()
            funding = {row.symbol: row.funding_rate for row in rows}
        except Exception:
            logging.exception("Binance funding refresh failed")
            return cached[1] if cached is not None else {}
        self._binance_funding_cache = (now_ms, funding)
        return funding

    def _get_oi_history(self, symbol: str) -> list[OpenInterestHistoryPoint]:
        try:
            return self.kucoin_futures_api.get_open_interest_history(
                symbol=symbol,
                interval="5min",
                page_size=self.OI_HISTORY_POINTS,
            )
        except Exception:
            logging.exception("KuCoin OI history refresh failed for %s", symbol)
            return []

    def _get_funding_history(self, symbol: str, now_ms: int) -> list[float]:
        cached = self._funding_history_cache.get(symbol)
        if cached is not None and now_ms - cached[0] < self.FUNDING_HISTORY_TTL_MS:
            return cached[1]
        try:
            rows = self.kucoin_futures_api.get_public_funding_history(
                symbol=symbol,
                start_at=(now_ms - self.FUNDING_HISTORY_DAYS * 24 * 60 * 60 * 1000),
                end_at=now_ms,
            )
            history = [row.funding_rate for row in rows]
        except Exception:
            logging.exception("KuCoin funding history refresh failed for %s", symbol)
            return cached[1] if cached is not None else []
        self._funding_history_cache[symbol] = (now_ms, history)
        return history

    @staticmethod
    def _oi_change(
        series: list[tuple[int, float]],
        cutoff: int,
        max_baseline_age_ms: int = OI_HISTORY_INTERVAL_MS,
    ) -> float | None:
        if not series:
            return None
        current = series[-1][1]
        previous = [item for item in series if item[0] <= cutoff]
        if not previous:
            return None
        previous_timestamp, previous_value = previous[-1]
        if cutoff - previous_timestamp > max_baseline_age_ms or previous_value == 0:
            return None
        return current / previous_value - 1.0

    @staticmethod
    def _zscore(values: list[float]) -> float | None:
        if len(values) < 2:
            return None
        mean = fmean(values)
        variance = fmean((value - mean) ** 2 for value in values)
        if variance == 0:
            return 0.0
        return (values[-1] - mean) / sqrt(variance)

    @staticmethod
    def _percentile_rank(value: float | None, history: list[float]) -> float | None:
        if value is None or not history:
            return None
        below = sum(item < value for item in history)
        equal = sum(item == value for item in history)
        return (below + 0.5 * equal) / len(history)

    @classmethod
    def _contract_oi_notional(cls, contract: FuturesContractMarketData) -> float:
        open_interest = contract.open_interest
        multiplier = abs(contract.multiplier)
        if contract.is_inverse:
            return open_interest * multiplier
        mark_price = contract.mark_price or 0.0
        return open_interest * multiplier * mark_price

    @classmethod
    def _margin_type_oi(
        cls,
        *,
        contract: FuturesContractMarketData,
        contracts: dict[str, FuturesContractMarketData],
    ) -> tuple[float, float]:
        base = canonical_derivatives_symbol(contract.symbol)
        stablecoin_oi = 0.0
        coin_oi = 0.0
        for candidate in contracts.values():
            if canonical_derivatives_symbol(candidate.symbol) != base:
                continue
            if candidate.expire_date is not None:
                continue
            notional = cls._contract_oi_notional(candidate)
            settle_currency = (candidate.settle_currency or "").upper()
            if settle_currency in {"USDT", "USDC"}:
                stablecoin_oi += notional
            else:
                coin_oi += notional
        return stablecoin_oi, coin_oi

    @classmethod
    def _normal_volume_notional(
        cls,
        *,
        candles_15m: list[list[int | float]],
        contract: FuturesContractMarketData,
        now_ms: int,
    ) -> float:
        multiplier = abs(contract.multiplier)
        is_inverse = contract.is_inverse
        notionals: list[float] = []
        for candle in candles_15m:
            if len(candle) < 6:
                continue
            close_time = int(candle[6] if len(candle) > 6 else candle[0])
            if close_time > now_ms:
                continue
            close = float(candle[4])
            volume = float(candle[5])
            notional = (
                volume * multiplier if is_inverse else volume * multiplier * close
            )
            notionals.append(notional)
        return fmean(notionals[-20:]) if notionals else 0.0

    @staticmethod
    def _stress_score(
        *,
        annualized_funding_rate: float | None,
        funding_dispersion: float | None,
        mark_index_basis_bps: float | None,
        oi_zscore: float | None,
        liquidation_intensity: float | None,
        oi_to_volume_ratio: float | None,
    ) -> float:
        funding = clamp(abs(annualized_funding_rate or 0.0) / 0.50, 0.0, 1.0)
        dispersion = clamp((funding_dispersion or 0.0) * 10_000 / 5.0, 0.0, 1.0)
        basis = clamp(abs(mark_index_basis_bps or 0.0) / 50.0, 0.0, 1.0)
        crowded_oi = clamp(((oi_zscore or 0.0) - 1.0) / 2.0, 0.0, 1.0)
        liquidations = clamp((liquidation_intensity or 0.0) / 0.10, 0.0, 1.0)
        thin_turnover = clamp(((oi_to_volume_ratio or 0.0) - 1.0) / 3.0, 0.0, 1.0)
        return clamp(
            0.25 * funding
            + 0.15 * dispersion
            + 0.15 * basis
            + 0.20 * crowded_oi
            + 0.15 * liquidations
            + 0.10 * thin_turnover,
            0.0,
            1.0,
        )
