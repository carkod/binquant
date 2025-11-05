import logging
from os import getenv, path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd

from models.signals import HABollinguerSpread, SignalsConsumer
from shared.enums import Strategy
from shared.heikin_ashi import HeikinAshi
from shared.utils import safe_format

if TYPE_CHECKING:
    from producers.analytics import CryptoAnalytics


class WhaleSignals:
    def __init__(
        self,
        cls: "CryptoAnalytics",
    ):
        self.symbol = cls.symbol
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/spikehunter_model_v2.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        self.bundle = joblib.load(abs_file_path)
        df = cls.clean_df.copy()
        self.df = HeikinAshi.get_heikin_ashi(df)
        self.binbot_api = cls.binbot_api
        self.current_symbol_data = cls.current_symbol_data
        self.btc_correlation = cls.btc_correlation
        self.telegram_consumer = cls.telegram_consumer
        self.at_consumer = cls.at_consumer

    def cleanup(self):
        self.df.dropna(inplace=True)
        self.df.reset_index(drop=True, inplace=True)

    def gini(self, x: np.ndarray) -> float:
        x = x[x > 0]
        if len(x) == 0:
            return 0.0
        x = np.sort(x)
        n = len(x)
        cum = np.cumsum(x)
        return (2 * np.sum(np.arange(1, n + 1) * x)) / (n * cum[-1]) - (n + 1) / n

    def calculate_money_flow_indicators(self) -> pd.DataFrame:
        self.df["inflow"] = self.df["taker_buy_base_asset_volume"]
        self.df["outflow"] = (
            self.df["quote_asset_volume"] - self.df["taker_buy_base_asset_volume"]
        )
        self.df["net_money_flow"] = self.df["inflow"] - self.df["outflow"]
        self.df["money_flow_ratio"] = self.df["inflow"] / (self.df["outflow"] + 1e-10)
        self.df["taker_buy_ratio"] = self.df["taker_buy_base_asset_volume"] / (
            self.df["volume"] + 1e-10
        )
        self.df["money_flow_strength"] = (self.df["inflow"] - self.df["outflow"]) / (
            self.df["inflow"] + self.df["outflow"] + 1e-10
        )
        self.cleanup()
        return self.df

    def detect_large_orders(self) -> pd.DataFrame:
        volume_thr = self.df["volume"].quantile(0.80)
        quote_thr = self.df["quote_asset_volume"].quantile(0.85)
        trades_thr = self.df["number_of_trades"].quantile(0.75)
        self.df["is_large_order"] = (
            (self.df["volume"] >= volume_thr)
            | (self.df["quote_asset_volume"] >= quote_thr)
        ) & (self.df["number_of_trades"] >= trades_thr)
        self.df["large_order_pct"] = self.df["is_large_order"].rolling(24).mean() * 100
        self.df["avg_trade_size"] = self.df["volume"] / (
            self.df["number_of_trades"] + 1e-10
        )
        self.cleanup()
        return self.df

    def calculate_concentration_metrics(self) -> pd.DataFrame:
        self.df["volume_concentration"] = (
            self.df["volume"]
            .rolling(24)
            .apply(lambda s: self.gini(s.values) * 100, raw=False)
        )
        self.df["volume_concentration"] = (
            self.df["volume"]
            .rolling(24)
            .apply(lambda s: self.gini(s.values) * 100, raw=False)
        )
        self.df["large_order_clustering"] = (
            self.df["large_order_pct"].rolling(12).std() * 2
        )
        self.df["concentration_score"] = 0.6 * self.df["volume_concentration"].fillna(
            0
        ) + 0.4 * self.df["large_order_clustering"].fillna(0)
        self.cleanup()
        return self.df

    def detect_price_and_whale_signals(self) -> pd.DataFrame:
        self.df["price_change_4h"] = self.df["close"].pct_change(4) * 100
        self.df["whale_activity_clustering"] = (
            self.df["large_order_pct"].rolling(3).sum()
        )
        high_activity_thr = self.df["whale_activity_clustering"].quantile(0.80)
        self.df["high_whale_activity"] = (
            self.df["whale_activity_clustering"] > high_activity_thr
        )
        self.df["taker_buy_surge"] = self.df["taker_buy_ratio"] > self.df[
            "taker_buy_ratio"
        ].rolling(24).quantile(0.85)
        self.df["whale_price_predictor"] = (
            self.df["high_whale_activity"]
            & self.df["taker_buy_surge"]
            & (self.df["concentration_score"] > 25)
        )
        self.df["whale_accumulation"] = (self.df["large_order_pct"] > 15) & (
            self.df["money_flow_strength"] > 0.1
        )
        self.df["whale_distribution"] = (self.df["large_order_pct"] > 15) & (
            self.df["money_flow_strength"] < -0.1
        )
        self.cleanup()
        return self.df

    # ---------------------- Real-Time Boolean Signals ----------------------
    def get_whale_signals_current(self) -> dict[str, bool] | None:
        if self.df.empty:
            return None

        self.calculate_money_flow_indicators()
        self.detect_large_orders()
        self.calculate_concentration_metrics()
        self.detect_price_and_whale_signals()

        if self.df.empty:
            return None

        latest = self.df.iloc[-1]
        large_orders_threshold = latest["large_order_pct"] > 15.0
        if len(self.df) >= 3:
            price_mom = (
                self.df["close"].iloc[-1] / self.df["close"].iloc[-3] - 1
            ) * 100
            conc_mom = (
                self.df["concentration_score"].iloc[-1]
                - self.df["concentration_score"].iloc[-3]
            )
            price_concentration_cross = (
                price_mom > 2.0
                and conc_mom > 5.0
                and latest["concentration_score"] > 30
            )
        else:
            price_concentration_cross = False
        whale_activity_surge = (
            latest["whale_activity_clustering"]
            > self.df["whale_activity_clustering"].quantile(0.85)
            and latest["concentration_score"] > 25
            and latest["money_flow_strength"] > 0.05
        )
        taker_buy_threshold = self.df["taker_buy_ratio"].quantile(0.25)
        buying_pressure_low = (
            latest["taker_buy_ratio"] < taker_buy_threshold
            and latest["taker_buy_ratio"] < 0.45
            and latest["money_flow_strength"] < -0.1
        )
        return {
            "large_orders_threshold": bool(large_orders_threshold),
            "price_concentration_cross": bool(price_concentration_cross),
            "whale_activity_surge": bool(whale_activity_surge),
            "buying_pressure_low": bool(buying_pressure_low),
        }

    async def signal(
        self,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ):
        last_spike = self.get_whale_signals_current()

        if not last_spike:
            logging.debug("No recent spike detected for breakout.")
            return

        # When no bullish conditions, check for breakout spikes
        # btc correlation avoids tightly coupled assets
        # if btc price ↑ and btc is negative, we can assume prices will go up
        if (
            last_spike["large_orders_threshold"]
            or last_spike["price_concentration_cross"]
            or last_spike["whale_activity_surge"]
            or last_spike["buying_pressure_low"]
        ):
            algo = "whale_signals"
            autotrade = False

            # Guard against None current_symbol_data (mypy: Optional indexing)
            symbol_data = self.current_symbol_data
            base_asset = symbol_data["base_asset"] if symbol_data else "Base asset"
            quote_asset = symbol_data["quote_asset"] if symbol_data else "Quote asset"

            volume = self.df["volume"].iloc[-1] if "volume" in self.df else 0
            quote_volume = (
                self.df["quote_asset_volume"].iloc[-1]
                if "quote_asset_volume" in self.df
                else 0
            )

            msg = f"""
                - 🔥 [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.symbol}
                - $: +{current_price:,.4f}
                - 📊 {base_asset} volume: {volume}
                - 📊 {quote_asset} volume: {quote_volume}
                - ₿ Correlation: {safe_format(self.btc_correlation)}
                - Autotrade?: {"Yes" if autotrade else "No"}
                - <a href='https://www.binance.com/en/trade/{self.symbol}'>Binance</a>
                - <a href='http://terminal.binbot.in/bots/new/{self.symbol}'>Dashboard trade</a>
                """

            value = SignalsConsumer(
                autotrade=autotrade,
                current_price=current_price,
                msg=msg,
                symbol=self.symbol,
                algo=algo,
                bot_strategy=Strategy.long,
                bb_spreads=HABollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )
            await self.telegram_consumer.send_signal(value.model_dump_json())
            await self.at_consumer.process_autotrade_restrictions(value)

            return True
