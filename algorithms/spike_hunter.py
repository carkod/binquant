from os import getenv, path
from typing import TYPE_CHECKING

import joblib
import numpy as np
import pandas as pd

from models.signals import BollinguerSpread, SignalsConsumer
from shared.apis.binbot_api import BinbotApi
from shared.enums import KafkaTopics, MarketDominance, Strategy

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


class SpikeHunter:
    def __init__(self, cls: "TechnicalIndicators"):
        """
        Initialize the Isolation Forest model with a specified contamination level.

        :param contamination: The proportion of outliers in the data.
        """
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/spikehunter_model.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        self.model = joblib.load(abs_file_path)
        self.binbot = BinbotApi()
        self.ti = cls
        self.current_market_dominance = cls.current_market_dominance
        self.feature_cols = [
            "price_change",
            "body_size",
            "upper_wick_ratio",
            "lower_wick_ratio",
            "is_bullish",
            "close_open_ratio",
            "adp_ma",
            "adp",
            "advancers",
            "decliners",
        ]

    def match_loser(self, symbol: str):
        """
        Match the symbol with the top losers of the day.
        """
        for loser in self.ti.top_losers_day:
            if loser["symbol"] == symbol:
                return True
        return False

    def fetch_adr_series(self, size=500):
        data = self.binbot.get_adr_series(size=size)
        df_adr = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(data["timestamp"]),
                "adp_ma": data["adp_ma"],
                "advancers": data["advancers"],
                "decliners": data["decliners"],
                "adp": data["adp"],
                "total_volume": data["total_volume"],
            }
        )
        return df_adr

    def label_spike_patterns_with_adr(
        self,
        df,
        adp_col="adp",
        adp_thresh=0.5,
        price_thresh=0.015,
        vol_ratio_thresh=1.3,
        window=10,
    ):
        labels = []
        for i in range(len(df)):
            if i < window:
                labels.append(0)
                continue
            adp_val = df.loc[i, adp_col]
            if np.isnan(adp_val) or adp_val >= adp_thresh:
                labels.append(0)
                continue
            prev_close = df.loc[i - 1, "close"]
            curr_close = df.loc[i, "close"]
            price_change = (curr_close - prev_close) / prev_close
            avg_vol = df.loc[i - window : i - 1, "volume"].mean()
            vol_ratio = df.loc[i, "volume"] / (avg_vol + 1e-6)
            labels.append(
                int(price_change > price_thresh and vol_ratio > vol_ratio_thresh)
            )
        df = df.copy()
        df["label"] = labels
        return df

    def feature_engineering(self, df, window=10, adp_col="adp", adp_thresh=0.5):
        df["timestamp"] = pd.to_datetime(df["close_time"])
        df_adr = self.fetch_adr_series()
        df = df.sort_values("timestamp").reset_index(drop=True)
        df_adr = df_adr.sort_values("timestamp").reset_index(drop=True)

        # Align ADR to candles by nearest previous timestamp
        df = pd.merge_asof(df, df_adr, on="timestamp", direction="backward")
        df[[adp_col, "adp_ma", "advancers", "decliners", "total_volume"]] = df[
            [adp_col, "adp_ma", "advancers", "decliners", "total_volume"]
        ].ffill()

        # Label spikes using the same thresholds as model training
        df = self.label_spike_patterns_with_adr(
            df,
            adp_col=adp_col,
            adp_thresh=adp_thresh,
            price_thresh=0.015,
            vol_ratio_thresh=1.3,
            window=window,
        )

        # Feature calculations
        df["price_change"] = df["close"].pct_change()
        df["body_size"] = abs(df["close"] - df["open"])
        df["upper_wick"] = df["high"] - df[["close", "open"]].max(axis=1)
        df["lower_wick"] = df[["close", "open"]].min(axis=1) - df["low"]
        df["upper_wick_ratio"] = df["upper_wick"] / (df["body_size"] + 1e-6)
        df["lower_wick_ratio"] = df["lower_wick"] / (df["body_size"] + 1e-6)
        df["is_bullish"] = (df["close"] > df["open"]).astype(int)
        df["close_open_ratio"] = (df["close"] - df["open"]) / (df["open"] + 1e-6)

        # Drop NaNs from pct_change and other features
        df = df.dropna().reset_index(drop=True)
        return df

    def predict_spikes(self, df, threshold=0.3):
        df_features = self.feature_engineering(df)
        X = df_features[self.feature_cols]
        probs = self.model.predict_proba(X)[:, 1]
        preds = (probs >= threshold).astype(int)
        df_features["spike_prob"] = probs
        df_features["spike_pred"] = preds

        return df_features.iloc[-1]

    async def signal(
        self,
        df: pd.DataFrame,
        current_price: float,
        bb_high: float,
        bb_low: float,
        bb_mid: float,
    ):
        """
        Generate a signal based on the spike prediction.
        """
        spike_data = self.predict_spikes(df)
        adp_diff = (
            self.ti.market_breadth_data["adp"][-1]
            - self.ti.market_breadth_data["adp"][-2]
        )
        adp_diff_prev = (
            self.ti.market_breadth_data["adp"][-2]
            - self.ti.market_breadth_data["adp"][-3]
        )
        if (
            bool(spike_data["spike_pred"] == 1)
            # Test without ADP because there are spikes when market is bullish
            and adp_diff > 0
            and adp_diff_prev > 0
        ):
            algo = "spike_hunter"

            if self.match_loser(self.ti.symbol):
                algo = "top_loser_spike_hunter"

            msg = f"""
            - [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
            - Current price: {current_price}
            - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
            - <a href='https://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
            """

            value = SignalsConsumer(
                autotrade=False,
                current_price=current_price,
                msg=msg,
                symbol=self.ti.symbol,
                algo=algo,
                bot_strategy=Strategy.long,
                bb_spreads=BollinguerSpread(
                    bb_high=bb_high,
                    bb_mid=bb_mid,
                    bb_low=bb_low,
                ),
            )
            await self.ti.producer.send(
                KafkaTopics.signals.value, value=value.model_dump_json()
            )
