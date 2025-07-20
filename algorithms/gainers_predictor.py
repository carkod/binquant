import copy
import time
from os import getenv, path
from typing import TYPE_CHECKING

import joblib
import pandas as pd

from models.signals import BollinguerSpread, SignalsConsumer
from shared.apis.binbot_api import BinbotApi
from shared.enums import KafkaTopics, Strategy

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


class GainersPredictor:
    """
    Class to detect anomalies using Isolation Forest algorithm.
    Should use 15m candlesticks for best results.
    The model was trained with contamination set to 0.01 so that it
    detects only the extremme peaks
    """

    def __init__(self, cls: "TechnicalIndicators"):
        """
        Initialize the Isolation Forest model with a specified contamination level.

        :param contamination: The proportion of outliers in the data.
        """
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/usdc_gainer_model.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        self.model = joblib.load(abs_file_path)
        self.binbot = BinbotApi()
        self.ti = cls

    def prep_kline_data(self, symbol: str):
        """
        Prepare the DataFrame for feature extraction.
        This includes renaming columns and ensuring the DataFrame is sorted by time.
        """
        data = self.ti.binbot_api.get_timeseries(symbol)
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        return df

    def compute_features(self, df):
        df["return_5m"] = df["close"].pct_change(5)
        df["return_15m"] = df["close"].pct_change(15)
        df["volatility"] = df["close"].rolling(10).std()
        df["ema5"] = df["close"].ewm(span=5).mean()
        df["ema15"] = df["close"].ewm(span=15).mean()
        df["ema_ratio"] = df["ema5"] / df["ema15"]
        df["volume_ratio"] = df["volume"] / df["volume"].rolling(15).mean()
        return df.iloc[-1][
            ["return_5m", "return_15m", "volatility", "ema_ratio", "volume_ratio"]
        ]

    def build_live_features(self, pairs, df):
        """
        Build features for live predictions
        """
        rows = []
        for symbol in pairs:
            df = self.prep_kline_data(symbol=symbol)
            if df.empty:
                continue
            features = self.compute_features(df)
            features["symbol"] = symbol
            features["current_price"] = df["close"].iloc[-1]
            rows.append(features)
            time.sleep(0.1)
        return pd.DataFrame(rows)

    def predict(self, pairs, df):
        """
        Predict top gainers using the trained model
        """
        new_df = copy.deepcopy(df)
        feature_df = self.build_live_features(pairs=pairs, df=new_df)
        features = feature_df.drop(["symbol", "current_price"], axis=1)
        feature_df["score"] = self.model.predict_proba(features)[:, 1]
        top10 = feature_df.sort_values("score", ascending=False).head(10)
        return top10[["symbol", "score", "current_price"]]

    async def signal(self, pairs, df, current_price, bb_high, bb_mid, bb_low):
        """
        Generate trading signals based on the predictions.
        """
        top10 = self.predict(pairs=pairs, df=df)
        algo = "gainers_predictor"

        if top10.empty:
            return

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
