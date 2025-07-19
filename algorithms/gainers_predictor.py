import copy
from os import path

import joblib
import pandas as pd

from shared.apis.binbot_api import BinbotApi


class GainersPredictor:
    """
    Class to detect anomalies using Isolation Forest algorithm.
    Should use 15m candlesticks for best results.
    The model was trained with contamination set to 0.01 so that it
    detects only the extremme peaks
    """

    def __init__(self):
        """
        Initialize the Isolation Forest model with a specified contamination level.

        :param contamination: The proportion of outliers in the data.
        """
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/usdc_gainer_model.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        self.model = joblib.load(abs_file_path)
        self.binbot = BinbotApi()

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
            features = self.compute_features(df)
            features["symbol"] = symbol
            features["current_price"] = df["close"].iloc[-1]
            rows.append(features)
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
