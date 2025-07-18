from os import path

import joblib
import pandas as pd

from shared.apis.binbot_api import BinbotApi


class IsolationForestAnomalies:
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
        rel_path = "checkpoints/isolation_forest_anomalies.pkl"
        abs_file_path = path.join(script_dir, rel_path)
        self.model = joblib.load(abs_file_path)
        self.binbot = BinbotApi()

    def predict(self, df):
        """
        Predict anomalies in the data.

        :param X: The input data for prediction.
        :return: An array of predictions where -1 indicates an anomaly and 1 indicates normal data.
        """

        # Convert types
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        df["open_time"] = pd.to_datetime(df["open_time"])
        df.set_index("open_time", inplace=True)

        # Calculate relative price change (percentage change)
        df["close_pct_change"] = df["close"].pct_change().fillna(0)

        # Use relative price change for anomaly detection
        X = df[["close_pct_change"]].values

        # Predict anomalies using the loaded model
        predictions = self.model.predict(X)
        df["anomaly_loaded"] = predictions
        anomalies_loaded = df[df["anomaly_loaded"] == -1]
        return anomalies_loaded.index[-1] if not anomalies_loaded.empty else None
