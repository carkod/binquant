from os import path

import pandas as pd
from darts import TimeSeries
from darts.dataprocessing.transformers import MissingValuesFiller, Scaler
from darts.models import NBEATSModel

from shared.apis.binbot_api import BinbotApi


class NBeatsMarketBreadth:
    """
    Market Breadth Algorithm
    This class implements the N-BEATS algorithm for market breadth analysis.
    It is designed to analyze the breadth of market movements using a neural network architecture.
    """

    def __init__(self, input_chunk_length=240, forecast_horizon=72):
        # Initialize pre-trained model
        script_dir = path.dirname(__file__)  # <-- absolute dir the script is in
        rel_path = "dist/market_breadth_nbeats_model_v2.pth"
        abs_file_path = path.join(script_dir, rel_path)
        self.model = NBEATSModel.load(abs_file_path)
        self.binbot_api = BinbotApi()
        # Parameters
        self.input_chunk_length = input_chunk_length
        self.forecast_horizon = forecast_horizon

    async def predict(self, data):
        """
        Predict future market breadth based on the trained model.

        Production prediction script for N-BEATS model with covariates
        """
        
        # Fetch data (production endpoint)
        data = await self.binbot_api.get_market_breadth()
        df = pd.DataFrame(data)

        # Clean and preprocess
        # Use only columns required for inference
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S")
        # Floor all timestamps to the nearest hour to match model training
        df["timestamp"] = df["timestamp"].dt.floor("h")
        # Clean up missing values
        df = df.dropna()
        df = df[(df != 0).all(axis=1)]
        df = df.iloc[1:]
        if df.isnull().values.any():
            df = df.interpolate()
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        # Remove duplicate timestamps, keeping the last occurrence
        df = df[~df.index.duplicated(keep='last')]

        # Feature engineering
        df["diff"] = (df["advancers"] - df["decliners"]).astype("float32")
        df["advancers"] = df["advancers"].astype("float32")
        df["decliners"] = df["decliners"].astype("float32")
        df["total_volume"] = df["total_volume"].astype("float32")

        # Calculate normalized AD diff
        df["adp"] = (df["diff"] / (df["advancers"] + df["decliners"])).astype("float32")

        # Add moving averages if required by the model
        df["adp_ma"] = df["adp_ma"].astype("float32")
        df["adp_ma7"] = (df["adp"].rolling(7).mean()).astype("float32")

        df.dropna(inplace=True)

        # --- Extend covariates into the future ---
        input_chunk_length = 240
        forecast_horizon = 72

        # Create a future index for the forecast horizon
        last_timestamp = df.index[-1]
        future_index = pd.date_range(start=last_timestamp + pd.Timedelta(1, unit='h'), periods=forecast_horizon, freq="h")

        # Forward-fill the last row for covariates
        future_covariates_df = pd.DataFrame(
            [df.iloc[-1][["total_volume", "adp_ma", "adp_ma7"]].values] * forecast_horizon,
            columns=["total_volume", "adp_ma", "adp_ma7"],
            index=future_index
        )

        # Append to the original DataFrame for covariates
        covariate_df_full = pd.concat([
            df[["total_volume", "adp_ma", "adp_ma7"]],
            future_covariates_df
        ])

        # Create TimeSeries for target and covariates
        covariate_series = TimeSeries.from_dataframe(
            covariate_df_full,
            value_cols=["total_volume", "adp_ma", "adp_ma7"],
            fill_missing_dates=True, freq="h"
        )

        # Fill missing values
        filler = MissingValuesFiller()
        covariate_series_filled = filler.transform(covariate_series, method="linear")

        # Z-score normalization (fit on all available data)
        scaler_series = Scaler()
        scaler_covariate = Scaler()

        # Fit scaler_series on the target series (adp)
        series_for_scaling = TimeSeries.from_dataframe(df, value_cols=["adp"], fill_missing_dates=True, freq="h")
        series_for_scaling_filled = filler.transform(series_for_scaling, method="linear")
        scaler_series.fit(series_for_scaling_filled)

        covariate_series_scaled = scaler_covariate.fit_transform(covariate_series_filled)

        # Use the latest available covariates for prediction
        future_covariates = covariate_series_scaled[-(input_chunk_length + forecast_horizon):]

        # Predict
        y_pred_scaled = self.model.predict(n=forecast_horizon, past_covariates=future_covariates)
        y_pred = scaler_series.inverse_transform(y_pred_scaled)

        # Output forecast as DataFrame
        forecast_df = y_pred.to_dataframe()
        latest_prediction = forecast_df.iloc[-1]["adp"]
        print("Latest forecasted adp:", latest_prediction)
        return latest_prediction
