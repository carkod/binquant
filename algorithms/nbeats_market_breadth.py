from os import path

import pandas as pd
import torch.optim as optim
from darts import TimeSeries
from darts.dataprocessing.transformers import MissingValuesFiller, Scaler
from darts.models import NBEATSModel
from torch.optim.lr_scheduler import ReduceLROnPlateau

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
        rel_path = "dist/market_breadth_nbeats_model_v1.pth"
        abs_file_path = path.join(script_dir, rel_path)
        self.model = NBEATSModel.load(abs_file_path)
        self.binbot_api = BinbotApi()
        # Parameters
        self.input_chunk_length = input_chunk_length
        self.forecast_horizon = forecast_horizon

    def pre_process(self, data):
        """
        Pre-process data the same way
        as training data
        """

        df = pd.DataFrame(data)

        # Clean and preprocess
        df["dates"] = pd.to_datetime(df["dates"], format="%Y-%m-%d %H:%M:%S.%f")
        df = df.dropna()
        df = df[(df != 0).all(axis=1)]
        df = df.iloc[1:]
        if df.isnull().values.any():
            df = df.interpolate()
        df.set_index("dates", inplace=True)
        df.sort_index(inplace=True)

        # Feature engineering
        df["diff"] = (df["gainers_count"] - df["losers_count"]).astype("float32")
        df["reversal"] = (df["diff"].shift(1) * df["diff"] < 0).astype("float32")
        df["gainers_count"] = df["gainers_count"].astype("float32")
        df["losers_count"] = df["losers_count"].astype("float32")
        df["total_volume"] = df["total_volume"].astype("float32")

        # Calculate normalized AD diff
        df["adp"] = (df["gainers_count"] - df["losers_count"]) / (
            df["gainers_count"] + df["losers_count"]
        )

        # Add moving average covariates for adp
        for window in [3, 7]:
            df[f"adp_ma{window}"] = df["adp"].rolling(window).mean()
            df[f"adp_ma{window}"] = df[f"adp_ma{window}"].astype("float32")

        # Drop rows with NaNs after adding lags and moving averages
        df.dropna(inplace=True)

        # Convert to Darts TimeSeries
        series = TimeSeries.from_dataframe(
            df, value_cols=["adp"], fill_missing_dates=True, freq="h"
        )

        # Create covariate series for lagged and MA features
        covariate_series = TimeSeries.from_dataframe(
            df,
            value_cols=[
                "total_volume",
                "adp_ma3",
                "adp_ma7",
            ],
            fill_missing_dates=True,
            freq="h",
        )

        # Fill missing values for both series and covariate series
        filler = MissingValuesFiller()
        series_filled = filler.transform(series, method="linear")
        covariate_series_filled = filler.transform(covariate_series, method="linear")

        # Z-score normalization for both series and covariate series
        scaler_series = Scaler()
        scaler_covariate = Scaler()
        series_scaled = scaler_series.fit_transform(series_filled)
        covariate_series_scaled = scaler_covariate.fit_transform(
            covariate_series_filled
        )

        # Train/validation split for both series and covariate series
        train_scaled, val_scaled = (
            series_scaled[: -self.forecast_horizon],
            series_scaled[-self.forecast_horizon :],
        )
        train_orig, val_orig = (
            series_filled[: -self.forecast_horizon],
            series_filled[-self.forecast_horizon :],
        )
        train_covariate_scaled = covariate_series_scaled[: -self.forecast_horizon]

        # Define N-BEATS model with future covariates
        model = NBEATSModel(
            input_chunk_length=self.input_chunk_length,
            output_chunk_length=self.forecast_horizon,
            n_epochs=600,
            batch_size=16,
            random_state=42,
            optimizer_cls=optim.AdamW,
            optimizer_kwargs={"lr": 1e-4},
            lr_scheduler_cls=ReduceLROnPlateau,
            lr_scheduler_kwargs={
                "patience": 15,
                "factor": 0.5,
                "monitor": "train_loss",
            },
        )

        # Fit model with future covariates
        model.fit(train_scaled, past_covariates=covariate_series_scaled, verbose=True)

        # Forecast beyond training data using the last available covariate window
        future_covariates = covariate_series_scaled[
            -(self.input_chunk_length + self.forecast_horizon) :
        ]

        return (
            series_filled,
            future_covariates,
            scaler_series,
            scaler_covariate,
            train_orig,
            val_scaled,
            val_orig,
            model,
            self.forecast_horizon,
            train_covariate_scaled,
        )

    def predict(self, data):
        """
        Predict future market breadth based on the trained model.
        """
        # Production prediction script for N-BEATS model with covariates

        # Load the saved model
        (
            series_filled,
            future_covariates,
            scaler_series,
            scaler_covariate,
            train_orig,
            val_scaled,
            val_orig,
            model,
            self.forecast_horizon,
            train_covariate_scaled,
        ) = self.pre_process(data)

        # Forecast using the latest covariates
        y_pred_scaled = model.predict(
            n=self.forecast_horizon, past_covariates=future_covariates
        )

        # Inverse transform to get forecast in original scale
        y_pred = scaler_series.inverse_transform(y_pred_scaled)

        # Output forecast as DataFrame
        forecast_df = y_pred.to_dataframe()

        # Get the latest prediction value
        latest_prediction = forecast_df.iloc[-1]["adp"]
        print("Latest forecasted adp:", latest_prediction)
