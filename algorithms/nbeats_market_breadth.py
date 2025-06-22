from datetime import datetime
from os import getenv, path
from typing import TYPE_CHECKING, Any

import pandas as pd
from darts import TimeSeries
from darts.dataprocessing.transformers import MissingValuesFiller, Scaler
from darts.models import NBEATSModel

from models.signals import BollinguerSpread, SignalsConsumer
from shared.apis.binbot_api import BinbotApi
from shared.enums import KafkaTopics, Strategy

if TYPE_CHECKING:
    from producers.technical_indicators import TechnicalIndicators


class NBeatsMarketBreadth:
    """
    Market Breadth Algorithm
    This class implements the N-BEATS algorithm for market breadth analysis.
    It is designed to analyze the breadth of market movements using a neural network architecture.
    """

    def __init__(
        self,
        cls: "TechnicalIndicators",
        close_price: float,
        bb_high: float,
        bb_mid: float,
        bb_low: float,
        input_chunk_length=240,
        forecast_horizon=72,
    ):
        self.ti = cls
        self.close_price = close_price
        self.bb_high = bb_high
        self.bb_mid = bb_mid
        self.bb_low = bb_low
        self.binbot_api: BinbotApi
        self.market_breadth_data: dict[Any, Any] | None = None
        self.btc_change_perc = 0.0
        # Initialize pre-trained model directly from zipfile using Darts utility
        script_dir = path.dirname(__file__)
        rel_path = "checkpoints/market_breadth_nbeats_model_v1.pth"
        abs_file_path = path.join(script_dir, rel_path)
        self.model = NBEATSModel.load(abs_file_path)
        self.binbot_api = BinbotApi()
        # Parameters
        self.input_chunk_length = input_chunk_length
        self.forecast_horizon = forecast_horizon
        self.predicted_market_breadth: pd.Series | None = None

    async def predict(self, data) -> pd.Series:
        """
        Predict future market breadth based on the trained model.

        Production prediction script for N-BEATS model with covariates
        """

        # Fetch data (production endpoint)
        self.market_breadth_data = await self.binbot_api.get_market_breadth(size=700)
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
        df = df[~df.index.duplicated(keep="last")]

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
        future_index = pd.date_range(
            start=last_timestamp + pd.Timedelta(1, unit="h"),
            periods=forecast_horizon,
            freq="h",
        )

        # Forward-fill the last row for covariates
        future_covariates_df = pd.DataFrame(
            [df.iloc[-1][["total_volume", "adp_ma", "adp_ma7"]].values]
            * forecast_horizon,
            columns=["total_volume", "adp_ma", "adp_ma7"],
            index=future_index,
        )

        # Append to the original DataFrame for covariates
        covariate_df_full = pd.concat(
            [df[["total_volume", "adp_ma", "adp_ma7"]], future_covariates_df]
        )
<<<<<<< HEAD
        # Ensure index is sorted and unique
        covariate_df_full = covariate_df_full[
            ~covariate_df_full.index.duplicated(keep="last")
        ]
        covariate_df_full = covariate_df_full.sort_index()

=======
>>>>>>> 597698f (Fix Nbeats prediction with improved performance)
        # Ensure index is sorted and unique
        covariate_df_full = covariate_df_full[
            ~covariate_df_full.index.duplicated(keep="last")
        ]
        covariate_df_full = covariate_df_full.sort_index()

        # Create TimeSeries for target and covariates
        covariate_series = TimeSeries.from_dataframe(
            covariate_df_full,
            value_cols=["total_volume", "adp_ma", "adp_ma7"],
            fill_missing_dates=True,
            freq="h",
        )

        # Fill missing values
        filler = MissingValuesFiller()
        covariate_series_filled = filler.transform(covariate_series, method="linear")

        # Z-score normalization (fit on all available data)
        scaler_series = Scaler()
        scaler_covariate = Scaler()

        # Fit scaler_series on the target series (adp)
        series_for_scaling = TimeSeries.from_dataframe(
            df, value_cols=["adp"], fill_missing_dates=True, freq="h"
        )
        series_for_scaling_filled = filler.transform(
            series_for_scaling, method="linear"
        )
        scaler_series.fit(series_for_scaling_filled)

        covariate_series_scaled = scaler_covariate.fit_transform(
            covariate_series_filled
        )

        # Use the latest available covariates for prediction
        future_covariates = covariate_series_scaled[
            -(input_chunk_length + forecast_horizon) :
        ]

        # Predict
        y_pred_scaled = self.model.predict(
            n=forecast_horizon, past_covariates=future_covariates
        )
        y_pred = scaler_series.inverse_transform(y_pred_scaled)

        # Output forecast as DataFrame
        forecast_df = y_pred.to_dataframe()
        return forecast_df["adp"]

    async def market_breadth_signal(self):
        if not self.market_breadth_data:
            self.market_breadth_data = await self.binbot_api.get_market_breadth(
                size=700
            )

        # Reduce network calls
        if (
            # Minimum model data requirement
            self.market_breadth_data is not None
            and "timestamp" in self.market_breadth_data
            and len(self.market_breadth_data["timestamp"]) > 312
            and datetime.now().minute % 30 == 0
            and datetime.now().second == 0
        ):
            self.predicted_market_breadth = await self.predict(self.market_breadth_data)

            if (
                float(self.predicted_market_breadth.iloc[-1]) > 0
                and float(self.predicted_market_breadth.iloc[-2]) > 0
                and float(self.predicted_market_breadth.iloc[-3]) > 0
            ):
                strategy = Strategy.long

                algo = "market_breadth_signal"
                msg = f"""
                - [{getenv("ENV")}] <strong>#{algo} algorithm</strong> #{self.ti.symbol}
                - Current price: {self.close_price}
                - Strategy: {strategy.value}
                - Market breadth: {round(float(self.predicted_market_breadth.iloc[-1]), 2)}
                - <a href='https://www.binance.com/en/trade/{self.ti.symbol}'>Binance</a>
                - <a href='https://terminal.binbot.in/bots/new/{self.ti.symbol}'>Dashboard trade</a>
                """

                value = SignalsConsumer(
                    autotrade=False,
                    current_price=self.close_price,
                    msg=msg,
                    symbol=self.ti.symbol,
                    algo=algo,
                    bot_strategy=strategy,
                    bb_spreads=BollinguerSpread(
                        bb_high=self.bb_high,
                        bb_mid=self.bb_mid,
                        bb_low=self.bb_low,
                    ),
                )

                await self.ti.producer.send(
                    KafkaTopics.signals.value, value=value.model_dump_json()
                )
