import os
from nixtla import NixtlaClient


class TimeseriesGPT:
    def __init__(self, df):
        self.nixtla_client = NixtlaClient(api_key=os.environ["NIXTLA_API_KEY"])

    def multiple_series_forecast(self, df, period_hr=24):
        """
        Multiple series forecast
        """
        confidence_levels = [80.0, 90.0]
        timegpt_fcst_multiseries_df = self.nixtla_client.forecast(
            df=df,
            h=24,
            level=confidence_levels,
            freq="H",
            time_col="dates",
            target_col="gainers_count",
            hist_exog_list=df["losers_count"],
        )

        return timegpt_fcst_multiseries_df
