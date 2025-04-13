import os

from nixtla import NixtlaClient
from requests import Session


class TimeseriesGPT:
    def __init__(self):
        self.nixtla_client = NixtlaClient(api_key=os.environ["NIXTLA_API_KEY"])
        self.base_url = "https://api.nixtla.io/"
        self.session = Session()

    def request(self, path, method="GET", payload=None):
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f'Bearer {os.environ["NIXTLA_API_KEY"]}',
        }

        url = f"{self.base_url}{path}"
        response = self.session.request(
            url=url, method=method, json=payload, headers=headers
        )
        data = response.json()

        return data

    def forecast_multi_series(self, payload):
        return self.request(
            path="forecast_multi_series", method="POST", payload=payload
        )

    def forecast(self, payload):
        return self.request(path="forecast", method="POST", payload=payload)

    def multiple_series_forecast(self, df, df_x=None):
        """
        Raw forecast
        """
        payload = {
            "model": "timegpt-1",
            "freq": "H",
            "fh": 1,
            "y": {"columns": ["ds", "y", "unique_id"], "data": df.values.tolist()},
            "clean_ex_first": True,
            "finetune_steps": 0,
            "finetune_loss": "default",
            "level": [95],
        }

        if df_x is not None:
            payload["x"] = {
                "columns": ["ds", "ex_1", "unique_id"],
                "data": df_x.values.tolist(),
            }

        data = self.forecast_multi_series(payload=payload)
        return data["forecast"]["data"]
