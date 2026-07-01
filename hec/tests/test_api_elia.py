import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from hec.data_sources.api_elia import fetch_and_process_forecast


class TestApiElia(unittest.TestCase):
    def test_fetch_forecast_accepts_utc_target_datetime(self):
        response = MagicMock()
        response.json.return_value = {
            "results": [
                {
                    "datetime": "2100-07-02T00:00:00+02:00",
                    "mostrecentforecast": 1.23456,
                    "monitoredcapacity": 7.8912,
                }
            ]
        }
        http_client = MagicMock()
        http_client.get.return_value = response
        app_config = {
            "_http_client": http_client,
            "elia": {
                "api_base_url": "https://example.test/api",
                "timezone": "Europe/Brussels",
                "dataset_solar": "ods087",
                "dataset_solar_hist": "ods032",
                "dataset_wind": "ods086",
                "dataset_wind_hist": "ods031",
                "dataset_grid_load": "ods002",
                "dataset_grid_load_hist": "ods001",
            },
        }

        result = fetch_and_process_forecast(datetime(2100, 7, 2, tzinfo=timezone.utc), app_config, "solar")

        self.assertEqual(
            [
                {
                    "timestamp_utc": "2100-07-02T00:00:00+02:00",
                    "forecast_type": "solar",
                    "resolution_minutes": 15,
                    "most_recent_forecast_mwh": 1.235,
                    "monitored_capacity_mw": 7.891,
                }
            ],
            result,
        )
        requested_url = http_client.get.call_args.args[0]
        self.assertIn("&timezone=Europe/Brussels", requested_url)
        self.assertIn("&refine=datetime%3A2100/07/02", requested_url)


if __name__ == "__main__":
    unittest.main()
