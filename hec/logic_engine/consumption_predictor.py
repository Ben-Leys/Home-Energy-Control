import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta, date, time, timezone
from typing import List, Dict, Any, Optional

import pytz

from hec.database_ops.db_handler import DatabaseHandler

logger = logging.getLogger(__name__)


class ConsumptionPredictor:
    def __init__(self, db_handler: DatabaseHandler):
        self.db = db_handler

    @staticmethod
    def _get_historical_periods(forecast_start_date: datetime) -> List[tuple[datetime, datetime]]:
        """
        Determines 4 historical days needed for forecast.
        Same day last year + 3 days before forecast.
        """
        periods = []

        start_last_year = forecast_start_date.replace(year=forecast_start_date.year - 1)
        end_last_year = start_last_year + timedelta(days=1, hours=1)
        periods.append((start_last_year, end_last_year))

        for i in [1, 2, 3]:
            start_prev_day = forecast_start_date - timedelta(days=i)
            end_prev_day = start_prev_day + timedelta(days=1, hours=1)
            periods.append((start_prev_day, end_prev_day))

        return periods

    def _calculate_period_baseline(self, start_utc: datetime, end_utc: datetime) -> Optional[pd.Series]:
        """
        Calculates house consumption per 15 minute interval.
        """
        try:
            # 1. Get raw data
            p1_data = self.db.get_p1_meter_data_for_period(start_utc, end_utc)
            inv_data = self.db.get_inverter_data(start_utc, end_utc)
            bat_data = self.db.get_battery_data_for_period(start_utc, end_utc)
            evcc_data = self.db.get_evcc_data_for_period(start_utc, end_utc)

            if not p1_data or not inv_data:
                logger.debug(f"No p1 meter or inverter data for {start_utc}")
                return None

            # 15 minute base
            idx_15min = pd.date_range(start=start_utc, end=end_utc, freq='15min', tz='UTC')

            # 2. P1 data
            p1_df = pd.DataFrame(p1_data)
            p1_df['timestamp_utc'] = pd.to_datetime(p1_df['timestamp_utc'])
            p1_df = p1_df.set_index('timestamp_utc')

            # Resample to 15min, interpolate
            combined_index = p1_df.index.union(idx_15min).sort_values()
            p1_combined = p1_df.reindex(combined_index)
            p1_interpolated = p1_combined.infer_objects(copy=False).interpolate(method='time')
            p1_resampled = p1_interpolated.reindex(idx_15min)

            p1_import_delta = p1_resampled['total_power_import_kwh'].diff()
            p1_export_delta = p1_resampled['total_power_export_kwh'].diff()
            net_grid_kwh = (p1_import_delta - p1_export_delta).fillna(0)

            # 3. Inverter data
            if not inv_data:
                logger.warning(f"No inverter data for {start_utc}, assume 0")
                solar_kwh = pd.Series(0, index=idx_15min)
            else:
                inv_df = pd.DataFrame(inv_data)
                inv_df['timestamp_utc'] = pd.to_datetime(inv_df['timestamp_utc'])
                inv_df = inv_df.set_index('timestamp_utc')
                inv_df = inv_df[['daily_yield_wh']]

                # Resample to 15min, interpolate
                combined_index = inv_df.index.union(idx_15min).sort_values()
                inv_combined = inv_df.reindex(combined_index)
                inv_interpolated = inv_combined.infer_objects(copy=False).interpolate(method='time')
                inv_resampled = inv_interpolated.reindex(idx_15min)

                solar_delta_wh = inv_resampled['daily_yield_wh'].diff()
                # Midnight
                solar_delta_wh[solar_delta_wh < 0] = inv_resampled['daily_yield_wh'][solar_delta_wh < 0]
                solar_kwh = (solar_delta_wh / 1000.0).fillna(0)

            # 4. Battery data
            if not bat_data:
                logger.warning(f"No battery data for {start_utc}, assume zero.")
                net_battery_kwh = pd.Series(0, index=idx_15min)
            else:
                bat_df = pd.DataFrame(bat_data)
                bat_df['timestamp_utc'] = pd.to_datetime(bat_df['timestamp_utc'])

                total_net_battery_kwh = pd.Series(0.0, index=idx_15min)

                for battery_name, single_bat_df in bat_df.groupby('battery_name'):
                    logger.debug(f"Battery data for: {battery_name}")

                    single_bat_df = single_bat_df.set_index('timestamp_utc')
                    single_bat_df = single_bat_df[['energy_import_kwh', 'energy_export_kwh']].sort_index()

                    bat_combined_index = single_bat_df.index.union(idx_15min).sort_values()
                    bat_combined = single_bat_df.reindex(bat_combined_index)
                    bat_interpolated = bat_combined.infer_objects(copy=False).interpolate(method='time')
                    bat_resampled = bat_interpolated.reindex(idx_15min)

                    bat_import_delta = bat_resampled['energy_import_kwh'].diff().fillna(0)
                    bat_export_delta = bat_resampled['energy_export_kwh'].diff().fillna(0)

                    net_battery_kwh_single = (bat_export_delta - bat_import_delta)

                    total_net_battery_kwh += net_battery_kwh_single

                net_battery_kwh = total_net_battery_kwh.fillna(0)

            # 5. EVCC data
            if not evcc_data:
                logger.debug(f"No EVCC data for {start_utc}, assume zero.")
                evcc_kwh = pd.Series(0.0, index=idx_15min)
            else:
                evcc_df = pd.DataFrame(evcc_data)
                evcc_df['timestamp_utc'] = pd.to_datetime(evcc_df['timestamp_utc']).dt.tz_localize('UTC')
                evcc_df = evcc_df.set_index('timestamp_utc')
                evcc_resampled = evcc_df[['energy_delta']].reindex(idx_15min, fill_value=0.0)
                evcc_kwh = evcc_resampled['energy_delta']

            # 6. Total house consumption
            gross_consumption_kwh = solar_kwh + net_grid_kwh + net_battery_kwh
            house_consumption_kwh = (gross_consumption_kwh - evcc_kwh).clip(lower=0.0)

            # Return data but shifted to show future consumption at the requested time
            return house_consumption_kwh.iloc[2:]

        except Exception as e:
            logger.error(f"Error calculating baseline for {start_utc}: {e}", exc_info=True)
            return None

    def generate_consumption_forecast(self,
                                      forecast_start_date: datetime,
                                      forecast_end_date: datetime) -> Optional[pd.Series]:
        """
        Generates home usage forecast with history averages.

        Args:
            forecast_start_date (datetime): Start of the forecast period.
            forecast_end_date (datetime): End of the forecast period.

        Returns:
            pd.Series: Forecast home usage in 15 minute intervals.
        """

        # 1. Calculate how many 15-minute intervals we actually need
        total_seconds = (forecast_end_date - forecast_start_date).total_seconds()
        num_periods = int(total_seconds / (15 * 60)) + 1

        logger.info(f"Start home usage forecast generation for {num_periods} intervals...")

        # 2. Adjust historical fetching
        query_start_date = forecast_start_date - timedelta(minutes=15)
        hist_periods = self._get_historical_periods(query_start_date)

        historical_data = []
        for start, end in hist_periods:
            baseline = self._calculate_period_baseline(start, end)

            if baseline is not None:
                baseline_segment = baseline.head(num_periods).values
                if len(baseline_segment) == num_periods:
                    historical_data.append(baseline_segment)
                else:
                    logger.warning(f"Insufficient data for {start.date()}: expected {num_periods}, got {len(baseline_segment)}")

        if not historical_data:
            logger.error("Insufficient historical data found. Unable to forecast.")
            return None

        # 3. Make average forecast
        data_matrix = np.array(historical_data)
        mean_forecast_values = np.mean(data_matrix, axis=0)

        forecast_index = pd.date_range(
            start=forecast_start_date,
            periods=num_periods,
            freq='15min',
            tz='UTC'
        )

        # Return requested period
        final_forecast = pd.Series(mean_forecast_values, index=forecast_index)

        logger.info("Successful home consumption forecast")
        return final_forecast


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_main = logging.getLogger(__name__)

    # Initialize
    app_config = {"database": {"type": "sqlite", "path": "home_energy.db"}}
    db_handler = DatabaseHandler(app_config['database'])
    db_handler.initialize_database()

    cd = ConsumptionPredictor(db_handler)

    start_dt = datetime(2026, 3, 10, 23, 0, 0, tzinfo=pytz.UTC)
    end_dt = datetime(2026, 3, 11, 22, 45, 0, tzinfo=pytz.UTC)

    ff = cd.generate_consumption_forecast(start_dt, end_dt)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(ff)
