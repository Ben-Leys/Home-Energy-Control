# hec/forecasting/price_predictor.py
import logging
from datetime import datetime, timedelta, date, timezone, time
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from hec.database_ops.db_handler import DatabaseHandler
from hec.utils.utils import is_a_holiday

logger = logging.getLogger(__name__)


class EnergyPricePredictor:
    def __init__(self, db_handler: DatabaseHandler):
        self.db_handler = db_handler
        self.model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        self.is_trained = False
        self.features: List[str] = []

    def get_historical_training_data(self, start_train_date: date, end_train_date: date) \
            -> Optional[pd.DataFrame]:
        """
        Fetches and preprocesses historical data for price prediction training.
        Combines Belpex spot prices (hourly until June 2025) with Elia renewable forecasts (15′).
        Returns a DataFrame with 15‐min UTC intervals from start to end.
        """
        logger.info(f"Fetching historical training data from {start_train_date} to {end_train_date}")

        # 1. Build the 15-min UTC index for the entire period
        start_dt_utc = datetime.combine(start_train_date, time.min, tzinfo=timezone.utc)
        end_dt_local = datetime.combine(end_train_date + timedelta(days=1), time.min) - timedelta(minutes=15)
        end_dt_local = end_dt_local.astimezone()
        full_idx_utc = pd.date_range(
            start=start_dt_utc,
            end=end_dt_local.astimezone(timezone.utc) - timedelta(minutes=15),
            freq="15min"
        )

        # 2. Fetch & upsample DA prices day by day
        da_points = self.db_handler.get_da_prices(target_day_local=start_dt_utc, day_end_local=end_dt_local)
        price_records = []
        grouped_data = {}
        for pt in da_points:
            day = pt.timestamp_utc.date()  # Group by UTC date
            if day not in grouped_data:
                grouped_data[day] = []
            grouped_data[day].append({
                "timestamp_utc": pt.timestamp_utc,
                "price_eur_per_mwh": pt.price_eur_per_mwh,
            })

        # Process grouped data
        for day, records in grouped_data.items():
            price_records.extend(records)

        price_df = (
            pd.DataFrame(price_records)
            .assign(timestamp_utc=lambda df: pd.to_datetime(df["timestamp_utc"]).dt.tz_convert('UTC'))
            .set_index("timestamp_utc")
            .sort_index()
        )
        # drop any exact-duplicate timestamp rows
        price_df = price_df[~price_df.index.duplicated(keep='first')]

        # upsample from hourly → 15T by forward‐fill
        price_15 = (
            price_df
            .resample("15min")
            .ffill()
            .reindex(full_idx_utc)  # ensure we cover every 15-min
        )
        price_15 = price_15.rename(columns={"price_eur_per_mwh": "price_eur_per_mwh"}) \
            .reset_index() \
            .rename(columns={"index": "timestamp_utc"}) \
            .assign(
            gross_price_kwh=lambda df: df.price_eur_per_mwh / 1000,
            resolution_minutes=15
        )

        # 3. Fetch all three Elia forecasts in one go (they are already at 15-min resolution)
        elia_frames = [
            self._prepare_elia_frame(self.db_handler.get_elia_forecasts(f_type, start_dt_utc, end_dt_local),
                                     full_idx_utc, f_type) for f_type in ("solar", "wind", "grid_load")
        ]

        # 4. Merge everything on the common 15-min UTC index
        df_all = price_15.set_index("timestamp_utc")
        for df_fc in elia_frames:
            df_all = df_all.join(df_fc, how="left")

        df_all = df_all.reset_index()

        # 5. Done!
        df_all['day_of_week'] = df_all['timestamp_utc'].dt.dayofweek  # Monday=0, Sunday=6
        df_all['is_weekend'] = df_all['day_of_week'].isin([5, 6]).astype(int)
        df_all['is_holiday'] = df_all['timestamp_utc'].dt.date.apply(is_a_holiday).astype(int)

        logger.info(f"Prepared {len(df_all)} historical records for training.")
        return df_all

    def train_model(self, start_train_date: date = None, end_train_date: date = None):
        """Trains the Random Forest Regressor model."""
        historical_df = self.get_historical_training_data(start_train_date, end_train_date)
        if historical_df is None or historical_df.empty:
            logger.error("Price predictor training failed: No historical data.")
            self.is_trained = False
            return

        self.features = ['day_of_week', 'is_weekend', 'solar_factor', 'wind_factor', 'grid_load_mwh']

        X = historical_df[self.features]
        y = historical_df['gross_price_kwh']

        X = X.fillna(X.mean())  # Simple NaN fill
        y = y.fillna(y.mean())

        if X.empty or y.empty:
            logger.error("Price predictor training failed: Feature matrix or target vector is empty after processing.")
            self.is_trained = False
            return

        logger.info(f"Training price prediction model with {len(X)} samples and features: {self.features}")
        try:
            self.model.fit(X, y)
            self.is_trained = True
            logger.info("Price prediction model trained successfully.")
        except Exception as e:
            logger.error(f"Error during model training: {e}", exc_info=True)
            self.is_trained = False

    def predict_prices_for_day(self, target_predict_date: date,
                               elia_forecasts_for_day: Dict[str, List[Dict[str, Any]]],
                               save_to_db: bool = True) -> Optional[pd.DataFrame]:
        """
        Predicts gross electricity prices for a target day using history Elia forecasts.
        elia_forecasts_for_day: {'solar': [...], 'wind': [...], 'grid_load': [...]}
                                 Each list item is a dict from elia_forecast_api transformed output.
        """
        if not self.is_trained:
            logger.warning("Price prediction model is not trained. Cannot predict.")
            return None

        logger.info(f"Predicting prices for {target_predict_date}")

        num_intervals = 96

        # Create a DataFrame with future timestamps for the target_predict_date
        start_dt_utc = datetime.combine(target_predict_date, datetime.min.time(), tzinfo=timezone.utc)
        future_timestamps_utc = [start_dt_utc + timedelta(minutes=15 * i) for i in range(num_intervals)]
        future_df = pd.DataFrame({'timestamp_utc': future_timestamps_utc})
        future_df['day_of_week'] = future_df['timestamp_utc'].dt.dayofweek
        future_df['is_weekend'] = future_df['day_of_week'].isin([5, 6]).astype(int)
        future_df['is_holiday'] = future_df['timestamp_utc'].dt.date.apply(is_a_holiday).astype(int)

        future_idx = pd.DatetimeIndex(future_df["timestamp_utc"])
        for f_type in ("solar", "wind", "grid_load"):
            recs = elia_forecasts_for_day.get(f_type, [])
            df_fc = self._prepare_elia_frame(recs, future_idx, f_type)
            future_df = future_df.set_index("timestamp_utc").join(df_fc).reset_index()

        X_future = (future_df[self.features].fillna(0))

        predicted_prices_kwh = self.model.predict(X_future)
        future_df['predicted_gross_price_kwh'] = predicted_prices_kwh

        result_df = future_df[
            ['timestamp_utc', 'predicted_gross_price_kwh', 'solar_factor', 'wind_factor', 'grid_load_mwh']]

        if save_to_db and self.db_handler is not None:
            try:
                self.db_handler.store_predicted_prices(result_df)
            except Exception as e:
                logger.error(f"Failed to save predictions to database: {e}", exc_info=True)

        logger.info(f"Predicted {len(future_df)} price intervals for {target_predict_date}.")
        return future_df[['timestamp_utc', 'predicted_gross_price_kwh', 'solar_factor', 'wind_factor', 'grid_load_mwh']]

    @staticmethod
    def _prepare_elia_frame(
            recs: List[Dict[str, Any]],
            full_idx: pd.DatetimeIndex,
            f_type: str
    ) -> pd.DataFrame:
        """
        Turn a list of Elia forecast dicts into a 15′-indexed DataFrame
        with columns: {f_type}_mwh, (optionally) {f_type}_capacity_mw,
        and {f_type}_factor (if capacity exists).
        """
        if not recs:
            # build an all-zeros DataFrame with the right columns for missing grid_load forecast
            cols = {f"{f_type}_mwh": 0}
            if f_type != "grid_load":
                cols[f"{f_type}_capacity_mw"] = 0
                cols[f"{f_type}_factor"] = 0
            return pd.DataFrame(cols, index=full_idx)

        df_fc = (
            pd.DataFrame(recs)
            .assign(timestamp_utc=lambda df: pd.to_datetime(df["timestamp_utc"]).dt.tz_convert("UTC"))
            .set_index("timestamp_utc")
        )

        # rename and select
        cols = [("most_recent_forecast_mwh", f"{f_type}_mwh")]
        if f_type != "grid_load":
            cols.append(("monitored_capacity_mw", f"{f_type}_capacity_mw"))
        df_fc = df_fc.rename({old: new for old, new in cols}, axis=1)
        df_fc = df_fc[[new for _, new in cols]]

        # align + fill
        df_fc = df_fc.reindex(full_idx).ffill().fillna(0)

        # compute factor if capacity exists
        if f_type != "grid_load":
            df_fc[f"{f_type}_factor"] = (
                    df_fc[f"{f_type}_mwh"] /
                    df_fc[f"{f_type}_capacity_mw"].replace({0: np.nan})
            ).fillna(0)

        return df_fc
