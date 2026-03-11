from time import process_time

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List
from datetime import datetime, timezone, timedelta

from hec.core.app_state import GLOBAL_APP_STATE
from hec.logic_engine.consumption_predictor import ConsumptionPredictor
from hec.database_ops.db_handler import DatabaseHandler
from hec.utils.utils import process_price_points_to_app_state

logger = logging.getLogger(__name__)


class BatteryPredictor:
    def __init__(self, app_config: Dict):
        self.app_config = app_config
        self.capacity_kwh = 0
        self.max_charge_kw = 0
        self.max_discharge_kw = 0

        for battery in app_config.get("batteries"):
            self.capacity_kwh += battery.get("capacity_kwh", 0)
            self.max_charge_kw += battery.get("max_charge_W", 0) / 1000
            self.max_discharge_kw += battery.get("max_discharge_W", 0) / 1000

        self.panel_kw = self.app_config.get('inverter', {}).get('panel_peak_w', 0) / 1000.0

        logger.info(
            f"BatteryPredictor initialised: {self.capacity_kwh} kWh capacity, charge max {self.max_charge_kw} kW, "
            f"discharge max {self.max_discharge_kw} kW"
        )

    def _fetch_aligned_solar(self, start_dt: datetime, end_dt: datetime, db_handler: Any,
                             freq_index: pd.Index) -> pd.Series:
        """Fetches solar data and aligns it to the consumption index."""
        solar_records = db_handler.get_elia_forecasts("solar", start_dt, end_dt)

        if not solar_records:
            return pd.Series(0.0, index=freq_index)

        df_solar = pd.DataFrame(solar_records)
        df_solar.set_index('timestamp_utc', inplace=True)

        # Calculate Solar Power in kW based on relative production * installation capacity
        df_solar['relative_prod'] = df_solar['most_recent_forecast_mwh'] / df_solar['monitored_capacity_mw']
        df_solar['solar_kw'] = df_solar['relative_prod'] * self.panel_kw

        # Convert power (kW) to energy (kWh) per 15 mins
        df_solar['solar_kwh'] = df_solar['solar_kw'] * 0.25

        # Align to the consumption index (forward fill or fillna with 0)
        df_solar_aligned = df_solar['solar_kwh'].reindex(freq_index).fillna(0.0)
        return df_solar_aligned

    def generate_plan(self, start_dt: datetime, end_dt: datetime, consumption_s: pd.Series, db_handler: Any,
                      initial_soc_kwh: float = 0.0) -> pd.DataFrame:
        """
        Generates a 15-minute resolution battery plan based on excess solar and consumption.
        """
        # Ensure consumption is a Series
        if isinstance(consumption_s, pd.DataFrame):
            consumption_s = consumption_s.iloc[:, 0]

        solar_s = self._fetch_aligned_solar(start_dt, end_dt, db_handler, consumption_s.index)

        # Build the initial state DataFrame
        df_plan = pd.DataFrame(index=consumption_s.index)
        df_plan['consump_kwh'] = consumption_s
        df_plan['solar_kwh'] = solar_s
        df_plan['net_kwh'] = df_plan['solar_kwh'] - df_plan['consump_kwh']

        # Output columns
        charge_amounts = []
        discharge_amounts = []
        soc_list = []

        current_soc = initial_soc_kwh
        dt_hours = 0.25
        eff = 0.9

        for net in df_plan['net_kwh']:
            charge_amt = 0.0
            discharge_amt = 0.0

            if net > 0:  # Excess Solar -> Charge
                # 1. Calculate max acceptable energy from solar in this 15-min block considering the 95% taper
                e_to_95 = max(0.0, 0.95 * self.capacity_kwh - current_soc)
                t_to_95 = min(dt_hours, e_to_95 / (self.max_charge_kw * eff)) if self.max_charge_kw > 0 else 0
                t_after_95 = dt_hours - t_to_95

                max_solar_accepted = (t_to_95 * self.max_charge_kw) + (t_after_95 * (self.max_charge_kw / 2))

                attempted_charge = min(net, max_solar_accepted)
                actual_added = min(attempted_charge * eff, self.capacity_kwh - current_soc)

                charge_amt = actual_added / eff
                current_soc += actual_added

            elif net < 0:  # Deficit -> Discharge
                deficit = abs(net)

                # 1. Calculate max deliverable energy to the house in this 15-min block considering the 5% taper
                e_above_5 = max(0.0, current_soc - 0.05 * self.capacity_kwh)
                t_to_5 = min(dt_hours, e_above_5 / (self.max_discharge_kw / eff)) if self.max_discharge_kw > 0 else 0
                t_after_5 = dt_hours - t_to_5

                max_house_delivered = (t_to_5 * self.max_discharge_kw) + (t_after_5 * (self.max_discharge_kw / 2))

                attempted_discharge = min(deficit, max_house_delivered)
                actual_removed = min(attempted_discharge / eff, current_soc)

                discharge_amt = actual_removed * eff
                current_soc -= actual_removed

            charge_amounts.append(charge_amt)
            discharge_amounts.append(discharge_amt)
            soc_list.append(current_soc)

        df_plan['charge_kwh'] = charge_amounts
        df_plan['discharge_kwh'] = discharge_amounts
        df_plan['soc_kwh'] = soc_list
        df_plan['soc_pct'] = (df_plan['soc_kwh'] / self.capacity_kwh) * 100 if self.capacity_kwh > 0 else 0

        # Optional: Grid interaction after battery
        df_plan['grid_in'] = np.clip(
            np.where(df_plan['net_kwh'] < 0,
                     abs(df_plan['net_kwh']) - df_plan['discharge_kwh'],
                     0),
            a_min=0, a_max=None
        )

        df_plan['grid_out'] = np.clip(
            np.where(df_plan['net_kwh'] > 0,
                     df_plan['net_kwh'] - df_plan['charge_kwh'],
                     0),
            a_min=0, a_max=None
        )

        df_plan = df_plan.map(lambda x: 0.0 if abs(x) < 1e-9 else x)

        return df_plan

    def optimize_plan(self, df_plan, actual_soc: float, state: Dict) -> pd.DataFrame:
        """
            Optimizes the battery plan using look-ahead pricing logic.
            Calculates exact amounts needed and provides duration for the mediator.
            """

        # 1. Extract and align prices from global_app_state
        prices_today = state.get("electricity_prices_today", [])
        prices_tomorrow = state.get("electricity_prices_tomorrow", [])
        all_prices = prices_today + prices_tomorrow

        price_map = {}
        for p in all_prices:
            try:
                contract = p.active_contract_type
                import_price = p.net_prices_eur_per_kwh[contract]['buy']
                export_price = p.net_prices_eur_per_kwh[contract]['sell']
                price_map[p.interval_start_local] = {'import_price': import_price, 'export_price': export_price}
            except (KeyError, TypeError):
                pass

        df_prices = pd.DataFrame.from_dict(price_map, orient='index')
        df_opt = df_plan.merge(df_prices, left_index=True, right_index=True, how='left')
        df_opt['import_price'] = df_opt['import_price'].ffill()
        df_opt['export_price'] = df_opt['export_price'].ffill()

        # 2. Setup decision and recalculation columns
        df_opt['block_discharge'] = False
        df_opt['block_charge'] = False
        df_opt['forge_charge'] = False
        df_opt['forge_charge_minutes'] = 0.0
        df_opt['limit_inverter'] = None

        df_opt['new_charge_kwh'] = 0.0
        df_opt['new_discharge_kwh'] = 0.0
        df_opt['new_soc_kwh'] = 0.0
        df_opt['new_soc_pct'] = 0.0

        current_soc = actual_soc
        dt_hours = 0.25
        eff = 0.9

        timestamps = df_opt.index.tolist()

        # 3. Step through time and apply logic
        for i, current_time in enumerate(timestamps):
            row = df_opt.loc[current_time]
            imp_price = row['import_price']
            exp_price = row['export_price']
            net = row['net_kwh']

            new_charge_amt = 0.0  # Energy pulled from grid/solar
            new_discharge_amt = 0.0  # Energy delivered to house

            if pd.isna(imp_price):
                df_opt.at[current_time, 'new_soc_kwh'] = current_soc
                continue

            future_window = df_opt.iloc[i + 1:]

            # Rule C: Limit inverter
            if exp_price < 0:
                consumption_w = row['consump_kwh'] / dt_hours * 1000
                df_opt.at[current_time, 'limit_inverter'] = consumption_w + (self.max_charge_kw * 1000)

            # Rule B: Force Charge (Analyze exact future deficits)
            future_high_prices = future_window[future_window['import_price'] >= (imp_price + 0.10)]
            future_deficits = future_high_prices[future_high_prices['net_kwh'] < 0]

            # Calculate exactly how much energy we will need from the battery during high prices
            # We divide by eff because to deliver 1kWh to the house, we need 1.11kWh in the battery
            total_future_deficit_kwh = abs(future_deficits['net_kwh'].sum()) / eff

            # How much more energy does the battery need right now to cover that?
            energy_needed_in_battery = total_future_deficit_kwh - current_soc

            if energy_needed_in_battery > 0.01 and current_soc < self.capacity_kwh:
                df_opt.at[current_time, 'forge_charge'] = True

                # 1. Max we can physically add in 15 mins
                max_addable = self.max_charge_kw * dt_hours * eff
                # 2. Max room left in battery
                room_left = self.capacity_kwh - current_soc

                # The actual amount we will add to the SOC
                actual_added = min(energy_needed_in_battery, max_addable, room_left)

                # Energy pulled from the grid to achieve this
                new_charge_amt = actual_added / eff
                current_soc += actual_added

                # Calculate duration in minutes: (Energy_pulled / Power) * 60
                charge_time_hours = new_charge_amt / self.max_charge_kw if self.max_charge_kw > 0 else 0
                df_opt.at[current_time, 'forge_charge_minutes'] = min(15.0, charge_time_hours * 60)

            else:
                # Rule A: Block Actions
                # A.2 Block Discharge: High prices coming, but we have enough SOC. Save it.
                future_deficits_all = future_window[future_window['net_kwh'] < 0]
                if not future_deficits_all.empty and net < 0:
                    max_future_imp_price = future_deficits_all['import_price'].max()
                    if max_future_imp_price > imp_price + 0.02:
                        df_opt.at[current_time, 'block_discharge'] = True

                # A.1 Block Charge: If future import prices are heavily negative, we might
                # want to export our solar now (if export price is good) and charge later.
                future_cheap = future_window[future_window['import_price'] < exp_price - 0.05]
                if not future_cheap.empty and net > 0:
                    df_opt.at[current_time, 'block_charge'] = True

                # Calculate normal flows based on block flags
                if net > 0 and not df_opt.at[current_time, 'block_charge']:
                    attempted_charge = min(net, self.max_charge_kw * dt_hours)
                    actual_added = min(attempted_charge * eff, self.capacity_kwh - current_soc)
                    new_charge_amt = actual_added / eff
                    current_soc += actual_added

                elif net < 0 and not df_opt.at[current_time, 'block_discharge']:
                    attempted_discharge = min(abs(net), self.max_discharge_kw * dt_hours)
                    actual_removed = min(attempted_discharge / eff, current_soc)
                    new_discharge_amt = actual_removed * eff
                    current_soc -= actual_removed

            # Ensure floats don't accumulate noise
            current_soc = round(current_soc, 6)

            # Save recalculated columns
            df_opt.at[current_time, 'new_charge_kwh'] = new_charge_amt
            df_opt.at[current_time, 'new_discharge_kwh'] = new_discharge_amt
            df_opt.at[current_time, 'new_soc_kwh'] = current_soc
            df_opt.at[current_time, 'new_soc_pct'] = (
                                                                 current_soc / self.capacity_kwh) * 100 if self.capacity_kwh > 0 else 0

        return df_opt


if __name__ == "__main__":
    import pytz
    from hec.core.app_initializer import load_app_config
    from zoneinfo import ZoneInfo

    # Make sure database with prices is copied to local drive before running test

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_main = logging.getLogger(__name__)

    app_config = {"database": {"type": "sqlite", "path": "home_energy.db"}}
    brussels_tz = ZoneInfo("Europe/Brussels")
    db_handler = DatabaseHandler(app_config['database'])
    db_handler.initialize_database()

    config = load_app_config()
    bp = BatteryPredictor(config)
    cd = ConsumptionPredictor(db_handler)

    first_day_start = datetime(2026, 3, 10, 23, 0, 0, tzinfo=pytz.UTC)
    first_day_end = datetime(2026, 3, 11, 22, 45, 0, tzinfo=pytz.UTC)
    price_points = db_handler.get_da_prices(first_day_start.astimezone(brussels_tz))
    process_price_points_to_app_state(price_points, first_day_start, "electricity_prices_today", config, db_handler)

    ff = cd.generate_consumption_forecast(first_day_start, first_day_end)
    first_plan_df = bp.generate_plan(first_day_start, first_day_end, ff, db_handler, initial_soc_kwh=2)
    last_soc_day1 = first_plan_df['soc_kwh'].iloc[-1]

    second_day_start = datetime(2026, 3, 11, 23, 0, 0, tzinfo=pytz.UTC)
    second_day_end = datetime(2026, 3, 12, 22, 45, 0, tzinfo=pytz.UTC)
    price_points = db_handler.get_da_prices(first_day_start, first_day_end)
    process_price_points_to_app_state(price_points, first_day_start, "electricity_prices_tomorrow", config, db_handler)

    ff = cd.generate_consumption_forecast(second_day_start, second_day_end)
    second_plan_df = bp.generate_plan(second_day_start, second_day_end, ff, db_handler, initial_soc_kwh=last_soc_day1)

    plan_df = pd.concat([first_plan_df, second_plan_df])

    opt_plan_df = bp.optimize_plan(plan_df, 2, GLOBAL_APP_STATE)

    with pd.option_context(
            'display.max_rows', None,
            'display.max_columns', None,
            'display.width', 2000,
            'display.expand_frame_repr', False
    ):
        print(opt_plan_df)
