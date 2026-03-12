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


def add_prices_to_plan(df_plan: pd.DataFrame, state: Dict) -> pd.DataFrame:
    prices_today = state.get("electricity_prices_today", [])
    prices_tomorrow = state.get("electricity_prices_tomorrow", [])
    all_prices = prices_today + prices_tomorrow

    price_map = {}
    for p in all_prices:
        try:
            contract = p.active_contract_type
            buy_price = p.net_prices_eur_per_kwh[contract]['buy']
            sell_price = p.net_prices_eur_per_kwh[contract]['sell']
            price_map[p.interval_start_local] = {'buy_price': buy_price, 'sell_price': sell_price}
        except (KeyError, TypeError):
            pass

    df_prices = pd.DataFrame.from_dict(price_map, orient='index')
    df = df_plan.merge(df_prices, left_index=True, right_index=True, how='left')
    df['buy_price'] = df['buy_price'].ffill()
    df['sell_price'] = df['sell_price'].ffill()

    return df


class BatteryPredictor:
    def __init__(self, app_config: Dict):
        self.app_config = app_config
        self.capacity_kwh = 0
        self.max_charge_kw = 0
        self.max_discharge_kw = 0
        self.dt_hours = 0.25
        self.eff = 0.9
        self.cur_dt = None

        for battery in app_config.get("batteries"):
            self.capacity_kwh += battery.get("capacity_kwh", 0)
            self.max_charge_kw += battery.get("max_charge_W", 0) / 1000
            self.max_discharge_kw += battery.get("max_discharge_W", 0) / 1000

        self.panel_kw = self.app_config.get('inverter', {}).get('panel_peak_w', 0) / 1000.0
        self.inv_kw = self.app_config.get('inverter', {}).get('standard_power_limit', 0) / 1000.0

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
                      initial_soc_pct: float = 0.0) -> pd.DataFrame:
        """
        Generates a 15-minute resolution battery plan based on excess solar and consumption.
        """
        # Ensure consumption is a Series
        if isinstance(consumption_s, pd.DataFrame):
            consumption_s = consumption_s.iloc[:, 0]

        solar_s = self._fetch_aligned_solar(start_dt, end_dt, db_handler, consumption_s.index)

        # Build the initial state DataFrame
        df_plan = pd.DataFrame(index=consumption_s.index)
        df_plan['cons_kwh'] = consumption_s
        df_plan['solar_kwh'] = solar_s
        df_plan['net_kwh'] = df_plan['solar_kwh'] - df_plan['cons_kwh']

        # Output columns
        charge_amounts = []
        soc_list = []

        current_soc = initial_soc_pct * self.capacity_kwh / 100

        for net in df_plan['net_kwh']:
            charge_amt = 0.0

            if net > 0:  # Excess Solar -> Charge
                # 1. Calculate max acceptable energy from solar in this 15-min block considering the 95% taper
                e_to_95 = max(0.0, 0.95 * self.capacity_kwh - current_soc)
                t_to_95 = min(self.dt_hours, e_to_95 / (self.max_charge_kw * self.eff)) if self.max_charge_kw > 0 else 0
                t_after_95 = self.dt_hours - t_to_95

                max_solar_accepted = (t_to_95 * self.max_charge_kw) + (t_after_95 * (self.max_charge_kw / 2))

                attempted_charge = min(net, max_solar_accepted)
                actual_added = min(attempted_charge * self.eff, self.capacity_kwh - current_soc)

                charge_amt = actual_added / self.eff
                current_soc += actual_added

            elif net < 0:  # Deficit -> Discharge
                deficit = abs(net)

                # 1. Calculate max deliverable energy to the house in this 15-min block considering the 5% taper
                e_above_5 = max(0.0, current_soc - 0.05 * self.capacity_kwh)
                t_to_5 = min(self.dt_hours, e_above_5 / (self.max_discharge_kw / self.eff)) if self.max_discharge_kw > 0 else 0
                t_after_5 = self.dt_hours - t_to_5

                max_house_delivered = (t_to_5 * self.max_discharge_kw) + (t_after_5 * (self.max_discharge_kw / 2))

                attempted_discharge = min(deficit, max_house_delivered)
                actual_removed = min(attempted_discharge / self.eff, current_soc)

                charge_amt = -(actual_removed * self.eff)
                current_soc -= actual_removed

            charge_amounts.append(charge_amt)
            soc_list.append((current_soc / self.capacity_kwh) * 100 if self.capacity_kwh > 0 else 0)

        df_plan['charge_kwh'] = charge_amounts
        df_plan['soc_pct'] = soc_list

        # Optional: Grid interaction after battery
        df_plan['grid_in'] = np.clip(
            np.where(df_plan['net_kwh'] < 0,
                     abs(df_plan['net_kwh']) + df_plan['charge_kwh'],
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

    def calculate_impact(self, df: pd.DataFrame, cur_dt: datetime) -> pd.DataFrame:
        """
        Recalculates new_c and new_pct based on optimization flags:
        block_d, block_c, force_c, and limit_i.
        """
        # Calculate the initial SoC before the first interval
        first_row = df.iloc[0]
        first_charge = first_row['charge_kwh']
        first_soc_kwh = (first_row['soc_pct'] / 100.0) * self.capacity_kwh

        if first_charge >= 0:
            initial_soc_kwh = first_soc_kwh - (first_charge * self.eff)
        else:
            initial_soc_kwh = first_soc_kwh - (first_charge / self.eff)

        current_soc_kwh = max(0.0, min(initial_soc_kwh, self.capacity_kwh))

        # Storage for results
        new_c_list = []
        new_pct_list = []

        for idx, row in df.iterrows():
            # 1. Handle Inverter Limit (limit_i) on Solar
            max_solar_kwh = (row['limit_i'] / 1000.0) * self.dt_hours
            effective_solar = min(row['solar_kwh'], max_solar_kwh)

            # Recalculate net based on restricted solar vs original consumption
            effective_net = effective_solar - row['cons_kwh']

            # 2. Base Logic: default charge_kwh or the effective_net
            rem_c = max(row['charge_kwh'], row['grid_out'])
            max_c = min(rem_c, self.max_charge_kw * self.dt_hours)
            new_c = min(effective_net, max_c)

            # 3. Apply Block Flags
            if row['block_d'] and new_c < 0:
                new_c = 0.0

            if row['block_c'] and new_c > 0:
                new_c = 0.0

            # 4. Apply Force Charge
            if row['force_c'] and row['force_time'] > 0:
                # Energy = Power (kW) * Time (hours)
                forced_energy_kwh = self.max_charge_kw * (row['force_time'] / 60.0)
                # Add forced charge to existing solar/net logic
                new_c += forced_energy_kwh

            # 5. Apply Battery Physical Constraints (Capacity & Efficiency)
            if new_c > 0:  # Charging
                # Can't charge more than the gap to 100%
                actual_added = min(new_c * self.eff, self.capacity_kwh - current_soc_kwh)
                new_c = actual_added / self.eff  # The gross amount taken from solar/grid
                current_soc_kwh += actual_added
            elif new_c < 0:  # Discharging
                # Can't discharge more than what is in the tank
                actual_removed = min(abs(new_c) / self.eff, current_soc_kwh)
                new_c = -(actual_removed * self.eff)  # The net amount delivered to house
                current_soc_kwh -= actual_removed

            # 6. Update SoC Percentage
            soc_pct = (current_soc_kwh / self.capacity_kwh) * 100 if self.capacity_kwh > 0 else 0

            new_c_list.append(new_c)
            new_pct_list.append(np.clip(soc_pct, 0, 100))

        df['new_c'] = new_c_list
        df['new_pct'] = new_pct_list

        return df

    def optimize_plan(self, df_plan, cur_dt, actual_soc, state: Dict) -> pd.DataFrame:
        self.cur_dt = cur_dt
        opt_plan = df_plan[df_plan.index > self.cur_dt].copy()

        # Extract and align prices from global_app_state
        opt_plan = add_prices_to_plan(opt_plan, state)

        # Assign empty calculation columns
        opt_plan = opt_plan.assign(
            block_d=False,
            block_c=False,
            force_c=False,
            force_time=0,
            limit_i=self.inv_kw * 1000,
            new_c=0.0,
            new_pct=0.0
        )

        # Apply rule: block charge when charging later is cheaper while still achieving max capacity
        opt_plan = self.apply_rule_block_charge(opt_plan)
        self.calculate_impact(opt_plan, self.cur_dt)

        # Clean up: remove block_c when battery is full
        opt_plan.loc[opt_plan['new_pct'] >= 98, 'block_c'] = False

        # Apply rule: block discharge if later buy_price is higher
        opt_plan = self.apply_rule_block_discharge(opt_plan)
        # self.calculate_impact(opt_plan, self.cur_dt)

        return opt_plan


    def temp(self):
        # 2. Setup decision and recalculation columns
        df_opt['block_d'] = False
        df_opt['block_c'] = False
        df_opt['force_c'] = False
        df_opt['force_time'] = 0.0
        df_opt['limit_i'] = False

        df_opt['new_c'] = 0.0
        df_opt['new_soc'] = 0.0
        df_opt['new_pct'] = 0.0

        current_soc = actual_soc
        dt_hours = 0.25
        eff = 0.9

        timestamps = df_opt.index.tolist()

        df_opt = self.apply_rule_block_charge(df_opt)

        # 3. Step through time and apply logic
        for i, current_time in enumerate(timestamps):
            row = df_opt.loc[current_time]
            imp_price = row['buy_price']
            exp_price = row['sell_price']
            net = row['net_kwh']

            new_charge_amt = 0.0

            if pd.isna(imp_price):
                df_opt.at[current_time, 'new_soc'] = current_soc
                continue

            future_window = df_opt.iloc[i + 1:]

            # Rule C: Limit inverter
            if exp_price < 0:
                consumption_w = row['cons_kwh'] / dt_hours * 1000
                df_opt.at[current_time, 'limit_i'] = True

            # Rule B: Force Charge (Analyze exact future deficits)
            future_high_prices = future_window[future_window['buy_price'] >= (imp_price + 0.10)]
            future_deficits = future_high_prices[future_high_prices['net_kwh'] < 0]

            # Calculate exactly how much energy we will need from the battery during high prices
            # We divide by eff because to deliver 1kWh to the house, we need 1.11kWh in the battery
            total_future_deficit_kwh = abs(future_deficits['net_kwh'].sum()) / eff

            # How much more energy does the battery need right now to cover that?
            energy_needed_in_battery = total_future_deficit_kwh - current_soc

            if energy_needed_in_battery > 0.01 and current_soc < self.capacity_kwh:
                df_opt.at[current_time, 'force_c'] = True

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
                df_opt.at[current_time, 'force_time'] = min(15.0, charge_time_hours * 60)

            else:
                # Rule A: Block Actions
                # A.2 Block Discharge: High prices coming, but we have enough SOC. Save it.
                future_deficits_all = future_window[future_window['net_kwh'] < 0]
                if not future_deficits_all.empty and net < 0:
                    max_future_imp_price = future_deficits_all['buy_price'].max()
                    if max_future_imp_price > imp_price + 0.02:
                        df_opt.at[current_time, 'block_d'] = True

                # A.1 Block Charge: If future import prices are heavily negative, we might
                # want to export our solar now (if export price is good) and charge later.
                future_cheap = future_window[future_window['buy_price'] < exp_price - 0.05]
                # if not future_cheap.empty and net > 0:
                #     df_opt.at[current_time, 'block_c'] = True

                # Calculate normal flows based on block flags
                if net > 0 and not df_opt.at[current_time, 'block_c']:
                    attempted_charge = min(net, self.max_charge_kw * dt_hours)
                    actual_added = min(attempted_charge * eff, self.capacity_kwh - current_soc)
                    new_charge_amt = actual_added / eff
                    current_soc += actual_added

                elif net < 0 and not df_opt.at[current_time, 'block_d']:
                    attempted_discharge = min(abs(net), self.max_discharge_kw * dt_hours)
                    actual_removed = min(attempted_discharge / eff, current_soc)
                    new_charge_amt = -(actual_removed * eff)
                    current_soc -= actual_removed

            # Ensure floats don't accumulate noise
            current_soc = round(current_soc, 6)

            # Save recalculated columns
            df_opt.at[current_time, 'new_c'] = new_charge_amt
            df_opt.at[current_time, 'new_soc'] = current_soc
            df_opt.at[current_time, 'new_pct'] = (current_soc / self.capacity_kwh) * 100 \
                if self.capacity_kwh > 0 else 0

        return df_opt

    def apply_rule_block_charge(self, df_opt: pd.DataFrame) -> pd.DataFrame:
        """
        Blocks charging during the most expensive export periods of the day
        if solar production exceeds battery capacity.
        """
        # Process each day independently
        for date in df_opt.index.normalize().unique():
            day_mask = df_opt.index.normalize() == date
            day_df = df_opt[day_mask].copy()

            # 1. Identify all intervals where the battery COULD charge (net_energy > 0)
            charge_intervals = day_df[day_df['net_kwh'] > 0].copy()

            if charge_intervals.empty:
                continue

            # 2. Sort these intervals by Export Price (Ascending)
            # We want to "allow" charging when selling to the grid earns us the LEAST.
            charge_intervals = charge_intervals.sort_values(by='sell_price', ascending=True)

            theoretical_soc = 0.0  # We start from "empty" to see how much daily solar we can fit
            allowed_timestamps = []

            for ts, row in charge_intervals.iterrows():
                if theoretical_soc >= self.capacity_kwh:
                    break

                potential_charge = min(row['net_kwh'], self.max_charge_kw * self.dt_hours)
                actual_added = min(potential_charge * self.eff, self.capacity_kwh - theoretical_soc)

                theoretical_soc += actual_added
                allowed_timestamps.append(ts)

            # 3. Block charge on any interval that wasn't in our "allowed" (cheapest) list
            # only for rows where there is actually solar production (net_energy > 0)
            all_solar_ts = day_df[day_df['net_kwh'] > 0].index
            blocked_candidates = [ts for ts in all_solar_ts if ts not in allowed_timestamps]

            # If we have blocked slots, remove the first chronological one to act as a buffer
            if len(blocked_candidates) > 1:
                # Sort chronologically to find the "first" one in the day
                blocked_candidates.sort()
                blocked_candidates.pop(0)
            elif len(blocked_candidates) == 1:
                # If there's only one, we leave it out entirely
                blocked_candidates = []

            df_opt.loc[blocked_candidates, 'block_c'] = True

        return df_opt

    def apply_rule_block_discharge(self, df_opt: pd.DataFrame, min_price_diff: float = 0.02) -> pd.DataFrame:
        """
        Continuous optimization: For every interval, checks if the energy currently
        being used would be more valuable at a future more expensive peak.
        """
        # We loop through every interval except the very last one
        for i in range(len(df_opt) - 1):
            current_ts = df_opt.index[i]
            current_row = df_opt.iloc[i]

            # 1. Skip if the battery is already essentially empty
            if current_row['new_pct'] <= 5.0:
                continue

            current_buy_price = current_row['buy_price']

            # 2. Look ahead: Find future intervals where the price is higher
            # and the battery is projected to be empty (the 'need' window)
            future_df = df_opt.iloc[i + 1:]

            # Criteria for a "better" future slot:
            # - Price is significantly higher
            # - The battery is projected to be empty (< 5%) or we are buying from grid
            expensive_needs = future_df[
                (future_df['buy_price'] >= current_buy_price + min_price_diff) &
                ((future_df['new_pct'] <= 5.5) | (future_df['grid_in'] > 0))
                ]

            if expensive_needs.empty:
                continue

            # 3. Check for "The Sun Trap":
            # If the battery hits 100% between 'now' and the 'future peak',
            # blocking discharge now is useless because the sun will refill it anyway.
            peak_ts = expensive_needs.index[0]  # Look at the first upcoming peak
            intervening_df = future_df.loc[:peak_ts]

            if (intervening_df['new_pct'] >= 98.0).any():
                # Battery will overflow before we reach the expensive price; no point in saving.
                continue

            # 4. If we passed the checks, block discharge for this interval
            # This "saves" the energy for that future expensive_needs window.
            df_opt.at[current_ts, 'block_d'] = True

            # Optional: We should ideally update a 'virtual' SOC to let the
            # next iteration in this loop know we just saved energy.
            # But for simplicity, the sequential calculate_impact handles this perfectly.

        self.calculate_impact(df_opt, self.cur_dt)

        return df_opt


if __name__ == "__main__":
    import pytz
    from hec.core.app_initializer import load_app_config
    from zoneinfo import ZoneInfo

    # Make sure database with prices is copied to local drive before running test

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_main = logging.getLogger(__name__)

    config = load_app_config()
    local_tz = ZoneInfo(config['scheduler']['timezone'])
    db_handler = DatabaseHandler(config['database'])
    db_handler.initialize_database()

    bp = BatteryPredictor(config)
    cd = ConsumptionPredictor(db_handler)

    # Fill app_state with NEPIs from PricePoints in database
    first_day_start = datetime(2026, 3, 11, 23, 0, 0, tzinfo=pytz.UTC)
    first_day_end = datetime(2026, 3, 12, 22, 45, 0, tzinfo=pytz.UTC)
    price_points = db_handler.get_da_prices(first_day_start.astimezone(local_tz))
    process_price_points_to_app_state(price_points, first_day_start, "electricity_prices_today", config, db_handler)

    # First day plan (today)
    ff = cd.generate_consumption_forecast(first_day_start, first_day_end)
    first_plan_df = bp.generate_plan(first_day_start, first_day_end, ff, db_handler, initial_soc_pct=30)
    last_soc_day1 = first_plan_df['soc_pct'].iloc[-1]

    # Fill app_state with NEPIs
    second_day_start = datetime(2026, 3, 12, 23, 0, 0, tzinfo=pytz.UTC)
    second_day_end = datetime(2026, 3, 13, 22, 45, 0, tzinfo=pytz.UTC)
    price_points = db_handler.get_da_prices(second_day_start.astimezone(local_tz))
    process_price_points_to_app_state(price_points, second_day_start, "electricity_prices_tomorrow", config, db_handler)

    # Second day plan (tomorrow)
    ff = cd.generate_consumption_forecast(second_day_start, second_day_end)
    second_plan_df = bp.generate_plan(second_day_start, second_day_end, ff, db_handler, initial_soc_pct=last_soc_day1)

    plan_df = pd.concat([first_plan_df, second_plan_df])

    time_stamp = datetime(2026, 3, 11, 7, 55, 0, tzinfo=pytz.UTC)
    opt_plan_df = bp.optimize_plan(plan_df, time_stamp, 2, GLOBAL_APP_STATE)

    with pd.option_context(
            'display.max_rows', None,
            'display.max_columns', None,
            'display.width', 2000,
            'display.expand_frame_repr', False,
            'display.precision', 3,
    ):
        pd.options.display.float_format = '{:,.3f}'.format
        print(opt_plan_df)
