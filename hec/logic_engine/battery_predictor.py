import pandas as pd
import numpy as np
import logging
from typing import Dict
from datetime import datetime, timedelta

from hec.core.app_state import GLOBAL_APP_STATE
from hec.logic_engine.consumption_predictor import ConsumptionPredictor
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.cost_calculator import calculate_net_intervals_for_day
from hec.utils.utils import get_predicted_price_points_for_date, process_price_points_to_app_state, is_daylight

logger = logging.getLogger(__name__)


class BatteryPredictor:
    def __init__(self, app_config: Dict):
        self.app_config = app_config
        self.capacity_kwh = 0
        self.max_charge_kw = 0
        self.max_discharge_kw = 0
        self.dt_hours = 0.25
        self.charge_eff = 0.90
        self.discharge_eff = 0.80
        self.cur_dt = None
        self.max_peak_kw = 0

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

    def _fetch_aligned_solar(self, start_dt: datetime, end_dt: datetime, db: DatabaseHandler,
                             freq_index: pd.Index) -> pd.Series:
        """Fetches solar data and aligns it to the consumption index."""
        solar_records = db.get_elia_forecasts("solar", start_dt, end_dt)

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

    @staticmethod
    def add_prices_to_plan(df_plan: pd.DataFrame, state: Dict, app_config, db) -> pd.DataFrame:
        prices_today = state.get("electricity_prices_today") or []
        prices_tomorrow = state.get("electricity_prices_tomorrow") or []

        # Fallback: tomorrow is empty, try to get predictions
        if not prices_tomorrow and db and app_config:
            try:
                tomorrow_date = datetime.now() + timedelta(days=1)

                # Fetch raw PricePoints from DB
                pred_points = get_predicted_price_points_for_date(db, tomorrow_date)

                if pred_points:
                    # Convert PricePoints to NetElectricityPriceIntervals
                    prices_tomorrow = calculate_net_intervals_for_day(
                        db, app_config, tomorrow_date, pred_points
                    )
                    logger.info(f"Using {len(prices_tomorrow)} predicted price intervals for tomorrow.")
            except Exception as e:
                logger.error(f"Fallback to predicted prices failed: {e}")
                prices_tomorrow = []

        all_prices = prices_today + prices_tomorrow

        price_map = {}
        for p in all_prices:
            try:
                contract = p.active_contract_type
                price_map[p.interval_start_local] = {
                    'buy_price': p.net_prices_eur_per_kwh[contract]['buy'],
                    'sell_price': p.net_prices_eur_per_kwh[contract]['sell']
                }
            except (KeyError, TypeError):
                pass

        if not price_map:
            logger.warning("No price data found in state. Returning plan without price updates.")
            return df_plan

        df_prices = pd.DataFrame.from_dict(price_map, orient='index')

        # Drop columns
        cols_to_drop = [c for c in ['buy_price', 'sell_price'] if c in df_plan.columns]
        if cols_to_drop:
            df_plan = df_plan.drop(columns=cols_to_drop)

        df = df_plan.merge(df_prices, left_index=True, right_index=True, how='left')

        return df

    def generate_plan(self, start_dt: datetime, end_dt: datetime, consumption_s: pd.Series, db: DatabaseHandler,
                      max_peak_kw = 2.5, initial_soc_pct: float = 0.0) -> pd.DataFrame:
        """
        Generates a 15-minute resolution battery plan based on excess solar and consumption.
        """
        self.max_peak_kw = max_peak_kw

        # Ensure consumption is a Series
        if isinstance(consumption_s, pd.DataFrame) and consumption_s is not None:
            consumption_s = consumption_s.iloc[:, 0]

        solar_s = self._fetch_aligned_solar(start_dt, end_dt, db, consumption_s.index)

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
                t_to_95 = min(self.dt_hours, e_to_95 / (self.max_charge_kw * self.charge_eff)) if self.max_charge_kw > 0 else 0
                t_after_95 = self.dt_hours - t_to_95

                max_solar_accepted = (t_to_95 * self.max_charge_kw) + (t_after_95 * (self.max_charge_kw / 2))

                attempted_charge = min(net, max_solar_accepted)
                actual_added = min(attempted_charge * self.charge_eff, self.capacity_kwh - current_soc)

                charge_amt = actual_added / self.charge_eff
                current_soc += actual_added

            elif net < 0:  # Deficit -> Discharge
                deficit = abs(net)

                # 1. Calculate max deliverable energy to the house in this 15-min block considering the 5% taper
                e_above_5 = max(0.0, current_soc - 0.05 * self.capacity_kwh)
                t_to_5 = min(self.dt_hours, e_above_5 / (self.max_discharge_kw / self.discharge_eff)) if self.max_discharge_kw > 0 else 0
                t_after_5 = self.dt_hours - t_to_5

                max_house_delivered = (t_to_5 * self.max_discharge_kw) + (t_after_5 * (self.max_discharge_kw / 2))

                attempted_discharge = min(deficit, max_house_delivered)
                actual_removed = min(attempted_discharge / self.discharge_eff, current_soc)

                charge_amt = -(actual_removed * self.discharge_eff)
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
        df_plan = df_plan[~df_plan.index.duplicated(keep='first')]

        logger.info("Battery prediction plan generated")

        return df_plan

    def calculate_impact(self, df: pd.DataFrame, current_soc_kwh = None) -> pd.DataFrame:
        """
        Recalculates new_c and new_pct based on optimization flags:
        block_d, block_c, force_c, and limit_i.
        """
        # Calculate the initial SoC before the first interval
        if not current_soc_kwh:
            first_row = df.iloc[0]
            first_charge = float(first_row['new_c'])
            first_soc_kwh = float(first_row['new_pct']) / 100 * self.capacity_kwh

            if first_charge >= 0:
                initial_soc_kwh = first_soc_kwh - (first_charge * self.charge_eff)
            else:
                initial_soc_kwh = first_soc_kwh - (first_charge / self.discharge_eff)

            current_soc_kwh = max(0.0, min(initial_soc_kwh, self.capacity_kwh))

        # Storage for results
        new_c_list = []
        new_pct_list = []
        new_grid_list = []

        for idx, row in df.iterrows():
            # 1. Apply Force Charge
            if row['force_c'] and row['force_time'] > 0:
                grid_ceiling_kwh = self.max_peak_kw * self.dt_hours
                planned_grid_usage = max(0, row['cons_kwh'] - row['solar_kwh'])
                available_headroom = max(0, grid_ceiling_kwh - planned_grid_usage)
                target_charge_kwh = self.max_charge_kw * (row['force_time'] / 60.0)
                forced_energy_kwh = min(target_charge_kwh, available_headroom)
            else:
                forced_energy_kwh = 0.0

            # 2. Handle Inverter Limit (limit_i) on Solar
            rem_c = max(row['charge_kwh'], row['grid_out'])
            max_c = min(rem_c, self.max_charge_kw * self.dt_hours)
            ac_space_left = (self.capacity_kwh - current_soc_kwh) / self.charge_eff
            pot_c = max(0.0, min(max_c, ac_space_left))
            max_inverter_kwh = self.inv_kw * 1000 * self.dt_hours

            if row.get('sell_price', 0.0) < 0:
                zero_export_limit = max(0.0, row['cons_kwh'] + max(forced_energy_kwh, pot_c))
                max_inverter_kwh = min(max_inverter_kwh, zero_export_limit)
                df.at[idx, 'limit_i'] = (max_inverter_kwh * 1000.0) / self.dt_hours

            if row.get('buy_price', 0.0) < 0:
                df.at[idx, 'limit_i'] = 0

            # Recalculate net based on restricted solar vs original consumption
            effective_solar = min(row['solar_kwh'], max_inverter_kwh)
            effective_net = effective_solar - row['cons_kwh']

            # 3. Base Logic: default charge_kwh or the effective_net
            new_c = float(min(max(effective_net, -self.max_charge_kw * self.dt_hours), max_c))

            # 4. Apply Block Flags
            if row['block_d'] and new_c < 0:
                new_c = 0.0

            if row['block_c'] and new_c > 0:
                new_c = 0.0

            # 5. Apply force charge
            new_c = min(new_c + forced_energy_kwh, self.max_charge_kw * self.dt_hours)

            # 6. Apply Battery Physical Constraints (Capacity & Efficiency)
            if new_c > 0:  # Charging
                # Can't charge more than the gap to 100%
                actual_added = min(new_c * self.charge_eff, self.capacity_kwh - current_soc_kwh)
                new_c = actual_added / self.charge_eff  # The gross amount taken from solar/grid
                current_soc_kwh += actual_added
            elif new_c < 0:  # Discharging
                # Can't discharge more than what is in the tank and stopping at 2%
                min_soc_kwh = self.capacity_kwh * 0.02
                actual_removed = min(abs(new_c) / self.discharge_eff, current_soc_kwh - min_soc_kwh)
                new_c = -(actual_removed * self.discharge_eff)  # The net amount delivered to house
                current_soc_kwh -= actual_removed

            # 7. Update SoC Percentage
            soc_pct = (current_soc_kwh / self.capacity_kwh) * 100 if self.capacity_kwh > 0 else 0

            # 8. New grid
            new_grid = effective_net - new_c

            new_c_list.append(new_c)
            new_pct_list.append(np.clip(soc_pct, 0, 100))
            new_grid_list.append(new_grid)

        df['new_c'] = new_c_list
        df['new_pct'] = new_pct_list
        df['new_grid'] = new_grid_list

        return df

    @staticmethod
    def calculate_cost(df: pd.DataFrame, print_on_screen = False) -> Dict[str, float]:
        """
        Calculates the financial impact of the optimized plan.
        Note: new_c > 0 (charging) is energy taken from potential exports.
        Note: new_c < 0 (discharging) is energy used to avoid imports.
        """
        if df.empty:
            return {"total_net_cost": 0.0}

        buy_cost, sell_revenue = 0, 0
        # 1. Grid Interaction
        grid_in = df['new_grid'].clip(upper=0).abs()
        buy_cost = (grid_in * df['buy_price']).sum()

        grid_out = df['new_grid'].clip(lower=0)
        sell_revenue = (grid_out * df['sell_price']).sum()

        # 2. Battery Value (Opportunity Cost/Gain)
        # new_c > 0 is charging. This energy COULD have been sold if it wasn't stored.
        # We treat this as "Sell Loss" or "Investment in Storage"
        charging_mask = df['new_c'] > 0
        sell_loss = (df.loc[charging_mask, 'new_c'] * df.loc[charging_mask, 'sell_price']).sum()

        # new_c < 0 is discharging. This energy is covering house load.
        # The value is the "Buy Price" we didn't have to pay.
        discharging_mask = df['new_c'] < 0
        buy_avoided = (df.loc[discharging_mask, 'new_c'].abs() * df.loc[discharging_mask, 'buy_price']).sum()

        # 3. All-in Net Cost
        # (What you paid) - (What you earned)
        total_net_cost = buy_cost - sell_revenue + sell_loss - buy_avoided

        last_price = df.iloc[-1]['buy_price']
        current_kwh = (df.iloc[-1]['new_pct'] / 100.0) * 5.36

        load_profile = [
            (2, 0.25, 0.9),  # 2h at 0.25kW (Night)
            (2, 0.50, 0.7),  # 2h at 0.40kW (Maybe heat pump)
            (2, 2.00, 1.2),  # 2h at 2.00kW (Heat pump)
            (4, 0.25, 1.0)  # 4h at 0.25kW (Morning peak)
        ]

        inventory_value = 0.0

        for hours, kw_load, multiplier in load_profile:
            if current_kwh <= 0:
                break
            total_window_demand = hours * kw_load
            energy_served = min(current_kwh, total_window_demand)
            period_price = max(0.15, min(0.25, last_price * multiplier))
            inventory_value += energy_served * period_price
            current_kwh -= energy_served

        results = {
            "buy_cost": round(buy_cost, 4),
            "sell_revenue": round(sell_revenue, 4),
            "charging_opportunity_cost": round(sell_loss, 4),
            "discharging_avoided_cost": round(buy_avoided, 4),
            "total_net_cost": round(total_net_cost, 4),
            "total_net_cost_and_inventory": round(total_net_cost - inventory_value, 4),
        }

        if print_on_screen:
            # Print Statement for Debugging/Optimization monitoring
            print(f"\n--- Financial Breakdown ---")
            print(f"Grid Buy Cost:       €{results['buy_cost']:.4f} ({grid_in.sum()} kWh)")
            print(f"Grid Sell Revenue:   €{results['sell_revenue']:.4f} ({sell_revenue.sum()} kWh)")
            print(f"Charge Opp. Cost:    €{results['charging_opportunity_cost']:.4f} ({sell_loss.sum()} kWh)")
            print(f"Discharge Avoided:   €{results['discharging_avoided_cost']:.4f} ({buy_avoided.sum()} kWh)")
            print(f"TOTAL NET COST:      €{results['total_net_cost']:.4f}")
            print(f"Net cost - inventory:€{results['total_net_cost_and_inventory']:.4f}")
            print(f"---------------------------\n")

        return results

    def optimize_plan(self, df_plan, cur_dt, actual_soc_pct, state: Dict, app_config, db_handler,
                      print_on_screen=False) -> pd.DataFrame:
        self.cur_dt = cur_dt
        plan_start_dt = cur_dt.replace(minute=(cur_dt.minute // 15) * 15, second=0, microsecond=0)
        opt_plan = df_plan[df_plan.index >= plan_start_dt].copy()
        opt_plan = opt_plan[~opt_plan.index.duplicated(keep='first')]

        # Extract and align prices from global_app_state
        opt_plan = self.add_prices_to_plan(opt_plan, state, app_config, db_handler)

        # Assign empty calculation columns
        opt_plan = opt_plan.assign(
            block_d=False,
            block_c=False,
            force_c=False,
            force_time=0,
            limit_i=self.inv_kw * 1000,
            new_c=0.0,
            new_pct=0.0,
            new_grid=0.0
        )

        # First line soc is calculated in calculate_impact
        # if not opt_plan.empty:
        #     # 1. Calculate the first row's ending SOC
        #     start_kwh = (actual_soc_pct / 100.0) * self.capacity_kwh
        #     first_row = opt_plan.iloc[0]
        #
        #     # End kWh = Start kWh + Net Flow (Respecting battery limits)
        #     end_kwh = max(0, min(self.capacity_kwh, start_kwh + first_row['net_kwh']))
        #
        #     # Convert back to percent and store in 'new_pct'
        #     opt_plan.iat[0, opt_plan.columns.get_loc('new_pct')] = (end_kwh / self.capacity_kwh) * 100.0

        start_kwh = (actual_soc_pct / 100.0) * self.capacity_kwh
        opt_plan = self.calculate_impact(opt_plan, start_kwh)
        # pre_block_plan = opt_plan.copy()

        initial_cost_data = self.calculate_cost(opt_plan, print_on_screen)
        initial_total = float(initial_cost_data.get('total_net_cost'))
        logger.info(f"Battery optimization plan start | Initial Cost: €{initial_total:.4f}")

        # Apply rule: block charge when charging later is cheaper while still achieving max capacity
        opt_plan = self.apply_rule_block_charge(opt_plan)

        # Clean up: remove block_c when battery is full
        opt_plan.loc[opt_plan['new_pct'] >= 98, 'block_c'] = False
        opt_plan = self.calculate_impact(opt_plan)

        # diff_mask = opt_plan['new_pct'] != pre_block_plan['new_pct']
        # differences = opt_plan[diff_mask]

        # print("\n--- [DEBUG] Optimization Rule Impact ---")
        # print(f"{'Timestamp':<20} | {'Old New_C':>10} | {'New New_C':>10} | {'Old SoC%':>10} | {'New SoC%':>10} | Blocked_c")
        # print("-" * 75)
        # for idx in differences.index:
        #     old_c = pre_block_plan.at[idx, 'new_c']
        #     new_c = opt_plan.at[idx, 'new_c']
        #     old_soc = pre_block_plan.at[idx, 'new_pct']
        #     new_soc = opt_plan.at[idx, 'new_pct']
        #     block_c = opt_plan.at[idx, 'block_c']
        #
        #     print(f"{str(idx):<20} | {old_c:>10.4f} | {new_c:>10.4f} | {old_soc:>10.2f} | {new_soc:>10.2f} | {block_c}")
        # print("----------------------------------------\n")

        # Apply rule: block discharge if later buy_price is higher
        opt_plan = self.apply_rule_block_discharge(opt_plan)
        opt_plan = self.calculate_impact(opt_plan)

        # Apply rule: force charge if price difference high enough
        opt_plan = self.apply_rule_force_charge(opt_plan)
        opt_plan = self.calculate_impact(opt_plan)

        end_cost_data = self.calculate_cost(opt_plan, print_on_screen)
        end_total = float(end_cost_data.get('total_net_cost'))

        logger.info(f"Battery optimization plan end | Final Cost: €{end_total:.4f}")
        logger.info(f"Total Net Savings: €{end_total - initial_total:.4f}")

        return opt_plan

    def apply_rule_block_charge(self, df_opt: pd.DataFrame) -> pd.DataFrame:
        """
        Blocks charging only within a safe window: after the last 5% SOC
        and only if 100% SOC is actually reached that day.
        """
        for date in pd.to_datetime(df_opt.index).normalize().unique():
            day_mask = pd.to_datetime(df_opt.index).normalize() == date
            day_df = df_opt[day_mask].copy()

            # 1. Locate the Finish Line (100% SOC)
            full_indices = day_df.index[day_df['new_pct'] >= 99.9]
            if full_indices.empty:
                logger.debug(f"Rule Block Charge: {date.date()} - No 100% SOC reached. Skipping.")
                continue
            last_full_ts = full_indices[-1]

            # 2. Locate the Safety Start (Last 5% before/at the 100% mark)
            # We look at all data up to the point it was full
            pre_full_df = day_df.loc[day_df.index <= last_full_ts]
            critical_indices = pre_full_df.index[pre_full_df['new_pct'] <= 5.0]

            if not critical_indices.empty:
                # We start optimization AFTER the last time it was critical
                last_critical_ts = critical_indices[-1]
                opt_zone_df = pre_full_df.loc[pre_full_df.index > last_critical_ts].copy()
            else:
                # No 5% event found, optimization can apply to the whole day
                opt_zone_df = pre_full_df.copy()

            # 3. Volume Check on the Optimization Zone
            day_excess = opt_zone_df.loc[opt_zone_df['net_kwh'] > 0, 'net_kwh'].sum()
            if day_excess <= self.capacity_kwh * 1.2:
                continue

            # 4. Sorting & Simulation (Identify WHICH solar hours to block)
            solar_intervals = opt_zone_df[opt_zone_df['net_kwh'] > 0].copy()
            if solar_intervals.empty:
                continue

            # Sort by sell price to find the most expensive hours to block
            solar_intervals = solar_intervals.sort_values(by='sell_price', ascending=True)

            # Start simulation from the SOC at the beginning of our opt_zone
            start_soc_pct = opt_zone_df['new_pct'].iloc[0]
            theoretical_soc = (start_soc_pct / 100) * self.capacity_kwh
            allowed_timestamps = []

            for ts, row in solar_intervals.iterrows():
                if theoretical_soc >= self.capacity_kwh:
                    break

                potential_charge = min(row['net_kwh'], self.max_charge_kw * self.dt_hours)
                actual_added = min(potential_charge * self.charge_eff, self.capacity_kwh - theoretical_soc)
                theoretical_soc += actual_added
                allowed_timestamps.append(ts)

            # 5. Apply Blocks
            all_opt_solar_ts = opt_zone_df[opt_zone_df['net_kwh'] > 0].index
            blocked_candidates = [ts for ts in all_opt_solar_ts if ts not in allowed_timestamps]

            # Guard: Never block negative sell prices
            blocked_candidates = [ts for ts in blocked_candidates if df_opt.at[ts, 'sell_price'] >= 0]

            df_opt.loc[blocked_candidates, 'block_c'] = True

        return df_opt

    def apply_rule_block_charge_old(self, df_opt: pd.DataFrame) -> pd.DataFrame:
        """
        Blocks charging during the most expensive export periods of the day
        if solar production exceeds battery capacity.
        """
        # Process each day independently
        for date in pd.to_datetime(df_opt.index).normalize().unique():
            day_mask = pd.to_datetime(df_opt.index).normalize() == date
            day_df = df_opt[day_mask].copy()

            day_excess = day_df.loc[day_df['net_kwh'] > 0, 'net_kwh'].sum()
            if day_excess <= self.capacity_kwh * 1.3:
                logger.debug(f"Rule Block Charge: Skipped {date.date()} - "
                             f"Excess production ({day_excess:.1f} kWh) is insufficient "
                             f"(threshold: {self.capacity_kwh * 1.2:.1f} kWh)")
                continue

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
                actual_added = min(potential_charge * self.charge_eff, self.capacity_kwh - theoretical_soc)

                theoretical_soc += actual_added
                allowed_timestamps.append(ts)

            # 3. Block charge on any interval that wasn't in our "allowed" (cheapest) list
            # only for rows where there is actually solar production (net_energy > 0)
            all_solar_ts = day_df[day_df['net_kwh'] > 0].index
            blocked_candidates = [ts for ts in all_solar_ts if ts not in allowed_timestamps]

            # If sell_price is < 0, we MUST NOT block charging.
            blocked_candidates = [
                ts for ts in blocked_candidates
                if df_opt.at[ts, 'sell_price'] >= 0
            ]

            df_opt.loc[blocked_candidates, 'block_c'] = True

        # Remove last block_c in block of 4 or more as a buffer
        is_blocked = df_opt['block_c'].astype(int)
        # Check if the next interval is the end of the block
        is_end_of_block = (is_blocked == 1) & (is_blocked.shift(-1, fill_value=0) == 0)
        # Check if we have had at least 4 consecutive True values leading up to/including this one
        has_four_consecutive = is_blocked.rolling(window=4).sum() >= 4
        # Identify the specific index to flip
        tail_buffer_mask = is_end_of_block & has_four_consecutive
        # Flip those specific True values back to False
        df_opt.loc[tail_buffer_mask, 'block_c'] = False
        logger.debug(f"Rule Block Charge: Removed {tail_buffer_mask.sum()} tail-buffer slots.")

        return df_opt

    def apply_rule_block_discharge_old(self, df_opt: pd.DataFrame, min_price_diff: float = 0.01) -> pd.DataFrame:
        """
        Reactive Peak Shaving: Solves the most expensive grid-import peak,
        recalculates the plan, and repeats until no solvable peaks remain.
        """
        max_iterations = 96  # Safety cap to prevent infinite loops
        iteration = 0

        while iteration < max_iterations:
            # 1. Recalculate impact to get the latest 'new_grid' and 'new_pct'
            df_opt = self.calculate_impact(df_opt)

            # 2. Find the current WORST peak (highest buy_price not necessarily importing)
            # Sort by price descending to fix the most expensive problem first
            worst_peak = df_opt.sort_values(by='buy_price', ascending=False).iloc[0]
            t_peak = worst_peak.name
            peak_price = worst_peak['buy_price']
            energy_needed = abs(worst_peak['new_grid'])

            # 3. Find candidates to block (Look BACKWARDS from this peak)
            # Stop at the most recent "Full Battery" event
            full_times = df_opt.index[(df_opt.index < t_peak) & (df_opt['new_pct'] >= 98.0)]
            t_start_search = full_times[-1] if not full_times.empty else df_opt.index[0]

            # Candidates are cheaper slots currently discharging before the peak
            candidates = df_opt.loc[t_start_search:t_peak].iloc[:-1]
            candidates = candidates[
                (candidates['block_d'] == False) &  # Not already blocked
                (candidates['buy_price'] <= peak_price - min_price_diff)
                ]
            candidates = candidates.sort_values(by='buy_price', ascending=True)

            if candidates.empty:
                break

            # 4. Attempt to solve this specific peak by blocking candidates
            saved_for_this_peak = 0
            for t_cand, _ in candidates.iterrows():
                if saved_for_this_peak >= energy_needed:
                    break

                # Block the slot
                df_opt.at[t_cand, 'block_d'] = True

                # We must recalculate within the candidate loop to see if the "bridge"
                # to the peak is actually working
                df_opt = self.calculate_impact(df_opt)

                # Check if our target peak actually improved
                new_peak_grid = df_opt.at[t_peak, 'new_grid']
                improvement = new_peak_grid - worst_peak['new_grid']

                if improvement > 0.001:
                    saved_for_this_peak += improvement
                    # Update our reference for the next candidate in this inner loop
                    worst_peak['new_grid'] = new_peak_grid

            iteration += 1

        return df_opt

    def apply_rule_block_discharge(self, df_opt: pd.DataFrame, min_price_diff: float = 0.01) -> pd.DataFrame:
        """
        Cumulative Peak Shaving: Builds a bridge of blocked slots to solve expensive peaks.
        Evaluates total plan cost at each step to find the most efficient bridge.
        """
        # 1. Sort peaks by price (descending) to solve the most expensive problems first
        # We only consider peaks where we aren't already blocked or fully charged
        potential_peaks = df_opt.sort_values(by='buy_price', ascending=False)

        # We'll limit iterations to the number of intervals to prevent runaway loops
        max_peaks_to_check = 48
        processed_peaks = 0

        for t_peak, peak_row in potential_peaks.iterrows():
            if processed_peaks >= max_peaks_to_check:
                break
            if peak_row['new_grid'] >= -0.001 or peak_row['block_c']:
                continue

            peak_price = peak_row['buy_price']

            # 2. Find candidates to block (Look BACKWARDS from this peak to the last 98% SOC)
            full_times = df_opt.index[(df_opt.index < t_peak) & (df_opt['new_pct'] >= 98.0)]
            t_start_search = full_times[-1] if not full_times.empty else df_opt.index[0]

            candidates = df_opt.loc[t_start_search:t_peak].iloc[:-1]
            candidates = candidates[
                (candidates['block_d'] == False) &
                (candidates['block_c'] == False) &
                (candidates['buy_price'] <= peak_price - min_price_diff)
                ]

            # Sort candidates by price (cheapest first) to build the most profitable bridge
            candidates = candidates.sort_values(by='buy_price', ascending=True)

            if candidates.empty:
                continue

            # 3. Trial Loop: Build the bridge incrementally
            initial_impact = self.calculate_impact(df_opt)
            initial_cost = float(self.calculate_cost(initial_impact).get('total_net_cost_and_inventory', 0))

            trials = [(initial_cost, df_opt['block_d'].copy())]
            current_df = df_opt.copy()

            for t_cand, _ in candidates.iterrows():
                # Add a block to the cumulative bridge
                current_df.at[t_cand, 'block_d'] = True

                # Recalculate impact to see how the SOC "flows" toward the peak
                impacted_df = self.calculate_impact(current_df)
                current_cost = float(self.calculate_cost(impacted_df).get('total_net_cost_and_inventory', 0))

                # Store the result of this bridge length
                trials.append((current_cost, current_df['block_d'].copy()))

                # If the peak is covered, we skip
                if impacted_df.at[t_peak, 'new_pct'] >= 3: # was new_grid >= -0.001
                    break

            # 4. Find the best trial (the one with the lowest total cost)
            best_cost, best_blocks = min(trials, key=lambda x: x[0])

            if best_cost < initial_cost - 0.001:
                logger.debug(f"Bridge found for peak {t_peak}: Reduced cost from {initial_cost:.4f} to {best_cost:.4f}")
                df_opt['block_d'] = best_blocks
                # Apply the impact of the best blocks before moving to the next peak
                df_opt = self.calculate_impact(df_opt)

            processed_peaks += 1

        return df_opt

    def apply_rule_force_charge(self, df_opt: pd.DataFrame, min_price_diff: float = 0.08) -> pd.DataFrame:
        """
        Precision Force Charge: Finds the optimal number of minutes to charge
        from the grid to maximize profit.
        """
        df_opt = self.calculate_impact(df_opt)
        initial_cost_dict = self.calculate_cost(df_opt)
        initial_cost = float(initial_cost_dict.get('total_net_cost_and_inventory', 0))
        best_total_cost = initial_cost

        # Identify expensive peaks (grid imports)
        # peaks = df_opt[df_opt['new_grid'] < -0.01].sort_values(by='buy_price', ascending=False)

        threshold = df_opt['buy_price'].quantile(0.75)
        peaks = df_opt[df_opt['buy_price'] >= threshold].sort_values(by='buy_price', ascending=False)

        for t_peak, peak_row in peaks.iterrows():
            # Check live grid deficit
            # if abs(float(df_opt.at[t_peak, 'new_grid'])) < 0.01:
            #     continue

            peak_price = float(peak_row['buy_price'])

            # Find the 'Sun Trap' boundary
            before_peak = df_opt.loc[:t_peak].iloc[:-1]
            full_times = before_peak[before_peak['new_pct'] >= 98.0].index
            t_start_search = full_times.max() if not full_times.empty else df_opt.index[0]

            candidates_mask = (df_opt.index >= t_start_search) & (df_opt.index < t_peak)
            valid_candidates = df_opt.loc[candidates_mask].copy()

            # Filter by price and sort CHEAPEST first
            price_mask = valid_candidates['buy_price'] <= (peak_price - min_price_diff)
            valid_candidates = valid_candidates.loc[price_mask].sort_values(by='buy_price', ascending=True)

            for t_cand in valid_candidates.index:
                # If the peak is already covered by a previous candidate, skip to next peak
                if abs(float(df_opt.at[t_peak, 'new_grid'])) < 0.01:
                    break

                # Best duration: 15m, 10m, or 5m
                for minutes in [15, 10, 5]:
                    # Store state for rollback
                    old_force_c = df_opt.at[t_cand, 'force_c']
                    old_force_time = df_opt.at[t_cand, 'force_time']

                    # Apply trial minutes
                    df_opt.at[t_cand, 'force_c'] = True
                    df_opt.at[t_cand, 'force_time'] = minutes

                    df_opt = self.calculate_impact(df_opt)
                    trial_cost = float(self.calculate_cost(df_opt).get('total_net_cost_and_inventory', 0))

                    if trial_cost < best_total_cost - 0.01:
                        best_total_cost = trial_cost
                        break
                    else:
                        # REVERT: mins wasn't profitable
                        df_opt.at[t_cand, 'force_c'] = old_force_c
                        df_opt.at[t_cand, 'force_time'] = old_force_time
                        df_opt = self.calculate_impact(df_opt)

        return df_opt

    def apply_rule_force_charge_v2(self, df_opt: pd.DataFrame, min_price_diff: float = 0.05) -> pd.DataFrame:
        """
        Opportunistic Force Charge: Starts from the cheapest hours and checks if
        charging there reduces total cost before the battery next hits 98%.
        """
        # 1. Baseline
        df_opt = self.calculate_impact(df_opt)
        best_total_cost = float(self.calculate_cost(df_opt).get('total_net_cost', 0))

        # 2. Candidates: Sort all intervals by buy_price (cheapest first)
        # We skip intervals where battery is already full
        cheap_candidates = df_opt[df_opt['new_pct'] < 98.0].sort_values(by='buy_price', ascending=True)

        for t_cand, cand_row in cheap_candidates.iterrows():
            cand_price = float(cand_row['buy_price'])
            print(f"Force charge test for {t_cand}: {cand_price}")

            # 3. Define the look-ahead window: from now until the battery is full (>=98%)
            future_full = df_opt.index[(df_opt.index > t_cand) & (df_opt['new_pct'] >= 98.0)]
            t_end_search = future_full[0] if not future_full.empty else df_opt.index[-1]

            window = df_opt.loc[t_cand:t_end_search]

            # 4. Quick filter: Is there any price in this window high enough to justify charging?
            # If the highest price in the window isn't at least min_price_diff higher, skip.
            if window['buy_price'].max() < (cand_price + min_price_diff):
                continue

            # 5. Trial durations
            for minutes in [15, 10, 5]:
                print(f"   {minutes} minutes")
                # Save original state
                old_force_c = df_opt.at[t_cand, 'force_c']
                old_force_time = df_opt.at[t_cand, 'force_time']

                # Apply force charge
                df_opt.at[t_cand, 'force_c'] = True
                df_opt.at[t_cand, 'force_time'] = minutes

                # Calculate impact and check cost
                df_opt = self.calculate_impact(df_opt)
                df_opt = self.apply_rule_block_discharge(df_opt)
                df_opt = self.calculate_impact(df_opt)
                trial_cost = float(self.calculate_cost(df_opt, False).get('total_net_cost', 0))
                print(f"   New limit_i {df_opt.at[t_cand, 'limit_i']}")
                print(f"   {trial_cost} new cost. Old cost was {best_total_cost}")
                # Profit check: Did we actually save money?
                if trial_cost < (best_total_cost - 0.005):
                    best_total_cost = trial_cost
                    logger.info(f"Force Charge: {t_cand} ({minutes}m) saved cost. New Total: €{best_total_cost:.4f}")
                    break  # Keep these minutes and move to the next cheapest interval
                else:
                    # REVERT
                    df_opt.at[t_cand, 'force_c'] = old_force_c
                    df_opt.at[t_cand, 'force_time'] = old_force_time
                    df_opt = self.calculate_impact(df_opt)
                # input("continue?")

        return df_opt

    def apply_rule_force_charge_v3(self, df_opt: pd.DataFrame, min_price_diff: float = 0.05) -> pd.DataFrame:
        """
        Cumulative Force Charge: Stacks the cheapest intervals of the day and
        re-calculates the block-discharge logic to find the true global minimum.
        """
        # 1. Setup
        df_working = df_opt.copy()
        unique_days = df_working.index.normalize().unique()

        # Track the absolute best version of the whole plan
        final_best_df = df_working.copy()
        current_best_total_cost = float(self.calculate_cost(final_best_df).get('total_net_cost_and_inventory', 0))

        for day in unique_days:
            logger.info(f"Analyzing Force Charge potential for {day.date()}...")

            # Get candidates for this day, sorted by buy_price
            day_mask = df_working.index.normalize() == day
            day_df = df_working[day_mask]

            # We only look at the first 15 cheapest slots (approx 3.75h of charging)
            # Skip slots where battery is already 100% or price is too high vs the day's max
            day_max_price = day_df['buy_price'].max()
            candidates = day_df[day_df['buy_price'] < (day_max_price - min_price_diff)]
            candidates = candidates.sort_values(by='buy_price', ascending=True).head(15)

            if candidates.empty:
                continue

            best_day_df = final_best_df.copy()
            best_day_cost = current_best_total_cost

            # Cumulative stack: Each loop keeps the previous ones 'Forced'
            trial_df = final_best_df.copy()

            for i, (t_cand, _) in enumerate(candidates.iterrows()):
                # 4. Set current candidate to 15 mins
                trial_df.at[t_cand, 'force_c'] = True
                trial_df.at[t_cand, 'force_time'] = 15

                # 5. The "Truth" Calculation: Impact -> Blocks -> Impact
                temp_df = trial_df.copy()
                temp_df = self.calculate_impact(temp_df)
                temp_df = self.apply_rule_block_discharge(temp_df)
                temp_df = self.calculate_impact(temp_df)

                # 6. Cost Evaluation
                trial_cost = float(self.calculate_cost(temp_df).get('total_net_cost_and_inventory', 0))

                if trial_cost < (best_day_cost - 0.005):
                    # 7. Refinement: Check if 10 or 5 minutes is even better for this specific slot
                    refined_best_t = 15
                    refined_best_cost = trial_cost

                    for mins in [10, 5]:
                        refine_df = trial_df.copy()
                        refine_df.at[t_cand, 'force_time'] = mins
                        refine_df = self.calculate_impact(refine_df)
                        refine_df = self.apply_rule_block_discharge(refine_df)
                        refine_df = self.calculate_impact(refine_df)

                        refine_cost = float(self.calculate_cost(refine_df).get('total_net_cost_and_inventory', 0))
                        if refine_cost < (refined_best_cost - 0.005):
                            refined_best_cost = refine_cost
                            refined_best_t = mins

                    # Commit this slot to the cumulative trial
                    trial_df.at[t_cand, 'force_time'] = refined_best_t
                    best_day_cost = refined_best_cost
                    best_day_df = temp_df.copy()
                    logger.debug(f"Added {t_cand} ({refined_best_t}m) to stack. New cost: {best_day_cost:.4f}")
                else:
                    # If adding the i-th cheapest slot doesn't help,
                    # we don't 'Force' it in trial_df for the next iterations
                    trial_df.at[t_cand, 'force_c'] = False
                    trial_df.at[t_cand, 'force_time'] = 0

            # Update the global plan with the best version found for this day
            final_best_df = best_day_df
            current_best_total_cost = best_day_cost

        return final_best_df


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
    first_day_start = datetime(2026, 4, 2, 23, 00, 0, tzinfo=pytz.UTC)
    first_day_end = datetime(2026, 4, 3, 21, 45, 0, tzinfo=pytz.UTC)
    price_points = db_handler.get_da_prices(first_day_start.astimezone(local_tz))
    process_price_points_to_app_state(price_points, first_day_start, "electricity_prices_today", config, db_handler)

    # First day plan (today)
    ff = cd.generate_consumption_forecast(first_day_start, first_day_end)
    first_plan_df = bp.generate_plan(first_day_start, first_day_end, ff, db_handler, 2.5, initial_soc_pct=11)
    last_soc_day1 = first_plan_df['soc_pct'].iloc[-1]

    # Fill app_state with NEPIs
    second_day_start = datetime(2026, 4, 3, 22, 00, 0, tzinfo=pytz.UTC)
    second_day_end = datetime(2026, 4, 4, 21, 45, 0, tzinfo=pytz.UTC)
    price_points = db_handler.get_da_prices(second_day_start.astimezone(local_tz))
    process_price_points_to_app_state(price_points, second_day_start, "electricity_prices_tomorrow", config, db_handler)

    # Second day plan (tomorrow)
    ff = cd.generate_consumption_forecast(second_day_start, second_day_end)
    second_plan_df = bp.generate_plan(second_day_start, second_day_end, ff, db_handler, 2.5, initial_soc_pct=last_soc_day1)

    plan_df = pd.concat([first_plan_df, second_plan_df])

    cur_dt = datetime(2026, 4, 3, 14, 1, 0, tzinfo=pytz.UTC)
    opt_plan_df = bp.optimize_plan(plan_df, cur_dt, 100, GLOBAL_APP_STATE, config, db_handler, True)
    # cur_dt = datetime(2026, 3, 28, 22, 33, 0, tzinfo=pytz.UTC)
    # opt_plan_df = bp.optimize_plan(opt_plan_df, cur_dt, 82, GLOBAL_APP_STATE)

    with pd.option_context(
            'display.max_rows', None,
            'display.max_columns', None,
            'display.width', 2000,
            'display.expand_frame_repr', False,
            'display.precision', 3,
    ):
        pd.options.display.float_format = '{:,.3f}'.format
        print(opt_plan_df)

