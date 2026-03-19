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

    def generate_plan(self, start_dt: datetime, end_dt: datetime, consumption_s: pd.Series, db: DatabaseHandler,
                      max_peak_kw = 2.5, initial_soc_pct: float = 0.0) -> pd.DataFrame:
        """
        Generates a 15-minute resolution battery plan based on excess solar and consumption.
        """
        self.max_peak_kw = max_peak_kw

        # Ensure consumption is a Series
        if isinstance(consumption_s, pd.DataFrame):
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
        df_plan = df_plan[~df_plan.index.duplicated(keep='first')]

        logger.info("Battery prediction plan generated")

        return df_plan

    def calculate_impact(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Recalculates new_c and new_pct based on optimization flags:
        block_d, block_c, force_c, and limit_i.
        """
        # Calculate the initial SoC before the first interval
        first_row = df.iloc[0]
        first_charge = float(first_row['new_c'])
        first_soc_kwh = float(first_row['new_pct']) / 100 * self.capacity_kwh

        if first_charge >= 0:
            initial_soc_kwh = first_soc_kwh - (first_charge * self.eff)
        else:
            initial_soc_kwh = first_soc_kwh - (first_charge / self.eff)

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
            ac_space_left = (self.capacity_kwh - current_soc_kwh) / self.eff
            pot_c = max(0.0, min(max_c, ac_space_left))
            # pot_c = max(0.0, max_c)
            max_inverter_kwh = self.inv_kw * 1000 * self.dt_hours

            if row['sell_price'] < 0:
                # if row['new_pct'] == 100:
                #     pot_c = 0
                zero_export_limit = max(0.0, row['cons_kwh'] + forced_energy_kwh + pot_c)
                max_inverter_kwh = min(max_inverter_kwh, zero_export_limit)
                df.at[idx, 'limit_i'] = (max_inverter_kwh * 1000.0) / self.dt_hours

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
            new_c += forced_energy_kwh

            # 65. Apply Battery Physical Constraints (Capacity & Efficiency)
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

        results = {
            "buy_cost": round(buy_cost, 4),
            "sell_revenue": round(sell_revenue, 4),
            "charging_opportunity_cost": round(sell_loss, 4),
            "discharging_avoided_cost": round(buy_avoided, 4),
            "total_net_cost": round(total_net_cost, 4)
        }

        if print_on_screen:
            # Print Statement for Debugging/Optimization monitoring
            print(f"\n--- Financial Breakdown ---")
            print(f"Grid Buy Cost:       €{results['buy_cost']:.4f}")
            print(f"Grid Sell Revenue:   €{results['sell_revenue']:.4f}")
            print(f"Charge Opp. Cost:    €{results['charging_opportunity_cost']:.4f}")
            print(f"Discharge Avoided:   €{results['discharging_avoided_cost']:.4f}")
            print(f"TOTAL NET COST:      €{results['total_net_cost']:.4f}")
            print(f"---------------------------\n")

        return results

    def optimize_plan(self, df_plan, cur_dt, actual_soc_pct, state: Dict) -> pd.DataFrame:
        self.cur_dt = cur_dt
        opt_plan = df_plan[df_plan.index > self.cur_dt].copy()
        opt_plan = opt_plan[~opt_plan.index.duplicated(keep='first')]

        # Extract and align prices from global_app_state
        opt_plan = self.add_prices_to_plan(opt_plan, state)

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

        if not opt_plan.empty:
            # 1. Calculate the first row's ending SOC
            start_kwh = (actual_soc_pct / 100.0) * self.capacity_kwh
            first_row = opt_plan.iloc[0]

            # End kWh = Start kWh + Net Flow (Respecting battery limits)
            end_kwh = max(0, min(self.capacity_kwh, start_kwh + first_row['net_kwh']))

            # Convert back to percent and store in 'new_pct'
            opt_plan.iat[0, opt_plan.columns.get_loc('new_pct')] = (end_kwh / self.capacity_kwh) * 100.0

        opt_plan = self.calculate_impact(opt_plan)
        # pre_block_plan = opt_plan.copy()

        initial_cost_data = self.calculate_cost(opt_plan, True)
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

        end_cost_data = self.calculate_cost(opt_plan, True)
        end_total = float(end_cost_data.get('total_net_cost'))

        logger.info(f"Battery optimization plan end | Final Cost: €{end_total:.4f}")
        logger.info(f"Total Net Savings: €{end_total - initial_total:.4f}")

        return opt_plan

    def apply_rule_block_charge(self, df_opt: pd.DataFrame) -> pd.DataFrame:
        """
        Blocks charging during the most expensive export periods of the day
        if solar production exceeds battery capacity.
        """
        # Process each day independently
        for date in pd.to_datetime(df_opt.index).normalize().unique():
            day_mask = pd.to_datetime(df_opt.index).normalize() == date
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

            # If sell_price is < 0, we MUST NOT block charging.
            blocked_candidates = [
                ts for ts in blocked_candidates
                if df_opt.at[ts, 'sell_price'] >= 0
            ]

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

    def apply_rule_block_discharge(self, df_opt: pd.DataFrame, min_price_diff: float = 0.03) -> pd.DataFrame:
        """
        Look-ahead Peak Shaving: Finds expensive future peaks and blocks
        enough cumulative discharge slots to ensure energy is available for them.
        """
        # 1. Identify "Expensive Peaks" where we are currently buying from the grid
        # and the battery is empty (or grid_in is high).
        peaks = df_opt[df_opt['new_grid'] < -0.01].copy()
        if peaks.empty:
            return df_opt

        # Sort peaks by price (highest first) to prioritize the most valuable savings
        peaks = peaks.sort_values(by='buy_price', ascending=False)

        for t_peak, peak_row in peaks.iterrows():
            peak_price = peak_row['buy_price']

            # How much energy do we actually need to save to cover this peak?
            # (Negative grid_in means we need to import)
            energy_needed = abs(peak_row['new_grid'])

            # 2. Look BACKWARDS from the peak to find candidate slots to block
            # We stop looking back if we hit a "Sun Trap" (battery was full)
            full_times = df_opt.index[(df_opt.index <= t_peak) & (df_opt['new_pct'] >= 98.0)]
            t_start_search = full_times[-1] if not full_times.empty else df_opt.index[0]

            # Candidates are slots between 'now' (or last full) and the 'peak'
            # that are currently discharging and are cheaper than the peak.
            candidates = df_opt.loc[t_start_search:t_peak].iloc[:-1]
            candidates = candidates[
                # (candidates['new_c'] < -0.01) &
                (candidates['buy_price'] <= peak_price - min_price_diff)
                ]

            if candidates.empty:
                continue

            # 3. Block "Chain": We block slots chronologically to "push" the charge forward.
            # If we block 02:30, we MUST also block 02:45, 03:00...
            # otherwise 02:30 just discharges at 02:45.

            # We iterate through candidates and test if blocking them helps.
            # To avoid the "spill-over" you saw, we can try blocking in segments.
            saved_so_far = 0
            for t_cand, cand_row in candidates.iterrows():
                if saved_so_far >= energy_needed:
                    break

                # Temporarily block this slot
                df_opt.at[t_cand, 'block_d'] = True

                # Re-calculate to see if that energy actually reached the peak
                df_opt = self.calculate_impact(df_opt)

                # Check if the grid_in at our TARGET peak improved
                new_peak_grid = df_opt.at[t_peak, 'new_grid']

                if new_peak_grid > peak_row['new_grid'] + 0.001:
                    # Success: This block actually pushed energy to the peak!
                    saved_so_far += (new_peak_grid - peak_row['new_grid'])
                    # Update peak_row for the next candidate check
                    peak_row['new_grid'] = new_peak_grid
                else:
                    # Failure: The energy just "leaked" into the next available slot.
                    # We leave it blocked anyway for now because we likely need
                    # to block the NEXT slot too to create a "bridge" to the peak.
                    pass

        return df_opt

    def apply_rule_force_charge(self, df_opt: pd.DataFrame, min_price_diff: float = 0.05) -> pd.DataFrame:
        """
        Precision Force Charge: Finds the optimal number of minutes to charge
        from the grid to maximize profit.
        """
        df_opt = self.calculate_impact(df_opt)
        initial_cost_dict = self.calculate_cost(df_opt)
        initial_cost = float(initial_cost_dict.get('total_net_cost', 0))
        best_total_cost = initial_cost

        # Identify expensive peaks (grid imports)
        peaks = df_opt[df_opt['new_grid'] < -0.01].sort_values(by='buy_price', ascending=False)

        for t_peak, peak_row in peaks.iterrows():
            # Check live grid deficit
            if abs(df_opt.at[t_peak, 'new_grid']) < 0.01:
                continue

            peak_price = float(peak_row['buy_price'])

            # Safely find the 'Sun Trap' boundary
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
                if abs(df_opt.at[t_peak, 'new_grid']) < 0.01:
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
                    trial_cost = float(self.calculate_cost(df_opt).get('total_net_cost', 0))

                    if trial_cost < best_total_cost - 0.01:
                        best_total_cost = trial_cost
                        break
                    else:
                        # REVERT: mins wasn't profitable
                        df_opt.at[t_cand, 'force_c'] = old_force_c
                        df_opt.at[t_cand, 'force_time'] = old_force_time
                        df_opt = self.calculate_impact(df_opt)

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
    first_day_start = datetime(2026, 3, 16, 23, 00, 0, tzinfo=pytz.UTC)
    first_day_end = datetime(2026, 3, 17, 22, 45, 0, tzinfo=pytz.UTC)
    price_points = db_handler.get_da_prices(first_day_start.astimezone(local_tz))
    process_price_points_to_app_state(price_points, first_day_start, "electricity_prices_today", config, db_handler)

    # First day plan (today)
    ff = cd.generate_consumption_forecast(first_day_start, first_day_end)
    first_plan_df = bp.generate_plan(first_day_start, first_day_end, ff, db_handler, 2.5, initial_soc_pct=11)
    last_soc_day1 = first_plan_df['soc_pct'].iloc[-1]

    # Fill app_state with NEPIs
    second_day_start = datetime(2026, 3, 17, 23, 00, 0, tzinfo=pytz.UTC)
    second_day_end = datetime(2026, 3, 18, 22, 45, 0, tzinfo=pytz.UTC)
    price_points = db_handler.get_da_prices(second_day_start.astimezone(local_tz))
    process_price_points_to_app_state(price_points, second_day_start, "electricity_prices_tomorrow", config, db_handler)

    # Second day plan (tomorrow)
    ff = cd.generate_consumption_forecast(second_day_start, second_day_end)
    second_plan_df = bp.generate_plan(second_day_start, second_day_end, ff, db_handler, 2.5, initial_soc_pct=last_soc_day1)

    plan_df = pd.concat([first_plan_df, second_plan_df])

    time_stamp = datetime(2026, 1, 10, 23, 0, 0, tzinfo=pytz.UTC)
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
