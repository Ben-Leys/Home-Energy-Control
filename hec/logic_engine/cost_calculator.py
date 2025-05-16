# hec/pricing/cost_calculator.py
import logging
from datetime import datetime, date, timedelta, time
from typing import Optional, Dict, Any, Tuple, List
import calendar

from hec.models.models import NetElectricityPriceInterval, PricePoint
from hec.database_ops.db_handler import DatabaseHandler
from hec.core.tariff_manager import TariffManager

logger = logging.getLogger(__name__)


def calculate_net_intervals_for_day(db: DatabaseHandler, tm: TariffManager,
                                    target_date_local: datetime) -> List[NetElectricityPriceInterval]:
    """
    Fetches all price points for `target_date`, computes net buy/sell prices
    according to that day’s tariffs, and returns a list of price intervals.

    Args:
        db: has method get_da_prices(date) -> List[PricePoint]
        tm: TariffManager
        target_date_local (local): the date for which to build intervals

    Returns:
        List of NetElectricityPriceInterval, one per PricePoint.
    """
    # 1. Pull PricePoints
    local_tz = target_date_local.tzinfo or datetime.now().astimezone().tzinfo
    target_date_local = datetime.combine(target_date_local, time.min, local_tz)

    price_points: List[PricePoint] = db.get_da_prices(target_date_local)

    if not price_points:
        logger.warning(f"No price points found for target date {target_date_local}")
        return []

    # 2. Fetch the day's tariffs once
    tariffs = tm.get_all_tariffs(target_date_local.date())

    intervals: List[NetElectricityPriceInterval] = []
    try:
        for pp in price_points:
            # Convert MWh->kWh
            gross_kwh = pp.price_eur_per_mwh / 1000

            # Supplier & grid & government slices
            supplier = tariffs["energy_supplier"]
            grid = tariffs["grid_operator"]
            gov = tariffs["government"]
            active_contract = tariffs["active_contract"]

            # --- NET BUY ---
            fixed_buy = supplier["fixed"]["buy_price_per_kwh"]
            dynamic_buy = (gross_kwh * supplier["dynamic"]["spot_buy_multiplier"]
                           + supplier["dynamic"]["spot_buy_fixed_fee_per_kwh"])

            # Supplier certificates
            for fee in ("green_certificate_fee_per_kwh", "chp_certificate_fee_per_kwh"):
                fixed_buy += supplier["fixed"][fee]
                dynamic_buy += supplier["dynamic"][fee]

            # Grid usage
            fixed_buy += grid["grid_usage_fee_per_kwh"]
            dynamic_buy += grid["grid_usage_fee_per_kwh"]

            # Government tax
            fixed_buy += gov["energy_contribution_per_kwh"]
            dynamic_buy += gov["energy_contribution_per_kwh"]

            # Tiered excise: based on year consumption - already add the lowest value
            excise = min(gov["rate_per_kwh_below"], gov["rate_per_kwh_above"])
            fixed_buy += excise
            dynamic_buy += excise

            # VAT
            fixed_buy *= gov["vat"]
            dynamic_buy *= gov["vat"]

            # Post-VAT levies
            fixed_buy += gov["federal_contribution_fund_per_kwh"]
            dynamic_buy += gov["federal_contribution_fund_per_kwh"]

            # --- NET SELL ---
            fixed_sell = supplier["fixed"]["sell_price_per_kwh"]
            dynamic_sell = (gross_kwh * supplier["dynamic"]["spot_sell_multiplier"]
                            - supplier["dynamic"]["spot_sell_fixed_fee_per_kwh"])

            # Instantiate interval
            local_start = pp.timestamp_utc.astimezone(local_tz)
            interval = NetElectricityPriceInterval(
                interval_start_local=local_start,
                resolution_minutes=pp.resolution_minutes,
                active_contract_type=active_contract,
                net_prices_eur_per_kwh={
                    "fixed": {"buy": fixed_buy, "sell": fixed_sell},
                    "dynamic": {"buy": dynamic_buy, "sell": dynamic_sell}
                }
            )
            intervals.append(interval)

        return intervals

    except ValueError as e:
        logger.warning(f"Cannot calculate net interval prices for {target_date_local.isoformat()}: {e}")
        return []


def calculate_total_costs_for_period(start_date: date, end_date: date,
                                     db: DatabaseHandler, tm: TariffManager) -> Dict[str, Any]:
    """
    Calculates total electricity costs and revenues for a given period
    for both fixed and dynamic contract.

    Args:
        start_date (local date): start date for calculation
        end_date (local date): end date for calculation (inclusive)
        db (DatabaseHandler): database handler
        tm (TariffManager): tariff manager

    Returns a dict with:
      - Total kWh imported/exported
      - Total cost & revenue for fixed & dynamic plans
      - Active contract (if the same during the period)
      - Time-based costs
      - details_by_day: per-day breakdown
    """
    logger.debug(f"Calculating total costs for period: {start_date} to {end_date}")
    results: Dict[str, Any] = {
        "total_kwh_imported": 0.0,
        "total_kwh_exported": 0.0,
        "active_contract": None,
        "fixed": {"total_cost_eur_import": 0.0, "total_revenue_eur_export": 0.0},
        "dynamic": {"total_cost_eur_import": 0.0, "total_revenue_eur_export": 0.0},
        "time_based_costs_fixed": 0.0,
        "time_based_costs_dynamic": 0.0,
        "capacity_costs_eur": 0.0,
        "details_by_day": []  # type: List[Dict[str,Any]]
    }

    day = start_date
    while day <= end_date:
        # 1. Build midnight
        midnight = datetime.combine(day, time.min)

        # 2. Fetch price intervals
        intervals: List[NetElectricityPriceInterval] = calculate_net_intervals_for_day(db, tm, midnight)
        if not intervals:
            logger.warning(f"No net intervals found for {day}. Skipping calculation.")
            day += timedelta(days=1)
            continue

        # Keep track of active contract in case it changes
        first_contract = intervals[0].active_contract_type
        if results["active_contract"] is None:
            results["active_contract"] = first_contract
        elif results["active_contract"] != first_contract:
            results["active_contract"] = "mixed"

        # 3. Per-interval energy deltas
        interval_deltas = db.get_energy_deltas_for_intervals(intervals)

        # accumulate totals for this day
        day_imported = 0.0
        day_exported = 0.0
        dyn_cost = dyn_rev = 0.0

        for iv in intervals:
            key = iv.interval_start_local.isoformat()
            delta = interval_deltas.get(key, {"imported_kwh": 0.0, "exported_kwh": 0.0})
            imp = delta["imported_kwh"]
            exp = delta["exported_kwh"]

            day_imported += imp
            day_exported += exp

            # dynamic net prices for this interval
            buy_dyn = iv.net_prices_eur_per_kwh["dynamic"]["buy"]
            sell_dyn = iv.net_prices_eur_per_kwh["dynamic"]["sell"]

            dyn_cost += imp * buy_dyn
            dyn_rev += exp * sell_dyn

        # 4. fixed-plan: use first interval’s fixed price (constant through day)
        fixed_buy = intervals[0].net_prices_eur_per_kwh["fixed"]["buy"]
        fixed_sell = intervals[0].net_prices_eur_per_kwh["fixed"]["sell"]
        fix_cost = day_imported * fixed_buy
        fix_rev = day_exported * fixed_sell

        # 5. time-based costs (prorated)
        tariffs = tm.get_all_tariffs(day)
        grid_cfg = tariffs["grid_operator"]
        supplier_cfg = tariffs["energy_supplier"]
        # Daily data-management (per year):
        days_in_year = 365 + calendar.isleap(day.year)
        data_mgmt_yr = grid_cfg.get("data_management", 0.0)
        daily_data_mgmt = data_mgmt_yr / days_in_year
        # Daily subscription (dynamic plan only, per month):
        days_in_month = calendar.monthrange(day.year, day.month)[1]
        sub_monthly = supplier_cfg["dynamic"].get("subscription_cost", 0.0)
        daily_sub = sub_monthly / days_in_month

        # Add to running total
        results["time_based_costs_fixed"] += daily_data_mgmt
        results["time_based_costs_dynamic"] += daily_data_mgmt + daily_sub

        # 6. Accumulate into results
        results["total_kwh_imported"] += day_imported
        results["total_kwh_exported"] += day_exported
        results["fixed"]["total_cost_eur_import"] += fix_cost
        results["fixed"]["total_revenue_eur_export"] += fix_rev
        results["dynamic"]["total_cost_eur_import"] += dyn_cost
        results["dynamic"]["total_revenue_eur_export"] += dyn_rev

        # 7. Capacity tariff based on 12m peaks
        avg_peak_w = db.get_avg_monthly_peak_w_last_12m(day, grid_cfg['capacity_tariff_minimum_kw'] * 1000)
        cap_tariff_yr_per_kw = grid_cfg.get("capacity_tariff_per_kw_per_year", 0.0)
        results["capacity_costs_eur"] += (avg_peak_w / 1000 * cap_tariff_yr_per_kw) / days_in_year

        # 8. Store per-day breakdown
        results["details_by_day"].append({
            "date": day.isoformat(),
            "imported_kwh": round(day_imported, 3),
            "exported_kwh": round(day_exported, 3),
            "cost_fixed": round(fix_cost, 3),
            "rev_fixed": round(fix_rev, 3),
            "cost_dynamic": round(dyn_cost, 3),
            "rev_dynamic": round(dyn_rev, 3),
            "time_costs_fixed": round(daily_data_mgmt, 3),
            "time_costs_dynamic": round(daily_data_mgmt + daily_sub, 3),
            "capacity_costs_eur": round((avg_peak_w / 1000 * cap_tariff_yr_per_kw) / days_in_year, 3),
        })

        day += timedelta(days=1)
    return results


# For testing only
if __name__ == "__main__":
    from hec.core.tariff_manager import initialize_tariff_manager
    from hec.core.app_initializer import load_app_config, initialize_database_handler

    prepare_time = datetime.now()
    app_config = load_app_config()
    db = initialize_database_handler(app_config)
    tm = initialize_tariff_manager(app_config)

    print(calculate_total_costs_for_period(date(2025, 5, 14), date(2025, 5, 14), db, tm))
    exit(0)
    start_time = datetime.now()
    net_prices = []
    t_date = datetime.now()
    for i in range(365):
        net_prices += calculate_net_intervals_for_day(db, tm, t_date - timedelta(days=365-i))
    end_time = datetime.now()
    for nepi in net_prices:
        print(nepi)
    print(f"Calc time: {end_time - start_time}")
