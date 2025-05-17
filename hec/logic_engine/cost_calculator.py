# hec/pricing/cost_calculator.py
import calendar
import logging
from datetime import datetime, date, timedelta, time
from typing import Optional, Dict, Any, List

from hec.core.tariff_manager import TariffManager, initialize_tariff_manager
from hec.database_ops.db_handler import DatabaseHandler
from hec.models.models import NetElectricityPriceInterval, PricePoint

logger = logging.getLogger(__name__)
debug_logger = logging.getLogger('Only Debug')
debug_logger.setLevel(logging.INFO)


def calculate_net_intervals_for_day(db: DatabaseHandler, app_config, target_date_local: datetime,
                                    price_points: List[PricePoint] = None) -> List[NetElectricityPriceInterval]:
    """
    Fetches all price points for `target_date` if None received, computes net buy/sell prices
    according to that day’s tariffs, and returns a list of price intervals.

    Args:
        db: has method get_da_prices(date) -> List[PricePoint]
        app_config: Dict with application configuration data.
        target_date_local (local): the date for which to build intervals
        price_points (Optional[List[PricePoint]]): list of price points to build intervals for

    Returns:
        List of NetElectricityPriceInterval, one per PricePoint.
    """
    local_tz = target_date_local.tzinfo or datetime.now().astimezone().tzinfo
    target_date_local = datetime.combine(target_date_local, time.min, local_tz)

    # 1. Pull PricePoints
    if price_points is None:
        price_points: List[PricePoint] = db.get_da_prices(target_date_local)

    if not price_points:
        logger.warning(f"No price points found for target date {target_date_local}")
        return []

    # 2. Fetch the day's tariffs once
    tm = initialize_tariff_manager(app_config)
    tariffs = tm.get_all_tariffs(target_date_local.date())

    intervals: List[NetElectricityPriceInterval] = []
    try:
        for pp in price_points:
            debug_logger.debug(f"Price Point: {pp.position}")
            # Convert MWh->kWh
            gross_kwh = pp.price_eur_per_mwh / 1000
            debug_logger.debug(f"   Gross kWh price: {gross_kwh}")

            # Supplier & grid & government slices
            supplier = tariffs["energy_supplier"]
            grid = tariffs["grid_operator"]
            gov = tariffs["government"]
            active_contract = tariffs["active_contract"]

            # --- NET BUY ---
            fixed_buy = supplier["fixed"]["buy_price_per_kwh"]
            dynamic_buy = (gross_kwh * supplier["dynamic"]["spot_buy_multiplier"]
                           + supplier["dynamic"]["spot_buy_fixed_fee_per_kwh"])
            debug_logger.debug(f"   Fixed buy price: {fixed_buy}")
            debug_logger.debug(f"   Dynamic buy price: {dynamic_buy}")

            # Supplier certificates
            for fee in ("green_certificate_fee_per_kwh", "chp_certificate_fee_per_kwh"):
                fixed_buy += supplier["fixed"][fee]
                dynamic_buy += supplier["dynamic"][fee]
            debug_logger.debug(f"   Fixed + cert: {fixed_buy}")
            debug_logger.debug(f"   Dynamic + cert: {dynamic_buy}")

            # Grid usage
            fixed_buy += grid["grid_usage_fee_per_kwh"]
            dynamic_buy += grid["grid_usage_fee_per_kwh"]
            debug_logger.debug(f"   Fixed + grid_usage: {fixed_buy}")
            debug_logger.debug(f"   Dynamic + grid_usage: {dynamic_buy}")

            # Government tax
            fixed_buy += gov["energy_contribution_per_kwh"]
            dynamic_buy += gov["energy_contribution_per_kwh"]
            debug_logger.debug(f"   Fixed + gov_tax: {fixed_buy}")
            debug_logger.debug(f"   Dynamic + gov_tax: {dynamic_buy}")

            # Tiered excise: based on yet unknown year consumption - already add the lowest value
            excise = gov["rate_per_kwh_below"]
            fixed_buy += excise
            dynamic_buy += excise
            debug_logger.debug(f"   Fixed + excise: {fixed_buy}")
            debug_logger.debug(f"   Dynamic + excise: {dynamic_buy}")

            # VAT
            fixed_buy *= gov["vat"]
            dynamic_buy *= gov["vat"]
            debug_logger.debug(f"   Fixed * vat: {fixed_buy}")
            debug_logger.debug(f"   Dynamic * vat: {dynamic_buy}")

            # Post-VAT levies
            fixed_buy += gov["federal_contribution_fund_per_kwh"]
            dynamic_buy += gov["federal_contribution_fund_per_kwh"]

            # --- NET SELL ---
            fixed_sell = supplier["fixed"]["sell_price_per_kwh"]
            dynamic_sell = (gross_kwh * supplier["dynamic"]["spot_sell_multiplier"]
                            - supplier["dynamic"]["spot_sell_fixed_fee_per_kwh"])
            debug_logger.debug(f"   Fixed net sell: {fixed_sell}")
            debug_logger.debug(f"   Dynamic net sell: {dynamic_sell}")

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


def calculate_total_costs_for_period(start_date: date, end_date: date, app_config,
                                     db: DatabaseHandler, tm: TariffManager) -> Dict[str, Any]:
    """
    Calculates total electricity costs and revenues for a given period
    for both fixed and dynamic contract.

    Args:
        start_date (local date): start date for calculation
        end_date (local date): end date for calculation (inclusive)
        app_config: Dict with application configuration data.
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
        "fixed": {"energy_cost_import": 0.0, "energy_revenue_export": 0.0, "time_based_costs": 0.0,
                  "capacity_costs_eur": 0.0, "total_cost_excl_rev": 0.0, "total_bill": 0.0},
        "dynamic": {"energy_cost_import": 0.0, "energy_revenue_export": 0.0, "time_based_costs": 0.0,
                    "capacity_costs_eur": 0.0, "total_cost_excl_rev": 0.0, "total_bill": 0.0},
        "yearly_imported_kwh": 0.0,
        "details_by_day": []  # type: List[Dict[str,Any]]
    }

    day = start_date
    previous_year = day.year
    yearly_imported_kwh = 0.0
    while day <= end_date:
        # 1. Build midnight
        midnight = datetime.combine(day, time.min)
        debug_logger.debug(f"Calculating total costs for {midnight}")

        # 2. Fetch tariffs and price intervals
        tariffs = tm.get_all_tariffs(start_date)
        intervals: List[NetElectricityPriceInterval] = calculate_net_intervals_for_day(db, app_config, midnight)
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

        # create totals for this day
        day_imported = 0.0
        day_exported = 0.0
        dyn_cost = dyn_rev = 0.0

        debug_logger.debug(f"---Looping intervals---")
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
            debug_logger.debug(f"  {key}: imp: {imp}, exp: {exp} * "
                         f"buy_dyn: {buy_dyn}, sell_dyn: {sell_dyn} = "
                         f"dyn_cost: {dyn_cost}, dyn_rev: {dyn_rev}")

        # 4. Fixed-plan: use first interval’s fixed price (constant through day)
        fixed_buy = intervals[0].net_prices_eur_per_kwh["fixed"]["buy"]
        fixed_sell = intervals[0].net_prices_eur_per_kwh["fixed"]["sell"]
        fix_cost = day_imported * fixed_buy
        fix_rev = day_exported * fixed_sell
        debug_logger.debug(f"Fixed cost: {fix_cost}, fix rev: {fix_rev}")

        # 5. Time-based costs (prorated)
        grid_cfg = tariffs["grid_operator"]
        supplier_cfg = tariffs["energy_supplier"]
        gov_cfg = tariffs["government"]
        vat = gov_cfg["vat"]
        # Daily data-management (per year):
        days_in_year = 365 + calendar.isleap(day.year)
        data_mgmt_yr = grid_cfg.get("data_management", 0.0) * vat
        daily_data_mgmt = data_mgmt_yr / days_in_year
        debug_logger.debug(f"Daily data mgmt: {daily_data_mgmt}")
        # Daily subscription (dynamic plan only, per month):
        days_in_month = calendar.monthrange(day.year, day.month)[1]
        sub_monthly = supplier_cfg["dynamic"].get("subscription_cost", 0.0) * vat
        daily_sub = sub_monthly / days_in_month
        debug_logger.debug(f"Daily sub cost: {daily_sub}")

        # 6. Tariff configurations for government taxes
        # Keep track of the cumulative imported energy for the year so far
        if day.year != previous_year:
            yearly_imported_kwh = 0.0
            previous_year = day.year
        previous_yearly_imported_kwh = yearly_imported_kwh
        yearly_imported_kwh += day_imported
        excise_threshold_kwh = gov_cfg["excise_duty_tiers"]
        rate_below = gov_cfg["rate_per_kwh_below"]
        rate_above = gov_cfg["rate_per_kwh_above"] * vat
        additional_tax_rate = max(rate_above - rate_below, 0)  # Rate below already in day intervals

        # Calculate tax for imports exceeding threshold
        excise_tax = 0
        if yearly_imported_kwh > excise_threshold_kwh:
            excess_kwh = yearly_imported_kwh - excise_threshold_kwh
            taxable_kwh_today = min(day_imported, excess_kwh -
                                    max(0, previous_yearly_imported_kwh - excise_threshold_kwh))
            excise_tax = taxable_kwh_today * additional_tax_rate
        results["yearly_imported_kwh"] = yearly_imported_kwh
        debug_logger.debug(f"Excise tax: {excise_tax}")

        # 7. Capacity tariff based on 12m peaks
        avg_peak_w = db.get_avg_monthly_peak_w_last_12m(day, grid_cfg['capacity_tariff_minimum_kw'] * 1000)
        cap_tariff_yr_per_kw = grid_cfg.get("capacity_tariff_per_kw_per_year", 0.0) * vat
        debug_logger.debug(f"Capacity tariff: {(avg_peak_w / 1000 * cap_tariff_yr_per_kw) / days_in_year}")

        # 8. Accumulate into results
        results["total_kwh_imported"] += day_imported
        results["total_kwh_exported"] += day_exported
        results["fixed"]["energy_cost_import"] += fix_cost + excise_tax
        results["fixed"]["energy_revenue_export"] += fix_rev
        results["fixed"]["time_based_costs"] += daily_data_mgmt
        results["fixed"]["capacity_costs_eur"] += (avg_peak_w / 1000 * cap_tariff_yr_per_kw) / days_in_year
        results["dynamic"]["energy_cost_import"] += dyn_cost + excise_tax
        results["dynamic"]["energy_revenue_export"] += dyn_rev
        results["dynamic"]["time_based_costs"] += daily_data_mgmt + daily_sub
        results["dynamic"]["capacity_costs_eur"] += (avg_peak_w / 1000 * cap_tariff_yr_per_kw) / days_in_year

        # 9. Store per-day breakdown
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

    # Calculate totals
    results["fixed"]["total_cost_excl_rev"] = (
            results["fixed"]["energy_cost_import"]
            + results["fixed"]["time_based_costs"]
            + results["fixed"]["capacity_costs_eur"]
    )
    results["fixed"]["total_bill"] = (
            results["fixed"]["total_cost_excl_rev"]
            - results["fixed"]["energy_revenue_export"]
    )

    results["dynamic"]["total_cost_excl_rev"] = (
            results["dynamic"]["energy_cost_import"]
            + results["dynamic"]["time_based_costs"]
            + results["dynamic"]["capacity_costs_eur"]
    )
    results["dynamic"]["total_bill"] = (
            results["dynamic"]["total_cost_excl_rev"]
            - results["dynamic"]["energy_revenue_export"]
    )

    return results


# For testing only
# if __name__ == "__main__":
#     from hec.core.tariff_manager import initialize_tariff_manager
#     from hec.core.app_initializer import load_app_config, initialize_database_handler
#
#     logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     debug_logger = logging.getLogger('debug')
#
#     prepare_time = datetime.now()
#     app_config = load_app_config()
#     db = initialize_database_handler(app_config)
#     tm = initialize_tariff_manager(app_config)
#
#     print(calculate_total_costs_for_period(date(2025, 5, 15), date(2025, 5, 15), app_config, db, tm))
#     exit(0)
#     start_time = datetime.now()
#     net_prices = []
#     t_date = datetime.now()
#     for i in range(365):
#         net_prices += calculate_net_intervals_for_day(db, tm, t_date - timedelta(days=365 - i))
#     end_time = datetime.now()
#     for nepi in net_prices:
#         print(nepi)
#     print(f"Calc time: {end_time - start_time}")
