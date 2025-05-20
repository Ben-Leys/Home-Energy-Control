# hec/reporting/daily_summary.py
import logging
from datetime import datetime, timedelta, date, timezone
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

from hec.core.app_state import GLOBAL_APP_STATE
from hec.core.models import NetElectricityPriceInterval
from hec.core.tariff_manager import TariffManager
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.cost_calculator import calculate_total_costs_for_period
from hec.logic_engine.price_predictor import EnergyPricePredictor
from hec.reporting.plot_generator import generate_price_solar_plot, generate_future_price_plot
from hec.utils.utils import send_email_with_attachments

logger = logging.getLogger(__name__)


class DailySummaryGenerator:
    def __init__(self, app_config: dict, db_handler: DatabaseHandler, tariff_manager: TariffManager):
        self.app_config = app_config
        self.db_handler = db_handler
        self.tariff_manager = tariff_manager
        self.price_predictor = EnergyPricePredictor(db_handler)  # Init predictor

    def _get_elia_forecasts_for_days(self, start_date_local: date, num_days: int) -> Dict[str, List[Dict[str, Any]]]:
        """Helper to fetch Elia forecasts for multiple days, keyed by forecast_type."""
        all_days_forecasts = {"solar": [], "wind": [], "grid_load": []}
        for i in range(num_days):
            current_day = start_date_local + timedelta(days=i)
            for fc_type in all_days_forecasts.keys():
                # get_elia_forecasts expects start and end, for one day, end is start + 1 day
                day_data = self.db_handler.get_elia_forecasts(
                    forecast_type=fc_type,
                    start_date_local=datetime.combine(current_day, datetime.min.time()).replace(
                        tzinfo=datetime.now().astimezone().tzinfo),
                    end_date_local=(datetime.combine(current_day, datetime.min.time()) + timedelta(days=1)).replace(
                        tzinfo=datetime.now().astimezone().tzinfo)
                )
                # Convert datetime objects in retrieved data back to ISO strings if needed for predictor input,
                # or ensure predictor handles datetime objects. For simplicity, assume predictor handles datetime.
                # The db_handler.get_elia_forecasts already converts to datetime.
                all_days_forecasts[fc_type].extend(day_data)
        return all_days_forecasts

    @staticmethod
    def _format_hours(self, hours_list: List[int]) -> str:
        if not hours_list:
            return "none"
        hours_list = sorted(set(hours_list))
        ranges = []
        start = hours_list[0]

        for i in range(1, len(hours_list)):
            if hours_list[i] != hours_list[i - 1] + 1:
                ranges.append((start, hours_list[i - 1] + 1))
                start = hours_list[i]
        ranges.append((start, hours_list[-1] + 1))

        return ', '.join(f"{start} - {end}" for start, end in ranges) + " h"

    @staticmethod
    def _format_hours_summary(self, t_day_nepi: List[NetElectricityPriceInterval], adjusted_solar,
                              res_min, solar_income) -> Tuple[str, str]:
        """Helper to format cheapest, most expensive and negative price hours."""

        # --- 1) Build a list of (hour, buy_price) & (hour, sell_price) ---
        hourly_buy = []
        hourly_sell = []
        for pi in t_day_nepi:
            h = pi.interval_start_local.hour
            contract = pi.active_contract_type
            buy = pi.net_prices_eur_per_kwh.get(contract, {}).get("buy")
            sell = pi.net_prices_eur_per_kwh.get(contract, {}).get("sell")
            if buy is not None:
                hourly_buy.append((h, buy))
            if sell is not None:
                hourly_sell.append((h, sell))

        # --- 2) Negative‐price summary & income saved by shutting off panels ---
        negative_hours = sorted({h for h, sp in hourly_sell if sp < 0})
        negative_income = sum(-sp * adjusted_solar[i] for i, (h, sp) in enumerate(hourly_sell) if sp < 0)

        # --- 3) Cheapest vs most expensive buy‐price hours ---
        hourly_buy_sorted = sorted(hourly_buy, key=lambda x: x[1])
        # cheapest 6‐interval average
        cheapest_avg = sum(p for _, p in hourly_buy_sorted[:6]) / 6
        # most expensive 6‐interval average
        expensive_avg = sum(p for _, p in hourly_buy_sorted[-6:]) / 6

        cheapest_hours = sorted({
            h for h, p in hourly_buy
            if p < 0 or p <= cheapest_avg + 0.01
        })
        expensive_hours = sorted({
            h for h, p in hourly_buy
            if (
                       (p >= cheapest_avg * 1.5) or
                       (p >= expensive_avg - 0.015)
               ) and h not in cheapest_hours
        })

        # 4) Format HTML snippets
        cheapest_str = self._format_hours(self, cheapest_hours)
        expensive_str = self._format_hours(self, expensive_hours)
        negative_str = self._format_hours(self, negative_hours)

        summary_html = (
            f"<td style='padding:4px;'>⬆️ Expensive: {expensive_str}<br>"
            f"⬇️ Cheap: {cheapest_str}</td>"
        )
        negative_html = ""
        if negative_hours:
            negative_html = (
                f"<td style='padding:4px;'>➖ Negative hrs:{negative_str}<br>"
                f"<span color:red;'>Shut off panels saves € {negative_income:,.2f}</span></td>"
            )
        else:
            negative_html = f"<td></td>"

        return summary_html, negative_html

    def _generate_html_content(self, data: Dict[str, Any]) -> str:
        """Generates the HTML body for the email."""

        # --- Target Day Summary (D+1) ---
        t_day_nepi: List[NetElectricityPriceInterval] = data.get("target_day_prices", [])
        t_day_solar = data.get("t_date_solar", [])

        # --- 1) Align solar to price resolution & compute solar income ---
        solar_income = 0
        res_min = t_day_nepi[0].resolution_minutes
        factor = res_min // 15  # Factor to aggregate solar data (1 for 15 min, 4 for 60 min)
        adjusted_solar = [sum(t_day_solar[i:i + factor]) / factor for i in range(0, len(t_day_solar), factor)]
        t_day_total_solar = sum(s for s in adjusted_solar if s is not None)

        for idx, pi in enumerate(t_day_nepi):
            if idx >= len(adjusted_solar):
                break
            kwh = adjusted_solar[idx]
            if kwh is None or kwh <= 0:
                continue
            contract = pi.active_contract_type
            net_sell = pi.net_prices_eur_per_kwh.get(contract, {}).get("sell")
            hours = pi.resolution_minutes / 60
            if net_sell is not None:
                solar_income += kwh * hours * net_sell

        # Build table header
        html = []
        html.append("<table style='width:100%;max-width:800px;border-collapse:collapse;'>")
        html.append(
            "<tr>"
            "<th style='width: 50%; text-align: center; border-bottom: 1px solid black;'>☀️ Solar info</th>"
            "<th style='text-align: center; border-bottom: 1px solid black;'>€ Price info</th>"
            "</tr>"
        )

        # Totals row
        html.append(
            f"<tr>"
            f"<td style='padding:4px;'>Total production: {t_day_total_solar:.2f} kWh</td>"
            f"<td style='padding:4px;'>Maximum income: € {solar_income:.2f}</td>"
            f"</tr>"
        )

        # Production bar
        html_bar_templ = """
            <div style="position: relative; width: 90%; height: 20px; border: 1px solid #000; 
            background-color: transparent;"><div style="position: absolute; width: {pos}%; 
            height: 100%; background-color: {color};{right}"></div></div>
        """
        prod_percent = round((t_day_total_solar / 52) * 100)
        prod_bar = html_bar_templ.format(pos=prod_percent, color='#50C878', right='')
        html.append(f"<tr><td>{prod_bar}</td>")

        # Income bar
        income_percent = round(solar_income / t_day_total_solar / 0.15 * 100)
        if solar_income > 0:
            income_bar = html_bar_templ.format(pos=income_percent, color='#50C878', right='')
        else:
            income_bar = html_bar_templ.format(pos=-income_percent, color='#CC5500',
                                               right='margin-left: auto; margin-right: 0;')
        html.append(f"<td>{income_bar}</td></tr>")

        # Hour summaries and negative/income rows
        summary_html, negative_html = self._format_hours_summary(self, t_day_nepi, adjusted_solar,
                                                                 res_min, solar_income)
        html.append(f"<tr>{negative_html}{summary_html}</tr>")
        # Close table
        html.append("</table>")

        # Recent costs
        d_data = data.get('d_costs').get(data.get('d_costs').get('active_contract'))
        m_data = data.get('m_costs').get(data.get('m_costs').get('active_contract'))
        y_data = data.get('y_costs').get(data.get('y_costs').get('active_contract'))
        html.append(
            f"<br><table style='width: 100%; max-width: 800px; border-collapse: collapse;'><tr>"
            f"<th style='width: 33%; text-align: center; border-bottom: 1px solid black;'>"
            f"Consumption {data.get('yesterday_date_str')}</th>"
            f"<th style='width: 33%; text-align: center; border-bottom: 1px solid black;'>"
            f"Consumption {data.get('month_name')}</th>"
            f"<th style='width: 33%; text-align: center; border-bottom: 1px solid black;'>"
            f"Consumption {data.get('year')}</th></tr>"
            f"<tr><td>Imp: {data.get('d_costs').get('total_kwh_imported'):.2f} kWh "
            f"(€ {d_data.get('total_cost_excl_rev'):.2f})</td>"
            f"<td>Imp: {data.get('m_costs').get('total_kwh_imported'):.2f} kWh "
            f"(€ {m_data.get('total_cost_excl_rev'):.2f})</td>"
            f"<td>Imp: {data.get('y_costs').get('total_kwh_imported'):.2f} kWh "
            f"(€ {y_data.get('total_cost_excl_rev'):.2f})</td></tr>"
            f"<tr><td>Exp: {data.get('d_costs').get('total_kwh_exported'):.2f} kWh "
            f"(€ {d_data.get('energy_revenue_export'):.2f})</td>"
            f"<td>Exp: {data.get('m_costs').get('total_kwh_exported'):.2f} kWh "
            f"(€ {m_data.get('energy_revenue_export'):.2f})</td>"
            f"<td>Exp: {data.get('y_costs').get('total_kwh_exported'):.2f} kWh "
            f"(€ {y_data.get('energy_revenue_export'):.2f})</td></tr>"
            f"<tr><td><u>Comparison</u></td><td><u>Comparison</u></td><td><u>Comparison</u></td></tr>"
            f"<tr><td>Dynamic: € {data.get('d_costs').get('dynamic').get('total_bill'):.2f}</td>"
            f"<td>Dynamic: € {data.get('m_costs').get('dynamic').get('total_bill'):.2f}</td>"
            f"<td>Dynamic: € {data.get('y_costs').get('dynamic').get('total_bill'):.2f}</td></tr>"
            f"<tr><td>Fixed: € {data.get('d_costs').get('fixed').get('total_bill'):.2f}</td>"
            f"<td>Fixed: € {data.get('m_costs').get('fixed').get('total_bill'):.2f}</td>"
            f"<td>Fixed: € {data.get('y_costs').get('fixed').get('total_bill'):.2f}</td></tr>"
            "</table>"
        )

        return "".join(html)

    def generate_and_send_summary(self, app_config) -> bool:
        """Orchestrates data fetching, processing, plotting, and emailing."""
        logger.info("Starting generation of daily energy summary email.")
        # n_date = now, t_date = tomorrow, y_date = yesterday
        n_date = datetime.now().astimezone()
        t_date = (n_date + timedelta(days=1)).date()
        y_date = (n_date - timedelta(days=1)).date()

        # --- 1. Get (D+1) Processed Net Prices from AppState ---
        n_date_nepi = GLOBAL_APP_STATE.get("electricity_prices_today", [])
        t_date_nepi = GLOBAL_APP_STATE.get("electricity_prices_tomorrow", [])
        if not (len(n_date_nepi) == 24 or len(n_date_nepi) == 96) or not \
                (len(t_date_nepi) == 24 or len(t_date_nepi) == 96):
            logger.info(f"Electricity price data length not as expected: 24 or 96. "
                        f"Today: {len(n_date_nepi)}, tomorrow: {len(t_date_nepi)}. DST change?")

        # --- 2. Get Elia Solar Forecast for "Tomorrow" (D+1) ---
        local_tz = n_date.tzinfo
        t_date_start = datetime.combine(t_date, datetime.min.time(), tzinfo=local_tz).astimezone(timezone.utc)
        t_date_end = t_date_start + timedelta(days=1)

        elia_solar_fc_raw = [f for f in GLOBAL_APP_STATE.get("forecasts").get("solar")
                             if t_date_start <= f["timestamp_utc"] < t_date_end]

        forecast_resolution = 15
        t_date_solar: List[Optional[float]] = [None] * (24 * 60 // 15)  # Init empty list with expected interval
        inverter_kw = self.app_config.get('inverter', {}).get('standard_power_limit') / 1000
        if elia_solar_fc_raw:
            forecast_resolution = elia_solar_fc_raw[0].get("resolution_minutes", 15)
            num_intervals_per_day = (24 * 60) // forecast_resolution
            t_date_solar: List[Optional[float]] = [None] * num_intervals_per_day
            panel_kw = self.app_config.get('inverter', {}).get('panel_peak_w') / 1000

            for item in elia_solar_fc_raw:
                # Map to interval index
                item_ts_utc = item['timestamp_utc']
                item_ts_local = item_ts_utc.astimezone(local_tz)

                interval_index = (item_ts_local.hour * (60 // forecast_resolution)) + (
                        item_ts_local.minute // forecast_resolution)
                if 0 <= interval_index < num_intervals_per_day:
                    forecast_mwh = item.get('most_recent_forecast_mwh', 0.0)
                    capacity_mw = item.get('monitored_capacity_mw')
                    estimated_kw = (forecast_mwh / capacity_mw) * panel_kw if capacity_mw > 0 else 0
                    t_date_solar[interval_index] = min(estimated_kw, inverter_kw)  # Cap at inverter_kw

        # --- 3. Generate Price/Solar Plot for "Tomorrow" (D+1) ---
        price_resolution = n_date_nepi[0].resolution_minutes
        fix_buy = t_date_nepi[0].net_prices_eur_per_kwh.get("fixed").get("buy")
        fix_sell = t_date_nepi[0].net_prices_eur_per_kwh.get("fixed").get("sell")
        plot_buffer_d1 = generate_price_solar_plot(
            t_date_local=t_date,
            n_date_nepi=n_date_nepi[- (10 * (60 // price_resolution)):],  # Today's 10 hours for context
            t_date_nepi=t_date_nepi,
            t_date_solar=t_date_solar,
            fixed_buy_price=fix_buy,
            fixed_sell_price=fix_sell,
            forecast_resolution=forecast_resolution,
            inverter_kw=inverter_kw
        )

        # --- 4. Price Prediction for D+1 to D+5 ---
        predicted_prices_dfs = []
        if not self.price_predictor.is_trained:
            logger.info("Price predictor model not trained. Training now for email report...")
            train_end = y_date
            train_start = date.fromisoformat(self.app_config.get('historic_data').get('start_date'))

            prediction_start_dt = datetime.now()
            self.price_predictor.train_model(train_start, train_end)
            logger.info(f"Training finished. Trained {(train_end - train_start).days} days of data "
                        f"in {(datetime.now() - prediction_start_dt).total_seconds():.2f} seconds")

        if self.price_predictor.is_trained:
            elia_forecasts_d1_d5 = self._get_elia_forecasts_for_days(
                start_date_local=t_date,
                num_days=5
            )
            for i in range(5):  # D+1 to D+5
                predict_date = t_date + timedelta(days=i)
                # Prepare Elia forecasts for this specific predict_date
                daily_elia_fc_for_predictor = {}
                for fc_type in ["solar", "wind", "grid_load"]:
                    daily_elia_fc_for_predictor[fc_type] = [
                        rec for rec in elia_forecasts_d1_d5.get(fc_type)
                        if rec['timestamp_utc'].astimezone(local_tz).date() == predict_date
                    ]

                day_prediction_df = self.price_predictor.predict_prices_for_day(predict_date,
                                                                                daily_elia_fc_for_predictor)
                if day_prediction_df is not None:
                    predicted_prices_dfs.append(day_prediction_df)

        plot_buffer_predictions = None
        if predicted_prices_dfs:
            plot_buffer_predictions = generate_future_price_plot(self.db_handler, app_config, predicted_prices_dfs,
                                                                 t_date, inverter_kw)

        # --- 5. Calculate recent costs ---
        if n_date.day == 1:  # First day of the month
            end_date = n_date.date() - timedelta(days=1)
            start_date = end_date.replace(day=1)
        else:  # Within the current month
            start_date = n_date.replace(day=1).date()
            end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(days=1)
            end_date = min(end_date, y_date)
        first_day_of_year = start_date.replace(day=1, month=1)

        y_date_costs = calculate_total_costs_for_period(y_date, y_date, self.app_config,
                                                        self.db_handler, self.tariff_manager)
        n_month_costs = calculate_total_costs_for_period(start_date, end_date, self.app_config,
                                                         self.db_handler, self.tariff_manager)
        n_year_costs = calculate_total_costs_for_period(first_day_of_year, end_date, self.app_config,
                                                        self.db_handler, self.tariff_manager)

        # --- 6. Prepare Email Content ---
        email_data = {
            "target_day_date_str": t_date.strftime('%d-%m-%Y (%A)'),
            "target_day_prices": t_date_nepi,
            "t_date_solar": t_date_solar,
            "predicted_prices_df": pd.concat(predicted_prices_dfs) if predicted_prices_dfs else None,
            "yesterday_date_str": y_date.strftime('%d-%m'),
            "month_name": start_date.strftime("%B"),
            "year": start_date.strftime("%Y"),
            "d_costs": y_date_costs,
            "m_costs": n_month_costs,
            "y_costs": n_year_costs,
        }
        html_body = self._generate_html_content(email_data)

        # --- 7. Send Email ---
        images_to_attach = []
        if plot_buffer_d1:
            images_to_attach.append(
                (plot_buffer_d1.getvalue(), "price_solar_tomorrow.png", "price_solar_plot_tomorrow"))
        if plot_buffer_predictions:
            images_to_attach.append(
                (plot_buffer_predictions.getvalue(), "price_prediction_multiday.png", "price_prediction_plot"))

        smtp_cfg = self.app_config.get('smtp', {})

        success = send_email_with_attachments(
            smtp_config=smtp_cfg,
            sender_email=smtp_cfg.get('sender_email'),
            recipients=smtp_cfg.get('default_recipients'),
            subject=f"Energy summary and forecast for {t_date.strftime('%d-%m-%Y')}",
            html_body=html_body,
            images=images_to_attach
        )
        return success
