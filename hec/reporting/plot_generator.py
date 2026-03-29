# hec/reporting/plot_generator.py
import io
import logging
from datetime import datetime, date, timedelta, time
from typing import List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from hec.core.models import NetElectricityPriceInterval, PricePoint
from hec.database_ops.db_handler import DatabaseHandler
from hec.logic_engine.cost_calculator import calculate_net_intervals_for_day


logger = logging.getLogger(__name__)


def _prepare_price_data_for_plot(
        price_intervals: List[NetElectricityPriceInterval],
        num_expected_intervals: int  # Usually 96 for 15-min intervals
) -> Tuple[List[float], List[float], List[datetime]]:
    buy_raw, sell_raw, ts_raw = [], [], []

    for interval in price_intervals:
        prices = interval.net_prices_eur_per_kwh.get(interval.active_contract_type, {})
        buy_raw.append(prices.get("buy", np.nan))
        sell_raw.append(prices.get("sell", np.nan))
        ts_raw.append(interval.interval_start_local)

    n = len(buy_raw)

    # Spring DST (Short day 92 intervals)
    if n == 92:
        # Duplicate the 8th element (index 7) 4 times to fill the jump
        # Insert at index 8
        for i in range(1, 5):
            buy_raw.insert(8, 0)
            sell_raw.insert(8, 0)
            new_ts = ts_raw[7] + pd.Timedelta(minutes=15 * i)
            ts_raw.insert(8, new_ts)

    # DST (Long day 100 intervals)
    elif n == 100:
        new_buy = buy_raw[:8]
        new_sell = sell_raw[:8]
        new_ts = ts_raw[:8]

        # Average the "double hour"
        for i in range(4):
            avg_buy = (buy_raw[8 + i] + buy_raw[12 + i]) / 2
            avg_sell = (sell_raw[8 + i] + sell_raw[12 + i]) / 2
            new_buy.append(avg_buy)
            new_sell.append(avg_sell)
            new_ts.append(ts_raw[8 + i])

        # Add the remaining
        new_buy.extend(buy_raw[16:])
        new_sell.extend(sell_raw[16:])
        new_ts.extend(ts_raw[16:])

        buy_raw, sell_raw, ts_raw = new_buy, new_sell, new_ts

    return buy_raw[:num_expected_intervals], sell_raw[:num_expected_intervals], ts_raw[:num_expected_intervals]


def generate_price_solar_plot(
        t_date_local: date,
        n_date_nepi: List[NetElectricityPriceInterval],
        t_date_nepi: List[NetElectricityPriceInterval],
        t_date_solar: List[Optional[float]],
        fixed_buy_price: Optional[float] = None,
        fixed_sell_price: Optional[float] = None,
        forecast_resolution: int = 15,
        inverter_kw: float = 10.0
) -> Optional[io.BytesIO]:
    """
    Generates a plot of net electricity prices and solar production forecast.
    Args:
        t_date_local: The target day for which to plot (probably "tomorrow").
        n_date_nepi: Processed net prices for today to show context.
        t_date_nepi: Processed net prices for the target day.
        t_date_solar: Solar production forecast in kW for each interval of target day.
        fixed_buy_price: For fixed line plotting.
        fixed_sell_price: For fixed line plotting.
        forecast_resolution: The interval resolution of the forecast data (15 or 60).
        inverter_kw: The inverter kw max for y-axis limit.
    Returns:
        A BytesIO buffer containing the PNG image, or None on error.
    """
    logger.info(f"Generating price/solar plot for {t_date_local.strftime('%Y-%m-%d')}")

    forecast_intervals_per_hour = 60 // forecast_resolution
    intervals_n_date_per_hour = 60 // n_date_nepi[0].resolution_minutes
    intervals_t_date_per_hour = 60 // t_date_nepi[0].resolution_minutes

    # --- Prepare data for plotting ---
    # Until 11th of June 2025 price resolution is 60 minutes while forecast resolution is 15 minutes
    intervals_to_show_per_hour = max(intervals_n_date_per_hour, intervals_t_date_per_hour, forecast_intervals_per_hour)
    buy_today, sell_today, _ = _prepare_price_data_for_plot(n_date_nepi, 10 * intervals_to_show_per_hour)
    buy_target, sell_target, _ = _prepare_price_data_for_plot(t_date_nepi, 24 * intervals_to_show_per_hour)

    plot_buy_prices = buy_today + buy_target
    plot_sell_prices = sell_today + sell_target

    # Solar production: pad with NaNs for "today" part, then use target day's forecast
    plot_solar_kw = (([np.nan] * intervals_to_show_per_hour * 10) +
                     [s if s is not None else np.nan for s in t_date_solar[:intervals_to_show_per_hour * 24]])
    while len(plot_solar_kw) < len(plot_buy_prices):  # Ensure same length
        plot_solar_kw.append(np.nan)

    if not any(plot_buy_prices) and not any(plot_sell_prices):  # Check if all are NaN or empty
        logger.warning("No price data available to plot.")
        return None

    # --- Plotting ---
    try:
        with plt.style.context('default'):
            fig, ax1 = plt.subplots(figsize=(16, 10))  # Wider for more intervals

            intervals_n_date_to_plot = intervals_to_show_per_hour * 10
            total_intervals_to_plot = intervals_n_date_to_plot + intervals_to_show_per_hour * 24
            x_axis_indices = np.arange(total_intervals_to_plot)

            # Convert numpy arrays for masking, handle NaNs gracefully for min/max
            np_buy = np.array(plot_buy_prices, dtype=float)
            np_sell = np.array(plot_sell_prices, dtype=float)
            np_solar = np.array(plot_solar_kw, dtype=float)

            # Inspect min and max price (of both contracts) to define y-axis
            sell_with_solar = np_sell[~np.isnan(np_solar)]
            if sell_with_solar.size > 0:
                min_price = np.nanmin([np.nanmin(np_buy), np.nanmin(sell_with_solar)])
            else:
                min_price = np.nanmin(np_buy)
            max_price = np.nanmax([np.nanmax(np_buy), np.nanmax(np_sell)])

            y_min_ax1 = min(min_price * 1.1, -0.025) if min_price < 0 else 0
            y_max_ax1 = max(max_price * 1.1, fixed_buy_price + 0.02)  # Ensure fixed buy price is visible
            ax1.set_ylim(y_min_ax1, y_max_ax1)

            bar_width = 1

            mask_today = np.zeros(len(np_buy), dtype=bool)
            mask_today[:intervals_n_date_to_plot] = True
            mask_buy_pos = (np_buy >= 0) & ~mask_today
            mask_buy_neg = (np_buy < 0) & ~mask_today

            # For sale price, only color if solar is producing (solar > 0, handle NaN)
            mask_solar_producing = ~np.isnan(np_solar) & (np_solar > 0)  # Was 0.05
            mask_sell_pos = np_sell >= 0
            mask_sell_neg = np_sell < 0

            # Plot Buy Prices
            ax1.bar(x_axis_indices[mask_today], np_buy[mask_today], width=bar_width,
                    color='lightgray', alpha=1, label='Buy price today')
            ax1.bar(x_axis_indices[mask_buy_pos], np_buy[mask_buy_pos], width=bar_width,
                    color='#2525E6', edgecolor=None, alpha=1, label='Buy price tomorrow', zorder=1)
            ax1.bar(x_axis_indices[mask_buy_neg], np_buy[mask_buy_neg], width=bar_width,
                    color='#6A0DAD', edgecolor=None, alpha=1, label='Buy price (neg)',
                    zorder=3)  # Purple for neg buy

            # Plot Sell Prices (only where solar is producing)
            ax1.bar(x_axis_indices[mask_solar_producing & mask_sell_pos], np_sell[mask_solar_producing & mask_sell_pos],
                    width=bar_width, color='#25E625', edgecolor=None, alpha=1,
                    label='Sell price (solar)', zorder=2)
            ax1.bar(x_axis_indices[mask_solar_producing & mask_sell_neg], np_sell[mask_solar_producing & mask_sell_neg],
                    width=bar_width, color='#E62525', edgecolor=None, alpha=1,
                    label='Sell price (solar, neg)', zorder=2)

            ax1.set_xlabel('Time interval')
            ax1.set_ylabel('Net price (€/kWh)')

            # Solar Production on ax2
            ax2 = ax1.twinx()
            ax2.plot(x_axis_indices[mask_solar_producing], np_solar[mask_solar_producing],
                     color='#E6B625', label='Solar Production (kW)', linewidth=1.0)
            ax2.fill_between(x_axis_indices[mask_solar_producing], np_solar[mask_solar_producing],
                             color='#E6B625', alpha=0.6, zorder=4)
            ax2.set_ylabel('Solar production (kW)')
            # Align solar y-axis with price axis for visual comparison
            ax2.set_ylim(y_min_ax1 / y_max_ax1 * inverter_kw * 1.1, inverter_kw * 1.1)

            # X-axis Ticks and Labels
            tick_positions = []
            tick_labels = []
            for i in range(total_intervals_to_plot):
                if i == 0 or (i % (intervals_to_show_per_hour * 2)) == 0:  # Every 2 hours, or first tick
                    tick_positions.append(i - 0.5)
                    hour_offset = (i - total_intervals_to_plot) // forecast_intervals_per_hour
                    actual_hour = hour_offset % 24
                    tick_labels.append(f"{actual_hour:02d}h")

            if (total_intervals_to_plot * 10 not in tick_positions and
                    0 < intervals_n_date_to_plot < total_intervals_to_plot):
                tick_positions.append(intervals_n_date_to_plot - 0.5)
                tick_labels.append(f"00h\n{t_date_local.strftime('%d/%m')}")

            # Ticks and grid
            ax1.tick_params(direction='out')
            ax1.set_xticks(tick_positions)
            ax1.set_xticklabels(tick_labels, ha="center", color="black")
            ax1.set_xlim(-0.5, total_intervals_to_plot - 0.1)
            ax1.grid(color='darkgray', which="major", linestyle='solid', alpha=0.6)

            # Title and Legend
            days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            day_name = days[t_date_local.weekday()]
            plt.title(f"Energy prices & solar forecast: {day_name} {t_date_local.strftime('%d-%m-%Y')}")

            handles1, labels1 = ax1.get_legend_handles_labels()
            handles2, labels2 = ax2.get_legend_handles_labels()
            plt.legend(handles=handles1 + handles2, labels=labels1 + labels2, loc='upper left', fontsize='medium')

            # Horizontal line with semi-fixed price contract prices
            ax1.axhline(y=fixed_buy_price, color='red', linestyle='--', label='Fixed buy price', linewidth=2)
            ax1.axhline(y=fixed_sell_price, color='green', linestyle='--', label='Fixed sell price', linewidth=2)

            fig.tight_layout()
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=300)  # Control DPI for image size/quality
            buffer.seek(0)
            plt.close(fig)
            return buffer
    except Exception as e:
        logger.error(f"Error generating price/solar plot: {e}", exc_info=True)
        return None


def generate_future_price_plot(
        db: DatabaseHandler,
        app_config,
        future_dfs: List[pd.DataFrame],
        future_date: date,
        inverter_kw: float = 10.0
) -> Optional[io.BytesIO]:
    """
    Plots net electricity price buy/sell predictions for multiple days, alongside
    solar_factor, wind_factor and grid_load from each day's DataFrame.

    Args:
        db: your DatabaseHandler instance
        app_config: application configuration (for calculate_net_intervals_for_day)
        future_dfs: list of DataFrames, one per day, each with columns
            ['timestamp_utc', 'predicted_gross_price_kwh', 'solar_factor',
             'wind_factor', 'grid_load_mwh']
        future_date: local date corresponding to the first predicted day
        inverter_kw: for solar axis scaling (passed through for plot limits)

    Returns:
        BytesIO of a PNG plot, or None on error.
    """
    future_dates = [datetime.combine(future_date, time(0, 0, 0)) + timedelta(days=i) for i in range(5)]
    logger.info(f"Generating future price plot for "
                f"{future_dates[0]}–{future_dates[-1]}")

    all_nepi = []
    intervals_per_day = []
    # --- build price intervals for each day ---
    for df, target_date in zip(future_dfs, future_dates):
        # build PricePoint list
        pps = [
            PricePoint(timestamp_utc=row.timestamp_utc, price_eur_per_mwh=row.predicted_gross_price_kwh * 1000,
                       position=index + 1, resolution_minutes=15)
            for index, row in enumerate(df.itertuples(index=False))
        ]

        nepi = calculate_net_intervals_for_day(db, app_config, target_date, pps)
        all_nepi.extend(nepi)
        intervals_per_day.append(len(pps))

    total_intervals = sum(intervals_per_day)
    if total_intervals == 0:
        logger.warning("No future price data to plot.")
        return None

    # --- prepare flat buy/sell price lists ---
    buy_prices, sell_prices, _ = _prepare_price_data_for_plot(all_nepi, total_intervals)

    # --- prepare solar, wind, grid series ---
    flat_solar = []
    flat_wind = []
    flat_grid = []
    for df in future_dfs:
        # v1.0: solar_factor * 7/100, wind_factor *7/100; grid_load normalized
        flat_solar.extend((df['solar_factor'] * inverter_kw * 1.1).tolist())
        flat_wind.extend((df['wind_factor'] * inverter_kw * 1.1).tolist())
        # normalize grid_load to max of that day's load
        max_load = df['grid_load_mwh'].max() or 1.0
        flat_grid.extend((df['grid_load_mwh'] / max_load * 100.0 / 14.0).tolist())

    # ensure same length
    def pad_to(lst, length):
        return lst + [np.nan] * (length - len(lst))
    np_buy = np.array(buy_prices, dtype=float)
    np_sell = np.array(sell_prices, dtype=float)
    np_solar = np.array(pad_to(flat_solar, total_intervals), dtype=float)
    np_wind = np.array(pad_to(flat_wind,  total_intervals), dtype=float)
    np_grid = np.array(pad_to(flat_grid,  total_intervals), dtype=float)

    # Mask zero or NaN values for solar, wind, and grid
    np_solar = np.ma.masked_where((np_solar == 0) | np.isnan(np_solar), np_solar)
    np_wind = np.ma.masked_where((np_wind == 0) | np.isnan(np_wind), np_wind)
    np_grid = np.ma.masked_where((np_grid == 0) | np.isnan(np_grid), np_grid)

    # determine y-limits
    min_price = min(np.nanmin(np_buy), np.nanmin(np_sell))
    max_price = max(np.nanmax(np_buy), np.nanmax(np_sell))
    y_min = min(min_price * 1.1, -0.025) if min_price < 0 else 0
    y_max = max_price * 1.1

    try:
        with plt.style.context('default'):
            fig, ax1 = plt.subplots(figsize=(16, 10))
            x = np.arange(total_intervals)

            # bars for buy/sell
            ax1.bar(x, np_buy, width=1, color='#2525E6', edgecolor=None, alpha=1, label='Buy price', zorder=1)
            ax1.bar(x, np_sell, width=1, color='#25E625', edgecolor=None, alpha=1, label='Sell price', zorder=2)

            ax1.set_ylim(y_min, y_max)
            ax1.set_xlabel('Interval')
            ax1.set_ylabel('Net price (€/kWh)')

            # twin axis for factors
            ax2 = ax1.twinx()
            ax2.plot(x, np_solar, linestyle='-', color='#E6B625', linewidth=1.5, label='Solar factor')
            ax2.fill_between(x, np_solar, color='#E6B625', alpha=0.5)
            ax2.plot(x, np_wind, linestyle='-', color='green', linewidth=1.5, label='Wind factor')
            ax2.fill_between(x, np_wind, color='green', alpha=0.4)
            ax2.plot(x, np_grid, linestyle='-', color='red', linewidth=1.5, label='Grid load')
            ax2.set_ylabel('Factors / load (normalized)')
            ax2.set_ylim(y_min / y_max * inverter_kw * 1.1, inverter_kw * 1.1)

            # vertical separators between days
            cum = 0
            for length in intervals_per_day[:-1]:
                cum += length
                ax1.axvline(x=cum, color='black', linestyle='--', linewidth=1, zorder=5)

            # legend & layout
            h1, l1 = ax1.get_legend_handles_labels()
            h2, l2 = ax2.get_legend_handles_labels()
            ax1.legend(h1 + h2, l1 + l2, loc='upper left', fontsize='medium')

            # Set x-axis ticks
            custom_ticks = range(0, len(np_buy), 24)
            custom_labels = [str((i // 4) % 24) for i in custom_ticks]
            ax1.set_xticks(custom_ticks)
            ax1.set_xticklabels(custom_labels, ha="center")

            # title date range
            day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            start, end = future_dates[0], future_dates[-1]
            title = (f"Future prices and factors: "
                     f"{day_names[start.weekday()]} {start.strftime('%d-%m-%Y')} – "
                     f"{day_names[end.weekday()]} {end.strftime('%d-%m-%Y')}")
            plt.title(title)

            # Add day names inside the graph, centered above each day's midpoint
            day_midpoints = [(sum(intervals_per_day[:i]) + intervals_per_day[i] // 2) for i in
                             range(len(intervals_per_day))]
            for i, midpoint in enumerate(day_midpoints):
                day_label = f"{day_names[future_dates[i].weekday()]}"  # {future_dates[i].strftime('%d-%m')}"
                ax1.text(
                    x=midpoint,
                    y=(y_min + y_max) / 2 * 0.9,  # Position text slightly above the center line
                    s=day_label,  # Display day name and short date
                    color="black",
                    fontsize=18,
                    ha="center",
                    va="center",
                    zorder=6,
                    bbox=dict(facecolor='white', alpha=0.6, boxstyle='round,pad=0.2')  # Optional: adds a background
                )

            fig.tight_layout()
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=300)
            buf.seek(0)
            plt.close(fig)
            return buf

    except Exception as e:
        logger.error(f"Error generating future price plot: {e}", exc_info=True)
        return None
