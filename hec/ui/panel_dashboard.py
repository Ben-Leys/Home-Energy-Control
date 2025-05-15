# hec/ui/panel_dashboard.py
import param
import panel as pn
import requests
from datetime import datetime
import asyncio  # For async updates in Panel

# Load Panel extensions (needed for some components and to run in notebook/server)
pn.extension(sizing_mode="stretch_width")

# Configuration for the API (same as for Streamlit example)
MAIN_APP_API_URL = "http://localhost:5000/api/v1/state"
FETCH_INTERVAL_SECONDS = 5  # How often to refresh data


# --- Helper to get data from Main App API ---
# This will be called by Panel's periodic callback
def fetch_main_app_state_sync():  # Synchronous version for initial load / simple updates
    try:
        response = requests.get(MAIN_APP_API_URL, timeout=3)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from main app: {e}")  # Panel might not show st.error
        return None
    except Exception as e:
        print(f"An unexpected error occurred fetching data: {e}")
        return None


# --- Panel Components ---

# 1. Gauges/Meters
# We need to store the current values to update the gauges
# Panel uses "param.Number" for values that can be dynamically updated.
class PowerIndicators(pn.viewable.Viewer):
    # Define parameters that will hold the values for the indicators
    # Ensure default values are numeric
    current_solar_production = param.Number(default=0, bounds=(0, 7000), label="Solar Production (W)")
    net_grid_flow = param.Number(default=0, bounds=(-10000, 10000),
                                    label="Net Grid (W)")  # Negative for import, positive for export
    app_status_text = param.String(default="UNKNOWN", label="App Status")

    def __init__(self, **params):
        super().__init__(**params)

        # Create the indicator widgets
        # Dial/Gauge typically needs a fixed name, value, bounds. Format can be added.
        self.solar_gauge = pn.indicators.Gauge(
            name='Solar Prod.', value=self.param.current_solar_production, bounds=(0, 7000),
            format='{value} W', title_size=10,
            sizing_mode='stretch_width', height=200
        )
        self.grid_dial = pn.indicators.Dial(
            name='Net Grid', value=self.param.net_grid_flow, bounds=(-7000, 7000),  # Adjust bounds as needed
            format='{value} W (Export > 0)',
            colors=[(0.33, 'red'), (0.66, 'gold'), (1, 'green')],  # Example colors
            sizing_mode='stretch_width', height=200
        )
        # For app status, a simple Markdown/StaticText that we update
        self.status_indicator = pn.pane.Markdown(f"**Status:** {self.app_status_text}", sizing_mode='stretch_width')

        # Layout for these indicators
        self._view = pn.Column(
            self.status_indicator,
            pn.Row(self.solar_gauge, self.grid_dial),
            sizing_mode='stretch_width'
        )

    def view(self):  # Required by pn.viewable.Viewer if not using _panel_layout
        return self._view

    # Method to update the parameters (and thus the indicators)
    def update_indicators(self, solar_w, grid_w_export, status_str):
        # The pn.param.Number parameters will automatically update linked widgets
        self.current_solar_production = float(solar_w) if solar_w is not None else 0.0
        self.net_grid_flow = float(grid_w_export) if grid_w_export is not None else 0.0
        self.app_status_text = str(status_str) if status_str is not None else "UNKNOWN"
        # Update the markdown pane directly
        self.status_indicator.object = f"**Status:** <font color='{get_status_color_panel(status_str)}'>{status_str.replace('_', ' ').title()}</font>"


# 2. Graph for Energy Prices (Using Bokeh directly via Panel)
# We'll create a Bokeh figure and update its data source
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, DatetimeTickFormatter

price_plot_source = ColumnDataSource(data=dict(x=[], y_price_kwh=[]))  # Empty initially

price_figure = figure(
    x_axis_type="datetime",
    height=300,
    sizing_mode="stretch_width",
    title="Today's Electricity Prices (EUR/kWh)",
    x_axis_label="Time (Local)",
    y_axis_label="Price (EUR/kWh)"
)
price_figure.line(x='x', y='y_price_kwh', source=price_plot_source, line_width=2, color="dodgerblue")
price_figure.xaxis.formatter = DatetimeTickFormatter(hours="%H:%M")  # Format x-axis ticks

# Panel pane to display the Bokeh figure
bokeh_price_pane = pn.pane.Bokeh(price_figure, sizing_mode="stretch_width")

# --- Data Update Logic ---
# Create an instance of our indicators
power_indicators_instance = PowerIndicators()


async def update_dashboard_data():
    """Periodically fetches data and updates Panel components."""
    print(f"Panel: Attempting to fetch data at {datetime.now()}")  # Debug print
    try:
        # Run the synchronous fetching function in a separate thread
        # to avoid blocking the asyncio event loop used by Panel/Bokeh server.
        new_state = await asyncio.to_thread(fetch_main_app_state_sync)
    except Exception as e:
        print(f"Panel: Error in asyncio.to_thread or fetch_main_app_state_sync: {e}")
        new_state = None  # Ensure new_state is defined

    if new_state:
        # Update Gauges/Meters
        p1_live = new_state.get("p1_meter_data")
        solar_w = 0  # Default, get from inverter_data later
        grid_w = 0  # Default

        solar_w = 717

        if p1_live and isinstance(p1_live, dict):
            active_power_w_val = p1_live.get("active_power_w")
            active_power_w = float(active_power_w_val) if active_power_w_val is not None else 0.0
            grid_w = active_power_w

        app_status = new_state.get("app_state", "UNKNOWN")
        power_indicators_instance.update_indicators(solar_w, -grid_w, app_status)

        # Update Price Plot
        prices_today_list = new_state.get("electricity_prices_today")
        new_plot_data = dict(x=[], y_price_kwh=[])
        if prices_today_list and isinstance(prices_today_list, list):
            # ... (logic to populate new_plot_data from prices_today_list as before) ...
            x_times = []
            y_prices = []
            for interval_data in prices_today_list:
                try:
                    start_local_dt = datetime.fromisoformat(interval_data["interval_start_local"])
                    x_times.append(start_local_dt)
                    price_val = interval_data.get("price_eur_per_kwh")
                    y_prices.append(float(price_val) if price_val is not None else None)
                except Exception as e:
                    print(f"Panel: Error parsing price interval for plot: {interval_data}, {e}")

            valid_plot_data = [(t, p) for t, p in zip(x_times, y_prices) if p is not None]
            if valid_plot_data:
                plot_x, plot_y = zip(*valid_plot_data)
                new_plot_data = dict(x=list(plot_x), y_price_kwh=list(plot_y))

        doc = pn.state.curdoc
        if doc:
            def update_cds_data():
                price_plot_source.data = new_plot_data

            doc.add_next_tick_callback(update_cds_data)
        else:
            price_plot_source.data = new_plot_data
        print(f"Panel: Dashboard components updated at {datetime.now()}")  # Debug print
    else:
        print(f"Panel: Failed to fetch new state or new_state is None at {datetime.now()}")  # Debug print

        # Display raw state for debugging (optional)
        # raw_state_pane.object = f"```json\n{json.dumps(new_state, indent=2)}\n```"


# Helper for status color (similar to Streamlit one)
def get_status_color_panel(app_status_str: str) -> str:
    # Assuming K.AppStatus from constants.py (or a fallback if import fails)
    try:
        from hec.core import constants as K
        status_map = {
            K.AppStatus.NORMAL.name: "green", K.AppStatus.STARTING.name: "blue",
            K.AppStatus.DEGRADED.name: "grey", K.AppStatus.WARNING.name: "orange",
            K.AppStatus.ALARM.name: "red", K.AppStatus.SHUTDOWN.name: "black",
        }
        return status_map.get(app_status_str, "black")
    except ImportError:  # Fallback if K is not available
        if app_status_str == "NORMAL":
            return "green"
        if app_status_str == "ALARM" or app_status_str == "ALARM":
            return "red"
        return "black"


# Debug pane for raw state
# import json
# raw_state_pane = pn.pane.Markdown("Waiting for data...", width=600, height=300, style={'overflow-y': 'auto'})


# --- Define the Dashboard Layout ---
dashboard_layout = pn.Column(
    pn.Row(
        pn.pane.Markdown("# 🏠 Home Energy Dashboard (Panel)", sizing_mode="stretch_width"),
        sizing_mode="stretch_width"
    ),
    power_indicators_instance.view(),  # Use the view() method of our Viewer class
    pn.Spacer(height=20),
    bokeh_price_pane,
    # pn.Spacer(height=20),
    # pn.pane.Markdown("### Raw Application State (Debug)"),
    # raw_state_pane, # Uncomment to show raw state
    sizing_mode="stretch_width"
)

# --- Setup Periodic Callback for Live Updates ---
# This is Panel's way of doing something like st.fragment(run_every=...)
# It will call update_dashboard_data every FETCH_INTERVAL_SECONDS * 1000 milliseconds
# `pn.state.add_periodic_callback` is for apps served with `panel serve`
# Ensure this is called only once when the script is run by the server
if pn.state.curdoc:  # Check if running under a Bokeh server (panel serve)
    pn.state.add_periodic_callback(update_dashboard_data, period=FETCH_INTERVAL_SECONDS * 1000)
    # Initial data load
    asyncio.create_task(update_dashboard_data())

# To make the dashboard servable:
dashboard_layout.servable(title="Home Energy Panel")  # For `panel serve`

# If running directly with `python panel_dashboard.py` for quick test (less ideal for periodic updates):
if __name__ == "__main__":
    print("This Panel app is best served with 'panel serve hec/ui/panel_dashboard.py'")
    print("Showing a static version. For live updates, use 'panel serve'.")
    # Load initial data for static view
    initial_state_for_static_view = fetch_main_app_state_sync()
    if initial_state_for_static_view:
        # Manually trigger an update for the static view
        p1_live = initial_state_for_static_view.get("p1_meter_live_data")
        solar_w = 0
        grid_w = 0
        if p1_live and isinstance(p1_live, dict):
            active_power_w = p1_live.get("active_power_w", 0)
            grid_w = active_power_w if active_power_w is not None else 0

        app_status = initial_state_for_static_view.get("app_state", "UNKNOWN")
        power_indicators_instance.update_indicators(solar_w, -grid_w, app_status)

        prices_today_list = initial_state_for_static_view.get("electricity_prices_today")
        if prices_today_list and isinstance(prices_today_list, list):
            x_times = [datetime.fromisoformat(d["interval_start_local"]) for d in prices_today_list if
                       "interval_start_local" in d]
            y_prices = [d.get("price_eur_per_kwh") for d in prices_today_list]
            valid_plot_data = [(t, p) for t, p in zip(x_times, y_prices) if p is not None]
            if valid_plot_data:
                plot_x, plot_y = zip(*valid_plot_data)
                price_plot_source.data = dict(x=list(plot_x), y_price_kwh=list(plot_y))

    dashboard_layout.servable(title="Home Energy Panel")
