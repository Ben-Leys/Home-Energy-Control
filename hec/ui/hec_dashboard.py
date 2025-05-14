# hec/ui/dashboard_app.py
import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# Assuming your constants are accessible if running streamlit from project root
# and PYTHONPATH includes the project root or 'hec' is installed.
# If running `streamlit run hec/ui/dashboard_app.py` from project root, this should work:
try:
    from hec import constants as c  # Use K as alias for constants
except ImportError:
    st.error("Could not import constants. Make sure you run Streamlit from the project root "
             "and 'hec' package is in PYTHONPATH.")
    exit(1)


# Configuration for the API
MAIN_APP_API_URL = "http://localhost:5000/api/v1/state"  # Adjust if main app runs on different host/port
FETCH_INTERVAL_SECONDS = 1  # How often to refresh data


# --- Helper to get data from Main App API ---
def get_main_app_state():
    try:
        response = requests.get(MAIN_APP_API_URL, timeout=3)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from main app: {e}")
        return None
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
        return None


# --- Helper for App Status Color ---
def get_status_color(app_status_str: str) -> str:
    # Ensure K_AppStatus is defined (either from import or fallback)
    status_map = {
        c.AppStatus.NORMAL.name: "green",
        c.AppStatus.STARTING.name: "blue",
        c.AppStatus.DEGRADED.name: "grey",
        c.AppStatus.WARNING.name: "orange",
        c.AppStatus.ALARM.name: "red",
        c.AppStatus.SHUTDOWN.name: "black",
    }
    return status_map.get(app_status_str, "black")  # Default to black if unknown


# --- UI Sections ---
@st.fragment(run_every=FETCH_INTERVAL_SECONDS)
def display_dashboard_fragment():
    state = get_main_app_state()

    if state:
        # --- Application Status ---
        app_status_str = state.get("app_state", "unknown" if hasattr(c, 'AppStatus') else "UNKNOWN")
        status_color = get_status_color(app_status_str)
        st.markdown(f"Application Status: <font color='{status_color}'>{app_status_str.replace('_', ' ').title()}</font>", unsafe_allow_html=True)
        st.divider()

        # --- P1 Meter Data ---
        st.subheader("P1 Meter (Live)")
        p1_live = state.get("p1_meter_data")  # Key used in scheduled_tasks example
        if p1_live and isinstance(p1_live, dict):
            col1, col2, col3 = st.columns(3)
            col1.metric("Active Power", f"{p1_live.get('active_power_w', 'N/A')} W")
            col2.metric("Total Import", f"{p1_live.get('total_power_import_kwh', 'N/A')} kWh")
            col3.metric("Total Export", f"{p1_live.get('total_power_export_kwh', 'N/A')} kWh")
            if p1_live.get('timestamp_utc_iso'):
                st.caption(f"P1 Last Update (UTC): {p1_live.get('timestamp_utc_iso')}")
        else:
            st.warning("P1 meter data not available.")
        st.divider()

        # --- Current Electricity Price ---
        st.subheader("Current Electricity Price")
        current_prices = state.get("electricity_prices_today")

        price_to_display = None
        if current_prices and isinstance(current_prices, dict):
            price_to_display = current_prices.get('price_eur_per_mwh') / 1000
            price_ts_str = current_prices.get('hour_start_local')  # From old structure
        else:  # Try to derive from the list of intervals if the above is not populated
            prices_today_list = state.get("electricity_prices_today")
            if prices_today_list and isinstance(prices_today_list, list):
                # Simplified: show the first price if list exists, or implement full current interval search
                # This would be where your get_current_interval_data utility (if callable by streamlit) would be useful
                # For this example, just show info that list is available
                # For a real current price, Streamlit would need similar logic to find the active interval
                now_local_st = datetime.now().astimezone()
                # Placeholder for actual current price lookup from list
                # current_interval_info = find_current_interval(now_local_st, prices_today_list)
                # if current_interval_info: price_to_display = current_interval_info.get('price_eur_per_kwh')

                # For now, just indicate data structure
                if prices_today_list:
                    st.caption(f"{len(prices_today_list)} price intervals loaded for today.")
                    # Display first one as example
                    if prices_today_list[0].get('price_eur_per_kwh') is not None:
                        price_to_display = prices_today_list[0].get('price_eur_per_mwh') / 1000
                        price_ts_str = prices_today_list[0].get('interval_start_local')
                        st.caption(f"Example from today's list (first interval): Valid from {price_ts_str}")

        if price_to_display is not None:
            st.metric("Price", f"{price_to_display:.4f} EUR/kWh")
        else:
            st.warning("Current electricity price not available.")
        st.divider()

        # --- Display raw state for debugging ---
        # with st.expander("Raw App State (Debug)"):
        #     st.json(state) 
    else:
        st.error("Could not connect to the main application API or no data received.")


# --- Log Tab ---
def display_log_tab():
    st.subheader("Application Logs")
    st.info("Log viewing functionality: To be implemented.")
    st.write("Options for log viewing:")
    st.markdown("""
    - **Read log file directly:** If the main app logs to a file accessible by Streamlit.
        - Requires file path configuration.
        - Can show recent lines.
        - Level switching would mean Streamlit tells main app to change its log level (e.g., via another API endpoint).
    - **Stream logs via a WebSocket:** More complex, provides true real-time logs.
    - **Query logs from a database:** If logs are also stored in a structured DB.
    """)

    # Example: Allow user to select log level (this doesn't change main app yet)
    log_levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
    selected_level = st.selectbox("Change Main App Log Level (Future Feature)", log_levels, index=1)
    if st.button("Apply Log Level (Not Implemented)"):
        st.toast(f"Request to change log level to {selected_level} (Not yet implemented in main app API).")


# --- Main UI ---
st.set_page_config(page_title="Home Energy Controller", layout="wide")
st.subheader("🏠 Home Energy Control Dashboard")

tab1, tab2 = st.tabs(["📊 Dashboard", "📜 Logs"])

with tab1:
    display_dashboard_fragment()

with tab2:
    display_log_tab()
