# hec/ui/dashboard_app.py
import json
import time
from typing import Any, Dict, List, Optional

import streamlit as st
import requests
from datetime import datetime, timedelta

# Set PYTHONPATH to . (project root)
# streamlit run hec/ui/hec_dashboard.py
try:
    from hec.core import constants as c
    from hec.core.app_initializer import load_app_config
except ImportError:
    st.error("Could not import from 'hec.core'. Make sure you run Streamlit from the project root "
             "and 'hec' package is in PYTHONPATH.")
    exit(1)

try:
    # Configuration of the API
    APP_CONFIG = load_app_config()
    server_config = APP_CONFIG.get('api_server', {})
    api_port = server_config.get('port', 8213)

    # Configuration of the dashboard
    dashboard_config = APP_CONFIG.get('dashboard', {})
    api_host = dashboard_config.get('host', 'localhost')
    refresh_interval = dashboard_config.get('refresh_interval', 15)
except Exception as e:
    st.error(f"Error loading application configuration: {e}. Using default API settings.")
    api_host = "localhost"
    api_port = 5000  # Default if config load fails
    refresh_interval = 15

MAIN_APP_API_URL = f"http://{api_host}:{api_port}/api/v1/"
STATE_URL_SUFFIX = "state"
SETTINGS_UPDATE_URL_SUFFIX = "settings/update"


# --- Helper to get data from Main App API ---
def get_main_app_state():
    try:
        response = requests.get(f"{MAIN_APP_API_URL}{STATE_URL_SUFFIX}", timeout=3)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as er:
        st.error(f"Error fetching data from main app: {er}")
        return None
    except Exception as er:
        st.error(f"An unexpected error occurred: {er}")
        return None


# --- Helper for App Status Color ---
def get_status_color(app_status_str: str) -> str:
    status_map = {
        c.AppStatus.NORMAL.value: "green",
        c.AppStatus.STARTING.value: "blue",
        c.AppStatus.DEGRADED.value: "grey",
        c.AppStatus.WARNING.value: "orange",
        c.AppStatus.ALARM.value: "red",
        c.AppStatus.SHUTDOWN.value: "black",
    }
    return status_map.get(app_status_str, "black")  # Default to black if unknown


# --- Helper to find current price interval (simplified for Streamlit) ---
def get_current_price_from_list(now_local: datetime, price_intervals: Optional[List[str]]) -> Optional[Dict]:
    if not price_intervals:
        return None
    for interval_data in price_intervals:
        try:
            interval_dict = json.loads(interval_data)

            start_str = interval_dict.get("interval_start_local")
            res_min = interval_dict.get("resolution_minutes")
            if not start_str or res_min is None:
                continue

            interval_start = datetime.fromisoformat(start_str)
            interval_end = interval_start + timedelta(minutes=res_min)

            if interval_start <= now_local < interval_end:
                return interval_dict
        except Exception as er:
            print(f"{er}")
    return None


# --- Send setting update to API ---
def send_setting_update(app_state_key: str, new_value: Any):
    """Sends an update to the main application's settings API."""
    payload = {"key": app_state_key, "value": new_value}
    st.session_state.api_call_inflight = True  # Indicate API call started
    try:
        response = requests.post(f"{MAIN_APP_API_URL}{SETTINGS_UPDATE_URL_SUFFIX}", json=payload, timeout=5)
        response.raise_for_status()
        result = response.json()
        if result.get("success"):
            st.toast(f"Setting '{app_state_key}' updated to '{result.get('new_value_stored', new_value)}'.", icon="✅")
            time.sleep(1)
            st.rerun()  # This will re-fetch state in the fragment
        else:
            st.error(f"Failed to update setting '{app_state_key}': {result.get('error', 'Unknown API error')}")
    except requests.exceptions.RequestException as er:
        st.error(f"API Error updating setting '{app_state_key}': {er}")
    except Exception as er:
        st.error(f"Unexpected error sending update for '{app_state_key}': {er}")
    finally:
        st.session_state.api_call_inflight = False


# --- UI Sections ---
def display_dashboard_tab(state: Optional[Dict[str, Any]]):
    if not state:
        st.error("🚨 Main application state not available. Cannot display dashboard.")
        if st.button("Retry Fetching State"):
            st.rerun()
        return

    # --- Application Status ---
    app_status_str = state.get("app_state", c.AppStatus.STARTING.value)
    status_color = get_status_color(app_status_str)
    st.markdown(
        f"<h5>Application Status: <span style='color:{status_color};'>"
        f"{app_status_str.replace('_', ' ').title()}</span></h5>",
        unsafe_allow_html=True
    )
    # st.divider()

    # --- Main Figures Box ---
    with st.container(border=True):
        st.subheader("⚡ Live Metrics")
        col1, col2, col3 = st.columns(3)

        # P1 Meter
        p1_live = state.get("p1_meter_data")
        p1_power_w_str = "N/A"
        if p1_live and isinstance(p1_live, dict):
            p1_power_w_val = p1_live.get('active_power_w', 0)
            if p1_power_w_val is not None:
                p1_power_w_str = f"{p1_power_w_val:.0f} W"
        col1.metric("Grid Power (P1)", p1_power_w_str, delta="Import > 0 > Export", delta_color="off")

        # Inverter Production
        inverter_live = state.get("inverter_data")
        pv_power_w_str = "N/A"
        if inverter_live and isinstance(inverter_live, dict):
            pv_power_val = inverter_live.get('pv_power_watts', 0)
            if pv_power_val is not None:
                pv_power_w_str = f"{pv_power_val:.0f} W"
        col2.metric("Solar Production", pv_power_w_str)

        # EVCC Charging Current
        evcc_lp_data = state.get("evcc_loadpoint_state")
        ev_charge_current_str = "N/A"
        ev_charging_status = "Not Charging"
        if evcc_lp_data and isinstance(evcc_lp_data, dict):
            if evcc_lp_data.get("is_charging", False):
                ev_charging_status = "Charging"
                charge_current_val = evcc_lp_data.get('charge_current', 0) * 230
                if charge_current_val is not None:
                    ev_charge_current_str = f"{charge_current_val:.0f} W"
            else:
                ev_charging_status = "Connected" if evcc_lp_data.get("is_connected") else "Disconnected"
        col3.metric(f"EV Status: {ev_charging_status}", ev_charge_current_str)
    # st.divider()

    # --- Current Electricity Price (from list) ---
    st.subheader("💡 Current Price")
    prices_today_list = state.get("electricity_prices_today", [])
    now_local = datetime.now().astimezone()
    current_price_interval_data = get_current_price_from_list(now_local, prices_today_list)

    if current_price_interval_data:
        buy_price = current_price_interval_data.get('net_prices_eur_per_kwh', {}).get('dynamic', {}).get('buy', 0)
        sell_price = current_price_interval_data.get('net_prices_eur_per_kwh', {}).get('dynamic', {}).get('sell', 0)
        start_time_str = current_price_interval_data.get('interval_start_local', "N/A")
        try:
            start_dt = datetime.fromisoformat(start_time_str)
            res_min = current_price_interval_data.get('resolution_minutes', 15)
            end_dt = start_dt + timedelta(minutes=res_min)
            st.caption(f"Interval: {start_dt.strftime('%H:%M')} - {end_dt.strftime('%H:%M')} (Local)")
        except Exception as er:
            print(f"{er}")
            pass

        col_p1, col_p2 = st.columns(2)
        col_p1.metric("Net Buy Price", f"{buy_price:.4f} €/kWh" if buy_price is not None else "N/A")
        col_p2.metric("Net Sell Price", f"{sell_price:.4f} €/kWh" if sell_price is not None else "N/A")
    else:
        st.warning("Current electricity price data not available.")
    # st.divider()

    # --- Controls Box ---
    with st.container(border=True):
        st.subheader("⚙️ System Controls")

        # Application Operating Mode
        cur_str = state.get("app_operating_mode", c.OperatingMode.MODE_AUTO.value)
        mode_options = [mode.value for mode in c.OperatingMode]

        mode_idx = mode_options.index(cur_str) if cur_str in mode_options else 0
        new_val = st.radio("Application Mode:", mode_options, index=mode_idx, key="app_op_mode", horizontal=True)

        if new_val != cur_str:
            selected_enum = c.OperatingMode(new_val)
            send_setting_update("app_operating_mode", selected_enum.name)

        # Application Mediator Goal
        cur_str = state.get("app_mediator_goal", c.MediatorGoal.NO_CHARGING.value)
        goal_options = [goal.value for goal in c.MediatorGoal]

        goal_idx = goal_options.index(cur_str) if cur_str in goal_options else 0
        new_val = st.selectbox("Mediator Goal:", goal_options, index=goal_idx, key="app_mediator_goal")

        if new_val != cur_str:
            selected_enum = c.MediatorGoal(new_val)
            send_setting_update("app_mediator_goal", selected_enum.name)
        st.markdown("---")

        # Inverter Controls
        st.markdown("##### Inverter Manual Control")
        cur_str = state.get("inverter_manual_state", c.InverterManualState.INV_CMD_LIMIT_STANDARD.value)
        inv_options = [mode.value for mode in c.InverterManualState]

        mode_idx = inv_options.index(cur_str) if cur_str in inv_options else 0
        new_val = st.selectbox("Inverter Manual Action:", inv_options, index=mode_idx, key="inv_man_cmd_select")

        if new_val != cur_str:
            selected_enum = c.InverterManualState(new_val)
            send_setting_update("inverter_manual_state", selected_enum.name)

        # Show and handle manual limit input if in manual mode
        if new_val == c.InverterManualState.INV_CMD_LIMIT_MANUAL.value:
            current_limit_val = state.get("inverter_manual_limit", 0)
            current_limit_val = 0 if current_limit_val is None else int(current_limit_val)
            new_limit_val = st.number_input(
                "Inverter Fixed Limit (Watts):", min_value=0, max_value=7000,
                value=current_limit_val, step=100, key="inv_fixed_limit_val"
            )
            if new_limit_val != current_limit_val:
                send_setting_update("inverter_manual_limit", new_limit_val)

        st.markdown("---")

        # EVCC Controls
        st.markdown("##### EVCC Manual Control")
        cur_str = state.get("evcc_manual_state", c.EVCCManualState.EVCC_CMD_STATE_OFF.value)
        evcc_options = [cmd.value for cmd in c.EVCCManualState]

        evcc_cmd_idx = evcc_options.index(cur_str) if cur_str in evcc_options else 0
        new_val = st.selectbox("EVCC Manual Action:", options=evcc_options, index=evcc_cmd_idx, key="evcc_select")

        if new_val != cur_str:
            selected_enum = c.EVCCManualState(new_val)
            send_setting_update("evcc_manual_state", selected_enum.name)


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

# Initialize session state for API calls if not present
if 'api_call_inflight' not in st.session_state:
    st.session_state.api_call_inflight = False

tab1, tab2 = st.tabs(["📊 Dashboard & Controls", "📜 Logs"])

with tab1:
    @st.fragment(run_every=refresh_interval)
    def dashboard_content_fragment():
        current_state = get_main_app_state()
        display_dashboard_tab(current_state)

    dashboard_content_fragment()

with tab2:
    display_log_tab()
