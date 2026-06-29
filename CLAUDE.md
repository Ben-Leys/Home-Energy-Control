# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Home Energy Control (HEC) is a single-process Python application that optimizes a home's energy use against the Belgian dynamic electricity market. It polls hardware (SMA inverter via Modbus, HomeWizard P1 smart meter, HomeWizard batteries, an EVCC EV charger), pulls market data (ENTSO-E day-ahead prices, Elia solar/wind/grid-load forecasts), and drives the controllers to minimize cost / avoid the monthly capacity-tariff peak.

## Running

Always run from the **project root** with the root on `PYTHONPATH` (the package is imported as `hec.*`). The virtualenv is `.venv`.

```powershell
# Main application (scheduler + Flask API + control loop)
python -m hec.main          # or: python hec/main.py

# Legacy Streamlit dashboard (deprecated 20/03/2026, replaced by Vue served from the API)
streamlit run hec/ui/hec_dashboard.py
```

The live UI is `hec/core/vue_dashboard.html`, served at `/` by the Flask API (default `0.0.0.0:8123`). It polls `GET /api/v1/state`, `GET /api/v1/logs`, and writes settings via `POST /api/v1/settings/update`.

### Tests

Tests use `unittest` (not pytest), under `hec/tests/`.

```powershell
python -m unittest discover -s hec/tests          # all tests
python -m unittest hec.tests.test_system_mediator # one module
python -m unittest hec.tests.test_system_mediator.TestSystemMediatorFunctional.test_transition_between_mediator_goals_updates_states  # one test
```

## Configuration

Two required files at the package root (`hec/`), both git-ignored and **not** in the repo — they must exist to run:
- `hec/config.yaml` — all tuning: `mediator`, `inverter`, `batteries`, `evcc`, `p1_meter`, `database`, `api_server`, `scheduler`, `tasks_schedule`, `historic_data`, `smtp`, location/lat-lon. Loaded by `load_app_config()` in `hec/core/app_initializer.py`.
- `hec/.env` — secrets read via `os.getenv` (e.g. `P1_METER` token, ENTSO-E API key, SMTP creds).

`tasks_schedule.<job_id>.trigger_args` in config drives APScheduler cron timing; a job with no `trigger_args` is **not scheduled**. Job IDs are the constants at the top of `hec/logic_engine/scheduled_tasks.py`.

The SQLite DB path comes from `config.database.path` (relative to `hec/`); it is created on first run. `historic_data.start_date` triggers a one-time backfill of ENTSO-E prices and Elia forecasts if the DB is empty.

## Architecture

The flow is **poll → AppState → mediate → command hardware**, all coordinated by an APScheduler running cron jobs at second/minute granularity. There is no message bus; everything communicates through one in-memory global state object.

### Central state: `GLOBAL_APP_STATE`
`hec/core/app_state.py` defines a single `AppState` singleton (`GLOBAL_APP_STATE`) imported everywhere. It is a dict wrapper with `get`/`set`. Key facts:
- Only keys predefined in `current_values` are accepted; `set()` on an unknown key logs a warning and is dropped (except the special `prediction_plan_df`, stored as an attribute, not in the dict).
- Keys in `persisted_keys` (operating mode, manual states/limits, mediator goal, `empty_since`) are written through to the DB `settings` table and reloaded on startup via `load_persisted_settings()`.
- Poll tasks write data in; the mediator and API read it out. This is the integration seam between every subsystem.

### Startup: `hec/main.py` → `app_initializer.py`
`run_application()` wires everything in order: DB → tariff manager → historic-data check → external clients → populate AppState → construct `SystemMediator` → start Flask API thread → set up scheduler → register jobs → `scheduler.start()`. Client init is fault-tolerant: a missing/unreachable device returns `None` and degrades functionality rather than crashing (`app_state` goes to `DEGRADED`). The main thread blocks on the API thread (or sleeps) while the BackgroundScheduler runs jobs.

### Scheduler & tasks: `hec/logic_engine/scheduled_tasks.py`
`register_all_jobs()` registers all cron jobs; each device only gets its poll jobs if its client initialized. Notable tasks:
- `task_poll_p1_meter` / `task_poll_inverter*` / `task_poll_evcc_state` / `task_poll_battery_for_db_logging` — refresh hardware data into AppState every few seconds; store to DB on 15-min (or configured) boundaries.
- `task_fetch_and_store_day_ahead_prices` — ENTSO-E D+1 auction; **self-reschedules** on failure (prices aren't published at a fixed time), resetting `fetch_prices_attempt_count` (a module global) when done.
- `task_midnight_rollover` — shifts `electricity_prices_tomorrow` → `electricity_prices_today`, recomputes sunrise/sunset.
- `task_run_battery_predictor` — builds the 48h plan (see below).
- `task_system_mediator` — the control loop; also handles `reboot_request` (sends SIGINT to self) and `summary_request` flags set via the API.

### The control loop: `hec/logic_engine/system_mediator.py`
`SystemMediator.run_system_mediation_logic()` runs on a short cron interval and is the heart of the system. Each tick:
1. `_prepare_data()` — refresh `MarketContext` prices and current peak.
2. `_handle_peak_consumption()` — **highest priority**: if 5/10/15-min rolling grid-import averages approach the capacity-tariff limit, force-throttle everything (EV off, inverter standard, battery on), email an alert, and skip all other logic until the peak clears. Restores prior states on exit.
3. Otherwise dispatch on `app_operating_mode`: `MODE_MANUAL` copies the user's UI-set states straight through; `MODE_AUTO` computes states from `app_mediator_goal` via `_determine_evcc_state` / `_recalculate_charging_amperage` / `_determine_battery_state` / `_determine_inverter_state` / `_recalculate_inverter_limit`.
4. `_apply_evcc_state` / `_apply_inverter_state` / `_apply_battery_state` — push to hardware only on change, with throttling. **Inverter writes are deliberately rate-limited** (deadbands, hysteresis, frequency multiplier) to protect the SMA inverter's flash memory from wear — preserve this when editing limit logic. Inverter is not touched outside daylight.

State enums (`MediatorGoal`, `OperatingMode`, `InverterManualState`, `EVCCManualState`, `BatteryState`, `AppStatus`, `InverterStatus`) all live in `hec/core/constants.py`.

### Battery prediction: `hec/logic_engine/battery_predictor.py` + `consumption_predictor.py` + `cost_calculator.py`
Once per day a 48h base plan is built at 15-min resolution (forecast consumption × forecast solar × net prices), then re-optimized every 15 min against live SOC/import. The result `prediction_plan_df` (a pandas DataFrame indexed by UTC timestamp, with `force_c`/`force_time`/`block_c`/`block_d`/`soc_pct` columns) is stored in AppState and read each tick by `_determine_battery_state` to decide force-charge / block-charge / block-discharge. `_calculate_safe_force_charge_minutes()` gates force-charging against the 15-min capacity budget.

### Layered packages
- `controllers/` — write-capable device clients: `modbus_sma_inverter.py` (pymodbus), `api_evcc.py`.
- `data_sources/` — read clients: `api_p1_meter_homewizard.py` (also commands battery mode), `api_battery_homewizard.py`, `api_entsoe.py`, `api_elia.py`.
- `core/` — `app_state`, `app_initializer`, `constants`, `market_prices` (`MarketContext`), `tariff_manager`, `models` (dataclasses like `EVCCLoadpointState`, `NetElectricityPriceInterval`, `PricePoint`), `api_server` (Flask), `app_logging` (logs to DB).
- `logic_engine/` — mediator, predictors, `scheduled_tasks`, `data_processors` (rolling averages, price/forecast population), `cost_calculator`.
- `database_ops/db_handler.py` — single `DatabaseHandler` wrapping SQLite (`check_same_thread=False`; shared across the scheduler threads and API thread).
- `reporting/` — `daily_summary.py`, `plot_generator.py` (emailed summary).

## Conventions & gotchas

- **Timezones:** market/scheduling logic mixes UTC and `Europe/Brussels`. AppState and DB timestamps are UTC ISO strings; user-facing/sun calculations are local. Be explicit with `tz=` — DST boundaries are handled deliberately in the predictor.
- **P1 meter field typo:** the HomeWizard API returns `montly_power_peak_w`/`montly_power_peak_timestamp` (misspelled); these are remapped to `monthly_*` in `task_poll_p1_meter`. Don't "fix" the source key.
- Hardware clients fail soft — always guard `None` clients and missing AppState keys (the code does this pervasively; match it).
- `requirements.txt` is UTF-16 encoded. APScheduler 3.x, Flask, pymodbus, pandas/numpy/scikit-learn, streamlit.
- No linter/formatter config is committed; match the existing style (module-level `logger = logging.getLogger(__name__)`, f-string logging, top-of-file job-ID constants).