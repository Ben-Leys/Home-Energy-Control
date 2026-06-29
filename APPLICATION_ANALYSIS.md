# Home Energy Control (HEC) — Application Analysis

*A senior system-analyst walkthrough of the entire codebase: what it does, how the pieces
connect, every API/DB/front-end surface, and a prioritized catalog of concrete improvements.*

---

## 1. Executive summary

HEC is a **single-process Python application** that optimizes a Belgian home's energy use against the
dynamic (day-ahead) electricity market. It continuously:

1. **Polls hardware** — an SMA PV inverter (Modbus TCP), a HomeWizard P1 smart meter, one or more
   HomeWizard batteries (HTTP), and an EVCC EV-charger (HTTP).
2. **Pulls market data** — ENTSO-E day-ahead prices (XML) and Elia solar/wind/grid-load forecasts (JSON).
3. **Stores** everything in a local SQLite database.
4. **Predicts** future consumption (historical averaging) and battery plans (15-min optimization), and
   optionally future prices (RandomForest).
5. **Decides and acts** every few seconds via a central `SystemMediator` that commands the inverter
   limit, EV charge mode/amperage, and battery charge/discharge mode — with the **monthly capacity-tariff
   peak** as the dominant safety constraint.
6. **Serves a dashboard** (Vue SPA over a Flask JSON API) and **emails a daily summary** (matplotlib plots).

**Tech stack:** Python 3.12, APScheduler (BackgroundScheduler), Flask, pymodbus 3.x, pandas/numpy,
scikit-learn, matplotlib, astral, SQLite. Front-end: Vue 3 (CDN, single HTML file). A deprecated Streamlit
dashboard remains in the tree.

**Overall assessment:** The domain logic is sophisticated and largely correct, with thoughtful handling of
DST, hardware flakiness, and inverter flash-wear. The main weaknesses are **architectural** (a single
global mutable state object, a monolithic 1500-line DB handler, repeated serialization boilerplate, a
1000-line "god method" mediator) and a number of **latent bugs** (see §13). None block operation but they
raise maintenance cost and CPU/RAM more than necessary.

---

## 2. High-level architecture

```
                      ┌─────────────────────────────────────────────────────────┐
                      │                    hec/main.py                          │
                      │   load config → init clients → build mediator →          │
                      │   start Flask thread → start APScheduler → block         │
                      └─────────────────────────────────────────────────────────┘
                                            │
       ┌──────────────────────┬────────────┼────────────────┬───────────────────────┐
       ▼                      ▼             ▼                ▼                       ▼
 ┌───────────┐        ┌──────────────┐ ┌──────────┐  ┌──────────────┐       ┌────────────────┐
 │ Poll jobs │        │ Fetch jobs   │ │ Mediator │  │ Predictor    │       │ Flask API      │
 │ (P1, INV, │  ───▶  │ (ENTSO-E,    │ │ job      │  │ job (battery │       │ thread         │
 │  EVCC,bat)│        │  Elia)       │ │ (~5-15s) │  │  plan 15min) │       │ /api/v1/*      │
 └─────┬─────┘        └──────┬───────┘ └────┬─────┘  └──────┬───────┘       └───────┬────────┘
       │  write               │ write        │ read/act      │ read/write           │ read/write
       ▼                      ▼              ▼               ▼                      ▼
 ┌─────────────────────────────────────────────────────────────────────────────────────────┐
 │                              GLOBAL_APP_STATE  (in-memory dict singleton)                  │
 └─────────────────────────────────────────────────────────────────────────────────────────┘
       │                                                                              ▲
       │ persist subset (settings) + log/time-series                                 │ poll every 10s
       ▼                                                                              │
 ┌─────────────────────────┐                                              ┌──────────────────────┐
 │  SQLite (DatabaseHandler)│                                              │ vue_dashboard.html   │
 │  9 tables                │                                              │ (Vue 3 SPA)          │
 └─────────────────────────┘                                              └──────────────────────┘
```

**The integration seam is `GLOBAL_APP_STATE`.** There is no message bus, no async event loop, no DI
container. Every subsystem reads/writes a single in-memory dict (`hec/core/app_state.py`), and APScheduler
thread-pool workers run all jobs concurrently against it.

### Layered packages

| Package             | Responsibility                                                                                                                                          |
|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hec/core/`         | App lifecycle, global state, constants/enums, dataclasses (`models`), Flask API, logging, market-price context, tariff manager                          |
| `hec/controllers/`  | **Write-capable** device clients: SMA inverter (Modbus), EVCC (HTTP)                                                                                    |
| `hec/data_sources/` | **Read** clients: P1 meter (also writes battery mode), HomeWizard battery, ENTSO-E, Elia                                                                |
| `hec/logic_engine/` | The brains: `system_mediator`, `battery_predictor`, `consumption_predictor`, `price_predictor`, `cost_calculator`, `data_processors`, `scheduled_tasks` |
| `hec/database_ops/` | `DatabaseHandler` — all SQLite access                                                                                                                   |
| `hec/reporting/`    | Daily-summary email + matplotlib plot generation                                                                                                        |
| `hec/ui/`           | Deprecated Streamlit dashboard                                                                                                                          |
| `hec/utils/`        | Sun/daylight, holidays, email, power conversion, price-point processing                                                                                 |
| `hec/tests/`        | `unittest` suite (utils, mediator, entsoe)                                                                                                              |

---

## 3. Runtime / process model

- **Entry point** `hec/main.py::run_application()`:
    1. `load_app_config()` reads `hec/config.yaml` + `hec/.env`.
    2. `start_logger()` installs console + rotating-file + a custom `GlobalStateHandler` that mirrors
       WARNING/ERROR levels into `app_state` and writes every record to the `logs` DB table.
    3. DB handler, tariff manager, historic-data check.
    4. `initialize_external_clients()` builds the four device clients, each **fault-tolerant** (returns
       `None`/empty on failure → degraded operation, not a crash).
    5. `populate_app_state()` seeds prices + EVCC state; persisted settings reloaded from DB.
    6. `SystemMediator` constructed.
    7. Flask API started in a **daemon thread**.
    8. `setup_scheduler()` → `BackgroundScheduler` (ThreadPoolExecutor, default 10 workers, tz
       Europe/Brussels) → `register_all_jobs()`.
    9. Main thread `join()`s the API thread; scheduler runs jobs in the background.
- **Shutdown:** SIGINT/SystemExit → scheduler shutdown → DB close. A `reboot_request` flag (set via API)
  makes `task_system_mediator` send `SIGINT` to its own PID.
- **Concurrency model:** APScheduler thread-pool workers + the Flask thread all touch `GLOBAL_APP_STATE`
  and the **single shared SQLite connection** (`check_same_thread=False`). There is **no locking** around
  either. SQLite serializes writes at the file level (and one `store_inverter_data` path retries on
  "database is locked"), but `GLOBAL_APP_STATE` read-modify-write sequences are not atomic.

### Scheduled jobs (`scheduled_tasks.register_all_jobs`)

| Job ID                         | Trigger (default)          | Function                                 | Purpose                                          |
|--------------------------------|----------------------------|------------------------------------------|--------------------------------------------------|
| `fetch_day_ahead_prices`       | cron (config)              | `task_fetch_and_store_day_ahead_prices`  | ENTSO-E D+1; **self-reschedules** on miss        |
| `midnight_rollover`            | cron                       | `task_midnight_rollover`                 | shift tomorrow→today prices; recompute sun times |
| `fetch_elia_forecast`          | cron                       | `task_fetch_elia_forecasts`              | solar/wind/grid_load D+1..D+5                    |
| `mediator_logic`               | cron (~5–15 s)             | `task_system_mediator`                   | **the control loop**                             |
| `battery_prediction`           | cron (~15 min)             | `task_run_battery_predictor`             | 48 h plan + 15-min optimization                  |
| `p1_meter_update`              | every 15 s                 | `task_poll_p1_meter`                     | live grid + battery summary; rolling averages    |
| `inverter_update_for_db`       | every 15 min               | `task_poll_inverter_for_db_logging`      | log to DB                                        |
| `inverter_update_for_mediator` | every 15 s                 | `task_poll_inverter_for_mediator_update` | live PV for mediator                             |
| `evcc_update`                  | every 15 s                 | `task_poll_evcc_state`                   | loadpoint state; 15-min session log              |
| `battery_update` / `_for_db`   | 15 s / 15 min              | `task_poll_battery_for_db_logging`       | per-battery SOC/energy                           |
| `daily_summary_email`          | one-shot after price fetch | `task_send_daily_energy_summary_email`   | email report                                     |
| `fetch_*_historical_*`         | one-shot at startup        | backfill ENTSO-E / Elia                  | first-run history                                |

---

## 4. Configuration & secrets

Two **git-ignored, not-in-repo** files at the package root `hec/` (the app exits if `config.yaml` is
missing):

- **`hec/config.yaml`** — every tunable. Inferred sections from the code:
  `application` (log level/file, tariffs_file_name), `database.path`, `api_server` (host/port/debug/enabled),
  `scheduler` (timezone, thread_pool_max_workers, coalesce, max_instances, misfire grace, run_in_background),
  `tasks_schedule.<job_id>.trigger_args` (+ per-job `max_retries`, `retry_after`, `second`/`minute`),
  `mediator` (standard_max_peak_consumption_kw, buffer_before_pv_limit_change),
  `inverter` (host/port/modbus_unit_id/grid_guard_code/standard_power_limit/timeout_sec/panel_peak_w/location),
  `batteries[]` (name/host/capacity_kwh/max_charge_W/max_discharge_W),
  `evcc` (api_url/default_loadpoint_id/max_current/request_timeout_seconds),
  `p1_meter.host`, `entsoe` (api_base_url/document_type/domain/auction_opening_hour),
  `elia` (api_base_url/timezone/dataset_* ids), `historic_data.start_date`, `smtp` (
  host/port/user/sender_email/default_recipients).
- **`hec/.env`** — secrets via `os.getenv`: `ENTSOE_API_KEY`, `P1_METER` (bearer token),
  `BATTERY_<NAME>` (per-battery token, name upper-cased), `GMAIL_SMTP_PASSWORD`.
- **`hec/tariffs.yaml`** — Belgian tariff structure (`tariff_manager.py`): `contract_types`,
  `active_contract`, `energy_supplier.{fixed,dynamic}`, `grid_operator`, `government`, each a list of
  date-versioned `{start_date, value}` entries sorted descending so `_find_active` returns the newest
  applicable.

---

## 5. End-to-end data flow

1. **Poll** (every 15 s): `task_poll_p1_meter` fetches `/api/v1/data` + `/api/batteries`, writes
   `p1_meter_data` and `battery_data` to AppState, recomputes rolling averages
   (`data_processors.update_rolling_averages`), and stores a row to `p1_meter_log` on 5-min boundaries.
   Inverter/EVCC/battery pollers do the same for their state slices.
2. **Prices** (daily): `fetch_entsoe_prices` → `PricePoint[]` → `store_da_prices` (raw) +
   `calculate_net_intervals_for_day` → `NetElectricityPriceInterval[]` into `electricity_prices_today/tomorrow`.
3. **Predict** (15 min): `task_run_battery_predictor` builds a 48 h base plan once/day
   (`ConsumptionPredictor` forecast × Elia solar × net prices) then re-optimizes against live SOC/grid →
   `prediction_plan_df` + `prediction_plan` in AppState.
4. **Decide** (5–15 s): `SystemMediator.run_system_mediation_logic` reads AppState (prices, p1, inverter,
   evcc, battery, plan), evaluates peak-shaving → manual/auto goal logic → target inverter limit, EV
   mode/amps, battery mode.
5. **Act:** the mediator's `_apply_*` methods push to the hardware clients (with throttling/deadbands) and
   write the confirmed states back to AppState (which persists the "manual_*" subset to `app_settings`).
6. **Observe:** Vue SPA polls `/api/v1/state` every 10 s; daily email aggregates cost/savings from the DB.

---

## 6. Back-end module reference

### `hec/core/app_state.py` — `AppState` / `GLOBAL_APP_STATE`

- Dict wrapper with `get`/`set`/`get_all`. **Only predefined keys accepted**; unknown key on `set` logs a
  warning and is dropped, except the special `prediction_plan_df` stored as an attribute (kept out of the
  dict because it's a DataFrame).
- `persisted_keys` (operating mode, manual states/limits, mediator goal, `empty_since`) are written through
  to `app_settings` on every `set` and reloaded at startup via `load_persisted_settings`.
- **Singleton** instantiated at import time.

### `hec/core/constants.py` — enums

`AppStatus`, `MediatorGoal`, `OperatingMode`, `InverterStatus`, `InverterManualState`, `EVCCManualState`,
`BatteryState`. These are the vocabulary of the whole control system.

### `hec/core/models.py` — dataclasses

`PricePoint` (frozen), `NetElectricityPriceInterval` (frozen, ordered; `to_dict`/`__repr__` as JSON),
`EVCCOverallState`, `EVCCLoadpointState` (both with `from_dict` filtering unknown keys). Note:
`EVCCOverallState`/`EVCCLoadpointState` use **mutable default via `datetime.now(...)` evaluated at class
definition time** — see §13.

### `hec/core/api_server.py` — Flask API

- `GET /api/v1/state` — serializes `get_all()` to JSON; hand-rolled recursion converts datetimes→ISO,
  Enums→`.name`, deques→list, and `clean_nas` replaces NaN/Inf with `None`.
- `GET /api/v1/logs?limit=` — last N rows from `logs` (capped 20 000).
- `POST /api/v1/settings/update` — `{key, value}`; validates against AppState keys, maps to Enum/int via
  `TYPE_MAP`, range-checks inverter limit (0–7000) and EVCC amps (6–32).
- `GET /` — serves `vue_dashboard.html`.
- Runs via Flask dev server (`use_reloader=False`).

### `hec/core/market_prices.py` — `MarketContext`

Lightweight cache of the current interval's net buy/sell price with `refresh_if_needed` keyed on
`next_update_at`; returns `False` for fixed contracts (mediator then skips price logic).

### `hec/core/tariff_manager.py`

Loads `tariffs.yaml`, sorts each parameter's date-versioned list descending, and `get_all_tariffs(date)`
resolves the active supplier/grid/government values for a date. Pure, side-effect-free, easily testable.

### `hec/controllers/modbus_sma_inverter.py` — `InverterSmaModbusClient`

- Reads PV power, daily/total yield, power-limit setpoint, device status (register map at top of file).
- **Grid Guard** login flow required before writing the power limit; re-logs in if >3 h elapsed.
- `set_active_power_limit` enforces a hardware rate limit (≤4 writes / 2 min via a `deque(maxlen=4)`) —
  protects inverter flash memory.
- Robust reconnect/disconnect on `ModbusIOException`.

### `hec/controllers/api_evcc.py` — `EvccApiClient`

REST wrapper: `get_current_state_data`, `set_charge_mode`, `set_max_current`, target/min SOC, smart-cost
limit, `sequence_force_pv_charging`. Tracks `is_available`; flips it off on connection errors.

### `hec/data_sources/api_p1_meter_homewizard.py` — `P1MeterHomewizardClient`

- `refresh_meter_data` (`http://host/api/v1/data`), `refresh_batteries_data`
  (`https://host/api/batteries`, bearer token, `verify=False`), and **`set_battery_mode`** mapping
  `BatteryState` → `{mode, permissions}`. (So the "P1 client" is actually the battery *controller* too.)

### `hec/data_sources/api_battery_homewizard.py` — `BatteryHomeWizard`

Per-battery SOC/energy/cycles via `https://host/api/measurement`. **Overrides the constructor `token` arg
with `os.getenv("BATTERY_<NAME>")`** at init.

### `hec/data_sources/api_entsoe.py`

`fetch_entsoe_prices` (auction-time guard, UTC period building, DST-aware) + `_parse_entsoe_price_xml`
(namespace handling, gap-filling carried-forward prices, position→timestamp anchoring). Returns
`[]` for "not yet available" vs `None` for hard errors — a meaningful tri-state the caller relies on.

### `hec/data_sources/api_elia.py`

`_fetch_raw_data` generic Open-Data v2.1 call + `fetch_and_process_forecast` (per type solar/wind/grid_load,
historical vs live dataset switch, region refine for solar, `group_by` for wind). 1 s sleep between calls
upstream to avoid throttling.

### `hec/logic_engine/data_processors.py`

- `populate_appstate_with_price_data` / `_with_forecast_data`.
- **Rolling averages**: deques of `(timestamp, cumulative_energy)` samples; `_calculate_average_power_from_samples`
  derives W over windows `{30s,60s,2m,3m,5m,10m,15m}` from energy deltas. Feeds peak detection and charge
  amperage. The deprecated "instantaneous pv_power" variant is left commented in-file.

### `hec/logic_engine/consumption_predictor.py` — `ConsumptionPredictor`

Reconstructs **house** consumption per 15 min from P1 + inverter + battery + EVCC deltas (interpolated to a
15-min grid), then forecasts by averaging 4 historical days (same day last year + previous 3 days). Pandas-heavy.

### `hec/logic_engine/battery_predictor.py` — `BatteryPredictor`

The optimizer. `generate_plan` builds a physically-constrained SOC trajectory (charge/discharge efficiency,
95%/5% tapers). `optimize_plan` injects live solar/consumption, attaches prices, then applies three rules:
`apply_rule_block_charge`, `apply_rule_block_discharge` (cumulative "bridge" peak-shaving with repeated cost
evaluation), `apply_rule_force_charge` (grid charge when price spread justifies). `calculate_impact` /
`calculate_cost` re-simulate after each rule. This is the most CPU-intensive component (many full-frame
recomputations — see §11/§12).

### `hec/logic_engine/price_predictor.py` — `EnergyPricePredictor`

`RandomForestRegressor` over `[day_of_week, is_weekend, solar_factor, wind_factor, grid_load_mwh]` →
gross €/kWh; trained on demand inside the daily email job. `_prepare_elia_frame` aligns forecasts to a
15-min index. Used only for the forecast email, not for live control.

### `hec/logic_engine/cost_calculator.py`

- `calculate_net_intervals_for_day` — turns gross `PricePoint`s into net buy/sell for fixed+dynamic
  contracts by applying the full Belgian tariff stack (supplier fee/multiplier, certificates, grid usage,
  gov contribution, excise, VAT, post-VAT levies).
- `calculate_battery_saving_for_period` and `calculate_total_costs_for_period` — per-day aggregation for the
  email (incl. tiered excise, capacity tariff from 12-month peaks, data-mgmt/subscription proration).

### `hec/logic_engine/system_mediator.py` — `SystemMediator`

The 1000-line heart. Per tick (`run_system_mediation_logic`):

1. `_prepare_data` (prices + current peak).
2. `_handle_peak_consumption` — **top priority**; throttles everything if 5/10/15-min import averages
   approach the capacity limit; emails alerts (5-min cooldown); saves/restores pre-peak states.
3. Manual mode (copy UI state) vs Auto mode (`_determine_*` from `MediatorGoal`).
4. `_determine_inverter_state` / `_recalculate_inverter_limit` — grace periods for car start, negative-price
   "limit to home use", deadbands + hysteresis + frequency multiplier to spare inverter flash, battery
   charging headroom boost.
5. `_determine_battery_state` — reads `prediction_plan_df`; force-charge timer with
   `_calculate_safe_force_charge_minutes`
   budget; sunrise block override; empty-too-long maintenance charge.
6. `_apply_evcc_state` / `_apply_inverter_state` / `_apply_battery_state` — push on change, with throttling,
   daylight guard for the inverter.

### `hec/database_ops/db_handler.py` — `DatabaseHandler`

~1500 lines, one class, all SQL. Lazy single connection (`Row` factory, `PARSE_DECLTYPES`). Methods per
table: store/get day-ahead prices, p1 meter, elia forecasts, inverter, battery, settings (typed JSON
round-trip), logs (insert + retention cleanup), predicted prices, evcc sessions (with gap backfill), and
several interval-delta/proration analytics used by the cost calculator.

### `hec/reporting/`

`daily_summary.DailySummaryGenerator` orchestrates: net prices (today/tomorrow), Elia solar alignment,
D+1 price/solar plot, 5-day price prediction + plot, cost & savings tables → HTML email with inline PNGs.
`plot_generator` builds the two matplotlib figures (with explicit 92/100-interval DST correction).

### `hec/utils/utils.py`

`get_interval_from_list`, `process_price_points_to_app_state`, `get_predicted_price_points_for_date`,
`is_daylight` (astral), `send_email_with_attachments`, Belgian `is_a_holiday` + `calculate_easter`,
`convert_power` (A↔kW @230 V).

### `hec/core/app_logging.py`

`GlobalStateHandler` couples logging to state (WARNING→`app_state=WARNING`, ERROR→`ALARM`) and persists each
record to the `logs` table. Console + `RotatingFileHandler`.

---

## 7. External integrations / APIs consumed

| System             | Protocol     | Endpoint(s)                                   | Auth                      | Direction                     |
|--------------------|--------------|-----------------------------------------------|---------------------------|-------------------------------|
| SMA inverter       | Modbus TCP   | registers 30201/30513/30517/30775/40212/43090 | Grid Guard code           | read + **write limit**        |
| HomeWizard P1      | HTTP/HTTPS   | `/api/v1/data`, `/api/batteries`              | bearer (`P1_METER`)       | read + **write battery mode** |
| HomeWizard battery | HTTPS        | `/api`, `/api/measurement`                    | bearer (`BATTERY_<NAME>`) | read                          |
| EVCC               | HTTP         | `/api/state`, `/api/loadpoints/{id}/...`      | none                      | read + **write mode/current** |
| ENTSO-E            | HTTPS (XML)  | transparency API                              | `ENTSOE_API_KEY`          | read                          |
| Elia Open Data     | HTTPS (JSON) | explore v2.1 catalog datasets                 | none                      | read                          |
| SMTP (Gmail)       | SMTP_SSL     | smtp.gmail.com:465                            | `GMAIL_SMTP_PASSWORD`     | send                          |

> ⚠️ `verify=False` is used for HomeWizard local HTTPS (self-signed). `urllib3` warnings are not suppressed
> and TLS is unverified — acceptable on a trusted LAN, but worth a conscious note.

---

## 8. Database schema (SQLite, 9 tables)

| Table              | Key                                                   | Notable columns                                                                    | Written by                                | Read by                       |
|--------------------|-------------------------------------------------------|------------------------------------------------------------------------------------|-------------------------------------------|-------------------------------|
| `belpex_da_prices` | (timestamp_utc, resolution_minutes) UNIQUE            | price_eur_per_mwh, fetched_at_utc, source_api                                      | `store_da_prices`                         | `get_da_prices`, predictor    |
| `p1_meter_log`     | timestamp_utc PK                                      | active/total power & per-phase V/A/W, `monthly_power_peak_w`, `..._timestamp`      | `store_p1_meter_data`                     | deltas, peak avg, consumption |
| `elia_open_data`   | (timestamp_utc, forecast_type, resolution_minutes) PK | most_recent_forecast_mwh, monitored_capacity_mw                                    | `store_elia_forecasts`                    | predictor, plan, email        |
| `inverter_log`     | timestamp_utc PK                                      | operational_status, pv_power_watts, daily/total_yield_wh, active_power_limit_watts | `store_inverter_data`                     | consumption predictor         |
| `battery_log`      | (timestamp_utc, battery_name) PK                      | energy_import/export_kwh, state_of_charge_pct, battery_mode, cycles                | `store_battery_data`                      | savings, consumption          |
| `app_settings`     | setting_key PK                                        | setting_value (JSON), value_type, last_updated_utc                                 | `save_setting`                            | `load_all_settings`           |
| `logs`             | id PK                                                 | timestamp, level, module, message                                                  | `store_log` (+ retention)                 | `/api/v1/logs`                |
| `predicted_prices` | timestamp_utc PK                                      | predicted_gross_price_kwh, solar/wind_factor, grid_load_mwh                        | `store_predicted_prices` (14-day cleanup) | fallback prices, email        |
| `evcc_log`         | timestamp_utc UNIQUE                                  | session_energy, energy_delta                                                       | `store_evcc_session` (gap backfill)       | consumption predictor         |

Indexes: `idx_price_timestamp_utc`, `idx_elia_forecast_type_ts`, `idx_logs_timestamp`, `idx_logs_level`.

---

## 9. In-memory state (`GLOBAL_APP_STATE` keys)

General: `app_state`, `app_operating_mode`, `app_mediator_goal`, `reboot_request`, `summary_request`,
`sunrise`, `sunset`, `sunrise_block_until`, `empty_since`.
P1/averages: `p1_meter_data`, `p1_meter_last_stored_boundary_slot_utc_iso`, `recent_p1_import/export_kwh_samples`,
`average_grid_import/export_watts`.
Inverter: `inverter_data`, `inverter_manual_state`, `inverter_manual_limit`, `recent_solar_production_wh_samples`,
`average_solar_production_watts`.
Prices/forecasts: `electricity_prices_today/tomorrow`, `forecasts` (deprecated).
EVCC: `evcc_overall_state`, `evcc_loadpoint_state`, `evcc_manual_state`, `evcc_manual_limit`, `evcc_last_logged_slot`.
Battery: `battery_data`, `battery_records`, `battery_manual_mode`, `prediction_plan`, `prediction_plan_df`
(attribute), `plan_generation_date`.

---

## 10. Front-end

- **`hec/core/vue_dashboard.html`** (688 lines): a self-contained Vue 3 SPA from CDN
  (`vue@3/dist/vue.global.js`), no build step. Tabs/cards/toasts in inline CSS.
  `createApp` polls `GET /api/v1/state` every **10 s** (`setInterval(fetchState, 10000)`), posts settings
  to `/api/v1/settings/update`, and reads `/api/v1/logs`. Served at `/` by Flask.
- **`hec/ui/hec_dashboard.py`** (377 lines): the **deprecated** Streamlit dashboard (replaced 20/03/2026),
  still in the repo and in `requirements.txt`.

---

## 11. Performance profile (where CPU/RAM actually go)

1. **`BatteryPredictor.optimize_plan`** — by far the heaviest. Each of `apply_rule_block_discharge` and
   `apply_rule_force_charge` loops over peaks and, *inside* the loop, repeatedly calls `calculate_impact`
   (a full Python `for idx, row in df.iterrows()` over ~96–192 intervals) **and** `calculate_cost` after
   every trial. This is potentially **thousands of full-frame Python iterations per 15-min cycle**.
   `iterrows()` is the slowest possible pandas access pattern.
2. **Rolling averages** recomputed for 7 windows every 15 s by scanning deques — cheap but constant.
3. **`/api/v1/state`** hand-rolled recursive serialization on every request (every 10 s per open browser).
4. **Daily email** trains a 100-tree RandomForest on months of 15-min data and renders two 300-DPI plots —
   heavy but once/day and off the control path.
5. **SQLite**: a single shared connection across many threads; mostly fine because writes are small, but the
   inverter path already hits "database is locked" enough to need a retry loop.

---

## 12. Improvements — by impact

Each item lists the **advantage** (efficiency / RAM / CPU / less code / readability / OO) it buys.

### 12.1 High impact

**H1 — Vectorize `BatteryPredictor.calculate_impact` / `calculate_cost`.**
The optimizer's `iterrows()` loops are the dominant CPU cost and run inside trial loops.
*Advantage: large CPU reduction.* Two tiers:

- Cheap win: cache `calculate_cost` inputs and avoid recomputing the whole frame when only a few rows'
  flags changed; compute cost on the slice affected by a trial rather than the entire plan.
- Real win: express the SOC recurrence with `numpy` (the charge/discharge state machine can be done with a
  scan over plain arrays instead of `df.iterrows()` + `df.at[]`), and replace per-trial full recompute with
  incremental updates. Expect order-of-magnitude speedups on the 15-min job.

**H2 — Make `GLOBAL_APP_STATE` access thread-safe (and stop persisting on the hot path).**
Multiple scheduler workers + Flask mutate the same dict and SQLite connection with no lock.
*Advantage: correctness + fewer mysterious races; lower I/O.* Add a `threading.RLock` around
`get`/`set`/`get_all`, and decouple persistence: `set()` currently writes a persisted key to SQLite
*synchronously inside the lock-free setter*, so a UI toggle does a disk write on the request thread. Batch
or queue setting persistence (or only persist on actual change).

**H3 — Split `DatabaseHandler` (1532 lines) and factor the serialization.**
One class mixes schema DDL, time-series CRUD, settings codec, log retention, and analytics
(proration/deltas). *Advantage: readability, testability, less code via shared helpers.*

- Extract a `_execute`/`_query` helper — every method repeats `with self._get_connection() as conn:
  cursor = conn.cursor(); try/except sqlite3.Error`. That boilerplate is duplicated ~25×.
- The `save_setting`/`load_setting`/`load_all_settings` type-tag codec is duplicated three times; collapse
  to one `_serialize`/`_deserialize` pair.
- Move schema DDL to a `schema.sql` resource executed once.

**H4 — Decompose `SystemMediator`.**
~1000 lines, deep nested closures (e.g. `_handle_peak_consumption` defines `_handle_peak_notifications`
which defines `_send_peak_email`), ~30 instance attributes. *Advantage: readability, OO, unit-testability.*
Introduce small collaborators: `PeakShaver`, `InverterController`, `EvccController`, `BatteryController`,
each owning its own state and exposing `decide()`/`apply()`. The mediator becomes a thin orchestrator.

**H5 — Use a connection-per-thread (or a tiny pool) for SQLite.**
The shared connection + `check_same_thread=False` is the root cause of the "database is locked" retries.
*Advantage: fewer lock errors, simpler reasoning.* Use `threading.local()` connections, set
`PRAGMA journal_mode=WAL` and `PRAGMA busy_timeout`, which together remove most contention for this
read-mostly workload.

### 12.2 Medium impact

**M1 — Replace hand-rolled JSON serialization in `api_server.get_app_state_api`.**
The 50-line nested type walk is fragile (it special-cases list-of-dict but not dict-of-list, etc.).
*Advantage: less code, fewer bugs.* Give each dataclass/enum a `to_json()` and use a single
`default=` function with `json`/Flask, or a custom `JSONProvider`. `clean_nas` also has a bug (§13).

**M2 — Add a `PRAGMA`-backed retention/index pass and stop the per-insert cleanup.**
`store_log` runs a `DELETE ... WHERE timestamp < ...` on **every** log insert. *Advantage: large reduction
in write amplification.* Run cleanup on a timer (e.g. hourly) instead of every record. Same pattern could
apply to `store_predicted_prices`' 14-day cleanup.

**M3 — Cache `TariffManager` / avoid re-instantiation.**
`cost_calculator.calculate_net_intervals_for_day` calls `initialize_tariff_manager(app_config)` — i.e.
**re-reads and re-parses `tariffs.yaml` for every day processed**. The email's yearly cost loop calls this
365× → 365 YAML parses. *Advantage: CPU + I/O.* Pass the already-constructed `TariffManager` through (the
caller in `main` already has one), or memoize `get_all_tariffs(date)`.

**M4 — De-duplicate the two near-identical interval-delta methods.**
`get_energy_deltas_for_intervals` and `get_battery_deltas_for_intervals` share ~90% logic (window build,
proration). *Advantage: less code, single place to fix proration bugs.* Parameterize the table/columns.

**M5 — Reuse one `requests.Session` per client.**
Every HTTP call (`api_evcc`, `api_p1`, `api_battery`, `api_elia`, `api_entsoe`) creates a fresh connection.
*Advantage: fewer TCP/TLS handshakes, lower latency/CPU* — these run every 15 s. A module/instance-level
`Session` with a small connection pool removes most of that overhead.

**M6 — Tame `iterrows()` in `consumption_predictor` and the cost calculator.**
`calculate_total_costs_for_period` and `calculate_battery_saving_for_period` loop per-interval in Python
over dict lookups; they could be vectorized with the interval list pre-joined to a price DataFrame.
*Advantage: CPU, esp. for the yearly email aggregation.*

**M7 — Suppress `urllib3` InsecureRequestWarning or pin a verified local CA.**
*Advantage: clean logs, explicit security posture.* At minimum call
`urllib3.disable_warnings(InsecureRequestWarning)` once, with a comment explaining the LAN trust model.

**M8 — Promote magic numbers to named config.**
The mediator is full of literals: `+1500` grace watts, `-0.15` peak buffer, `0.95`/`0.80` efficiencies,
`_SHORTAGE_CONFIG`, peak multipliers `1.1/1.25/1.05`, `200 W` negative-price reserve, `5.36` kWh hard-coded
in `calculate_cost` (line 329), `/4000.0` real-time conversion. *Advantage: readability + correctness*
(the hard-coded `5.36` capacity in `calculate_cost` contradicts the configured battery capacity and will
silently mis-price as the install changes).

### 12.3 Low impact / hygiene

**L1 — Remove dead code and the deprecated Streamlit UI** (and drop `streamlit`, and possibly `bokeh`,
`param`, `altair`, `holoviews`, `hvplot` if unused, from `requirements.txt`). The large commented blocks in
`data_processors`, `system_mediator` (bottom), and the `__main__` test harnesses bloat the files.
*Advantage: less code, smaller env, faster install/imports.* (Also delete the 14 MB committed `bfg-1.15.0.jar`
and `packages.dot/html` build artifacts — they're tracked but listed in `.gitignore`.)

**L2 — Fix the two `@staticmethod` methods that take `self`** in `daily_summary` (`_format_hours`,
`_format_hours_summary` are decorated `@staticmethod` yet called as `self._format_hours(self, ...)`).
*Advantage: readability/correctness; remove the decorator or the `self` param.*

**L3 — `requirements.txt` is UTF-16** — convert to UTF-8 so standard tooling and `pip` on minimal images
read it cleanly. *Advantage: tooling friction.*

**L4 — Centralize timezone handling.** UTC vs `Europe/Brussels` conversions are re-implemented in many
files (`pytz`, `zoneinfo`, and `datetime.now().astimezone()` are all used). *Advantage: fewer DST bugs,
readability.* A small `time_utils` module with `now_utc()`, `now_local()`, `to_local()`, `local_tz()` would
remove dozens of ad-hoc conversions and the mixed `pytz`/`zoneinfo` usage.

**L5 — Type-annotate and lint.** No linter/formatter config is committed. Adding `ruff`/`black` + a CI run
would catch the dead-code, unused imports, and the bugs in §13 cheaply. *Advantage: ongoing quality.*

**L6 — Make `register_all_jobs` data-driven for the device pollers.** The P1/inverter/EVCC/battery
registration blocks are copy-paste with one differing field; a small table + loop (like the CRON job
table already used above it) would shrink the function. *Advantage: less code.*

---

## 13. Latent bugs found during analysis

These are correctness issues independent of the efficiency work above; worth fixing regardless.

1. **`DatabaseHandler.store_da_prices` always returns 0 / mis-logs.** `inserted_count` is initialized to 0
   and never updated from `cursor.rowcount`, so the method returns `0` even on success and logs
   "stored or updated 0 price points" (db_handler.py ~256). Callers that check the count (e.g.
   `fetch_historic_da_data` summing `lines_added`) under-report.

2. **`get_app_state_api` builds `clean_state` inside the for-loop** (api_server.py:71): `clean_state =
   clean_nas(serializable_state)` is recomputed on every iteration over the state dict and only the last
   assignment is returned. Functionally correct but it runs the full recursive clean N times per request
   instead of once — move it after the loop.

3. **`get_predicted_prices_for_date` references undefined `target_date` in its except block** (db_handler.py
   ~1379: `f"...for {target_date}"` — the parameter is `target_date_local`). A logging path will raise
   `NameError` while handling another error.

4. **`EVCCOverallState.timestamp_utc_iso` default is evaluated once at import** (`models.py:59`
   `= datetime.now(timezone.utc).isoformat()`), so every default-constructed instance shares the process
   start time rather than "now". Use `field(default_factory=...)`.

5. **`_determine_inverter_state` reads `inv_data` `cur_limit_w` but `_recalculate_inverter_limit` recomputes
   it** — duplicated reads of `active_power_limit_watts` with the same fallback; minor, but they can diverge
   if AppState changes mid-tick (no snapshot).

6. **`task_run_battery_predictor` averages SOC with a `-0.001` sentinel** (`actual_soc = -0.001`, then
   `+= soc`, then `/= len`). The sentinel skews the mean by `-0.001/len` and the "no SOC" check
   (`if actual_soc == -0.001`) fails if any battery reports exactly enough to offset it. Use `None`/empty
   check instead.

7. **`set_min_current` (api_evcc.py:164) logs an invalid-range warning but then sends the command anyway** —
   the early-return is missing after the warning.

8. **`daily_summary` `@staticmethod` + `self`** (see L2) — works only because it's called as
   `self._format_hours(self, ...)`, which is confusing and breaks if ever called normally.

9. **`store_da_prices` uses `INSERT OR REPLACE` keyed on (timestamp, resolution)** while
   `predicted_prices`/others use `REPLACE` — consistent enough, but `cursor.rowcount` semantics for
   `INSERT OR REPLACE` make the "rows inserted" reporting meaningless (ties into #1).

10. **No snapshot isolation in the mediator tick:** each `_determine_*` re-reads `GLOBAL_APP_STATE`
    independently, so a poll job updating `p1_meter_data` mid-tick can make the EV-amperage and
    peak-shaving decisions use inconsistent inputs. Capturing a per-tick immutable snapshot at the top of
    `run_system_mediation_logic` would make decisions consistent and is also faster (fewer dict lookups).

---

## 14. Suggested order of work

1. **Correctness first (cheap):** §13 items #1, #3, #4, #6, #7; add `ruff` (L5).
2. **Hot-path CPU:** H1 (vectorize optimizer), M3 (stop re-parsing tariffs), M5 (sessions).
3. **Robustness:** H2/H5 (state lock + SQLite WAL/per-thread), M2 (log retention off the insert path).
4. **Maintainability:** H3/H4 (split DB handler + mediator), M1/M4/L6 (de-dup), M8 (config constants).
5. **Cleanup:** L1 (dead code, deprecated UI, tracked binaries), L3, L4.

These preserve all current behavior (the domain logic is sound) while cutting the per-cycle CPU, reducing
write amplification and lock contention, and making the two largest files tractable.

---

## 15. Component deep-dive: data sources, controllers, market API

This section answers "can each object be improved, and is there a *better solution* than the current one?"
For each, I separate **tactical** fixes (same shape, less waste) from a **better design** (different shape).

### 15.1 The four I/O clients share a problem they each re-solve badly

`P1MeterHomewizardClient`, `BatteryHomeWizard`, `EvccApiClient`, and the two fetch modules (`api_entsoe`,
`api_elia`) each hand-roll the *same* HTTP concerns: build URL, `requests.get(...)` with a fresh connection,
a 5-branch `except` ladder (`Timeout`/`ConnectionError`/`HTTPError`/`RequestException`/`JSONDecodeError`),
and an ad-hoc availability flag. That's ~40 lines of near-identical boilerplate per method, repeated ~12
times across the codebase.

**Better solution — one `HttpClient` base.** Introduce a small base class:

```python
class HttpDevice:
    def __init__(self, base_url, token=None, timeout=10, verify=True):
        self._session = requests.Session()  # connection reuse (M5)
        retry = Retry(total=2, backoff_factor=0.3,
                      status_forcelist=(500, 502, 503, 504))
        self._session.mount("http://", HTTPAdapter(max_retries=retry))
        self._session.mount("https://", HTTPAdapter(max_retries=retry))
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"
        self._session.verify = verify
        self.timeout = timeout

    def _get(self, path) -> Optional[dict]:
        try:
            r = self._session.get(self.base_url + path, timeout=self.timeout)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as e:
            logger.warning("%s GET %s failed: %s", type(self).__name__, path, e)
            return None
```

Each concrete client shrinks to declarative method bodies (`return self._get("/api/v1/data")`).
*Advantages: ~300 fewer lines, one place for timeout/retry/TLS policy, persistent connections cut the
per-15-s TCP/TLS handshake cost, and the `try/except` ladder can no longer drift between clients.* This also
makes them trivially mockable in tests (swap `_session`).

### 15.2 `P1MeterHomewizardClient` — mixed responsibilities (the biggest design smell here)

The "P1 meter" object is three things at once: (a) the grid meter reader, (b) the **battery group
controller** (`set_battery_mode` writes `mode`/`permissions` to `/api/batteries`), and (c) the battery
*summary* reader. The mediator therefore calls `self.p1_client.set_battery_mode(...)` to command batteries —
a confusing coupling that the code comments even apologize for ("Blocked by evcc" dead lines).

**Better solution — split by capability, not by device.** Three roles:

- `GridMeter` (read active power, monthly peak),
- `BatteryGateway` (the P1's `/api/batteries` group command surface — the thing that actually switches
  modes), and
- `Battery` per-unit reader (already `BatteryHomeWizard`).

The mediator then depends on a `BatteryController` interface, not on "the P1 client." *Advantage: the
dependency graph matches the physical reality, the mediator no longer reaches through the meter to steer
batteries, and you can unit-test battery control without a meter.* Also fixes the `montly_*` typo handling
by quarantining it in one mapping layer.

Two concrete bugs to fix while here: `_initialize_connection` uses `print(...)` instead of `logger` on the
failure path (P1 client, ~line 47), and `refresh_batteries_data` issues `verify=False` HTTPS without
suppressing the resulting `urllib3` warning spam.

### 15.3 `BatteryHomeWizard` — constructor lies about its token

`__init__(... token="")` then `_initialize_connection` immediately does
`self.token = os.getenv(f"BATTERY_{self.name.upper()}")`, silently discarding the passed argument and
coupling the class to both environment-variable naming *and* `os`. *Better:* resolve the token in the
composition root (`initialize_external_clients`) and inject it; the class should not read `os.getenv`. This
restores testability and makes the dependency explicit (currently a battery named `"Garage Two"` would look
up `BATTERY_GARAGE TWO`, an env var that can't exist — a latent footgun).

### 15.4 Controllers — `InverterSmaModbusClient` and `EvccApiClient`

`InverterSmaModbusClient` is the **best-written** object in the codebase: explicit register map, reconnect
on `ModbusIOException`, Grid-Guard session management, and a hardware rate-limiter. Keep its shape. Tactical
improvements:

- The connection is **not thread-safe**; the DB-logging poller (15 min) and the mediator-update poller
  (15 s) can both call `get_live_data`/`set_active_power_limit` concurrently on one socket. pymodbus
  transactions are not reentrant — add a `threading.Lock` around register I/O, or (better) confine all
  inverter I/O to a single owner (see §19, the "one actor per device" idea).
- `get_live_data` calls `get_operational_status()` (one round-trip) and then four more reads; the status
  read is redundant with the read inside `_read_registers`. Batch the contiguous registers where the SMA
  map allows, cutting round-trips.

`EvccApiClient` is solid REST. One real bug (already in §13 #7): `set_min_current` logs an out-of-range
warning but **falls through and sends the command anyway** (missing `return False`). Also `is_available` is
toggled from many places; with the §15.1 base, availability becomes "did the last call return None,"
removing the scattered flag writes.

### 15.5 The market API — `MarketContext` is fine but redundant with the plan

`market_prices.MarketContext` re-derives the "current interval" buy/sell every tick by scanning
`electricity_prices_today`. It works, but the *same* price information is already merged into
`prediction_plan_df` (`add_prices_to_plan`). Two sources of truth for "what is the price right now" invite
drift. **Better:** make the plan the single price oracle — the mediator reads the current row of
`prediction_plan_df` (which it already loads for battery decisions) and gets price, block flags, and SOC
target from one consistent place. `MarketContext` then collapses to a thin accessor or disappears.
*Advantage: one source of truth, fewer scans, removes the `is_fixed_contract` special-case duplication.*

---

## 16. The front-end: why it reverts, and what it should be

### 16.1 Root cause of the "switches back before confirming" behavior

The bug is structural, not cosmetic. The inputs are bound **directly** to the polled server state, e.g.
`<select :value="state.battery_manual_mode" @change="updateSetting('battery_manual_mode', $event)">`, and:

1. `fetchState()` runs on a **10 s `setInterval`** and overwrites `state.value` wholesale.
2. `updateSetting()` POSTs, and on success **immediately calls `fetchState()` again**.

So a user change races two reverting forces. Worse, many UI controls drive an *intent* (`evcc_manual_state`,
`app_mediator_goal`) whose visible effect only appears **after the mediator's next 5–15 s tick** writes the
*applied* state back. Between the POST and that tick, `/api/v1/state` legitimately still returns the old
applied value — so the immediate `fetchState()` paints the old value, looking like the change was rejected.
There is **no separation between "what I requested" and "what the system has applied."**

### 16.2 The minimal fix (keep Vue, keep polling)

Track per-field pending state and stop clobbering edited fields:

- On change: set a local `pending[key] = value`, render `pending[key] ?? state[key]` (optimistic UI).
- On POST success: keep `pending[key]` until a *subsequent* poll shows the server has converged
  (`state[key] === pending[key]`), then clear it. On failure: clear `pending[key]` and toast the error.
- Don't call `fetchState()` right after POST; let the regular poll reconcile, or poll once after a short
  delay (one mediator tick), not immediately.

This alone removes the flicker. *Advantage: correct perceived behavior with ~20 lines, no backend change.*

### 16.3 The better solution — push instead of poll, request/applied split

For a single-user control panel, **polling `/state` every 10 s is the wrong transport**: it's both too slow
(10 s lag to see a change land) and wasteful (full state re-serialized constantly, even idle). Replace it
with **server push**:

- **Server-Sent Events (SSE)** is the right fit here: one-directional server→client, trivial over HTTP,
  survives proxies, auto-reconnects, no extra deps. Flask can stream `text/event-stream`; emit a state diff
  whenever `GLOBAL_APP_STATE` changes (the `AppState.set` is the natural hook). The browser updates in
  real time; writes still go over the existing `POST /settings/update`.
- Model **two state channels**: `requested` (user intent, echoed back instantly by the POST response) and
  `applied` (what the mediator achieved). The UI shows requested with a "pending…" badge until applied
  catches up. This is exactly the information the system already has internally; it just isn't exposed.

If you want a sturdier app: **FastAPI + a small reactive front-end** (still single-file is fine). FastAPI
gives you native WebSocket/SSE, automatic request validation (replacing the hand-rolled `TYPE_MAP` in
`settings/update`), and OpenAPI docs for free. The front-end can stay CDN-Vue but should adopt a single
reactive **store** with the requested/applied split rather than binding controls straight to the poll.

### 16.4 Accessed from outside the LAN — security note

It's one user on a NAS but reachable from the internet, and the API currently has **no authentication** and
mutates hardware (`POST /settings/update` can change inverter limits, EV charging, battery mode). Anyone who
reaches the port controls the house's energy hardware. Feasible, proportionate hardening:

- Put it behind the NAS reverse proxy (most NAS units ship nginx/Traefik) with **HTTPS + a single auth
  layer** (Basic auth, or better, an authenticated tunnel like Tailscale/WireGuard so it's never exposed
  publicly at all). A WireGuard/Tailscale tunnel is the lowest-effort, highest-security option for a
  one-user app and removes the need to expose any port.
- Add a shared-secret token check in the Flask `before_request` for the write endpoints at minimum.
- Bind Flask to `127.0.0.1` and let only the proxy talk to it.

---

## 17. The 20-minute daily summary that stalls polling

### 17.1 Why it blocks (it's not just "it's big")

`task_send_daily_energy_summary_email` runs as an APScheduler job inside the **ThreadPoolExecutor**. Even
with 10 worker threads, the job is **CPU-bound Python** (RandomForest training, pandas `iterrows` cost loops,
matplotlib at 300 DPI), and CPU-bound Python holds the **GIL**. The 15-s pollers are scheduled but starve:
they get almost no CPU, their HTTP calls time out, `mediator_logic` misfires its 10-s grace, and—because
`max_instances` defaults to 3 and `coalesce` is on—missed runs pile up and then collapse. So the whole
control loop effectively pauses for ~20 minutes once a day. Threads cannot fix a GIL-bound workload.

### 17.2 Where the 20 minutes actually goes (all addressable)

1. **`calculate_total_costs_for_period` / `calculate_battery_saving_for_period` recompute the entire year
   from raw rows every day**, and call `initialize_tariff_manager(app_config)` **once per day in the loop**
   (re-parsing `tariffs.yaml` ~365×), plus `get_avg_monthly_peak_w_last_12m` runs 12 sub-queries *per day*.
   This is O(days²) work for a report that only gained one new day.
2. **`price_predictor.train_model` retrains from `historic_data.start_date` to yesterday** — a growing
   window, from scratch, every day (see §18).
3. **Two 300-DPI matplotlib renders.**

### 17.3 Better solution — precompute, cache, and isolate

**(a) Make daily aggregates incremental.** Add a `daily_costs` table keyed by date holding the per-day
imported/exported kWh, fixed/dynamic cost, capacity cost, and battery savings. Compute **only yesterday**
each night and `INSERT`; month/year/all-time totals become a `SUM(...)` query over that table. This turns
the dominant O(days²) loop into O(1) per day. *Advantage: minutes → milliseconds, and the email no longer
re-reads months of 15-min rows.*

**(b) Pass the existing `TariffManager` in** instead of constructing one per day (M3). One YAML parse
instead of 365.

**(c) Retrain the price model weekly, on a bounded window, and persist it** (see §18) rather than daily
from full history.

**(d) Run the heavy job in a separate process.** Whatever remains (model, plots) should not share the
control loop's interpreter. Options, lowest-effort first:

- Give APScheduler a `ProcessPoolExecutor` and route only this job to it (`executor='processpool'`), so the
  GIL-bound work runs in a child process and the pollers keep the CPU/GIL in the main process.
- Or split reporting into its own tiny scheduled script/cron entry that talks to the same DB read-only.

*Advantage: the control loop never pauses; reporting CPU is isolated; the report itself drops from ~20 min
to seconds once (a) and (c) are done, making (d) almost moot but still correct for the plots.*

**(e) Guard rails regardless:** set this job's `max_instances=1` and a generous `misfire_grace_time`, and
keep `mediator_logic` on its own executor so a heavy job can never delay control.

---

## 18. Will the predictor eventually fail? Two different answers

There are two predictors and they scale **oppositely**.

### 18.1 `ConsumptionPredictor` — bounded, will not degrade

`generate_consumption_forecast` uses exactly **four historical days** (same day last year + previous 3),
regardless of how much history exists. Its cost is constant over the app's lifetime. The only growth concern
is that `_get_historical_periods` reaches back exactly one year, so it needs ≥13 months of data to use the
"last year" sample; before that it silently drops that sample. *No action needed for scale*; optionally
weight recent days more, or fall back gracefully when last-year data is missing (it already does).

### 18.2 `EnergyPricePredictor` — unbounded, will get slow (the real answer to your question)

`train_model(train_start=historic_data.start_date, train_end=yesterday)` trains a 100-tree RandomForest on a
**monotonically growing** window: every day it loads *all* DA prices + *all* Elia forecasts since the fixed
start date, builds a 15-min DataFrame over the whole span, and fits from scratch. It won't "crash," but:
training time and RAM grow **linearly with calendar age** — at 15-min resolution that's ~35 k rows/year, so
after a few years you're fitting a forest on >100 k rows daily inside the email job. That is a meaningful
slice of the 20 minutes today and only grows.

**Faster, equivalent-or-better solution:**

- **Bound the training window** to what actually informs day-ahead price shape: a trailing **8–12 months**
  captures full seasonality; older data adds cost without accuracy. This makes training time *constant*.
- **Persist the model** (`joblib.dump`) and **retrain weekly**, not on every email. Daily prediction then
  just loads the fitted model and calls `.predict` (milliseconds). A RandomForest's day-to-day accuracy
  does not change materially from one extra day of data.
- **Down-sample features to hourly** for training if 15-min adds no signal (price drivers — day-of-week,
  solar/wind factor, grid load — are smooth), cutting rows 4×.
- The model is only used for the **forecast email**, never for live control, so a weekly cadence and a
  bounded window cost nothing operationally.

*Advantage: constant (not growing) training time, lower RAM, and the email's biggest single cost removed —
with statistically indistinguishable predictions.*

---

## 19. If I were starting from zero

The current design's two root choices — **one global mutable dict** as the bus, and **one thread pool**
running both control and heavy analytics — are the source of most issues above (races, GIL stalls, snapshot
inconsistency, testability). A greenfield design would keep the (good) domain logic and change the spine.

### 19.1 Shape

- **Async core, one event loop (`asyncio`).** Device polling is I/O-bound (HTTP, Modbus); `asyncio` handles
  hundreds of concurrent waits on a single thread with no GIL contention and no lock soup. Use
  `httpx.AsyncClient` (connection-pooled) for HTTP devices and run Modbus in a thread executor (or
  `pymodbus`'s async client). Polling cadence becomes `async` tasks, not thread-pool jobs.
- **Heavy/CPU work in a process pool.** Predictor training, cost aggregation, and plotting go to
  `ProcessPoolExecutor` (or a separate worker process / cron). The control loop is *never* in the same
  interpreter as sklearn/matplotlib. This is the structural fix for §17.
- **A typed, observable state store instead of a raw dict.** A small `Store` with `get/set`, an
  `RLock` (or, in async, single-loop confinement so no lock is needed), **change events**, and a clear split
  between `Measured` (sensor reads), `Requested` (user intent), and `Applied` (controller outcomes). The
  SSE/WebSocket push (front-end §16) and the persistence-on-change both subscribe to its change events —
  removing the synchronous DB write currently buried in `AppState.set`.
- **One owner ("actor") per physical device.** Each device has a single task that owns its socket/session
  and serializes all reads/writes — no shared pymodbus socket across threads, no scattered `is_available`
  flags. The mediator sends *commands* to these owners and never touches transports.
- **The mediator as a pure function.** `decide(snapshot) -> Commands`. Given an immutable snapshot of state
  it returns desired controller commands; a separate executor applies them. Pure decision logic is
  unit-testable without mocks and free of the mid-tick races in §13 #10. The current `SystemMediator`
  already *almost* separates "determine" from "apply"; this formalizes it.
- **API: FastAPI.** Pydantic request models replace the hand-rolled `TYPE_MAP`/range checks; native
  WebSocket/SSE replaces polling; OpenAPI for free; trivially add the single-token auth (§16.4).
- **Persistence: keep SQLite, but WAL + per-task connections + a thin repository per aggregate** (prices,
  meter, battery, settings, logs) instead of one 1500-line handler. Consider a dedicated `daily_costs`
  rollup table (§17) and, if time-series volume ever matters, the data is already shaped for a column store
  later — but SQLite is the right call for one house.
- **Front-end: a single reactive store with requested/applied split**, fed by SSE. Still can be one HTML
  file with CDN Vue; the discipline is in the store, not the build tooling.

### 19.2 What stays

The *domain* is good and would be ported nearly verbatim: the tariff stack in `cost_calculator`, the
battery physics + rule-based optimizer (after vectorization, §H1), the ENTSO-E/Elia parsing, the daylight/
grid-guard/peak-shaving heuristics. The rewrite is about the **spine** (state, concurrency, transport),
not the energy math.

### 19.3 Honest cost/benefit

A full async rewrite is weeks of work for a one-user app and is **not** required. The greenfield picture is
useful mainly as the *target* the incremental steps in §20 move toward. You get ~80% of the benefit from
four feasible changes (state lock + snapshot, process-pool the heavy job, vectorize the optimizer, SSE +
requested/applied) without adopting `asyncio` at all.

---

## 20. Feasible, high-leverage roadmap (readability + efficiency)

Ordered so each step is independently shippable and low-risk; behavior preserved throughout.

1. **Stop the daily-summary stall (highest user-visible win).**
   (a) Add the `daily_costs` rollup table + compute-yesterday-only; (b) pass the shared `TariffManager`;
   (c) bound + persist + weekly-retrain the price model; (d) route the report job to a `ProcessPoolExecutor`
   and set `max_instances=1`. → 20 min GIL stall → seconds, control loop never pauses. (§17, §18)

2. **Fix the front-end reactivity.** Add `pending`/optimistic state + requested-vs-applied display, stop the
   post-write `fetchState()`. Then upgrade transport to **SSE** off `AppState.set`. (§16)

3. **Snapshot + lock the state.** Take one immutable snapshot at the top of the mediator tick; wrap
   `AppState` access in an `RLock`; move the persisted-key DB write off the setter onto the change event.
   → removes races (§13 #10, H2) and a synchronous disk write per UI toggle.

4. **Vectorize the battery optimizer.** Replace `calculate_impact`'s `iterrows()` SOC scan with a numpy
   array scan and compute trial costs on the affected slice. → the per-15-min CPU spike drops sharply. (§H1)

5. **Extract the `HttpDevice` base + `requests.Session` reuse**; split the P1 "meter vs battery-gateway"
   responsibilities. → ~300 fewer lines, persistent connections, honest dependencies. (§15.1–15.3)

6. **Carve up `DatabaseHandler`** into per-aggregate repositories with a shared `_execute/_query` helper and
   a single settings codec; move schema to `schema.sql`; switch to WAL + per-thread connections. → the
   "database is locked" retries disappear and the file becomes navigable. (§H3, §H5)

7. **Decompose `SystemMediator`** into `PeakShaver` + per-device controller objects with a pure
   `decide()`/`apply()` split. → testable units, readable orchestration. (§H4)

8. **Cleanup pass:** delete the deprecated Streamlit UI and unused deps, the tracked 14 MB BFG jar and build
   artifacts; UTF-8 the requirements; add `ruff`; centralize timezone helpers; fix the §13 correctness bugs.

Steps 1–4 deliver essentially all the efficiency and responsiveness gains; 5–8 are the readability/
maintainability investment that makes the next year of changes cheap.

---

## 21. Deep insight: `GLOBAL_APP_STATE` — its goal, and the professional way to meet it

This deserves its own treatment because it is *the* central design decision of the app, and almost every
issue elsewhere in this document traces back to it.

### 21.1 What is it actually trying to be? (Five goals in one object)

`AppState` is not one thing — it is **five responsibilities fused into a single stringly-typed dict**:

1. **An inter-task message bus.** Poll jobs write sensor data; the mediator and predictor read it. There is
   no queue/event system, so the dict *is* the channel.
2. **A latest-value cache.** It holds the most recent meter/inverter/EVCC/battery reading so any consumer
   can read "now" without re-polling.
3. **A user-settings store with selective persistence.** `persisted_keys` are write-through to
   `app_settings` and reloaded at boot — so it doubles as a config/preferences repository.
4. **The front-end's read model.** `/api/v1/state` is literally `get_all()` serialized — the dict is the API
   contract.
5. **A system-status signal.** The logging handler writes `app_state` (WARNING/ALARM) into it, so it's also
   the health channel.

Recognizing these five is the whole point: **a professional design keeps the five goals but stops conflating
them into one untyped container.** Goal (4) — "readable from the front-end" — is genuinely good and worth
preserving; it does *not* require the thing being read to be a raw dict.

### 21.2 Is it object-oriented? (Honest answer: object-*based*, not object-*oriented*)

It is a class with methods, so it is nominally OO — but the encapsulation is *cosmetic*. True OO models a
domain as objects that **own data + behavior and enforce invariants**. `AppState` does the opposite:

- **Stringly-typed keys.** `get("evcc_manual_limit")` — a typo returns `None`; `set("evcc_manaul_limit", x)`
  logs a warning and is **silently dropped**. No compiler, IDE, or refactor tool can help. This is the
  classic *primitive obsession* + *stringly-typed* anti-pattern.
- **No invariants.** Nothing guarantees `evcc_manual_limit ∈ [6,32]` (that check lives far away in
  `api_server`), or that `prediction_plan_df` stays consistent with `prediction_plan`, or that
  `average_grid_import_watts` has the expected window keys. Any thread can set any key to any shape.
- **No type identity.** A value may be a `dict`, a frozen dataclass, a `deque`, an `Enum`, or `None`, and the
  reader must *know*. Hence the defensive `GLOBAL_APP_STATE.get('p1_meter_data', {}).get('active_power_w', 0)`
  scattered everywhere — primitive access with no contract.
- **Behavior lives elsewhere.** The data is here; the logic that interprets it is in the mediator, the API,
  the processors. That's the definition of an **anemic global**, the opposite of OO cohesion.
- **The abstraction already leaks.** `prediction_plan_df` is special-cased *out* of the dict into an
  attribute (because a DataFrame doesn't fit the JSON-ish dict model) — a visible crack showing the single
  container is the wrong shape.

So: it's a **service-locator / God-object singleton wrapping a dict**. "Object-based global state," not OO
domain modeling. The `get`/`set` wrapper gives the *appearance* of encapsulation while enforcing none of the
guarantees encapsulation exists to provide.

### 21.3 Why this radiates into the rest of the system

Almost every other finding is a symptom of this one design:

- The hand-rolled JSON walk in `api_server` (M1) exists **because there is no typed schema to serialize** —
  if the slices were dataclasses/pydantic models, serialization would be `.model_dump()`.
- The mid-tick inconsistency (§13 #10) and missing lock (H2) exist **because there is no snapshot boundary or
  ownership** — the store hands out live references.
- The synchronous DB write on a UI toggle (§17/H2) exists **because persistence is buried inside the generic
  `set()`** instead of owned by a settings service.
- The front-end revert bug (§16) exists **because the store has no "requested vs applied" distinction** — one
  flat namespace can't represent "the user asked for X but the mediator still shows Y."
- The `MarketContext` duplication (§15.5) and defensive `.get().get()` nesting everywhere are downstream of
  values having no typed home.

### 21.4 The professional design: typed slices behind one observable facade

Keep the one advantage (a single readable runtime snapshot); fix everything else. The goal becomes:
**a single, thread-safe, observable source of runtime truth composed of typed domain slices, with the
five responsibilities separated.**

**(a) Model each slice as a typed object** (dataclass or pydantic `BaseModel`), grouped by domain and by the
*measured / requested / applied* axis from §19:

```python
@dataclass(frozen=True)
class MeterReading:  # measured
    ts: datetime;
    active_power_w: float;
    monthly_peak_w: float;
    ...


@dataclass(frozen=True)
class InverterApplied:  # applied
    ts: datetime;
    pv_power_w: float;
    limit_w: int;
    status: InverterStatus


class Settings(BaseModel):  # requested / persisted, validated
    operating_mode: OperatingMode = OperatingMode.MODE_MANUAL
    evcc_manual_limit: conint(ge=6, le=32) = 6  # invariant lives WITH the data
    ...
```

Validation (the EVCC 6–32, inverter 0–7000 ranges) now lives **with the field**, not in the API handler.

**(b) A `Store` facade** that holds the slices, guards them with an `RLock` (or single-loop confinement in an
async core), exposes **typed accessors** (`store.meter`, `store.settings`, `store.inverter`) instead of
`get("magic_string")`, and emits **change events**:

```python
class Store:
    def snapshot(self) -> StateSnapshot: ...  # immutable, for mediator tick & API

    def update_meter(self, r: MeterReading): ...  # typed writers, fire on_change

    def subscribe(self, fn): ...  # observers: SSE push + persistence
```

`snapshot()` gives the mediator one consistent view per tick (kills §13 #10) and gives the API a typed object
to serialize (kills M1) — the front-end keeps its single readable state, now schema-stable.

**(c) Split the five responsibilities back out:**

- **Bus/cache** → the typed slices + `subscribe`.
- **Settings + persistence** → a `SettingsService` that owns write-through to `app_settings`, debounced/off
  the hot path (fixes the synchronous-write smell). The generic `set()` no longer touches the DB.
- **Front-end read model** → `snapshot().to_json()`; and expose `requested` vs `applied` so the UI can show
  "pending…" (this is the data §16 needs).
- **System status** → a dedicated `HealthState` updated by the log handler, not a string key in the same bag
  as sensor data (removes the hidden logging↔domain side channel).

**(d) Observers wire the rest of the app for free:** the SSE stream (§16.3) and the persistence service both
just `subscribe()` to change events — no polling of the dict, no DB write inside `set`.

### 21.5 Verdict and feasible path

The dict-with-`get`/`set` is the pragmatic choice that got the app working and made the front-end trivial —
but it is the **single biggest source of fragility**: no type safety, no invariants, no thread safety, no
ownership, and it forces defensive code into every consumer. It is "object-based," not the cohesive,
invariant-enforcing OO that a control system of this complexity warrants.

You do **not** need the full async rewrite (§19) to fix it. A feasible, incremental migration:

1. Introduce the typed slice models and a `Store` facade **alongside** the existing dict; have `Store` write
   both during transition.
2. Migrate readers from `GLOBAL_APP_STATE.get("x")` to `store.x` one domain at a time (start with the
   mediator, which benefits most from `snapshot()`).
3. Move settings persistence into `SettingsService`; move status into `HealthState`.
4. Switch the API to serialize `snapshot()`; add the `subscribe()`-based SSE.
5. Delete the raw dict once no reader references it.

Each step compiles and ships on its own, behavior is preserved, and at the end the "readable global state"
advantage remains — but now it is typed, validated, thread-safe, observable, and genuinely object-oriented.
This refactor underpins and de-risks §16 (front-end), H2/§13 #10 (races), M1 (serialization), and §17
(persistence off the hot path); doing it early makes those four cheaper.