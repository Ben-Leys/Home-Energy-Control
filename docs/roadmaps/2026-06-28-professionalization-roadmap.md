# Home Energy Control Professionalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:
> executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Branching instruction:** Create a new branch for each individual improvement or suggested batch before making code
> changes, so each improvement can be tested, reviewed, and committed separately. Keep `master` as the currently
> deployed working branch, `master-working-v2` as the safety copy from before this professionalization update, and
> `codex-improvements` as the planning branch that contains agent instructions, Codex setup, and this roadmap.

**Goal:** Evolve Home Energy Control into a reliable, maintainable, secure, lightweight NAS-hosted application while
preserving the currently effective mediator and battery optimizer behavior.

**Architecture:** Keep the app lightweight and single-home focused. Stabilize the current
Python/Flask/APScheduler/SQLite design first, then introduce safer boundaries around database access, state, dashboard
updates, lifecycle control, and persistent alerts. Avoid rewrites unless measurement shows the current component cannot
meet reliability or NAS resource constraints.

**Tech Stack:** Python, Flask, APScheduler, SQLite, unittest, Vue dashboard served by Flask,
pandas/numpy/matplotlib/scikit-learn for forecasting and reporting, HomeWizard/EVCC/SMA/ENTSO-E/Elia integrations.

---

## Scope And Constraints

- Deployment target: locally hosted on a NAS.
- Users: mostly the owner, sometimes household members.
- Network exposure: LAN and VPN, not intended for public internet.
- Home scope: one home, not a multi-tenant product.
- Sensitive data: location, email, API keys, energy history.
- Availability: recover as soon as possible; several hours of avoidable downtime is acceptable, but the app should
  recover cleanly after failures.
- Highest priorities: reliability, maintainability, UI, security, then other improvements.
- Critical preservation rule: `hec/logic_engine/system_mediator.py` and the battery optimizer currently make good
  decisions. Changes there require a clear bug or necessary improvement, characterization tests, careful inspection, and
  user approval.

## Decision Rationale

This roadmap is based on local repository inspection plus the user constraints above.

1. The mediator and battery optimizer are valuable and currently trusted, so the first professionalization step is a
   safety net around them rather than refactoring them.
2. The NAS constraint rules out heavy operational patterns as the default. SQLite, Flask, and APScheduler can remain
   appropriate if DB access, scheduling, and resource limits are made disciplined.
3. The daily summary likely blocks the system because it retrains price forecasting during email generation (
   `hec/reporting/daily_summary.py:324-332`), uses a `RandomForestRegressor` with `n_jobs=-1` (
   `hec/logic_engine/price_predictor.py:19`), queries historical data, and creates high-DPI matplotlib plots (
   `hec/reporting/plot_generator.py:221`, `hec/reporting/plot_generator.py:379`). This should be measured first, then
   moved to cached/incremental work.
4. Dashboard setting updates are generic state mutations (`hec/core/api_server.py:91-164`) followed by a full state
   refresh in the browser (`hec/core/vue_dashboard.html:637-648`). If another task or stale persisted state overwrites
   the key, the browser naturally flips back. The API should return the canonical updated state immediately and the
   frontend should track pending edits.
5. The shutdown button sets `reboot_request` (`hec/core/vue_dashboard.html:337-338`), and the mediator task kills the
   process with `SIGINT` (`hec/logic_engine/scheduled_tasks.py:563-569`). That is not a reliable NAS restart mechanism.
   A supervised process restart is most reliable; a same-process soft restart is possible but requires a controllable
   API server and runtime lifecycle.
6. The current warning/error system treats log records as application status (`hec/core/app_logging.py:17-21`). Logs are
   events; operational incidents need persistence, acknowledgement, and resolution.
7. Since this is LAN/VPN hosted, security should be practical and lightweight: local password, long-lived signed cookie,
   command allowlists, CSRF/origin checks, and protected logs/settings.

## Target Architecture

The target is still one process by default:

```text
config/env -> ApplicationRuntime
           -> SQLite repository layer with safe transactions
           -> APScheduler jobs
           -> hardware/API clients
           -> thread-safe AppState and persistent settings
           -> mediator and optimizer
           -> Flask API/dashboard
           -> daily summary/reporting
           -> persistent incidents and notifications
```

The app should support graceful shutdown and restart boundaries:

- Stop accepting risky API commands.
- Stop or pause scheduler jobs.
- Finish or cancel non-critical report jobs.
- Close DB connections.
- Reload `.env`, `config.yaml`, tariffs, clients, settings, and scheduler.
- Resume in one active runtime.

## Recommended Phased Roadmap

### Phase 0: Baseline, Measurement, And Safety Net

**Purpose:** Make future changes safe without altering trusted behavior.

**Dependencies:** None.

**Work:**

- Repair or re-baseline the current failing `unittest` suite.
- Add characterization tests for `SystemMediator` decision cases that are already considered correct.
- Add characterization tests for the current battery prediction output shape and critical decision signals, without
  changing optimizer behavior.
- Add a timing profiler around daily summary generation, split into data loading, price model training, prediction,
  plotting, cost calculation, and SMTP.
- Add a DB lock reproduction test or script that simulates concurrent scheduler/API writes.

**Files likely touched:**

- `hec/tests/test_system_mediator.py`
- new focused tests under `hec/tests/`
- possibly a non-production profiling helper under `hec/tests/` or `tools/`

**Acceptance criteria:**

- `python -m unittest discover -s hec/tests` passes.
- Daily summary timing report identifies the top two expensive steps on the NAS.
- Mediator characterization tests are reviewed by the user before mediator-adjacent changes.

**Risks:**

- Existing tests may represent old behavior. When they conflict with the working mediator, preserve the live intended
  behavior and update tests explicitly.

**Suggested batches:**

- PR 0.1: Fix or re-baseline current failing tests.
- PR 0.2: Add mediator and optimizer characterization tests.
- PR 0.3: Add daily summary profiling and DB lock reproduction helpers.

### Phase 1: Operational Safety And Lightweight Authentication

**Purpose:** Prevent accidental or unauthorized control changes on LAN/VPN.

**Dependencies:** Phase 0 test baseline preferred.

**Work:**

- Add a local password login with a long-lived signed cookie.
- Protect `/api/v1/state`, `/api/v1/logs`, and `/api/v1/settings/update`.
- Add CSRF or same-origin protection for state-changing POST requests.
- Replace generic "any AppState key" mutation with an allowlist of settings and commands.
- Add command audit logging for setting changes, summary requests, restart requests, and alert acknowledgements.
- Until restart is redesigned, make the dashboard restart/shutdown action explicit and guarded by confirmation.

**Files likely touched:**

- `hec/core/api_server.py`
- `hec/core/vue_dashboard.html`
- `hec/core/app_state.py`
- `hec/database_ops/db_handler.py`
- `hec/config.yaml.example` if added

**Acceptance criteria:**

- Unauthenticated requests cannot read logs or change settings.
- A browser login persists across restarts using a configured cookie secret.
- Allowed settings update successfully; unknown keys are rejected.
- Security remains lightweight and works over LAN and VPN without external identity services.

**Risks:**

- An "endless" cookie can become a long-lived credential. Mitigate with a strong cookie secret, manual logout, and local
  password rotation.

**Suggested batches:**

- PR 1.1: Add auth config, password verification, signed cookie, and login/logout.
- PR 1.2: Add settings/command allowlist and audit entries.
- PR 1.3: Add frontend login state and guarded destructive actions.

### Phase 2: SQLite Reliability And Professional DB Handling

**Purpose:** Fix DB locks, transactions, rollback behavior, and schema evolution while staying lightweight.

**Dependencies:** Phase 0 DB lock reproduction helps verify improvement.

**Recommendation:** Keep SQLite first. It is appropriate for one home on a NAS if writes are serialized or short, WAL
mode is enabled, transactions are explicit, and expensive report reads are isolated. Consider Postgres only if measured
lock contention remains after this phase.

**SQLite pros:**

- Lightweight.
- No extra service on NAS.
- Easy backup as one file.
- Good fit for one home and moderate write volume.

**SQLite cons:**

- One writer at a time.
- Long reads can block writes without careful connection/WAL handling.
- Needs disciplined transaction boundaries.

**Postgres pros:**

- Better concurrent read/write behavior.
- Stronger operational tools and constraints.

**Postgres cons:**

- Heavier NAS footprint.
- More maintenance, backup, and deployment complexity.
- Unnecessary if SQLite lock issues are caused by current connection handling.

**Work:**

- Replace the single shared SQLite connection (`hec/database_ops/db_handler.py:33-47`) with safe connection handling.
- Enable WAL, `busy_timeout`, foreign keys, and explicit transaction context helpers.
- Ensure every write commits or rolls back predictably.
- Add schema versioning and migrations.
- Add indexes for common report, prediction, dashboard, and retention queries.
- Implement 3-year history retention policy carefully, preserving data needed for calculations.
- Separate log retention from energy-history retention.

**Files likely touched:**

- `hec/database_ops/db_handler.py`
- new migration module or scripts under `hec/database_ops/`
- `hec/core/app_initializer.py`
- `hec/tests/`

**Acceptance criteria:**

- Concurrent API/scheduler simulation no longer produces DB lock failures.
- Failed writes roll back and do not leave partial records.
- Migration can run repeatedly without damaging an existing DB.
- Backup and restore procedure is documented and tested on a copy.

**Risks:**

- Timestamp and schema inconsistencies can surface during migration. Use a copied DB and migration dry run first.

**Suggested batches:**

- PR 2.1: Add DB transaction helper, WAL/busy timeout, and connection policy tests.
- PR 2.2: Migrate one write-heavy path to the helper and prove lock behavior improves.
- PR 2.3: Add schema version table and first no-op migration.
- PR 2.4: Add retention and backup/restore documentation.

### Phase 3: State, Settings, And Interactive Dashboard Consistency

**Purpose:** Make the dashboard live, predictable, and lightweight.

**Dependencies:** Phase 1 for auth; Phase 2 preferred for persistence reliability.

**Recommendation:** Start with state versioning plus short polling, not WebSockets. For one home, a 2-5 second poll with
version/ETag and small payloads is simpler and lighter. Add Server-Sent Events only if measurement shows polling is not
responsive enough.

**Short polling pros:**

- Simple.
- Works with Flask and VPN.
- Easy to test and recover after reconnect.

**Short polling cons:**

- Slight delay for sensor changes.
- Some repeated requests.

**Server-Sent Events pros:**

- More immediate updates with one browser connection.
- Lighter than frequent full polling if several dashboards are open.

**Server-Sent Events cons:**

- Requires more lifecycle and reverse-proxy care.
- More moving parts for little benefit in a one-home app.

**Work:**

- Add a thread-safe `AppState` lock and monotonically increasing `state_version`.
- Return canonical state and `state_version` from setting update calls.
- Frontend keeps a pending local edit until backend confirms or rejects it.
- Poll only changed state when possible.
- Split command state from observed hardware state where it matters.
- Fix the current key mismatch where `task_run_battery_predictor` reads `average_production_watts` instead of
  `average_solar_production_watts`.

**Files likely touched:**

- `hec/core/app_state.py`
- `hec/core/api_server.py`
- `hec/core/vue_dashboard.html`
- `hec/logic_engine/scheduled_tasks.py`
- `hec/tests/`

**Acceptance criteria:**

- Changing a setting updates the displayed value immediately after backend confirmation.
- A polling refresh does not revert confirmed values to old values.
- Failed updates show a clear error and restore the last confirmed value.
- Sensor values stay reasonably live without heavy CPU/network usage.

**Risks:**

- Some values may currently be overwritten by mediator or polling logic intentionally. Those must be classified as
  desired state, observed state, or computed state before changing behavior.

**Suggested batches:**

- PR 3.1: Add AppState locking/versioning and tests.
- PR 3.2: Make update API return canonical state and reject non-allowlisted keys.
- PR 3.3: Update Vue pending-state behavior and polling interval.
- PR 3.4: Fix the solar average key bug with a regression test.

### Phase 4: Daily Summary And Forecast Performance

**Purpose:** Stop daily email generation from blocking NAS resources for around 20 minutes.

**Dependencies:** Phase 0 profiling. Phase 2 helps with DB read/write contention.

**Recommendation:** Keep the daily email, but make it consume cached forecast outputs. Do not train a price model inside
the email request path unless explicitly forced.

**Work:**

- Measure actual runtime of `DailySummaryGenerator.generate_and_send_summary`.
- Separate "train price predictor", "predict D+1 to D+5", "generate plots", and "send email" into independently timed
  steps.
- Persist and reuse the price model or cache predictions in `predicted_prices`.
- Train at most once per day, preferably during a low-impact window.
- Make `RandomForestRegressor` resource use configurable; default to `n_jobs=1` on NAS and consider fewer estimators
  after accuracy comparison.
- Generate the daily email from cached predictions. If predictions are missing or stale, send the summary without
  blocking for a full model train.
- Reduce plot resource use: lower DPI, smaller figure size, close figures reliably, and skip optional future plot when
  stale.
- Move manual "Daily summary" dashboard action to a non-blocking job request with progress/status.

**Files likely touched:**

- `hec/reporting/daily_summary.py`
- `hec/logic_engine/price_predictor.py`
- `hec/reporting/plot_generator.py`
- `hec/logic_engine/scheduled_tasks.py`
- `hec/database_ops/db_handler.py`
- `hec/core/app_state.py`
- `hec/tests/`

**Acceptance criteria:**

- Daily email generation completes within a defined NAS budget, suggested target: under 2 minutes for normal cached
  path.
- Price model training does not use all CPU cores by default.
- Other scheduler jobs continue to run while summary/report jobs execute.
- Manual summary request returns immediately in the UI and exposes progress.
- If forecasting fails, the daily email still sends core energy information and records a persistent incident.

**Risks:**

- Reducing model size or training frequency may affect price forecast quality. Compare old and new predictions on
  historical days before accepting.

**Suggested batches:**

- PR 4.1: Add summary timing instrumentation and tests around non-blocking request behavior.
- PR 4.2: Split training/prediction/email steps without changing output.
- PR 4.3: Add prediction cache reuse and stale-cache fallback.
- PR 4.4: Tune NAS resource limits and plot generation.
- PR 4.5: Add dashboard progress/status for report jobs.

### Phase 5: Restart And Runtime Lifecycle

**Purpose:** Replace the broken shutdown button with reliable restart behavior.

**Dependencies:** Phase 1 for auth/confirmation; Phase 2 for safe shutdown; Phase 3 for dashboard command flow.

**Recommendation:** Prefer an external NAS supervisor restart if available. It is simpler and more reliable. If the user
requires same-process restart, introduce an `ApplicationRuntime` lifecycle and replace Flask `app.run` with a
controllable server.

**External supervisor pros:**

- Most reliable way to reload code, environment variables, files, and native resources.
- Simpler app code.
- Crash recovery can be handled by the NAS.

**External supervisor cons:**

- Depends on NAS platform support.
- Usually creates a new process, which the user said is not required but may be acceptable.

**Same-process soft restart pros:**

- Can reload `.env`, `config.yaml`, tariffs, clients, and scheduler without starting a new process.
- Fits the requested "single process" model.

**Same-process soft restart cons:**

- More complex.
- Python modules and third-party client state may not fully reset unless designed carefully.
- The current Flask development server is not easy to stop cleanly from inside the app.

**Work:**

- Define restart semantics: reload config/env/files, reinitialize DB handler, clients, AppState persisted settings,
  scheduler, and API runtime.
- Extract runtime startup/shutdown from `hec/main.py` into a lifecycle object.
- Use a controllable WSGI server wrapper if same-process restart is chosen.
- Replace `os.kill(pid, signal.SIGINT)` with a restart command handled by the runtime.
- Show restart progress in the dashboard and require confirmation.

**Files likely touched:**

- `hec/main.py`
- `hec/core/api_server.py`
- `hec/core/app_initializer.py`
- `hec/logic_engine/scheduled_tasks.py`
- `hec/core/vue_dashboard.html`
- `hec/tests/`

**Acceptance criteria:**

- Restart command reloads `.env` and `config.yaml`.
- After restart, exactly one scheduler and one API server are active.
- DB connections are closed and reopened cleanly.
- A failed restart returns the app to a known degraded state with a persistent incident.

**Risks:**

- Same-process restart can leave hidden global state behind. If that happens, a supervised process restart should be
  chosen.

**Suggested batches:**

- PR 5.1: Define restart command and dashboard confirmation without executing restart.
- PR 5.2: Extract application runtime lifecycle and tests.
- PR 5.3: Implement chosen restart strategy.
- PR 5.4: Add failure handling and user-visible restart status.

### Phase 6: Persistent Errors, Acknowledgement, And Push Notifications

**Purpose:** Replace overwritten status with a real incident system.

**Dependencies:** Phase 2 migrations; Phase 3 dashboard state model.

**Work:**

- Add an `incidents` or `app_alerts` table with id, severity, source, message, first_seen, last_seen, count, status,
  acknowledged_at, acknowledged_by, resolved_at.
- Keep logs as raw events, but derive app status from active incidents and component health.
- Add dashboard views for active incidents, acknowledged incidents, and recent resolved incidents.
- Add "mark as read" or "acknowledge" action. Acknowledged incidents should not keep the top-level app status in
  warning/alarm unless they recur or remain unresolved by policy.
- Add push notification abstraction for real errors and peak consumption exceedance.
- Rate-limit repeated notifications.

**Files likely touched:**

- `hec/core/app_logging.py`
- `hec/core/app_state.py`
- `hec/database_ops/db_handler.py`
- `hec/core/api_server.py`
- `hec/core/vue_dashboard.html`
- `hec/logic_engine/system_mediator.py` only if peak-alert creation needs a narrow integration point
- `hec/tests/`

**Acceptance criteria:**

- A real error persists until acknowledged or resolved.
- A new info/warning log does not erase an existing real error.
- Acknowledging an incident changes dashboard status according to clear rules.
- Repeated identical errors update occurrence count instead of flooding the UI and phone.
- Peak consumption exceedance can send a phone notification.

**Risks:**

- Not every warning should become an incident. Define incident creation rules carefully to avoid alert fatigue.

**Suggested batches:**

- PR 6.1: Add incident schema, repository methods, and tests.
- PR 6.2: Route selected warnings/errors to incidents.
- PR 6.3: Add dashboard incident UI and acknowledgement.
- PR 6.4: Add push notification provider integration.

### Phase 7: Maintainability, Configuration, Documentation, And Supply Chain

**Purpose:** Make the project easier to maintain for years.

**Dependencies:** Can run in parallel after Phase 0, but some docs depend on decisions above.

**Work:**

- Add typed config validation and a committed redacted sample config.
- Document deployment on NAS, backup/restore, restart, troubleshooting, and device configuration.
- Add CI running unit tests.
- Add dependency lock/audit workflow.
- Vendor or pin frontend assets and add a basic Content Security Policy.
- Improve dashboard accessibility: labels, focus, keyboard support, confirmation flows.
- Add a project license and privacy/data-retention note.

**Files likely touched:**

- `README.md`
- new `docs/` files
- `requirements.txt` plus lock/audit tooling
- `.github/workflows/` if GitHub Actions is used
- `hec/core/app_initializer.py`
- `hec/core/vue_dashboard.html`

**Acceptance criteria:**

- A new developer can configure and run the app from docs without private knowledge.
- CI catches failing tests.
- Dependency updates are deliberate and auditable.
- Dashboard remains usable from keyboard and does not depend on unpinned CDN code.

**Risks:**

- Over-tooling can make the NAS deployment heavier. Keep tooling mostly development-side.

**Suggested batches:**

- PR 7.1: Add config schema and example config.
- PR 7.2: Add NAS deployment and backup docs.
- PR 7.3: Add CI and dependency audit.
- PR 7.4: Improve frontend asset handling and accessibility.

## Behavior Preservation Gates

Before changing any of these areas, add or update characterization tests and get user approval:

- `hec/logic_engine/system_mediator.py`: operating mode, mediator goals, peak handling, EV grace period,
  inverter/EVCC/battery commands.
- `hec/logic_engine/battery_predictor.py`: plan generation, charging/discharging decisions, sunrise block behavior,
  optimizer outputs.
- `hec/logic_engine/price_predictor.py`: prediction quality and training strategy, because performance tuning may change
  forecasts.
- `hec/database_ops/db_handler.py`: interval/delta calculations for P1, inverter, battery, and EVCC history.
- `hec/utils/utils.py` and price processing: DST, hourly/15-minute prices, holidays.
- `hec/reporting/daily_summary.py`: email content and cost/savings calculations.

Allowed narrow bug fixes still need tests:

- `hec/logic_engine/scheduled_tasks.py:526` uses the wrong solar average key.
- `hec/database_ops/db_handler.py:226-265` returns an inserted count that is never incremented.
- `hec/database_ops/db_handler.py:1350-1380` logs `target_date` in an exception path even though the parameter is
  `target_date_local`.

## Edge Cases To Design And Test

- DST days with 23 or 25 hours.
- ENTSO-E delayed day-ahead price publication.
- Transition between hourly and 15-minute price resolution.
- Missing Elia solar, wind, or grid-load forecasts.
- NAS sleep, restart, full disk, or read-only DB file.
- DB lock during daily summary, logging, and device polling.
- Restart requested while a hardware command is in flight.
- Restart requested while daily summary or model training is running.
- Multiple dashboard tabs changing the same setting.
- Browser session cookie surviving app restart.
- VPN user with high latency or temporary disconnect.
- Device API timeout or partial failure: P1, EVCC, SMA inverter, HomeWizard battery.
- Battery data missing for one battery but not the other.
- Negative buy or sell prices.
- Peak consumption exceedance repeated many times in one day.
- Incident acknowledgement followed by the same error recurring.
- Prediction cache stale or unavailable.
- First install with little or no historical data.
- Month and year boundary cost calculations.
- Corrupted or partially edited `config.yaml`.

## Still Needs To Be Checked

- NAS model, CPU, RAM, storage type, OS, and Python version.
- Whether the NAS can supervise/restart the app externally.
- Actual daily summary timing on the NAS by step.
- Whether the 20-minute block is mostly price training, DB reads, plotting, SMTP, or lock contention.
- Current SQLite PRAGMA settings and lock behavior on the NAS filesystem.
- Whether HomeWizard HTTPS certificates can be verified or pinned.
- Exact dashboard stale-value sequence in browser developer tools.
- Whether a reverse proxy is in front of Flask on LAN/VPN.
- Preferred push notification provider: ntfy, Pushover, Home Assistant, Telegram, email fallback, or another option.
- Whether household members need separate passwords or one shared household login.
- Whether raw 3-year history is enough for all calculations or whether aggregated older history is useful.
- License choice for the repository.
- Dependency vulnerability and license audit status.

## User Decisions Still Needed

1. SQLite hardening first or move directly to Postgres. Recommendation: harden SQLite first.
2. Dashboard update mechanism. Recommendation: state version plus 2-5 second polling first; SSE only if needed.
3. Restart strategy. Recommendation: external NAS supervisor if available; same-process soft restart only if supervisor
   restart is not acceptable.
4. Cookie lifetime. Recommendation: long-lived signed cookie with manual logout and configurable secret, not literally
   unbounded if avoidable.
5. Push notification provider. Recommendation: choose the simplest provider already used on the phone; ntfy is
   lightweight if self-hosting is acceptable.
6. Daily summary fallback. Recommendation: send the daily email without expensive future price forecast if cached
   prediction is unavailable.
7. Forecast performance tradeoff. Recommendation: compare old and tuned predictions before reducing model complexity.
8. Data retention. Recommendation: keep core energy history for 3 years, logs much shorter, predictions only as long as
   useful for reports.
9. Security strictness on VPN/LAN. Recommendation: password plus optional trusted-network allowlist.
10. Whether Streamlit dashboard should be removed after Vue reaches feature parity. Recommendation: remove or clearly
    deprecate it.

## Recommended Order

1. Phase 0: tests, characterization, and measurement.
2. Phase 2 foundation pieces that directly address DB locks.
3. Phase 4 daily summary performance, using profiling evidence.
4. Phase 3 dashboard consistency and live state.
5. Phase 6 persistent incidents and push notifications.
6. Phase 5 restart lifecycle, after state/DB/API behavior is safer.
7. Phase 1 authentication can be done earlier if desired, but reliability remains the first priority.
8. Phase 7 maintainability work continues throughout in small batches.

This order intentionally avoids touching the trusted mediator decisions until the surrounding system is safer and
measurable.
