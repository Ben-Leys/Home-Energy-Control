# Home Energy Control Second Revision Roadmap

> **For agentic workers:** Treat this as a second-pass professionalization roadmap, not as permission to rewrite trusted
> control behavior. Create a separate branch for each batch. Keep changes small, testable, and reversible. Use the
> characterization tests from the 2026-06-28 roadmap before touching mediator or optimizer decisions.

**Goal:** Make Home Energy Control more expert, lightweight, and dependable by tightening stale-data handling, command
safety, reporting correctness, database maintenance, and dashboard usability after the first professionalization phases.

**Architecture:** Keep the single-home Flask, APScheduler, SQLite, AppState, and Vue-dashboard architecture. Improve
boundaries and operational semantics inside the current design before considering larger technology changes. The app
should act only on fresh enough data, report command truthfully, avoid avoidable NAS load, and remain understandable.

**Tech Stack:** Python, Flask, APScheduler, SQLite, unittest, Vue dashboard served by Flask, pandas/numpy/matplotlib/
scikit-learn for forecasting and reporting, HomeWizard/EVCC/SMA/ENTSO-E/Elia integrations.

---

## Context

The roadmap in `docs/roadmaps/2026-06-28-professionalization-roadmap.md` has largely been implemented and is being
tested. The repository now has the first-pass foundations: config validation, API auth, AppState versioning, SQLite WAL
and transaction helpers, runtime lifecycle, persistent incidents, notifications, shared HTTP/time helpers, reporting
timing, CI/docs, and phase tests.

This second revision should not repeat that work. It should refine the implemented app into a more precise energy
controller:

- make stale inputs visible and non-commandable where unsafe;
- ensure hardware command state reflects actual command success;
- reduce accidental scheduler overlap and retry risk;
- fix report correctness before adding richer reporting;
- make database migration and maintenance safe for real existing databases;
- improve the dashboard as an operator surface rather than a raw state viewer;
- reject micro-optimizations that add risk without measurable value.

This plan is based on local code review plus five read-only subagent reviews covering core/API/dashboard, database,
scheduling/optimizer, reporting, and integrations.

## Decision Principles

1. Preserve trusted mediator and optimizer behavior unless a bug is clear and covered by characterization tests.
2. Prefer correctness and observability over speculative speedups.
3. Measure before replacing loops that run over small bounded horizons.
4. Favor explicit degraded behavior over silently using stale or missing data.
5. Keep NAS operation lightweight: no Postgres, WebSockets, OAuth stack, or same-process restart work unless current
   evidence changes.
6. Make changes in batches that can be tested independently.

## Recommended Order

1. Phase 0: current bugfix batch with clear low-risk tests.
2. Phase 1: freshness, stale-state, and command-truth semantics.
3. Phase 2: scheduler, retry, and device command safety.
4. Phase 3: reporting correctness and rollup performance.
5. Phase 4: database migration and maintenance hardening.
6. Phase 5: forecast/model cache freshness and time-zone consistency.
7. Phase 6: dashboard operator UX and local assets.
8. Phase 7: codebase hygiene.

Phases 0 and 1 should happen before further optimizer refactors. Phase 3 should happen before adding richer report
features.

---

## Phase 0: Known Bugfix Batch

**Purpose:** Fix concrete defects found during second review before adding abstractions.

**Work:**

- Fix fixed-contract market handling. `MarketContext.refresh_if_needed()` should populate fixed buy/sell prices,
  set `next_update_at`, mark `is_fixed_contract`, and return `True` so the mediator does not skip control logic when
  the active tariff is fixed.
- Fix EVCC validation edge cases:
  - `set_min_current()` must return `False` instead of sending invalid commands.
  - partial EVCC payloads with no loadpoints, missing `chargeCurrents`, or missing `sessionEnergy` must not crash
    `task_poll_evcc_state()`.
- Fix import-time timestamp defaults in `EVCCOverallState` by using a per-instance default factory or requiring the
  timestamp from the poller.
- Fix the daily-summary static helper signatures. `_format_hours()` and `_format_hours_summary()` are static methods
  but still pass `self` around; make the call shape normal and test it.
- Harden plot generation basics:
  - force the Matplotlib `Agg` backend before importing `pyplot`;
  - close figures in `finally`, not only on the success path;
  - replace spring-DST zero-price filler bars with missing values so the chart does not show fake zero prices.
- Fix dashboard current price display to show 'Market price' because it's showing dynamic prices.
- Clamp `/api/v1/logs?limit=` to a real range such as `1..20000`; do not let negative limits behave like unlimited.
- Allow empty `region_name_for_astral_optional` if the field is truly optional.
- Fix `store_da_prices()` return count so historic backfill progress is meaningful.
- Fix battery delta calculations so valid cumulative counter values of `0.0` are not skipped.

**Files likely touched:**

- `hec/logic_engine/scheduled_tasks.py`
- `hec/core/market_prices.py`
- `hec/controllers/api_evcc.py`
- `hec/core/models.py`
- `hec/reporting/daily_summary.py`
- `hec/reporting/plot_generator.py`
- `hec/core/vue_dashboard.html`
- `hec/core/api_server.py`
- `hec/core/config_schema.py`
- `hec/database_ops/db_handler.py`
- focused tests under `hec/tests/`

**Acceptance criteria:**

- The existing targeted test for manual summary refresh passes.
- Fixed-contract price intervals no longer make mediator preparation fail.
- Invalid EVCC min-current commands do not call `_send_command()`.
- Empty/partial EVCC state payloads are logged as degraded data, not unhandled exceptions.
- Hourly and 15-minute daily-summary solar income tests produce expected values.
- Plot exceptions leave no open Matplotlib figures.
- Dashboard current price test covers both dynamic and fixed active contracts.
- Negative log limits are rejected or clamped.
- `python -m unittest discover -s hec/tests` passes.

**Risk:** Low to medium. These are localized fixes, but anything that changes report numbers needs explicit before/after
tests.

---

## Phase 1: Fresh Data And Truthful Commands

**Purpose:** Make the app expert about uncertainty. It should not act as if old data, missing data, or failed commands
are current facts.

**Work:**

- Add freshness metadata for observed hardware state. At minimum, each important source should expose:
  - `observed_at_utc`;
  - `last_success_at_utc`;
  - `status` such as `fresh`, `stale`, `unavailable`;
  - `stale_after_seconds` from config or a safe default.
- Preserve last-known data separately from source health. Avoid collapsing a failed poll into plain `None` when the
  dashboard and mediator need to distinguish "last value stale" from "no value ever".
- Add freshness and sample-coverage metadata to rolling averages:
  - sample count;
  - actual coverage seconds;
  - latest sample age;
  - `valid_for_control`.
- Make peak-shaving and EV/battery decisions ignore rolling averages that do not have enough fresh coverage.
- Add prediction-plan metadata:
  - generated date;
  - optimization time;
  - horizon start/end;
  - source status;
  - whether the current interval is inside the plan.
- Make the mediator ignore expired prediction plans. If plan generation fails, mark the plan stale instead of leaving an
  old `prediction_plan_df` command-relevant.
- Separate desired state, observed state, and last-applied command result for hardware where it matters:
  - EVCC mode/current;
  - inverter limit;
  - battery mode.
- Update AppState only after a hardware command succeeds, or write a clear failed-command status if the command fails.
  Do not update battery manual mode before checking `set_battery_mode()` success.
- Do not update EVCC throttle timestamps after a failed command in a way that suppresses retry unnecessarily.
- Decouple startup fatality from warning/error incidents. Optional device initialization errors should start the app in
  degraded health, not abort the whole runtime. Fatal startup should be reserved for config, DB, scheduler, or API
  binding failures.

**Files likely touched:**

- `hec/logic_engine/data_processors.py`
- `hec/logic_engine/scheduled_tasks.py`
- `hec/logic_engine/system_mediator.py`
- `hec/core/app_state.py`
- `hec/core/app_logging.py`
- `hec/core/runtime.py`
- `hec/core/vue_dashboard.html`
- `hec/tests/`

**Acceptance criteria:**

- A failed P1/inverter/battery/EVCC poll marks source health stale or unavailable without hiding last-known data.
- The mediator does not use stale peak averages to trigger or avoid peak mitigation.
- An expired prediction plan cannot command battery block/force states.
- Failed EVCC, SMA, or HomeWizard battery commands do not appear as successful dashboard state.
- Optional integration startup failures result in degraded runtime, not full startup failure.
- Persistent incidents distinguish stale data, command failure, and missing device separately.

**Risk:** Medium. This touches control semantics, so add characterization tests first and change one source at a time.

---

## Phase 2: Scheduler, Retry, And Device Command Safety

**Purpose:** Prevent overlapping jobs and unsafe retries from causing duplicate commands or inconsistent AppState/DB
updates.

**Work:**

- Override scheduler `max_instances=1` for hardware command/poll jobs, mediator, battery predictor, price-prediction
  refresh, and daily-summary/report jobs. The global default of `3` is too broad for shared clients and hardware.
- Add scoped locks where overlap is dangerous:
  - inverter Modbus read/write lock;
  - plan-generation/optimization lock;
  - price-prediction model/cache lock;
  - daily-summary single-flight manager.
- Replace manual summary thread globals with a small job object exposing:
  - `state`;
  - `started_at_utc`;
  - `last_finished_at_utc`;
  - `last_error`;
  - `requested_by`;
  - `is_running()`.
- Do not attempt unsafe Python thread cancellation. Prefer single-flight refusal, queued-once semantics, or "already
  running" feedback. Cooperative cancellation can be added only for explicitly cancellable steps.
- Split HTTP retry policy by operation type:
  - retries are allowed for idempotent GET/HEAD and selected external API fetches;
  - no automatic retry for local-device POST/PUT/DELETE commands unless the command is proven idempotent and protected
    by readback;
  - local device timeouts should be short and explicit.
- Serialize SMA Modbus access inside `InverterSmaModbusClient`; no concurrent reads/writes on the same client.
- Add command readback where practical, especially for inverter limit changes.
- Allow clients to recover after startup unavailability. Do not permanently discard EVCC or device clients that have
  built-in availability checks.
- Extend config validation to integration sections:
  - EVCC URL, loadpoint id, min/max current, timeout;
  - inverter host, port, unit id, standard limit, timeout;
  - P1 and battery timeout/TLS settings;
  - ENTSO-E and Elia required fields;
  - task schedule intervals versus request timeouts.
- Derive API and dashboard numeric limits from config or observed hardware state. Avoid hard-coded `0..7000 W` and
  `6..32 A` where configured devices differ.
- Keep HomeWizard TLS policy explicit. Do not simply flip all existing `verify_tls=False` uses without a device-specific
  migration path; document insecure exceptions and prefer verification/pinning where the devices support it.

**Files likely touched:**

- `hec/core/app_initializer.py`
- `hec/logic_engine/scheduled_tasks.py`
- `hec/utils/http_client.py`
- `hec/controllers/modbus_sma_inverter.py`
- `hec/controllers/api_evcc.py`
- `hec/controllers/homewizard_battery_gateway.py`
- `hec/data_sources/`
- `hec/core/config_schema.py`
- `hec/core/api_server.py`
- `hec/core/vue_dashboard.html`
- `hec/tests/`

**Acceptance criteria:**

- Slow hardware jobs cannot overlap themselves.
- Unsafe write commands are not retried automatically by the shared HTTP client.
- Concurrent fake Modbus read/write tests show serialized access.
- API validation rejects values above the configured EVCC/inverter limits.
- A device unavailable at startup can become available later without restarting the app.
- Timeout settings cannot exceed their scheduler interval unless explicitly configured and tested.

**Risk:** Medium. Retry changes can alter failure behavior, so test local devices with mocks and staged live validation.

---

## Phase 3: Reporting Correctness And Rollup Performance

**Purpose:** Make daily reporting accurate and fast on a NAS before adding more report features.

**Work:**

- Fix the correctness issues from Phase 0 first: manual summary refresh, solar income math, static helper call shape, and
  unused hard-coded all-time savings.
- Harden fallback behavior. A missing optional plot, missing future prediction, or partial future forecast should not
  prevent sending the core daily summary when yesterday/tomorrow core data is available.
- Move plot generation behind safe optional wrappers so list indexing and min/max errors are caught outside
  `plot_generator.py` too.
- Make plot size and DPI configurable:
  - email default can remain high enough to read;
  - dashboard preview or future inline use should be lower DPI;
  - record generated image sizes during profiling.
- Keep cached price predictions as the normal summary path. Manual dashboard summary should not retrain or refresh
  prices unless an explicit force action exists.
- Add daily rollups for report-heavy calculations after migration safety exists:
  - imported/exported kWh;
  - fixed/dynamic cost/revenue;
  - capacity-cost inputs;
  - battery import/export/savings by battery;
  - calculation version and source range.
- Use rollups for month/year summary totals. Recompute a day only when raw data or tariff version changed, or when a
  user requests a forced recalculation.
- Add a small cache for repeated monthly peak calculations inside cost summary loops as an immediate low-risk win.
- Add query-count or timing assertions for summary generation using realistic-sized test data where possible.
- Add dashboard summary job UX:
  - button disabled or relabelled while a summary is running;
  - `aria-live` or `role=status` for status text;
  - last success/failure time and failure reason.

**Files likely touched:**

- `hec/reporting/daily_summary.py`
- `hec/reporting/plot_generator.py`
- `hec/reporting/summary_timing.py`
- `hec/logic_engine/cost_calculator.py`
- `hec/database_ops/db_handler.py`
- `hec/logic_engine/scheduled_tasks.py`
- `hec/core/vue_dashboard.html`
- migrations/tests

**Acceptance criteria:**

- Cached normal daily summary target: under 2 minutes on NAS-sized data, excluding SMTP variability.
- Missing optional future predictions still send a core summary.
- Month/year summaries use rollups or measured cached subqueries instead of full raw interval scans every email.
- Report output matches existing calculations for a representative set of days, including DST and month/year
  boundaries.
- Dashboard shows clear running/success/failure summary status.

**Risk:** Medium. Report-number changes must be backed by parity tests against current calculations.

---

## Phase 4: Database Migration And Maintenance Hardening

**Purpose:** Make schema changes and maintenance safe for the real existing SQLite database.

**Work:**

- Replace the no-op schema-version assumption with migrations that can repair existing databases:
  - create missing columns;
  - create missing indexes;
  - preserve data;
  - run idempotently.
- Add migration tests using scratch "old" databases that intentionally miss newer columns/indexes.
- Schedule energy-history retention instead of leaving it as a callable helper only.
- Stop deleting old logs inside every `store_log()` transaction. Replace with scheduled or throttled log retention.
- Consider queued/asynchronous log persistence only if measured log writes still interfere with control work. Do not add
  a queue just for style.
- Add scheduled SQLite maintenance:
  - `PRAGMA optimize`;
  - optional WAL checkpoint policy after heavy report/maintenance windows;
  - database size and WAL size logging.
- Fix per-thread connection lifecycle for short-lived background threads. Repeated manual summary/report threads should
  not leave stale connections in `_thread_connections`.
- Keep writes through `transaction()` where practical. Use short-lived read connections for long report reads so worker
  threads do not accumulate persistent connections.
- Add only targeted indexes supported by `EXPLAIN QUERY PLAN` on realistic data:
  - likely candidate: `(battery_name, timestamp_utc)` for battery retention/delta paths;
  - do not broadly drop redundant indexes until a migration and performance measurement justify it.
- Keep SQLite. A move to Postgres is not justified by current evidence.

**Files likely touched:**

- `hec/database_ops/db_handler.py`
- `hec/logic_engine/scheduled_tasks.py`
- `hec/core/app_initializer.py`
- `docs/database-maintenance.md`
- `hec/tests/`

**Acceptance criteria:**

- A simulated old DB upgrades to the current schema without data loss and can rerun migrations safely.
- Energy retention and log retention are scheduled, observable, and independently testable.
- Log writes no longer run deletion SQL on every log insert.
- Repeated short-lived report threads do not grow the connection map.
- Query plans for retained hot paths are documented before/after.

**Risk:** Medium. Test migrations on a copied real DB before using them on the deployed database.

---

## Phase 5: Forecast, Cache, And Time Correctness

**Purpose:** Make forecasting/cache behavior robust before adding smarter prediction features.

**Work:**

- Use `hec/utils/time_utils.py` consistently for local-day bounds and UTC conversion. Remove ad hoc
  `datetime.now().astimezone().tzinfo`, hard-coded `ZoneInfo("Europe/Brussels")`, and mixed `pytz` usage as adjacent
  code is touched.
- Fix ENTSO-E auction gating to use the configured local timezone, not the host default timezone.
- Distinguish "not yet available" from "request failed" for Elia and ENTSO-E. Empty API results should not always be
  logged as failures.
- Add freshness metadata to `predicted_prices`:
  - generated_at_utc;
  - model trained_at_utc or model id;
  - forecast feature coverage;
  - source status.
- Make the daily summary skip stale predicted prices and report that future plots were skipped.
- Persist price models atomically:
  - write to a temp file;
  - fsync/close if practical;
  - replace the existing model file atomically.
- Consider `joblib` for sklearn model persistence only after atomicity and fallback behavior are in place. The benefit is
  likely smaller than reliable writes and stale detection.
- Add corruption/fallback tests for model files and prediction caches.
- Add limited backtesting before changing model complexity, estimator count, data hash logic, or feature set.
- Fix consumption forecast missing-data behavior:
  - leap-day last-year lookup;
  - missing inverter data should be explicit zero solar or stale forecast, not accidental abort;
  - no-history should mark plan unavailable instead of flowing `None` into plan generation.
- Add battery predictor timing around base-plan generation and optimization. Do not vectorize optimizer rules until a
  measured hotspot justifies it.

**Files likely touched:**

- `hec/utils/time_utils.py`
- `hec/data_sources/api_entsoe.py`
- `hec/data_sources/api_elia.py`
- `hec/logic_engine/price_predictor.py`
- `hec/logic_engine/consumption_predictor.py`
- `hec/logic_engine/battery_predictor.py`
- `hec/logic_engine/scheduled_tasks.py`
- `hec/database_ops/db_handler.py`
- `hec/tests/`

**Acceptance criteria:**

- DST days, local day bounds, and price/forecast fetches behave correctly under the configured timezone.
- Corrupt model files do not crash scheduled prediction refresh.
- Stale prediction caches are skipped or clearly marked.
- Consumption forecast tests cover Feb 29 and no-history cases.
- Optimizer profiling identifies whether vectorization is worth considering.

**Risk:** Medium. Time-zone changes are easy to get subtly wrong; test with spring and fall DST days.

---

## Phase 6: Dashboard Operator UX And Local Assets

**Purpose:** Make the dashboard a precise operator surface for the home, not just a raw state dump.

**Work:**

- Vendor the exact Vue build locally and serve it from Flask. The NAS dashboard should not depend on `unpkg.com` being
  reachable.
- Keep the existing simple Vue app for now. Do not introduce a frontend build pipeline unless the single-file dashboard
  becomes unmaintainable.
- Add device-health and freshness indicators:
  - P1 meter;
  - inverter;
  - EVCC;
  - battery gateway and per-battery data;
  - price and forecast data.
- Add command-result visibility:
  - last requested command;
  - last applied command;
  - failure reason;
  - time since command.
- Improve prediction-plan UI:
  - show generated/optimized time and plan freshness;
  - highlight the active interval;
  - show why the current battery mode is being commanded: block charge, block discharge, force charge, stale plan
    fallback, or manual mode.
- Improve incidents and notification delivery:
  - stable per-source fingerprints for repeated operational incidents;
  - notification queue delivery status and last check-in;
  - no broad template abstraction until a second provider is actually implemented.
- Make summary and restart controls clearer:
  - summary running state, last result, and error;
  - restart request status after supervised exit;
  - accessible live regions for changing status.
- Keep polling with `state_version` for now. Do not move to SSE/WebSockets unless multiple dashboards or latency
  measurements show a concrete need.

**Files likely touched:**

- `hec/core/vue_dashboard.html`
- `hec/core/api_server.py`
- `hec/core/app_state.py`
- `hec/core/incidents.py`
- `hec/core/notifications.py`
- `hec/database_ops/db_handler.py`
- tests around dashboard hooks

**Acceptance criteria:**

- Dashboard works without internet access after the Flask page is loaded.
- Operator can distinguish fresh, stale, unavailable, desired, and applied state.
- Current price uses the active contract.
- Summary, incident, notification, and restart statuses are accessible and understandable.
- State polling remains lightweight and does not ship unneeded large payloads on unchanged state.

**Risk:** Low to medium. The dashboard is one large file; keep edits narrow or split only if the touched area becomes
hard to test.

---

## Phase 7: Codebase Hygiene And Low-Risk Maintainability

**Purpose:** Reduce future bug risk without turning this into a rewrite.

**Work:**

- Add `AppState.set_many()` for atomic multi-key updates:
  - one lock acquisition;
  - one state-version bump;
  - clear persisted-setting behavior;
  - API path can still report persistence failure for persisted keys.
- Do not prioritize `MappingProxyType` for snapshots. The current issue is multi-key atomicity and clear ownership, not
  top-level dict mutability.
- Delete the unused duplicate `hec/models/models.py` after an import scan confirms no imports from `hec.models`.
- Add a small dataclass serialization helper only if it removes real duplication in model classes; do not force a generic
  pattern into every dataclass.
- Remove dead MAC-discovery comments from the P1 client unless discovery is actually implemented. Do not add `scapy` for
  this app unless the user needs discovery; static IP or DHCP reservation is simpler and safer on a NAS.
- Gradually split `db_handler.py` only around cohesive areas when touched:
  - incidents/notifications;
  - report rollups;
  - device history;
  - migrations.
  Avoid a broad split that changes behavior without user value.
- Keep external supervisor restart as the operational model. Do not add same-process restart unless external supervision
  is unavailable.
- Keep dependency/tooling development-side. NAS runtime should stay simple.

**Files likely touched:**

- `hec/core/app_state.py`
- `hec/models/`
- `hec/core/models.py`
- `hec/data_sources/api_p1_meter_homewizard.py`
- `hec/database_ops/`
- tests

**Acceptance criteria:**

- Multi-key AppState updates avoid repeated version bumps for scheduler ticks.
- Duplicate model module is gone and import scan passes.
- No new heavy runtime dependencies are added for convenience features.
- Any `db_handler.py` split has clear ownership and behavior-preserving tests.

**Risk:** Low if changes remain incremental.

---

## Triage Of The Pasted Suggestions

### Worth Doing Soon

- Force Matplotlib `Agg` and guarantee `plt.close(fig)` with `finally`.
- Replace spring-DST fake zero-price plot fillers with missing values.
- Clean up daily-summary static helper signatures.
- Add a proper summary job status object/single-flight manager.
- Use configured API/dashboard limits for inverter and EVCC commands.
- Fix invalid EVCC min-current command handling.
- Add EVCC availability check caching or better source health if repeated checks are measured to be noisy.
- Schedule retention and stop running log cleanup on every log insert.
- Fix `get_battery_deltas_for_intervals()` zero-counter handling.
- Fix `store_da_prices()` return count.
- Add model/cache atomicity and freshness metadata.
- Add `AppState.set_many()`.
- Delete unused duplicate `hec/models/models.py`.

### Already Largely Implemented

- Price predictor defaults to `n_jobs=1`.
- Price predictor has a bounded training window.
- Price model is persisted, though not yet atomically and still via pickle.
- SQLite WAL, busy timeout, transactions, migrations table, and several indexes exist.
- AppState has a lock, state version, snapshots, and snapshot context.
- Manual summary is non-blocking, but its status and refresh behavior still need work.
- Shared HTTP client and time utilities exist, but usage and policies need tightening.
- P1 meter and HomeWizard battery gateway have been split.
- API auth, CSRF/same-origin checks, command allowlist, incidents, and notification queue exist.

### Measure Or Design First

- `lru_cache` for `TariffManager.get_all_tariffs()`: probably useful after cache invalidation on reload is explicit, but
  profile first because tariff lookup is unlikely to be the current bottleneck.
- `MarketPricesSnapshot` dataclass: useful only if half-updated market state is observed or tests show it simplifies
  mediator reads.
- Plot content-hash caching: less valuable than rollups and lower DPI. Consider only for repeated manual report
  generation with identical inputs.
- `joblib`: reasonable after atomic persistence and fallback behavior; not the first model-persistence problem.
- `PRAGMA optimize`: good as scheduled DB maintenance, not as a per-connection silver bullet.
- `store_evcc_session()` bulk backfill: useful if real gaps are common or profiling shows it matters.
- Startup parallel warm-up: useful only after per-client timeout budgets and fatal/degraded startup semantics are clear.
- Notification templates/providers: add when a real second provider such as ntfy or Pushover is selected.

### Reject Or Defer

- Broad optimizer vectorization now. The optimizer loops are bounded and stateful; report rollups and stale-data safety
  are higher-value NAS wins. Profile before changing optimizer math.
- Treating four list inserts in plot DST handling as a meaningful O(n^2) performance issue. The correctness of fake
  zero-price fillers matters; the tiny list operation does not.
- `MappingProxyType` snapshots as a priority. It does not solve nested mutability or multi-key update churn.
- Requiring every controller default to be removed from Python constructors. Keep safe constructor defaults, but validate
  config and do not pass `None` accidentally.
- Implementing MAC discovery with `scapy` just to fill a dead branch. Prefer removing the branch or documenting static IP
  setup.
- A typed event bus for summary/reboot/prediction requests at this stage. A tiny summary job manager and explicit
  command handlers are enough.
- Moving to Postgres, SSE/WebSockets, OAuth/multi-user auth, a new web server stack, or same-process restart without new
  evidence.

## Behavior Preservation Gates

Before changing any of these areas, add or update characterization tests and review expected behavior explicitly:

- `hec/logic_engine/system_mediator.py`: operating mode, mediator goals, peak handling, EV grace period, inverter/EVCC/
  battery commands.
- `hec/logic_engine/battery_predictor.py`: plan generation, block/force flags, SOC simulation, optimizer output.
- `hec/logic_engine/cost_calculator.py`: report cost and savings numbers, especially after rollups.
- `hec/database_ops/db_handler.py`: cumulative delta calculations, migrations, retention, and connection policy.
- `hec/data_sources/api_entsoe.py` and `hec/data_sources/api_elia.py`: DST, local-day bounds, no-data semantics.
- `hec/reporting/daily_summary.py`: email content and report math.

## Edge Cases To Test

- Spring and fall DST days with 23 and 25 hours.
- Fixed active contract.
- ENTSO-E not-yet-published prices.
- Elia empty results versus API failure.
- Device unavailable at startup, then available later.
- Failed P1/inverter/EVCC/battery poll after previously fresh data.
- Stale rolling averages during peak-shaving decisions.
- Expired prediction plan during battery mediation.
- EVCC payload with no loadpoints or missing session energy.
- SMA command timeout after write may or may not have applied.
- Failed hardware command followed by retry.
- Peak notification path with slow SMTP or slow incident DB write.
- Manual summary while scheduled summary is running.
- Summary with missing optional plots.
- Hourly and 15-minute price resolution in solar income calculations.
- First install with no history.
- Leap day consumption forecast.
- Existing old database missing columns/indexes.
- WAL/log retention under concurrent scheduler/API writes.
- Dashboard with no internet access.
- Multiple dashboard tabs changing settings.

## Success Criteria For The Second Revision

- The app starts degraded, not dead, when optional integrations are down.
- Control decisions use fresh-enough source data or explicitly fall back to safe behavior.
- Dashboard command state reflects hardware success or failure.
- Scheduler jobs do not overlap in ways that can duplicate hardware commands.
- Unsafe local-device write commands are not retried automatically.
- Daily summaries are accurate across 15-minute/hourly price days and normally complete within the NAS budget.
- Existing databases migrate safely and maintenance runs predictably.
- The dashboard works offline on LAN/VPN and shows source health clearly.
- The full `unittest` suite passes after each batch.
