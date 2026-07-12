# Home Energy Control Application Efficiency Review And Improvement Plan

**Review date:** 2026-07-12

**Status:** Current-state whole-application review and implementation backlog.

**Relationship to earlier plans:** This document consolidates and updates
`2026-06-28-professionalization-roadmap.md` and `2026-06-29-second-revision-roadmap.md` after reviewing the code that
now
exists. Earlier plans remain useful implementation history. Where their assumptions conflict with this review, measure
the current code and use this plan.

## Goal

Improve efficiency at every useful level without replacing a suitable single-home architecture:

- household energy cost, self-consumption, peak-tariff control, and battery lifetime;
- control-loop safety, latency, consistency, and recovery time;
- NAS CPU, memory, disk, database, and network use;
- dashboard bandwidth and operator effort;
- developer feedback speed, test confidence, and change cost;
- security, privacy, backup, and incident-response effort.

"All improvements" in this document means the comprehensive set discoverable from the current repository. Live NAS,
device, tariff, and household measurements can reveal additional opportunities. Every estimate below is therefore a
hypothesis until it is measured on the deployed NAS.

## Scope And Constraints

- Reviewed all production Python modules, the Vue dashboard, configuration example, tariff data, tests, CI, and docs.
- Did not read the ignored production database, secrets, full local `config.yaml`, or household history.
- Did not send commands to live P1, inverter, EVCC, battery, ENTSO-E, Elia, SMTP, or notification endpoints.
- Preserve SQLite, Flask, APScheduler, and the single-process deployment unless measurements prove a limit.
- Preserve trusted mediator and optimizer behavior until characterization, replay, and shadow-mode comparisons approve a
  change.
- Prefer fewer, coherent operations over more threads, services, caches, dependencies, or abstraction layers.

## Executive Decisions

1. Keep SQLite. Current write amplification and query shape should be fixed before considering PostgreSQL.
2. Build one coherent acquisition-to-control tick. More scheduler concurrency is currently harmful, not helpful.
3. Make desired, commanded, and observed device state separate and timestamped before making control logic smarter.
4. Move routine logging off control threads and stop retention work on every log insert.
5. Build report rollups and calculate year-to-date once, then derive month/day views from the same result.
6. Split fast live dashboard state from large prices and prediction plans; keep conditional polling initially.
7. Evaluate forecast and optimizer changes with replay and cost/peak/battery metrics. Do not optimize only for execution
   speed.
8. Fail closed when authentication is configured but unusable, especially while binding to `0.0.0.0`.
9. Keep heavy reporting isolated and lazy-load reporting-only modules so the controller does not pay their full steady
   memory cost.
10. Treat documentation, configuration, migrations, and tests as executable product surfaces, not secondary files.

## Audit Baseline

### Validation Results

- `python -m unittest discover -s hec/tests`: 159 tests passed, 1 time-dependent test skipped.
- Test process: 11.266 seconds reported by `unittest`, 36.3 seconds observed wall time, with substantial console output.
- Ruff could not run because the existing `.venv` does not contain the declared development dependency.
- `pip-audit` did not complete within two and three minute attempts, including one attempt with network access.
  Dependency
  vulnerability status is therefore unknown, not clean.
- The worktree already contained an untracked `_scratch/` directory. Tests write into that directory; it was not treated
  as source.

### Size And Hotspots

- Production Python is approximately 11,750 physical lines, plus a 1,349-line dashboard.
- `hec/database_ops/db_handler.py` is over 2,200 physical lines and owns unrelated schemas and workflows.
- `hec/logic_engine/scheduled_tasks.py` is over 1,000 physical lines and mixes scheduling, polling, backfill, reporting,
  forecasting, and control orchestration.
- `hec/logic_engine/system_mediator.py` is over 1,000 physical lines and mixes decisions, command delivery, alerts, and
  state tracking.

### Local Indicative Measurements

These measurements used synthetic representative data on the development desktop, not the NAS:

| Measurement                              |            Result | Interpretation                                                                  |
|------------------------------------------|------------------:|---------------------------------------------------------------------------------|
| Prices plus 192-row prediction plan JSON |      99,730 bytes | Fast state polling sends slow-changing bulk data.                               |
| That payload every 15 seconds            | 547.8 MiB/day/tab | Live values make the global ETag change nearly every tick.                      |
| `AppState.get_all()` deep copy           |     2.998 ms/call | Full snapshots add avoidable allocation.                                        |
| Full state copy and JSON serialization   |     9.739 ms/call | Cost scales with tabs and payload growth.                                       |
| Current `store_log()` for 500 records    |        2,211.3 ms | Each record opens a write transaction and runs retention.                       |
| One transaction inserting 500 records    |            3.1 ms | Batching is an order-of-magnitude opportunity; exact NAS gain must be measured. |

### Structural Cost Estimates

- A 15-second job runs 5,760 times/day.
- One notification-enabled tab polls state and pending notifications 5,760 times/day each. Empty notification polls
  still
  start a write transaction to update `last_seen_utc` (`db_handler.py:1843-1874`).
- At each 15-minute boundary, separate display and DB jobs poll the same inverter and batteries
  (`scheduled_tasks.py:960-988`, `scheduled_tasks.py:1037-1063`).
- A daylight inverter poll reads status once in `task_poll_inverter()` and again inside `get_live_data()`, followed by
  additional register reads (`scheduled_tasks.py:205-224`, `modbus_sma_inverter.py:122-156`).
- On 2026-07-12, one daily report asks the cost calculator to traverse 204 day-invocations across day/month/year. The
  12-query monthly-peak method is called once per cost day, producing at least 2,448 peak SELECTs before other report
  queries (`daily_summary.py:459-471`, `cost_calculator.py:345-480`, `db_handler.py:1008-1068`).

### Priority And Expected Gain

Expected gain is qualitative until Phase 0 measures it on the NAS. "Risk" means risk of changing correct live behavior,
not the risk of leaving the issue unfixed.

| Order | Work package                                         | Primary efficiency gain                                          | Expected gain    | Effort       | Risk       |
|------:|------------------------------------------------------|------------------------------------------------------------------|------------------|--------------|------------|
|     1 | Control/optimizer correctness and command truth      | Avoid invalid or economically wrong hardware actions             | Very high        | Medium       | High       |
|     2 | Queued logs and scheduled retention                  | Shorter control latency, fewer locks and disk writes             | Very high        | Small-medium | Low        |
|     3 | Coherent tick, one poll, safe retries                | Fewer device calls, races, stale/mixed decisions, and recoveries | High             | Medium       | Medium     |
|     4 | Report rollups and year-once aggregation             | Thousands fewer SQL statements and bounded report memory/time    | Very high        | Medium-large | Medium     |
|     5 | Split live/prices/plan APIs                          | Target over 90% smaller recurring state response                 | High             | Medium       | Low-medium |
|     6 | Real migrations and bounded maintenance              | Lower lock/restore/upgrade effort and controlled DB growth       | High             | Medium       | Medium     |
|     7 | Freshness and desired/attempted/observed state       | Fewer unsafe fallbacks and faster diagnosis                      | High             | Medium-large | Medium     |
|     8 | Forecast backtests and unit-safe optimizer candidate | Better bill, peak, reserve, and battery-wear outcomes            | Potentially high | Large        | High       |
|     9 | Full config/runtime/security validation              | Fewer startup outages and silent unsafe deployments              | High             | Medium       | Medium     |
|    10 | Module boundaries, tooling, deterministic tests      | Lower compounding development and regression cost                | Medium           | Medium-large | Low-medium |

## Highest-Risk Findings

### P0: Control And Cost Correctness

| ID   | Finding                                                                                                                                        | Evidence                                                    | Efficiency impact                                                                                    |
|------|------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| C-01 | Zero actual battery SOC is treated as "not supplied" and inferred from the old plan. A local reproduction returned 50% after passing 0%.       | `battery_predictor.py:200-216`                              | Can make physically impossible decisions and damage cost/backup goals.                               |
| C-02 | Inverter energy limiting multiplies configured kW by 1,000 while comparing with kWh.                                                           | `battery_predictor.py:234-250`                              | The modeled inverter constraint is ineffective outside special negative-price handling.              |
| C-03 | Optimized discharge is bounded with `max_charge_kw`, not `max_discharge_kw`; reserve subtraction can become negative.                          | `battery_predictor.py:253-277`                              | Mis-models battery power and can create invalid SOC transitions.                                     |
| C-04 | Battery cost includes grid import/export effects and then adds avoided/opportunity value again; terminal inventory also hard-codes 5.36 kWh.   | `battery_predictor.py:295-356`                              | Likely double-counts value and can rank plans incorrectly. Requires domain confirmation.             |
| C-05 | EV capacity pause saves `lp.mode` as a string, but resume later expects an `EVCCManualState`. A local reproduction resumed with `str('now')`.  | `system_mediator.py:311-326`, `system_mediator.py:803-852`  | Resume can fail exactly when peak capacity becomes available again.                                  |
| C-06 | Battery state is persisted even when the hardware command fails.                                                                               | `system_mediator.py:855-900`                                | Dashboard/control state can claim a command was applied when it was not.                             |
| C-07 | Prediction selection takes the last row before now without checking interval end, generation age, feature age, or plan horizon.                | `system_mediator.py:615-637`                                | A stale final instruction can remain active indefinitely.                                            |
| C-08 | Mediator preparation requires a current price even for manual operation and has no source-freshness budget.                                    | `system_mediator.py:174-190`, `system_mediator.py:996-1024` | Missing price data can block unrelated manual control; stale data can still drive automatic control. |
| C-09 | Peak-mode restore reads a non-existent top-level inverter-limit key and stores the battery gateway mode string as if it were a `BatteryState`. | `system_mediator.py:957-990`                                | Exit from peak throttling can restore the wrong or invalid states.                                   |
| C-10 | Report solar-income conversion is resolution-dependent and the report assumes 24/96 intervals and a 24-hour UTC span.                          | `daily_summary.py:204-229`, `daily_summary.py:343-385`      | Hourly and DST reports can show materially wrong production/income.                                  |

### P0: Command And Runtime Safety

| ID   | Finding                                                                                                                | Evidence                                                      | Efficiency impact                                                                              |
|------|------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| S-01 | Shared HTTP retries include `POST`, `PUT`, and `DELETE`.                                                               | `http_client.py:20-33`                                        | A command that applied before a lost response can be repeated.                                 |
| S-02 | Scheduler defaults allow three instances of a job, and hardware jobs do not override the value.                        | `app_initializer.py:223-243`, `scheduled_tasks.py:1071-1086`  | Concurrent reads/writes can race on shared clients, state, and SQLite.                         |
| S-03 | Fast acquisition and mediator jobs use the same second boundaries.                                                     | `config.yaml.example` scheduler section                       | The mediator can run before some new samples and after others, creating a mixed-time decision. |
| S-04 | One Modbus client/socket and its rate-limit deque are shared across independent scheduler jobs without a lock.         | `modbus_sma_inverter.py:33-102`, `scheduled_tasks.py:960-988` | Concurrent protocol frames and commands can fail or be mis-associated.                         |
| S-05 | Optional clients that fail their startup probe are discarded or never scheduled; most cannot recover later.            | `app_initializer.py:86-209`                                   | A short startup outage becomes an app-lifetime outage and requires operator intervention.      |
| S-06 | An unexpected API-thread exit is classified as `STOPPED`, which maps to exit code 0.                                   | `runtime.py:175-213`                                          | A supervisor configured to restart failures may leave the controller stopped.                  |
| S-07 | Peak email is sent synchronously inside the mediator path with a 120-second SMTP timeout.                              | `system_mediator.py:903-955`, `utils.py:157-219`              | Alert latency can block control commands for multiple ticks.                                   |
| S-08 | Auth explicitly enabled without credentials logs a warning and disables itself while the example binds all interfaces. | `api_server.py:100-129`, `config.yaml.example` API section    | A configuration error silently turns protection off and increases incident cost.               |

### P1: Resource And Throughput

| ID   | Finding                                                                                                                      | Evidence                                                                         | Efficiency impact                                                                |
|------|------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| R-01 | Every `INFO`-or-higher log is synchronously persisted on the emitting thread. Warnings add incident, queue, and status work. | `app_logging.py:28-64`, `app_logging.py:132-137`                                 | Adds SQLite latency and locks directly to control jobs.                          |
| R-02 | Log retention runs inside every log insert.                                                                                  | `db_handler.py:2086-2094`                                                        | Repeated DELETE work and write-lock duration grow with log volume.               |
| R-03 | Normal state setters persist unchanged values, including `empty_since=None` every mediator tick.                             | `app_state.py:73-112`, `system_mediator.py:589-603`                              | Unnecessary setting writes, logs, state versions, and dashboard payloads.        |
| R-04 | Inverter and battery DB/display jobs duplicate device calls at 15-minute boundaries.                                         | `scheduled_tasks.py:240-257`, `scheduled_tasks.py:1037-1063`                     | More LAN traffic, thread use, device load, and race exposure.                    |
| R-05 | Full state polling includes prices and prediction plan whenever any live field changes.                                      | `api_server.py:196-199`, `api_server.py:368-385`, `vue_dashboard.html:979-1089`  | Approximately 100 KB per changed response in a representative state.             |
| R-06 | Pending-notification GET both consumes notifications and writes device `last_seen`; the dashboard runs it every 15 seconds.  | `api_server.py:509-517`, `db_handler.py:1843-1874`, `vue_dashboard.html:979-986` | At least one needless SQLite write per interval per active tab.                  |
| R-07 | Daily summary computes overlapping day, month, and year ranges independently for both costs and battery savings.             | `daily_summary.py:459-471`                                                       | Repeats most database, tariff, and delta work.                                   |
| R-08 | Monthly capacity lookup performs 12 SQL queries for every report day.                                                        | `db_handler.py:1008-1068`, `cost_calculator.py:446-452`                          | Thousands of small SELECTs in a normal year-to-date report.                      |
| R-09 | EVCC gap backfill inserts one synthetic row per missed 15-minute interval with no gap cap.                                   | `db_handler.py:2173-2240`                                                        | Long downtime can create a large synchronous loop and misleading data.           |
| R-10 | Heavy reporting and ML modules are imported by the always-on scheduler module.                                               | `scheduled_tasks.py:1-37`, `daily_summary.py:7-17`, `plot_generator.py:1-14`     | Process isolation limits leaks but not all steady controller memory/import cost. |

## Target Architecture

Keep one supervised controller process plus short-lived isolated workers for heavy offline work:

```text
typed config + secrets
        |
runtime owner -------------------------------- health/readiness
        |
coherent control tick
  acquire concurrently with per-device locks and deadlines
  validate + timestamp one observation snapshot
  update rolling windows once
  decide against one immutable snapshot
  send idempotency-aware commands
  record desired/attempted/observed results
        |
SQLite repositories <---- bounded async log/event writer
        |
small live-state API ---- prices API ---- plan API ---- incidents/log API
        |
local Vue dashboard with independent ETags and slower bulk polling

short-lived reporting/prediction worker -> cached rollups/model outputs -> SQLite
```

The target is not an event-bus or microservice rewrite. It is explicit ownership, coherent timing, bounded I/O, and
separation of live control from offline computation.

## Phase 0: Measurement And Safety Gates

**Purpose:** Establish proof before changing trusted energy behavior.

### Work

- [ ] Add a `tools/benchmark_runtime.py` harness with repeatable measurements for state payload bytes, serialization
  time, log throughput, SQLite write-lock time, report query count, report elapsed time, and import/steady RSS.
- [ ] Add APScheduler listeners that record job duration, lateness, overlap, exceptions, and skipped/coalesced runs.
- [ ] Add per-source counters for requests, retries, timeouts, bytes, last success, and consecutive failures.
- [ ] Add SQLite statement counting around daily summary tests and `EXPLAIN QUERY PLAN` fixtures with NAS-sized row
  counts.
- [ ] Record a privacy-safe control replay fixture containing timestamped inputs, decisions, desired commands, command
  results, costs, peak windows, and SOC. Strip hosts, tokens, email, and household-identifying values.
- [ ] Add shadow-mode support that runs old and candidate mediator/optimizer logic without sending candidate commands.
- [ ] Add outcome metrics: actual bill, import/export kWh, self-consumption, curtailed solar, maximum 15-minute import,
  battery throughput/cycles, forecast MAE, command failures, stale decisions, and fallback minutes.
- [ ] Establish NAS budgets: controller RSS, idle CPU, database size/WAL size, tick deadline, report duration, and
  dashboard
  bytes/day/tab.
- [ ] Make tests capture routine logging by default and print it only on failure.
- [ ] Move all tests to `tempfile`/`TemporaryDirectory`; stop writing persistent files under repository `_scratch/`.

### Acceptance

- The benchmark can run without live devices or secrets.
- A 24-hour replay is deterministic and produces a stable decision digest.
- Each scheduled job exposes p50/p95/max duration and overlap count.
- The report test fails on query-count or memory-budget regressions, not only output changes.
- No candidate optimizer change reaches hardware without an old-versus-new replay report.

## Phase 1: Correct Decisions And Truthful Commands

**Purpose:** Remove risks that can waste energy or misrepresent hardware before performance tuning.

### Battery And Optimizer

- [ ] Fix `current_soc_kwh is None` handling so zero SOC remains zero.
- [ ] Correct inverter kW/kWh units and use `max_discharge_kw` on discharge paths.
- [ ] Clamp available discharge above reserve to zero before applying a discharge.
- [ ] Replace hard-coded 5.36 kWh terminal inventory with configured aggregate capacity and a documented terminal-value
  policy.
- [ ] Audit the objective for double counting. Compute bill impact exactly once from grid import/export, then add only
  costs not already represented, such as degradation or terminal SOC value.
- [ ] Make charge/discharge efficiency, usable SOC floor/ceiling, standby loss, taper, and degradation cost configurable
  per battery. Aggregate by energy capacity, not simple average SOC.
- [ ] Validate that every plan has finite values, complete prices, monotonic timestamps, physical power/capacity bounds,
  and an explicit valid-until time before publishing it.
- [ ] Keep the old optimizer in shadow mode until the candidate reduces replay cost without increasing peak violations,
  invalid states, or battery-throughput budget.

### Mediator And Commands

- [ ] Convert EVCC observed mode strings to enums at the adapter boundary; keep the pause/resume state typed.
- [ ] Separate `desired_state`, `last_command`, `command_result`, and `observed_state` for EVCC, inverter, and battery.
- [ ] Update applied state only after a successful command and, where possible, read-back verification.
- [ ] Track ambiguous outcomes when a write may have applied before a timeout; reconcile by reading before retrying.
- [ ] Fix peak-mode state capture/restore and test entry, sustained throttle, exit, partial command failure, and
  restart.
- [ ] Make manual safety commands available without price data. Apply market-data requirements only to rules that need
  prices.
- [ ] Add stale-data policies per decision input. Safe fallback must be explicit for P1, EVCC, inverter, battery, price,
  forecast, rolling average, and prediction plan.
- [ ] Reject a plan row unless `row_start <= now < row_end` and the plan/model/features satisfy freshness budgets.
- [ ] Replace synchronous peak email with an outbox/worker; record the incident before attempting notification.
- [ ] Configure peak ignore windows and use the configured timezone instead of server-local time.
- [ ] Use exact current 15-minute-window energy/sample coverage for peak budget rather than a 5-minute proxy multiplied
  by elapsed time.

### Acceptance

- Zero SOC, below-reserve SOC, heterogeneous batteries, failed commands, stale observations, and expired plans have
  deterministic safe outcomes.
- Dashboard desired/applied/observed values cannot claim success after a failed write.
- Candidate replay never exceeds physical limits and improves or matches the baseline objective.
- Manual stop/limit commands work when ENTSO-E and Elia are unavailable.
- Peak notifications cannot delay the next control tick.

## Phase 2: Coherent Scheduling And Integration Efficiency

**Purpose:** Remove duplicate I/O, overlap, and recovery gaps.

### Work

- [ ] Set `max_instances=1` explicitly for every poll, mediator, predictor, backfill, maintenance, and summary job. Keep
  `coalesce=True` unless a specific job requires replay.
- [ ] Replace same-second independent fast jobs with one orchestrated tick or a dependency chain:
  acquire -> validate -> publish -> rolling averages -> decide -> command -> enqueue persistence.
- [ ] Acquire independent devices concurrently only within a shared tick deadline. Never run two operations against the
  same device client concurrently.
- [ ] Add per-device locks/command queues for Modbus, EVCC, P1 gateway, and each battery client.
- [ ] Merge live and DB polling. Poll once; update live state; persist the same sample only when its storage boundary is
  reached.
- [ ] Remove the duplicate inverter status read. Split fast fields (status/PV/limit) from slow energy counters when the
  protocol requires separate reads.
- [ ] Poll inverter less often at night while retaining a low-frequency health check and immediate wake near sunrise.
- [ ] Use adaptive backoff with jitter for unavailable devices and reset quickly on success. Keep the client scheduled
  so
  a device can recover after startup.
- [ ] Use idempotent retries only for safe reads. For writes, retry connection establishment before sending, not an
  ambiguous completed request; reconcile uncertain outcomes by read-back.
- [ ] Give connect and read timeouts separate, bounded values below the tick deadline.
- [ ] Close HTTP sessions and Modbus sockets during shutdown; expose connection pool and retry statistics.
- [ ] Fix EVCC backfill: cap the repair window, represent long outages as gaps, and use `executemany` for short
  validated
  repairs.
- [ ] Make historic bootstrap resumable and gap-based. Fetch only missing date ranges, rate-limit requests, persist a
  cursor, and run it in a low-priority worker outside the control executor.
- [ ] Replace global day-ahead retry counters with per-target-date retry state and a clear state machine.
- [ ] Add circuit-breaker incident transitions: one incident on degradation, occurrence counters while down, and one
  recovery event. Do not create an incident/write for every failed 15-second poll.

### Acceptance

- One physical sample per device per configured interval, including 15-minute boundaries.
- No same-device overlap under forced slow responses.
- The mediator sees one observation timestamp/version and either meets the tick deadline or takes a documented fallback.
- A device unavailable at startup recovers without process restart.
- No unsafe HTTP command is automatically replayed after an ambiguous timeout.
- Historic bootstrap cannot starve control jobs or flood upstream APIs.

## Phase 3: Logging, SQLite, Notifications, And Reporting

**Purpose:** Remove the largest write and query amplification while keeping SQLite lightweight.

### Logging And Incidents

- [ ] Set DB log persistence to `WARNING` by default. Allow selected structured `INFO` audit events rather than every
  routine message.
- [ ] Send logs/events to a bounded in-memory queue with one writer that batches by count/time. Define drop policy:
  coalesce/drop debug first, never silently drop audit/error records.
- [ ] Remove `_delete_old_logs()` from `store_log()` and schedule retention once per day or throttle it by monotonic
  time.
- [ ] Keep command audit records in a structured table with actor, target, old/new desired values, result, observed
  value,
  correlation ID, and independent retention.
- [ ] Create incidents only from explicit operational boundaries, not every warning log. Expected conditions such as a
  temporary pause or retry should not automatically become alerts.
- [ ] Normalize incident fingerprints so changing measurements do not create unbounded unique rows.
- [ ] Add incident and notification retention, inactive-device expiry, queue depth limits, and delivery-attempt
  metadata.
- [ ] Change notification delivery to fetch -> display -> acknowledge. Do not mark delivered before the browser
  confirms.
- [ ] Make empty pending-notification reads read-only; update `last_seen` at a throttled interval.

### SQLite And Schema

- [ ] Replace ad hoc `CREATE TABLE IF NOT EXISTS` evolution plus a no-op version with ordered, transactional migrations.
- [ ] Test every migration from representative old schemas and on a copied production database before deployment.
- [ ] Split repositories by cohesive ownership: telemetry, prices/forecasts, settings/audit, incidents/notifications,
  reports/rollups, and migrations.
- [ ] Choose one connection policy. Remove cached dead-thread connections or manage a bounded pool with explicit thread
  lifecycle; keep transactions short.
- [ ] Configure WAL/synchronous/foreign keys once per connection and measure the cost of setting `journal_mode` on every
  open.
- [ ] Replace `INSERT OR REPLACE` with targeted `ON CONFLICT DO UPDATE` so updates do not delete/reinsert rows.
- [ ] Remove indexes duplicated by primary/unique constraints after verifying `PRAGMA index_list` and query plans.
- [ ] Add `(battery_name, timestamp_utc)` if report/retention plans prove it useful; do not add speculative indexes.
- [ ] Store UTC timestamps in one canonical format and use half-open ranges everywhere.
- [ ] Add CHECK constraints for enums, nonnegative cumulative energy, SOC range, resolution, and notification state
  where
  migrations can safely validate existing data.
- [ ] Run energy/log/notification retention as observable maintenance jobs. Delete in bounded batches to limit WAL and
  write-lock spikes.
- [ ] Schedule `PRAGMA optimize`, passive WAL checkpoints, integrity checks, and size monitoring at safe frequencies.
- [ ] Automate online backup, verification, rotation, free-space checks, and periodic restore drills.

### Reporting And Rollups

- [ ] Add canonical 15-minute telemetry rollups with source coverage/gap flags and daily billing/battery rollups.
- [ ] Rebuild rollups idempotently from raw data for a date range after tariff or algorithm changes.
- [ ] Bulk-load a report range once. Avoid one price/delta/peak query per day and 12 peak queries per day.
- [ ] Calculate year-to-date once and derive month/yesterday summaries from daily breakdowns.
- [ ] Cache tariff resolution by effective-date segment and pass one `TariffManager` through all calculations.
- [ ] Correct hourly/15-minute/DST solar-income math and active/mixed-contract display with golden billing fixtures.
- [ ] Distinguish measured battery benefit from an explicitly modeled counterfactual; document charging-source
  assumptions.
- [ ] Make plot size/DPI configurable, lower NAS defaults, use `fig.savefig`, and avoid copying image buffers more than
  needed.
- [ ] Lazy-import Matplotlib/report code only inside the reporting worker. Consider putting price training/prediction in
  the same bounded offline worker so scikit-learn is not resident in the control path.
- [ ] Keep process isolation and enforce timeout, RSS/CPU budget, single-flight status, and cleanup. The worker should
  return a small result record, not large frames.
- [ ] Cache a generated report by input version so repeated manual requests do not recompute identical ranges.

### Acceptance

- No retention DELETE occurs on the log insert path.
- Routine control ticks perform zero synchronous log-database writes.
- Logging remains bounded during a database outage and reports any dropped low-severity count.
- Notification polling with an empty queue does not start a write transaction every 15 seconds.
- Cached daily summary target is under 30 seconds on the NAS; hard ceiling is 2 minutes excluding SMTP variability.
- Report query count is bounded by range-independent bulk queries, not number of days times 12.
- Billing golden tests cover 15/30/60-minute prices, 23/24/25-hour days, contract changes, meter resets, and gaps.

## Phase 4: State, API, Dashboard, And Operator Efficiency

**Purpose:** Send only what changed and show the operator what is actually true.

### State And API

- [ ] Add `AppState.set_many()` for one lock, one coherent version, and one persistence batch.
- [ ] Add equality-aware `set_if_changed()` for status/config values. Sensor samples still change by timestamp; static
  `None` and identical command states should not bump versions.
- [ ] Avoid deep-copying an entire DataFrame/list for every scalar read. Keep immutable published snapshots and replace
  them atomically; expose narrow selectors for hot-path scalar values.
- [ ] Split state resources and versions:
  `/api/v1/state/live`, `/api/v1/prices`, `/api/v1/plan`, `/api/v1/health`, and paginated incidents/logs.
- [ ] Keep the live response small and include source freshness, desired/observed command status, summary status, and
  references/versions for prices and plan.
- [ ] Poll live state at 15 seconds initially; poll prices/plan only when their version changes or at a much slower
  rate.
- [ ] Return small update confirmations rather than the full state after every setting/incident action.
- [ ] Add pagination/cursors and server-side severity/source/time filters for logs/incidents; reduce the 20,000-log
  maximum.
- [ ] Add request body limits, input length limits, structured validation, correlation IDs, and consistent error
  schemas.
- [ ] Build an application factory instead of mutable module-global Flask security/DB state, enabling isolated tests and
  controlled shutdown.

### Dashboard

- [ ] Vendor the exact Vue production build locally and remove network startup dependency, `unsafe-eval`, and unneeded
  CSP allowances.
- [ ] Display the interval's `active_contract_type`; current price rendering always selects `dynamic`.
- [ ] Use each plan row's actual resolution/end, not a hard-coded 15 minutes, to highlight the active row.
- [ ] Show fresh/stale/unavailable state and last success for every source.
- [ ] Show desired, command in progress, last attempt, applied/observed state, and failure reason per controlled device.
- [ ] Show plan generated-at, valid-until, feature age, model version, fallback reason, and current rule explanation.
- [ ] Preserve notification preferences from the server; show queue/delivery status and clarify that browser polling is
  not push when the page is closed.
- [ ] Replace blocking `alert()` calls with accessible inline/toast errors and retain keyboard/focus/live-region
  support.
- [ ] Split the 1,236-line file into a few locally served modules only when doing so reduces test/edit cost. Do not add
  a
  large frontend toolchain solely for style.
- [ ] Add browser-level tests for auth, commands, reconnect/restart version reset, stale payloads, multi-tab edits,
  notification acknowledgement, active contract, and source freshness.

### Acceptance

- Representative live response is below 10 KB; plan and prices are not retransmitted on every sensor tick.
- A continuously open tab uses below 75 MiB/day after initial load, with a stretch target below 25 MiB/day.
- Dashboard works after disconnecting internet access.
- Operator can distinguish requested, sent, uncertain, failed, and observed command states.
- Logs and incidents remain responsive with realistic retained row counts.

## Phase 5: Forecast And Household Energy Efficiency

**Purpose:** Improve decisions only after data quality, evaluation, and command truth are reliable.

### Time And Data Quality

- [ ] Inject one configured IANA timezone/clock into all components. Remove server-local `astimezone()` assumptions,
  hard-coded `Europe/Brussels`, mixed `pytz`/`zoneinfo`, and wall-clock use for elapsed timers.
- [ ] Use monotonic time for retry, command-throttle, connection, and grace-period durations.
- [ ] Test local-day bounds for 23/24/25-hour days across API fetch, DB queries, forecast features, reports, plan
  generation,
  and dashboard display.
- [ ] Treat Elia API errors before interpreting empty `results`; validate schema, pagination/limit, timestamps,
  coverage,
  and fetched age.
- [ ] Use configured timezone for ENTSO-E auction availability; reject unknown resolutions rather than silently
  defaulting
  to 60 minutes.
- [ ] Redact security tokens and sensitive query parameters from debug URLs and stored logs.
- [ ] Add maximum interpolation gaps and coverage/confidence to consumption data. Do not interpolate across outages as
  if
  energy were observed.
- [ ] Handle Feb 29 and map historical consumption by local wall-clock interval. Prefer comparable weekdays/weeks and
  robust medians over the immediate previous three days alone.
- [ ] Provide a conservative fallback profile with low confidence for first install/no history instead of returning no
  forecast.

### Price Forecast

- [ ] Add local time-of-day/cyclical features; current model computes holiday but does not train on it and has no direct
  hour feature.
- [ ] Add only causally available features: lagged day-ahead prices, calendar, renewable/load forecasts, and possibly
  weather after measuring value.
- [ ] Use rolling time-series backtests against simple baselines (same-hour previous day/week, daily mean).
- [ ] Track MAE/RMSE and, more importantly, downstream tariff/optimizer regret by horizon and hour.
- [ ] Promote a model only if it beats the current production model/baseline on held-out periods and stays within memory
  and runtime budgets.
- [ ] Persist model and metadata atomically with schema version, library versions, config/features, training range,
  validation metrics, and checksum. Keep last-known-good rollback.
- [ ] Write all D+1..D+4 predictions in one transaction with generated/fetched/model metadata and an explicit expiry.
- [ ] Do not forward-fill forecast features across gaps beyond their allowed freshness.

### Control Policy Opportunities

- [ ] Optimize against the actual active contract and all tariff terms, including capacity-peak marginal cost.
- [ ] Add configurable battery degradation/throughput cost, reserve targets, quiet hours, backup reserve, and cycle
  budget.
- [ ] Model heterogeneous battery limits and SOC separately even if the gateway command is group-wide.
- [ ] Use actual battery power/permissions and EV power where available rather than deriving all effects from cumulative
  counters or current times 230 V.
- [ ] Add EV departure/target-SOC constraints if EVCC exposes them; allocate cheap/solar charging while honoring peak
  headroom.
- [ ] Forecast peak risk over the exact remaining capacity interval and pre-emptively coordinate EV, battery, and
  inverter
  rather than reacting after rolling thresholds.
- [ ] Quantify the cost of solar curtailment, negative buy/sell prices, round-trip losses, and battery wear in one
  unit-safe
  objective.
- [ ] Compare heuristic improvements with a bounded linear/MILP/MPC candidate only in offline replay. Adopt it only if
  savings justify dependency, runtime, and explainability cost.
- [ ] Add a reason code to every decision so dashboard and replay can explain the selected action.

### Acceptance

- Forecasts beat named simple baselines on held-out data and publish confidence/freshness.
- Every optimizer input/output has explicit units and invariant tests.
- Shadow replay reports EUR, kWh, peak, SOC, degradation, fallback, and command-count deltas.
- New policy reduces measured cost or peak risk without exceeding cycle, reserve, command-frequency, or tick budgets.

## Phase 6: Runtime, Configuration, Security, And Maintainability

**Purpose:** Reduce outage, deployment, and future-change cost.

### Runtime And Configuration

- [ ] Extend config validation to every consumed integration/reporting/prediction field and cross-field constraint.
- [ ] Add missing example fields used by production code: battery capacity/charge/discharge power, panel peak power,
  efficiencies/reserves, price-model settings, maintenance schedule, and notification retention.
- [ ] Stop passing the raw dict after producing `TypedAppConfig`; either use the typed object or remove the false type
  boundary.
- [ ] Fail startup when auth is enabled without credentials/secret. Permit an explicit development-only loopback mode,
  never implicit fail-open on `0.0.0.0`.
- [ ] Validate API command ranges from the same inverter/EVCC config used by clients rather than hard-coded 7000 W/6-32
  A.
- [ ] Validate tariff schema, effective-date coverage, active contract, units, and stale/missing future fixed tariffs at
  startup/reload; keep the previous valid tariff snapshot after a bad reload.
- [ ] Replace `Flask.app.run()` with an owned lightweight WSGI server that can stop accepting requests and shut down
  cleanly. Keep one worker/process for this workload unless load measurements disagree.
- [ ] Treat unexpected API death as failure/degraded service and return a supervisor-restart exit code.
- [ ] Bound scheduler shutdown; report jobs should cancel/terminate according to policy instead of blocking restart
  indefinitely.
- [ ] Add `/health/live` and `/health/ready` with scheduler heartbeat, control tick age, DB write/read, source health,
  report worker, disk/WAL, and backup status. Do not expose secrets.
- [ ] Test startup with every optional integration down, recovery after startup, graceful stop, forced worker timeout,
  database read-only/full, and supervisor restart.

### Security And Privacy

- [ ] Require password hashes by default, add login rate limiting/backoff and audit, rotate sessions on password/secret
  change, and shorten the default cookie lifetime.
- [ ] Set secure cookies automatically when behind HTTPS and document trusted proxy/origin handling. Add HSTS only at
  the
  HTTPS termination layer.
- [ ] Pin or locally trust device certificates instead of globally suppressing verification warnings where device
  support
  allows it.
- [ ] Keep secrets out of query-string logs, exception payloads, backups outside policy, process listings, and support
  bundles.
- [ ] Encrypt or otherwise protect backups at rest and document permissions for DB, `.env`, config, model, logs, and
  backup directories.
- [ ] Add dependency locking with hashes, reproducible Python/Node tooling, scheduled vulnerability/license audit,
  update
  automation, and an SBOM/release manifest. Do not claim a clean audit until it completes.
- [ ] Pin CI actions by commit SHA for higher supply-chain assurance if repository policy requires it.

### Code And Tests

- [ ] Split `db_handler.py`, `scheduled_tasks.py`, and `system_mediator.py` incrementally around tested ownership; avoid
  a
  behavior-changing rewrite.
- [ ] Remove unused duplicate `hec/models/models.py` after the import scan (current scan found no production imports).
- [ ] Remove dead standalone examples/commented implementations or move useful diagnostics to `tools/`.
- [ ] Replace module globals with injected runtime-owned services: state, DB repositories, clock, clients, summary
  manager, and Flask app.
- [ ] Add protocols/interfaces for clients and repositories so tests use small fakes instead of broad `MagicMock`s.
- [ ] Expand Ruff beyond undefined names in staged batches: import order, unused code, common bug rules, modernization,
  and security rules with documented exceptions.
- [ ] Add a type checker for high-risk boundaries first: config, models, device payloads, database rows, and commands.
- [ ] Add coverage reporting with risk-based thresholds, mutation/property tests for tariff/time/optimizer invariants,
  and
  integration tests for scheduler/database concurrency.
- [ ] Make time-dependent tests deterministic; the current auction-time test can skip based on wall clock.
- [ ] Declare/pin Node for dashboard behavior tests or replace the custom extraction harness with a small deterministic
  frontend test setup.
- [ ] Add Windows and Linux CI where process spawning, paths, timezone data, and shutdown differ; keep Python 3.13 as
  the
  deployed target until a support policy is documented.
- [ ] Add one local preflight command that installs/checks dev tools and runs format check, lint, types, tests,
  coverage,
  dependency audit, migration check, and config-example validation.
- [ ] Update docs when behavior lands. Correct current claims that retention is operationally active and migrations can
  repair old schemas before those guarantees are true.

### Acceptance

- The redacted example config passes full validation and can start a safe degraded app with fake/unavailable devices.
- Auth misconfiguration cannot expose a bound-all-interfaces API.
- Unexpected API exit and failed readiness trigger the documented supervisor behavior.
- Fresh checkout preflight is one command and CI/local results agree.
- Every migration, config option, API route, and operator action has an owner and test/documentation path.

## Full Improvement Register By Efficiency Dimension

This register captures smaller opportunities that should be folded into the phases above rather than opened as isolated
micro-optimizations.

### Runtime, CPU, And Memory

- Parameterize debug logging instead of eagerly building f-strings on hot paths.
- Reuse one `now` value per tick/rolling-average calculation instead of repeated wall-clock calls.
- Publish immutable state objects once and avoid copy-on-read for every scalar.
- Cache tariff results by target date/effective segment with explicit invalidation on reload.
- Batch prediction writes and cleanup once per refresh, not once per predicted day.
- Avoid repeated DataFrame `set_index().join().reset_index()` in a loop; join prepared forecast frames once after
  profiling.
- Remove no-op grouping in price training and handle empty frames before column access.
- Add Random Forest depth/leaf/memory limits selected by validation, not merely more trees/cores.
- Prefer one core for offline ML by default on the NAS; bound CPU affinity/nice priority if the platform supports it.
- Skip `gc.collect()` in a worker that is about to exit unless measurement proves it useful; retain cleanup for
  in-process
  fallback.
- Measure Matplotlib import/RSS and image peak memory; use smaller figures/DPI before content-hash caching.
- Keep optimizer loop vectorization deferred until profiler data shows it matters after query/I/O fixes.

### Disk And Database

- Use `VACUUM` only as planned offline maintenance after measuring free pages; never on a control path.
- Monitor WAL growth and long-lived readers; prevent abandoned API/report connections from blocking checkpoints.
- Bound log/incident message lengths and notification labels/tokens to prevent accidental database growth.
- Add report/cache lineage so stale rollups can be invalidated after tariff or algorithm changes.
- Preserve one pre-cutoff cumulative boundary row per meter/battery correctly and test reset/replacement cases.
- Replace correlated retention deletes with indexed key selection/batches when query plans show poor scaling.
- Add data-quality flags rather than fabricated EVCC or interpolated telemetry rows.
- Consider integer epoch or consistently fixed-width UTC text only after migration/query benchmarks; consistency matters
  more than representation.

### Network And Device Load

- Share persistent sessions per integration and close them; configure bounded pools appropriate to one host.
- Add conditional requests/compression for large API resources where Flask/reverse proxy measurements justify it.
- Use jitter on slow external fetches so restart/fleet timing does not hit APIs at exact common boundaries.
- Respect upstream retry headers and cache not-yet-published ENTSO-E state until the next valid retry time.
- Fetch Elia with encoded query parameters and explicit pagination/coverage validation.
- Avoid logging response bodies by default; cap/redact diagnostic samples.
- Poll dashboard notifications less often than live power or replace with an authenticated long poll only if simpler and
  measured better.

### Operator And Product

- Add a startup/config diagnostic page that names disabled integrations and exact remediation.
- Add command history and one-click retry only when reconciliation says retry is safe.
- Expose last good data instead of replacing it with `None`; label age and invalidity clearly.
- Show maintenance/backfill/report progress without flooding normal logs.
- Add downloadable privacy-safe diagnostics with config schema version, health, job metrics, and redacted recent errors.
- Add tariff/model/plan version visibility so an unexpected bill or action can be reconstructed.
- Add explicit maintenance mode that stops new control commands while allowing health, backup, and safe shutdown.

### Energy And Cost

- Track forecast-vs-actual solar, load, price, SOC, and EV energy by interval to identify the largest decision error.
- Optimize measurement quality before model complexity; missing/stale actuals make a smarter optimizer worse.
- Include inverter clipping, battery taper, phase/voltage reality, standby consumption, and gateway group constraints
  only
  when measured values justify them.
- Use command deadbands/hysteresis based on device wear, economic value, and minimum dwell time.
- Penalize excessive state changes and failed/uncertain commands in the objective.
- Report counterfactual savings with uncertainty bounds, not a single precise value unsupported by source attribution.
- Add tariff-change simulation and what-if replay before switching contract/control policy.
- Consider weather/occupancy/Home Assistant inputs only if privacy and measured forecast gain justify the integration.

## Test And Edge-Case Matrix

All behavior-changing batches must add the relevant cases before implementation:

- 0%, 1.9%, 2%, 95%, 98%, and 100% SOC; heterogeneous capacities; missing/invalid SOC; counter reset.
- EV pause below minimum, resume, unplug during pause, mode change, missing currents/session energy, and uncertain
  command.
- Inverter login expiry, rate limit, concurrent poll/write, timeout after write, night/day boundary, and read-back
  mismatch.
- Battery gateway unavailable, stale group data, partial battery telemetry, failed mode write, and recovery.
- Exact 15-minute peak windows with sparse/delayed samples, month rollover, ignore window, and long SMTP/DB latency.
- Price unavailable, fixed/dynamic/mixed contract, negative buy/sell, 15/30/60-minute resolutions, and publication
  delay.
- Spring/fall DST for fetch/query/forecast/plan/plot/report/dashboard; leap day and server timezone not Brussels.
- First install, no history, isolated gaps, long outage, meter reset, corrupt cached prediction/model, and stale
  features.
- Scheduler slow jobs, misfire, overlap attempt, coalescing, restart during command, restart during summary, and API
  death.
- SQLite lock, disk full, read-only, corrupt migration, large WAL, backup during writes, and restore from each supported
  schema.
- Auth missing secret/password, brute attempts, reverse proxy HTTPS/origin, multi-tab edit, session rotation, and
  oversized input.
- Notification fetch without display, display without acknowledgement, duplicate delivery, expired device, and queue
  overflow.
- Report no optional plot, SMTP failure, mixed contract, missing day, hourly/quarter-hour solar, and year/month/day
  parity.

## KPIs And Release Gates

| Dimension  | KPI                                              | Initial target                                                    |
|------------|--------------------------------------------------|-------------------------------------------------------------------|
| Control    | p95 coherent tick duration                       | Less than 5 seconds; no overlap at a 15-second interval.          |
| Freshness  | Automatic decisions using stale critical input   | 0; fallback reason recorded.                                      |
| Commands   | Desired/applied mismatch not surfaced            | 0.                                                                |
| Safety     | Ambiguous write automatically retried            | 0.                                                                |
| Energy     | Peak threshold violations                        | No regression in replay/live staged rollout.                      |
| Cost       | Replay/live bill objective                       | Candidate improves or matches baseline within uncertainty.        |
| Battery    | Invalid SOC/power transitions                    | 0; throughput/cycle budget explicit.                              |
| DB         | Synchronous routine log writes on control thread | 0.                                                                |
| DB         | Lock errors in concurrency stress                | 0 under accepted workload.                                        |
| Reporting  | Cached summary time                              | Less than 30 seconds target, 2-minute ceiling excluding SMTP.     |
| Reporting  | SQL statements                                   | Bounded bulk-query budget, not proportional to days times 12.     |
| Dashboard  | Representative live payload                      | Less than 10 KB.                                                  |
| Dashboard  | Transfer per day per open tab                    | Less than 75 MiB, stretch below 25 MiB.                           |
| Runtime    | Controller RSS/CPU                               | Baseline in Phase 0, then no regression without approved value.   |
| Recovery   | Transient device outage                          | Automatic recovery without process restart.                       |
| Operations | Last verified backup age                         | Visible and within configured objective.                          |
| Quality    | Tests/lint/types/audit                           | Deterministic green preflight; audit must complete, not time out. |

## Suggested Implementation Batches

1. **Safety tests:** C-01 through C-09 reproductions, stale-plan tests, command-outcome model, shadow replay harness.
2. **Local correctness fixes:** EV enum resume, zero SOC, failed battery-state persistence, peak restore, plan expiry.
3. **Command safety:** idempotent retry policy, per-device locks, read-back reconciliation, explicit job
   `max_instances=1`.
4. **Coherent tick:** merge duplicate polling and publish one observation snapshot before mediation.
5. **Write amplification:** queued/batched logs, explicit incidents, scheduled retention, equality-aware state updates.
6. **Dashboard payload:** split live/prices/plan resources, independent ETags, notification acknowledgement/throttling.
7. **Report query shape:** statement benchmark, daily rollups, year-once derivation, DST/resolution golden tests.
8. **Migration/maintenance:** real migration runner, index cleanup/addition from query plans,
   retention/checkpoint/backup jobs.
9. **Forecast quality:** time/data correctness, gap handling, baseline backtests, atomic model/cache metadata.
10. **Optimizer candidate:** unit-safe objective/constraints, heterogeneous battery model, degradation and peak cost in
    shadow.
11. **Runtime/config security:** full schema, safe example, fail-closed auth, owned WSGI lifecycle and health endpoints.
12. **Maintainability:** split oversized modules at new boundaries, remove duplicate models/dead code, expand
    tooling/CI/docs.

Each batch should be independently reviewable, should include before/after measurements, and should avoid unrelated
style
churn. Do not combine the optimizer behavior batch with scheduler, persistence, or UI refactors.

## Measure First Or Defer

- **PostgreSQL:** defer until SQLite still misses lock/latency budgets after write batching, short transactions,
  rollups,
  and maintenance.
- **SSE/WebSockets:** defer until split payload polling is measured. For one home, independent ETags may be simpler and
  cheaper. A notification long poll is a narrower option.
- **Microservices/message broker:** reject for now. Runtime-owned services and one bounded writer provide the needed
  ownership without NAS operations overhead.
- **Frontend framework/build rewrite:** reject for now. Vendor Vue and split a few modules only when edit/test cost
  merits
  it.
- **Broad optimizer vectorization:** defer. Correct units/objective/data and report I/O are higher-value; profile after.
- **More Random Forest cores/trees:** reject without validation. It competes with the controller and may not improve
  downstream decisions.
- **New ML/MILP/MPC stack:** evaluate only in offline replay against simple baselines and include
  dependency/RSS/solve-time
  cost.
- **Content-hash plot cache:** defer until rollups, lower DPI, and input-version report caching are measured.
- **Generic event bus/template/provider abstractions:** defer until a second real consumer/provider proves the boundary.
- **Same-process soft restart:** defer. An external supervisor remains simpler if the API server and workers shut down
  cleanly.

## Definition Of Done

The improvement program is successful when:

- control decisions use coherent, fresh, unit-valid inputs and have replayed safe fallbacks;
- desired, attempted, and observed device state are truthful and explainable;
- no routine log, SMTP operation, retention scan, report query, or backfill can block the control loop;
- the same device is polled once per interval and cannot receive concurrent commands;
- daily reports are DST/resolution correct, query-efficient, and bounded in time/memory;
- dashboard live traffic is small, local/offline-capable, and shows freshness and command outcomes;
- SQLite maintenance, migration, backup, restore, and retention are automatic, tested, and observable;
- forecast/optimizer changes demonstrate measured household value without peak, reserve, wear, or reliability
  regression;
- a safe redacted config, deterministic test suite, lint/types/coverage, completed dependency audit, and current docs
  make
  a fresh deployment and future change reproducible.
