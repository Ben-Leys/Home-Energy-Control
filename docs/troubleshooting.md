# Troubleshooting

## App Fails At Startup

Run from the repository root:

```powershell
python -m hec.main
```

If startup reports `Invalid configuration file`, compare `hec/config.yaml` with `hec/config.yaml.example`. The validator
checks the database path, scheduler timezone, historic data start date, location, API port, runtime restart code, and
HTTP defaults.

## Dashboard Login Fails

Check `hec/.env` for one of:

- `HEC_AUTH_PASSWORD`
- `HEC_AUTH_PASSWORD_HASH`

Also set `HEC_AUTH_COOKIE_SECRET` so sessions survive restarts. If the dashboard is behind HTTPS, set
`api_server.auth.secure_cookie: true`.

## Dashboard Values Revert

The Vue dashboard keeps pending edits locally until the backend confirms the canonical state. If a confirmed value later
changes back, inspect scheduler or mediator logs for a task that intentionally changed that state.

## Database Locked Or Read Only

See `docs/database-maintenance.md`. Confirm the NAS share allows SQLite WAL sidecar files next to the database:

- `home_energy.db-wal`
- `home_energy.db-shm`

Make sure only one HEC process is running.

## Forecast Or Daily Summary Is Slow

Check summary timing logs and the `summary_job_status` state in the dashboard. Normal dashboard-triggered summary
requests should queue work in the background instead of blocking the API request.

## Device API Fails

Check hostnames and local firewall rules first. HomeWizard battery endpoints often use self-signed HTTPS certificates;
use the device-specific `verify_tls: false` option only for those LAN devices. Shared HTTP retry and timeout defaults
are in the `http` config section.

## Restart Button Stops The App

This is expected with the supervised restart strategy. The app exits with `runtime.restart_exit_code`; the NAS supervisor
must start a fresh process.
