# Privacy And Data Retention

Home Energy Control stores sensitive household data: location, device hostnames, energy history, charging behavior,
email addresses, API tokens, and dashboard sessions.

## Secrets

Do not commit:

- `hec/.env`
- `hec/config.yaml`
- SQLite databases
- logs
- local backups

Keep API keys and passwords in `hec/.env`. Rotate `HEC_AUTH_PASSWORD` and `HEC_AUTH_COOKIE_SECRET` if a device with
dashboard access is lost.

## Local Scope

The app is designed for LAN or VPN access. It is not hardened for direct public internet exposure.

## Retention Defaults

SQLite retention is configured in `database`:

- `history_retention_days`: default 1095 days for core energy history
- `log_retention_hours`: default 72 hours for warning and error logs
- `info_debug_log_retention_hours`: default 12 hours for noisy low-severity logs

Predictions and rollups should be kept only as long as they are useful for dashboard and reporting workflows.

## Backups

Backups contain sensitive energy and location history. Store them on trusted devices only and delete old copies that are
outside the retention policy.

## Dashboard Notifications

Browser notification device tokens are local application identifiers. They are not push-provider credentials, but they
still identify a browser profile and should be treated as private operational data.
