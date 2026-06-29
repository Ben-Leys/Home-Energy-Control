# Home Energy Control

Home Energy Control is a single-home energy controller for a local NAS or always-on PC. It coordinates day-ahead prices,
solar production, battery behavior, EVCC charging, the SMA inverter, and the HomeWizard P1 meter through a Flask API and
Vue dashboard.

## Quick Start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item hec\config.yaml.example hec\config.yaml
New-Item hec\.env -ItemType File
python -m unittest discover -s hec/tests
python -m hec.main
```

The dashboard is served by the main Flask app at `http://<host>:8123/` by default.

## Configuration

Runtime config is loaded from `hec/config.yaml`; secrets are loaded from `hec/.env`. Start from
`hec/config.yaml.example` and set these environment variables as needed:

- `HEC_AUTH_PASSWORD` or `HEC_AUTH_PASSWORD_HASH`
- `HEC_AUTH_COOKIE_SECRET`
- `ENTSOE_API_KEY`
- `P1_METER`
- `BATTERY_<NAME>`
- `GMAIL_SMTP_PASSWORD`

The app validates boot-critical config at startup, including database path, scheduler timezone, historic data start
date, location, API server port, runtime restart code, and shared HTTP defaults.

## Operations

- NAS deployment: `docs/nas-deployment.md`
- Device configuration: `docs/device-configuration.md`
- Backup and restore: `docs/database-maintenance.md`
- Troubleshooting: `docs/troubleshooting.md`
- Dependency updates and audit: `docs/dependency-management.md`
- Privacy and retention: `docs/privacy-data-retention.md`

## Development

```powershell
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m unittest discover -s hec/tests
python -m ruff check hec
python -m pip_audit -r requirements.txt
```

<img width="1021" height="1066" alt="image" src="https://github.com/user-attachments/assets/31d7ab37-bfc9-41a9-b4b1-6ecca21ecd63" />
