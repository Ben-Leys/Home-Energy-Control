# NAS Deployment

This app is intended to run on a LAN or VPN reachable NAS, not on the public internet.

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item hec\config.yaml.example hec\config.yaml
New-Item hec\.env -ItemType File
```

Edit `hec/config.yaml` for local hosts, schedules, timezone, and database path. Put passwords, API tokens, and SMTP
passwords in `hec/.env`.

## Run

Run from the repository root so `hec` imports resolve:

```powershell
python -m hec.main
```

The Flask API and dashboard default to `http://0.0.0.0:8123/`.

## Supervised Restart

The dashboard restart command asks the runtime to shut down gracefully. Configure the NAS supervisor to restart the
process when it exits with `runtime.restart_exit_code`, which defaults to `75`.

Minimum supervisor behavior:

- Start command: `python -m hec.main`
- Working directory: repository root
- Restart on non-zero exit code, including `75`
- Preserve `hec/.env` and `hec/config.yaml` outside git updates
- Run one process only

## Updates

1. Stop the supervised service.
2. Back up the SQLite database and `hec/config.yaml`.
3. Update the repository.
4. Activate the virtual environment and run `python -m pip install -r requirements.txt`.
5. Run `python -m unittest discover -s hec/tests`.
6. Start the service again and confirm the dashboard loads.

## Network

Keep the app on LAN or VPN. If a reverse proxy is used, forward only the Flask dashboard/API port and preserve same-origin
headers so CSRF checks keep working.
