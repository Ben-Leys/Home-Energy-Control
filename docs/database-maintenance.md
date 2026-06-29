# Database Maintenance

Home Energy Control uses SQLite for the NAS-hosted single-home deployment. The app opens configured connections with
WAL mode, a busy timeout, foreign keys, and explicit transaction helpers so API and scheduler writes stay short and
predictable.

## Backup

Prefer SQLite's online backup command while the app may be running:

```powershell
sqlite3 hec\home_energy.db ".backup 'backups\home_energy-YYYY-MM-DD.sqlite'"
```

If `sqlite3` is not available, stop the app first, then copy the database file together with any active WAL artifacts:

```powershell
Copy-Item hec\home_energy.db backups\home_energy-YYYY-MM-DD.sqlite
Copy-Item hec\home_energy.db-wal backups\home_energy-YYYY-MM-DD.sqlite-wal -ErrorAction SilentlyContinue
Copy-Item hec\home_energy.db-shm backups\home_energy-YYYY-MM-DD.sqlite-shm -ErrorAction SilentlyContinue
```

Keep backups outside the repository. Database files, logs, `.env`, and `config.yaml` are intentionally ignored.

## Restore Test

Always test restore on a copy before replacing the live DB:

```powershell
Copy-Item backups\home_energy-YYYY-MM-DD.sqlite _scratch\restore-test.sqlite
sqlite3 _scratch\restore-test.sqlite "PRAGMA integrity_check;"
sqlite3 _scratch\restore-test.sqlite "SELECT version, description, applied_at_utc FROM schema_version;"
```

Expected output from `PRAGMA integrity_check;` is `ok`. The app's migrations are idempotent, so starting against a
restored copy should not duplicate rows in `schema_version`.

## Restore

1. Stop the app or NAS supervisor service.
2. Move the current live database aside instead of deleting it.
3. Copy the verified backup into the configured database path.
4. Start the app and check the dashboard plus logs.

Example:

```powershell
Move-Item hec\home_energy.db backups\home_energy-before-restore.sqlite
Copy-Item backups\home_energy-YYYY-MM-DD.sqlite hec\home_energy.db
python -m unittest discover -s hec/tests
```

## Retention

Energy history retention is separate from log retention.

- Energy and forecast history defaults to 3 years.
- Cumulative meter-style tables preserve the newest row before the cutoff, so delta calculations still have a boundary
  reading.
- Logs keep `INFO` and `DEBUG` records for 12 hours by default and other records for 72 hours by default.
- Predicted prices still use their shorter operational cleanup path because they are cache data, not long-term history.

The retention helpers are safe to run repeatedly. Run them on a copied database first when changing the retention
window or recovering from a corrupted timestamp.

## Lock Handling Notes

SQLite still allows only one writer at a time. WAL mode lets readers continue while a writer commits, and the configured
busy timeout lets short scheduler/API write conflicts wait instead of failing immediately with `database is locked`.
Long report reads should still stay outside write transactions.
