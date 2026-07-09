# database_ops/db_handler.py
from contextlib import contextmanager
import hashlib
import json
import logging
import sqlite3
import threading
from time import sleep
from datetime import datetime, timezone, timedelta, time, date
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict, Any, Iterator
from zoneinfo import ZoneInfo

import pandas as pd
import pytz

from hec.core import constants as c

from hec.core.models import PricePoint, NetElectricityPriceInterval

logger = logging.getLogger(__name__)

CURRENT_SCHEMA_VERSION = 1
INITIAL_SCHEMA_MIGRATION_DESCRIPTION = "001_noop_initial_schema"

INCIDENT_ACTIVE = "active"
INCIDENT_ACKNOWLEDGED = "acknowledged"
INCIDENT_RESOLVED = "resolved"
INCIDENT_STATUSES = {INCIDENT_ACTIVE, INCIDENT_ACKNOWLEDGED, INCIDENT_RESOLVED}
INCIDENT_SEVERITIES = {"warning", "error"}
NOTIFICATION_TYPES = {"warning", "error", "peak_consumption"}

ENERGY_HISTORY_RETENTION_TABLES = {
    "belpex_da_prices": ("timestamp_utc", False, None),
    "elia_open_data": ("timestamp_utc", False, None),
    "p1_meter_log": ("timestamp_utc", True, None),
    "inverter_log": ("timestamp_utc", True, None),
    "battery_log": ("timestamp_utc", True, "battery_name"),
    "evcc_log": ("timestamp_utc", False, None),
}


class DatabaseHandler:
    def __init__(self, db_config: dict):
        db_path_str = db_config.get("path")
        if not db_path_str:
            raise ValueError("Database path is not specified in the configuration.")

        project_root = Path(__file__).resolve().parent.parent
        configured_path = Path(db_path_str)
        self.db_path = configured_path if configured_path.is_absolute() else project_root / configured_path
        self.busy_timeout_ms = int(db_config.get("busy_timeout_ms", 10000))
        self.history_retention_days = int(db_config.get("history_retention_days", 365 * 3))
        self.log_retention_hours = int(db_config.get("log_retention_hours", 72))
        self.info_debug_log_retention_hours = int(db_config.get("info_debug_log_retention_hours", 12))
        self.conn: Optional[sqlite3.Connection] = None
        self._connection_lock = threading.Lock()
        self._thread_connections: Dict[int, sqlite3.Connection] = {}
        logger.info(f"Database handler initialized for SQLite DB at: {self.db_path}")

    def _open_connection(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            self.db_path,
            timeout=self.busy_timeout_ms / 1000,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        self._configure_connection(conn)
        return conn

    def _configure_connection(self, conn: sqlite3.Connection) -> None:
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        """Yield a short-lived SQLite connection configured for this application."""
        conn = self._open_connection()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """Run SQL in an explicit transaction with predictable commit or rollback."""
        with self.connection() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _get_connection(self) -> sqlite3.Connection:
        """Returns a configured SQLite connection scoped to the current thread."""
        thread_id = threading.get_ident()
        with self._connection_lock:
            conn = self._thread_connections.get(thread_id)
            if conn is None or self._is_connection_closed(conn):
                try:
                    conn = self._open_connection()
                    self._thread_connections[thread_id] = conn
                    if thread_id == threading.main_thread().ident:
                        self.conn = conn
                    logger.info(f"Successfully connected to SQLite database: {self.db_path}")
                except sqlite3.Error as e:
                    logger.error(f"Error connecting to SQLite database {self.db_path}: {e}", exc_info=True)
                    raise
            return conn

    def _is_connection_closed(self, conn: Optional[sqlite3.Connection] = None) -> bool:
        """Checks if the connection is closed or unusable."""
        conn = conn or self.conn
        if conn is None:
            return True
        try:
            conn.execute("SELECT 1").fetchone()
            return False
        except sqlite3.ProgrammingError:
            return True
        except sqlite3.OperationalError:
            return True

    def close_connection(self):
        with self._connection_lock:
            connections = list(self._thread_connections.values())
            self._thread_connections.clear()
            self.conn = None

        seen_connection_ids = set()
        for conn in connections:
            if id(conn) in seen_connection_ids:
                continue
            seen_connection_ids.add(id(conn))
            conn.close()

        if connections:
            logger.info("SQLite database connection(s) closed.")

    def close_current_thread_connection(self):
        """Close the SQLite connection cached for the calling thread, if any."""
        thread_id = threading.get_ident()
        with self._connection_lock:
            conn = self._thread_connections.pop(thread_id, None)
            if conn is not None and conn is self.conn:
                self.conn = None

        if conn is not None:
            conn.close()

    def initialize_database(self):
        """Creates necessary tables if they don't exist."""
        try:
            with self.transaction(immediate=False) as conn:
                cursor = conn.cursor()

                # --- Day-Ahead Price Forecasts Table ---
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS belpex_da_prices (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp_utc TEXT NOT NULL,
                        price_eur_per_mwh REAL NOT NULL,
                        resolution_minutes INTEGER NOT NULL,  -- 15, 30, 60
                        fetched_at_utc TEXT NOT NULL,  -- When this data was retrieved
                        source_api TEXT DEFAULT 'ENTSO-E',
                        UNIQUE (timestamp_utc, resolution_minutes) -- Ensure no duplicate entries
                    );
                """)
                logger.info("Table belpex_da_prices checked/created.")

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_price_timestamp_utc 
                    ON belpex_da_prices (timestamp_utc);
                """)
                logger.info("Index idx_price_timestamp_utc checked/created.")

                # --- P1 Meter Log Table ---
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS p1_meter_log (
                                    timestamp_utc TEXT PRIMARY KEY,
                                    wifi_strength INTEGER,
                                    smr_version TEXT,
                                    active_tariff INTEGER,
                                    total_power_import_kwh REAL,
                                    total_power_import_t1_kwh REAL,
                                    total_power_import_t2_kwh REAL,
                                    total_power_export_kwh REAL,
                                    total_power_export_t1_kwh REAL,
                                    total_power_export_t2_kwh REAL,
                                    active_power_w REAL,
                                    active_power_l1_w REAL,
                                    active_power_l2_w REAL,
                                    active_power_l3_w REAL,
                                    active_voltage_l1_v REAL,
                                    active_voltage_l2_v REAL,
                                    active_voltage_l3_v REAL,
                                    active_current_l1_a REAL,
                                    active_current_l2_a REAL,
                                    active_current_l3_a REAL,
                                    active_power_average_w REAL,
                                    monthly_power_peak_w REAL,
                                    monthly_power_peak_timestamp TEXT
                                );
                            """)
                logger.info("Table p1_meter_log checked/created.")

                # --- Elia Open Data Table ---
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS elia_open_data (
                                    timestamp_utc TEXT NOT NULL,
                                    forecast_type TEXT NOT NULL,  -- solar, wind, grid_load
                                    resolution_minutes INTEGER NOT NULL,  -- 15, 60
                                    most_recent_forecast_mwh REAL,  -- For solar/wind/load
                                    monitored_capacity_mw REAL,  -- For solar/wind
                                    fetched_at_utc TEXT NOT NULL,  -- When this data was retrieved
                                    PRIMARY KEY (timestamp_utc, forecast_type, resolution_minutes) -- Unique
                                );
                            """)
                logger.info("Table elia_open_data checked/created.")

                cursor.execute("""
                                CREATE INDEX IF NOT EXISTS idx_elia_forecast_type_ts 
                                ON elia_open_data (forecast_type, timestamp_utc);
                            """)
                logger.info("Index idx_elia_forecast_type_ts checked/created.")

                # --- Inverter Log Table ---
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS inverter_log (
                                    timestamp_utc TEXT PRIMARY KEY,
                                    operational_status TEXT NOT NULL,
                                    pv_power_watts REAL,
                                    daily_yield_wh REAL,
                                    total_yield_wh REAL,
                                    active_power_limit_watts REAL
                                );
                            """)
                logger.info("Table 'inverter_log' checked/created.")

                # --- Battery Log Table ---
                cursor.execute("""
                            CREATE TABLE IF NOT EXISTS battery_log (
                                timestamp_utc TEXT,
                                battery_name TEXT,
                                energy_import_kwh REAL,
                                energy_export_kwh REAL,
                                state_of_charge_pct REAL,
                                battery_mode TEXT,
                                cycles REAL,
                                PRIMARY KEY (timestamp_utc, battery_name)
                            );
                        """)
                logger.info("Table 'battery_log' checked/created.")

                # --- Settings Table ---
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS app_settings (
                                    setting_key TEXT PRIMARY KEY,
                                    setting_value TEXT NOT NULL,
                                    value_type TEXT NOT NULL,
                                    last_updated_utc TEXT NOT NULL
                                );
                                """)
                logger.info("Table 'app_settings' checked/created.")

                # --- Logs Table ---
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS logs (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    timestamp TEXT NOT NULL,
                                    level TEXT NOT NULL,
                                    module TEXT NOT NULL,
                                    message TEXT NOT NULL
                               );
                               """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp);")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_level ON logs (level);")
                logger.info("Table 'logs' and indexes checked/created.")

                # --- Persistent Incident Table ---
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS app_incidents (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    severity TEXT NOT NULL,
                                    source TEXT NOT NULL,
                                    message TEXT NOT NULL,
                                    fingerprint TEXT NOT NULL UNIQUE,
                                    notification_type TEXT NOT NULL,
                                    first_seen_utc TEXT NOT NULL,
                                    last_seen_utc TEXT NOT NULL,
                                    occurrence_count INTEGER NOT NULL DEFAULT 1,
                                    status TEXT NOT NULL DEFAULT 'active',
                                    acknowledged_at_utc TEXT,
                                    acknowledged_by TEXT,
                                    resolved_at_utc TEXT,
                                    last_notified_utc TEXT
                               );
                               """)
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_app_incidents_status_severity "
                    "ON app_incidents (status, severity, last_seen_utc);"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_app_incidents_fingerprint "
                    "ON app_incidents (fingerprint);"
                )
                logger.info("Table 'app_incidents' and indexes checked/created.")

                # --- Dashboard Notification Device Tables ---
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS notification_devices (
                                    device_token TEXT PRIMARY KEY,
                                    label TEXT NOT NULL,
                                    enabled INTEGER NOT NULL DEFAULT 1,
                                    notify_warning INTEGER NOT NULL DEFAULT 0,
                                    notify_error INTEGER NOT NULL DEFAULT 0,
                                    notify_peak_consumption INTEGER NOT NULL DEFAULT 0,
                                    activated_at_utc TEXT NOT NULL,
                                    last_seen_utc TEXT NOT NULL
                                );
                                """)
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS notification_queue (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    device_token TEXT NOT NULL,
                                    notification_type TEXT NOT NULL,
                                    incident_id INTEGER,
                                    title TEXT NOT NULL,
                                    message TEXT NOT NULL,
                                    created_at_utc TEXT NOT NULL,
                                    delivered_at_utc TEXT,
                                    dedupe_key TEXT NOT NULL,
                                    UNIQUE (device_token, dedupe_key),
                                    FOREIGN KEY (device_token) REFERENCES notification_devices(device_token)
                                        ON DELETE CASCADE
                                );
                                """)
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_notification_queue_device_pending "
                    "ON notification_queue (device_token, delivered_at_utc, id);"
                )
                logger.info("Notification device and queue tables checked/created.")

                # --- Predicted Prices Table ---
                cursor.execute("""
                               CREATE TABLE IF NOT EXISTS predicted_prices
                               (
                                   timestamp_utc             TEXT PRIMARY KEY,
                                   predicted_gross_price_kwh REAL,
                                   solar_factor              REAL,
                                   wind_factor               REAL,
                                   grid_load_mwh             REAL
                               );
                               """)
                logger.info("Table predicted_prices checked/created.")

                # --- EVCC Charging Log Table ---
                cursor.execute("""
                                CREATE TABLE IF NOT EXISTS evcc_log (
                                    timestamp_utc TEXT NOT NULL,
                                    session_energy REAL NOT NULL,
                                    energy_delta REAL,
                                    UNIQUE (timestamp_utc)
                                );
                            """)
                logger.info("Table evcc_log checked/created.")

                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_p1_meter_log_timestamp "
                    "ON p1_meter_log (timestamp_utc);"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_inverter_log_timestamp "
                    "ON inverter_log (timestamp_utc);"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_battery_log_timestamp_name "
                    "ON battery_log (timestamp_utc, battery_name);"
                )
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_evcc_log_timestamp ON evcc_log (timestamp_utc);")
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_predicted_prices_timestamp "
                    "ON predicted_prices (timestamp_utc);"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_belpex_da_prices_timestamp_resolution "
                    "ON belpex_da_prices (timestamp_utc, resolution_minutes);"
                )
                logger.info("Common report and retention indexes checked/created.")

                self._run_migrations(conn)

        except sqlite3.Error as e:
            logger.error(f"Error initializing database tables: {e}", exc_info=True)
            raise
        finally:
            pass

    def _run_migrations(self, conn: sqlite3.Connection) -> None:
        """Create schema version tracking and apply idempotent migrations."""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY,
                description TEXT NOT NULL,
                applied_at_utc TEXT NOT NULL
            );
            """
        )

        row = conn.execute(
            "SELECT version FROM schema_version WHERE version = ?",
            (CURRENT_SCHEMA_VERSION,),
        ).fetchone()
        if row:
            return

        conn.execute(
            """
            INSERT INTO schema_version (version, description, applied_at_utc)
            VALUES (?, ?, ?)
            """,
            (
                CURRENT_SCHEMA_VERSION,
                INITIAL_SCHEMA_MIGRATION_DESCRIPTION,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        logger.info(
            "Applied database schema migration %s: %s",
            CURRENT_SCHEMA_VERSION,
            INITIAL_SCHEMA_MIGRATION_DESCRIPTION,
        )

    def store_da_prices(self, price_points: List[PricePoint]) -> int:
        """
        Stores a list of PricePoint objects into the database.
        Returns the number of rows inserted.
        """
        if not price_points:
            return 0

        rows_to_insert = []
        now_utc_str = datetime.now(timezone.utc).isoformat()

        for pp in price_points:
            rows_to_insert.append((
                pp.timestamp_utc.isoformat(),
                pp.price_eur_per_mwh,
                pp.resolution_minutes,
                now_utc_str
            ))

        inserted_count = 0
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                sql = """
                INSERT OR REPLACE INTO belpex_da_prices 
                (timestamp_utc, price_eur_per_mwh, resolution_minutes, fetched_at_utc)
                VALUES (?, ?, ?, ?);
            """
                cursor.executemany(sql, rows_to_insert)
                inserted_count = cursor.rowcount if cursor.rowcount >= 0 else len(rows_to_insert)

                if inserted_count > 0:
                    logger.info(
                        f"Successfully stored or updated {inserted_count} price points in the database.")
                else:
                    logger.info(f"No new price points stored.")

        except sqlite3.Error as e:
            logger.error(f"Error storing price forecasts in database: {e}", exc_info=True)

        return inserted_count

    def get_da_prices(self, target_day_local: datetime, day_end_local: datetime = None) -> List[PricePoint]:
        """
        Retrieves price forecasts for a specific local day from the database.
        Returns a list of PricePoint objects.
        """
        results = []
        local_tz = target_day_local.tzinfo or datetime.now().astimezone().tzinfo

        day_start_local = datetime.combine(target_day_local.date(), time.min, local_tz)
        if not day_end_local:
            day_end_local = day_start_local + timedelta(days=1)

        # Convert local day boundaries to UTC strings for querying
        day_start_utc_str = day_start_local.astimezone(timezone.utc).isoformat()
        day_end_utc_str = day_end_local.astimezone(timezone.utc).isoformat()

        logger.debug(f"Querying DB for prices between {day_start_utc_str} and {day_end_utc_str}")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = """
            SELECT timestamp_utc, price_eur_per_mwh, resolution_minutes
            FROM belpex_da_prices
            WHERE timestamp_utc >= ? 
              AND timestamp_utc < ? 
            ORDER BY timestamp_utc;
        """
            cursor.execute(sql, (day_start_utc_str, day_end_utc_str))
            rows = cursor.fetchall()

            for row in rows:
                # timestamp_utc is stored as TEXT, convert back to datetime
                ts_utc = datetime.fromisoformat(row["timestamp_utc"])
                results.append(PricePoint(
                    timestamp_utc=ts_utc,
                    price_eur_per_mwh=row["price_eur_per_mwh"],
                    position=0,  # Has no added value when retrieving
                    resolution_minutes=row["resolution_minutes"]
                ))
        except sqlite3.Error as e:
            logger.error(f"Error retrieving price forecasts from database: {e}", exc_info=True)

        return results

    def store_p1_meter_data(self, p1_data: Dict[str, Any]) -> bool:
        """
        Stores a single P1 meter data record unconditionally.
        Returns True if the DB insert/update affected rows, False on failure.
        """
        if not p1_data or "timestamp_utc_iso" not in p1_data:
            logger.warning("P1 Meter: Missing timestamp_utc_iso for DB storage.")
            return False

        # Map API keys to DB columns
        values = (
            p1_data.get('timestamp_utc_iso'),
            p1_data.get('wifi_strength'),
            str(p1_data.get('smr_version', '')),  # Ensure SMR version is string
            p1_data.get('active_tariff'),
            p1_data.get('total_power_import_kwh'),
            p1_data.get('total_power_import_t1_kwh'),
            p1_data.get('total_power_import_t2_kwh'),
            p1_data.get('total_power_export_kwh'),
            p1_data.get('total_power_export_t1_kwh'),
            p1_data.get('total_power_export_t2_kwh'),
            p1_data.get('active_power_w'),
            p1_data.get('active_power_l1_w'),
            p1_data.get('active_power_l2_w'),
            p1_data.get('active_power_l3_w'),
            p1_data.get('active_voltage_l1_v'),
            p1_data.get('active_voltage_l2_v'),
            p1_data.get('active_voltage_l3_v'),
            p1_data.get('active_current_l1_a'),
            p1_data.get('active_current_l2_a'),
            p1_data.get('active_current_l3_a'),
            p1_data.get('active_power_average_w'),
            p1_data.get('montly_power_peak_w'),  # P1_meter spelling error
            p1_data.get('montly_power_peak_timestamp')  # P1_meter spelling error
        )

        sql = """
            INSERT OR REPLACE INTO p1_meter_log (
                timestamp_utc, wifi_strength, smr_version, active_tariff,
                total_power_import_kwh, total_power_import_t1_kwh, total_power_import_t2_kwh,
                total_power_export_kwh, total_power_export_t1_kwh, total_power_export_t2_kwh,
                active_power_w, active_power_l1_w, active_power_l2_w, active_power_l3_w,
                active_voltage_l1_v, active_voltage_l2_v, active_voltage_l3_v,
                active_current_l1_a, active_current_l2_a, active_current_l3_a,
                active_power_average_w, monthly_power_peak_w, monthly_power_peak_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); 
        """

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, values)

                if cursor.rowcount > 0:
                    logger.debug(f"P1 Meter: Successfully stored data for timestamp {p1_data['timestamp_utc_iso']} in DB.")
                    return True
                else:
                    logger.warning(f"P1 Meter: Data for timestamp {p1_data['timestamp_utc_iso']} was not stored.")
                    return False
        except sqlite3.Error as e:
            logger.error(f"P1 Meter: Error storing data in database: {e}", exc_info=True)
            return False

    def get_energy_deltas_for_period(self, start_date: date, end_date: date) -> Optional[Dict[str, Any]]:
        """
        Calculate total power imported and exported between the start_date_str
        and the end of end_date_str (local time).

        It finds the closest P1 meter reading on or before the start of the period,
        and the closest reading on or after the end of the period, then calculates the delta.

        Args:
            start_date (date): Start date for calculation (local).
            end_date (date): End date for calculation (local, inclusive).

        Returns:
            Optional[Dict[str, Any]]: A dictionary with 'total_power_imported_kwh',
                                      'total_power_exported_kwh', 'actual_start_timestamp_utc',
                                      'actual_end_timestamp_utc', and 'duration_seconds',
                                      or None if data is insufficient.
        """
        # Define the period in local time, then convert to UTC. Add 150 seconds as 300 seconds between meter readings.
        local_tz = datetime.now().astimezone().tzinfo
        period_start_local = datetime.combine(start_date, datetime.min.time(), tzinfo=local_tz) + timedelta(seconds=150)
        period_end_local = datetime.combine(end_date, datetime.max.time(), tzinfo=local_tz) + timedelta(seconds=150)

        period_start_utc_iso = period_start_local.astimezone(timezone.utc).isoformat()
        period_end_utc_iso = period_end_local.astimezone(timezone.utc).isoformat()

        logger.debug(f"Calculating P1 deltas for local period: {start_date} to {end_date}")
        logger.debug(f"Corresponds to UTC range for query: {period_start_utc_iso} to {period_end_utc_iso}")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Get the last reading on or before the start of the period_start_local
            cursor.execute(
                """
                SELECT total_power_import_kwh, total_power_export_kwh, timestamp_utc
                FROM p1_meter_log
                WHERE timestamp_utc <= ?
                ORDER BY timestamp_utc DESC 
                LIMIT 1
                """,
                (period_start_utc_iso,)
            )
            start_boundary_row = cursor.fetchone()

            if not start_boundary_row:
                logger.warning(f"P1 Meter deltas: No data found on or before {period_start_utc_iso}.")
                return None

            start_import_val, start_export_val, actual_start_ts_str = start_boundary_row
            logger.debug(f"Start boundary record: Import={start_import_val}, "
                         f"Export={start_export_val}, TS_UTC={actual_start_ts_str}")

            # Get the first reading on or after the end of period_end_local
            cursor.execute(
                """
                SELECT total_power_import_kwh, total_power_export_kwh, timestamp_utc
                FROM p1_meter_log
                WHERE timestamp_utc <= ? 
                ORDER BY timestamp_utc DESC
                LIMIT 1
                """,
                (period_end_utc_iso,)
            )
            end_boundary_row = cursor.fetchone()

            # If no reading found by the end of the period, try to find the very next one available after.
            if not end_boundary_row:
                logger.info(
                    f"No P1 reading found by end of period ({period_end_utc_iso}). Looking for next available after.")
                cursor.execute(
                    """
                    SELECT total_power_import_kwh, total_power_export_kwh, timestamp_utc
                    FROM p1_meter_log
                    WHERE timestamp_utc > ? 
                    ORDER BY timestamp_utc 
                    LIMIT 1
                    """,
                    (period_end_utc_iso,)
                )
                end_boundary_row = cursor.fetchone()

            if not end_boundary_row:
                logger.warning(f"P1 Meter deltas: No data found on or after {period_end_utc_iso}.")
                return None

            end_import_val, end_export_val, actual_end_ts_str = end_boundary_row
            logger.debug(f"End boundary record: Import={end_import_val}, "
                         f"Export={end_export_val}, TS_UTC={actual_end_ts_str}")

            # Ensure valid numbers
            if start_import_val is None or start_export_val is None or \
                    end_import_val is None or end_export_val is None:
                logger.error("P1 Meter deltas: Null values found for import/export totals in boundary records.")
                return None

            # Ensure the end timestamp is actually after the start timestamp
            actual_start_dt = datetime.fromisoformat(actual_start_ts_str)
            actual_end_dt = datetime.fromisoformat(actual_end_ts_str)

            if actual_end_dt <= actual_start_dt:
                logger.warning(
                    f"P1 Meter deltas: End time ({actual_end_ts_str}) is not after start time ({actual_start_ts_str}). "
                    "This might happen if query range is too small for available data. No delta calculated.")
                return None

            total_power_imported_kwh = round(end_import_val - start_import_val, 3)
            total_power_exported_kwh = round(end_export_val - start_export_val, 3)

            duration_seconds = int((actual_end_dt - actual_start_dt).total_seconds())

            # Sanity check: if import/export decreased, it means meter reset or bad data.
            if total_power_imported_kwh < 0:
                logger.warning(f"P1 Meter deltas: Calculated negative import ({total_power_imported_kwh} kWh). "
                               f"Start Import: {start_import_val} @ {actual_start_ts_str}, "
                               f"End Import: {end_import_val} @ {actual_end_ts_str}. "
                               "This might indicate a meter reset or data issue. Reporting as 0 for this period.")
                total_power_imported_kwh = 0.0

            if total_power_exported_kwh < 0:
                logger.warning(f"P1 Meter deltas: Calculated negative export ({total_power_exported_kwh} kWh). "
                               f"Start Export: {start_export_val} @ {actual_start_ts_str}, "
                               f"End Export: {end_export_val} @ {actual_end_ts_str}. "
                               "This might indicate a meter reset or data issue. Reporting as 0 for this period.")
                total_power_exported_kwh = 0.0

            return {
                "total_power_imported_kwh": total_power_imported_kwh,
                "total_power_exported_kwh": total_power_exported_kwh,
                "actual_start_timestamp_utc": actual_start_ts_str,
                "actual_end_timestamp_utc": actual_end_ts_str,
                "duration_seconds": duration_seconds
            }

        except sqlite3.Error as e:
            logger.error(f"P1 Meter deltas: DB error for range {start_date}-{end_date}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"P1 Meter deltas: Unexpected error for range {start_date}-{end_date}: {e}",
                         exc_info=True)
            return None

    def get_battery_deltas_for_intervals(self, ivs: List['NetElectricityPriceInterval']) \
            -> Dict[str, Dict[str, Dict[str, float]]]:
        """
        For each NetElectricityPriceInterval, computes the net energy deltas (imported/exported)
        for ALL batteries in the log. The deltas are prorated to match the 15-minute resolution
        of the intervals.

        Returns a mapping from battery_name to:
            {interval_start_local.isoformat(): {"imported_kwh": float, "exported_kwh": float}}
        """
        if not ivs:
            return {}

        # Determine overall UTC window (with 150-second buffer)
        ivs = sorted(ivs, key=lambda i: i.interval_start_local)
        first_start = ivs[0].interval_start_local
        last_end = ivs[-1].interval_start_local + timedelta(minutes=ivs[-1].resolution_minutes)

        buf = timedelta(seconds=150)
        window_start_utc = (first_start - buf).astimezone(timezone.utc).isoformat()
        window_end_utc = (last_end + buf).astimezone(timezone.utc).isoformat()

        conn = self._get_connection()
        cursor = conn.cursor()

        # --- 1. Identify all batteries in the time window ---
        cursor.execute("""
            SELECT DISTINCT battery_name
            FROM battery_log
            WHERE timestamp_utc >= ? AND timestamp_utc <= ?
        """, (window_start_utc, window_end_utc))
        battery_names = [row[0] for row in cursor.fetchall()]

        if not battery_names:
            return {}

        # --- 2. Fetch all readings for all batteries ---
        # The ORDER BY is critical for the later iteration logic
        cursor.execute(f"""
            SELECT timestamp_utc, battery_name, energy_import_kwh, energy_export_kwh
            FROM battery_log
            WHERE timestamp_utc >= ? AND timestamp_utc <= ?
            ORDER BY battery_name, timestamp_utc
        """, (window_start_utc, window_end_utc))
        all_rows = cursor.fetchall()

        if not all_rows:
            return {}

        # --- 3. Group readings by battery_name ---
        battery_readings: Dict[str, List[tuple]] = {name: [] for name in battery_names}
        for ts_str, name, imp, exp in all_rows:
            dt = datetime.fromisoformat(ts_str)
            battery_readings[name].append((dt, imp, exp))

        # --- 4. Process Deltas for Each Battery ---
        final_result: Dict[str, Dict[str, Dict[str, float]]] = {name: {} for name in battery_names}

        for battery_name, readings in battery_readings.items():
            if len(readings) < 2:
                logger.debug(f"Skipping {battery_name}: less than 2 readings found in window.")
                continue

            # The logic iterates through the readings and  prorates the deltas to the 15-minute intervals (ivs).
            # Readings structure: (dt, import, export)

            # Index to track position in the readings list
            read_idx = 0
            n = len(readings)

            for iv in ivs:
                iv_start_utc = iv.interval_start_local.astimezone(timezone.utc)
                iv_end_utc = (iv.interval_start_local + timedelta(minutes=iv.resolution_minutes)).astimezone(
                    timezone.utc)

                # Find the two closest readings that bracket the interval window (iv_start_utc to iv_end_utc)

                # 1. Find the starting reading: last reading <= iv_start_utc
                # Rewind read_idx to the starting point for this interval search
                start_idx = read_idx

                # Find the start point (ts_start <= iv_start_utc)
                while start_idx < n and readings[start_idx][0] <= iv_start_utc + timedelta(seconds=60):
                    start_idx += 1

                # The start reading is the one BEFORE the loop broke
                start_idx = max(0, start_idx - 1)

                if start_idx >= n:
                    # No reading at or before interval start (shouldn't happen with the buffer, but safe check)
                    continue

                ts_start, imp_start, exp_start = readings[start_idx]

                if imp_start is None or exp_start is None:
                    continue

                # 2. Find the end reading: first reading >= iv_end_utc
                end_idx = start_idx
                while end_idx < n and readings[end_idx][0] < iv_end_utc:
                    end_idx += 1

                if end_idx >= n:
                    # No reading at or after interval end—use the last available reading
                    ts_end, imp_end, exp_end = readings[-1]
                    end_idx = n - 1
                else:
                    ts_end, imp_end, exp_end = readings[end_idx]

                if imp_end is None or exp_end is None:
                    continue

                # The actual duration covered by these two readings (in seconds)
                actual_duration_sec = (ts_end - ts_start).total_seconds()

                # The interval duration (15 minutes = 900 seconds)
                target_duration_sec = iv.resolution_minutes * 60

                # If the actual duration is too short or zero, skip (avoids division by zero and bad data)
                # We enforce a minimum reading frequency here (e.g., at least 1 reading per hour)
                if actual_duration_sec < 60:  # Less than 1 minute of data between points
                    # If the points are too close, it often means the same timestamp or bad data.
                    continue

                # Compute gross deltas
                gross_imported = max(0.0, imp_end - imp_start)
                gross_exported = max(0.0, exp_end - exp_start)

                # --- Proration Logic ---
                # Determine the fraction of the actual duration that overlaps with the target interval.
                # We need to compute the change over a 15-minute period from the change over the actual duration.

                # Prorate the deltas to the 15-minute resolution
                proration_factor = target_duration_sec / actual_duration_sec

                imported_prorated = gross_imported * proration_factor
                exported_prorated = gross_exported * proration_factor

                # 5. Store the result
                final_result[battery_name][iv.interval_start_local.isoformat()] = {
                    "imported_kwh": round(imported_prorated, 3),
                    "exported_kwh": round(exported_prorated, 3)
                }

                # Prepare for next interval:
                # The starting point for the next interval search is the same as the starting point
                # for the current interval's search (ts_start), as the intervals are contiguous.
                # We don't advance the read_idx here because the interval processing uses
                # fixed windows (iv_start_utc to iv_end_utc) over the continuous data stream.
                # Let the next loop iteration re-calculate its start point based on iv_start_utc.
                # Resetting read_idx to the beginning of the dense data for the new interval is more robust
                # than trying to track the window boundaries precisely.
                # For simplicity and correctness with contiguous time intervals, we just make sure
                # the next interval search starts from the index that satisfied the last interval's end point.

                # Set the starting index for the next search to be the index of the current end reading.
                # This is key to prevent redundant work.
                read_idx = end_idx

        return final_result

    def get_energy_deltas_for_intervals(self, ivs: List[NetElectricityPriceInterval]) -> Dict[str, Dict[str, float]]:
        """
        For each NetElectricityPriceInterval, compute how many kWh were imported
        and exported during that interval.

        Returns a mapping from interval_start_local.isoformat() to
        {"imported_kwh": float, "exported_kwh": float}.
        """
        if not ivs:
            return {}

        # Determine overall UTC window (with 150-second buffer)
        # Sort intervals by start just in case
        ivs = sorted(ivs, key=lambda i: i.interval_start_local)
        first_start = ivs[0].interval_start_local
        last_end = ivs[-1].interval_start_local + timedelta(minutes=ivs[-1].resolution_minutes)

        buf = timedelta(seconds=150)
        window_start_utc = (first_start - buf).astimezone(timezone.utc).isoformat()
        window_end_utc = (last_end + buf).astimezone(timezone.utc).isoformat()

        # Fetch all readings
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
               SELECT timestamp_utc, total_power_import_kwh, total_power_export_kwh
               FROM p1_meter_log
               WHERE timestamp_utc >= ? AND timestamp_utc <= ?
               ORDER BY timestamp_utc
           """, (window_start_utc, window_end_utc))
        rows = cursor.fetchall()
        if not rows:
            return {}

        # Convert to list of (dt, import, export)
        readings = [
            (datetime.fromisoformat(ts_str), imp, exp)
            for ts_str, imp, exp in rows
        ]

        # For each interval, find start/end readings
        result: Dict[str, Dict[str, float]] = {}
        read_idx = 0
        n = len(readings)

        for iv in ivs:
            iv_start_utc = iv.interval_start_local.astimezone(timezone.utc)
            iv_end_utc = (iv.interval_start_local + timedelta(minutes=iv.resolution_minutes)).astimezone(timezone.utc)

            # Find start reading: last reading <= iv_start_utc
            start_imp = start_exp = start_ts = None
            while read_idx < n and readings[read_idx][0] <= iv_start_utc + buf:
                start_ts, start_imp, start_exp = readings[read_idx]
                read_idx += 1
            # If we never found a ≤ start, use the first reading
            if start_imp is None:
                start_ts, start_imp, start_exp = readings[min(read_idx, n) - 1]  # readings[0]

            # Find end reading: first reading ≥ iv_end_utc
            end_idx = read_idx
            while end_idx < n and readings[end_idx][0] < iv_end_utc - buf:
                end_idx += 1
            if end_idx < n:
                end_ts, end_imp, end_exp = readings[end_idx]
            else:
                # No reading ≥ end—use last available
                end_ts, end_imp, end_exp = readings[-1]
            if start_ts is None:
                start_ts = end_ts

            # # Compute deltas (clamped at 0)
            # imported = max(0.0, end_imp - start_imp)
            # exported = max(0.0, end_exp - start_exp)
            #
            # result[iv.interval_start_local.isoformat()] = {
            #     "imported_kwh": round(imported, 3),
            #     "exported_kwh": round(exported, 3)
            # }

            # 1. Calculate the actual time gap between the readings used
            gap_delta = end_ts - start_ts
            gap_minutes = gap_delta.total_seconds() / 60
            interval_minutes = iv.resolution_minutes

            # 2. Compute raw deltas
            raw_imported = max(0.0, end_imp - start_imp)
            raw_exported = max(0.0, end_exp - start_exp)

            # 3. Check if the gap is significantly larger than the resolution
            # We use a small multiplier (e.g., 1.1) to allow for minor jitter in sensor timing
            if gap_minutes > (interval_minutes * 1.1):
                # Scale the energy to match exactly one interval's worth of time
                scaling_factor = interval_minutes / gap_minutes
                imported = raw_imported * scaling_factor
                exported = raw_exported * scaling_factor
            else:
                # Normal behavior: use the raw delta
                imported = raw_imported
                exported = raw_exported

            result[iv.interval_start_local.isoformat()] = {
                "imported_kwh": round(imported, 3),
                "exported_kwh": round(exported, 3)
            }

            # Prepare for next interval:
            # rewind read_idx one if end_idx < n so next start can use this same point
            if end_idx < n:
                read_idx = max(0, end_idx - 1)

        return result

    def get_avg_monthly_peak_w_last_12m(self, reference_date: date, minimum_peak_w: int) -> Optional[float]:
        """
        Returns the average monthly peak power (in W) for the 12 months ending
        with the month of `reference_date`. If the latest month is not complete,
        it still uses the last logged peak of that month.

        Relies on p1_meter_log.monthly_power_peak_w being updated only when
        a new higher peak occurs, and reset shortly after month rollover.

        Args:
            reference_date (date): any day in the last month to include.
            minimum_peak_w: the minimum peak for tariff calculation

        Returns:
            Optional[float]: average peak in kW, or None if no data.
        """
        peaks_kw = []

        conn = self._get_connection()
        cursor = conn.cursor()

        # Helper to roll back months
        yr, m = reference_date.year, reference_date.month
        for i in range(12):
            # compute year/month for this offset
            mon = m - i
            year = yr + (mon - 1) // 12
            month = (mon - 1) % 12 + 1

            # start and end of that month in localtime
            start_local = datetime(year, month, 1)
            if month == 12:
                next_start_local = datetime(year + 1, 1, 1)
            else:
                next_start_local = datetime(year, month + 1, 1)

            # convert bounds to UTC‐iso
            start_utc = start_local.astimezone(tz=datetime.now().astimezone().tzinfo) \
                .astimezone(tz=timezone.utc).isoformat()
            end_utc = next_start_local.astimezone(tz=datetime.now().astimezone().tzinfo) \
                .astimezone(tz=timezone.utc).isoformat()

            # grab the last reading in that month
            cursor.execute("""
                SELECT monthly_power_peak_w
                FROM p1_meter_log
                WHERE timestamp_utc >= ? AND timestamp_utc < ?
                ORDER BY timestamp_utc DESC
                LIMIT 1
            """, (start_utc, end_utc))

            row = cursor.fetchone()
            if not row:
                continue

            peak_w, = row
            if peak_w is not None:
                peaks_kw.append(max(peak_w, minimum_peak_w))

        if not peaks_kw:
            return None

        return sum(peaks_kw) / len(peaks_kw)

    def store_elia_forecasts(self, forecasts: List[Dict[str, Any]]) -> int:
        """
        Stores a list of Elia forecast records into the database. Each dict in the list contains
        forecast_type, resolution_minutes, timestamp_utc and forecast values.
        """
        if not forecasts:
            logger.info("No Elia forecast points provided to store.")
            return 0

        rows_to_insert = []
        now_utc_str = datetime.now(timezone.utc).isoformat()

        for rec in forecasts:
            # For type wind: monitoredcapacity becomes 0 for same-day data
            # In that case, we want to keep the previous values
            if rec.get('forecast_type') == 'wind' and rec.get('monitored_capacity_mw') == 0:
                continue

            # Ensure all required keys are present or handled
            row_data = (
                rec.get('timestamp_utc'),
                rec.get('forecast_type'),
                rec.get('resolution_minutes'),
                rec.get('most_recent_forecast_mwh'),
                rec.get('monitored_capacity_mw'),
                now_utc_str  # fetched_at_utc
            )
            rows_to_insert.append(row_data)

        inserted_count = 0
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                sql = """
                INSERT OR REPLACE INTO elia_open_data (
                    timestamp_utc, forecast_type, resolution_minutes,
                    most_recent_forecast_mwh, monitored_capacity_mw,
                    fetched_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?);
            """
                cursor.executemany(sql, rows_to_insert)

                inserted_count = cursor.rowcount
                if inserted_count > 0:
                    logger.info(f"Successfully stored/updated {inserted_count} Elia forecast records.")
                else:
                    logger.info(f"No new Elia forecast records to store (or all were duplicates).")
        except sqlite3.Error as e:
            logger.error(f"Error storing Elia forecasts in database: {e}", exc_info=True)
        return inserted_count

    def get_elia_forecasts(self, forecast_type: str, start_date_local: datetime,
                           end_date_local: datetime) -> List[Dict[str, Any]]:
        """
        Retrieves Elia forecasts for a specific type and date range.
        start/end_date_local should be timezone-aware.
        """

        results = []

        start_utc_str = start_date_local.astimezone(timezone.utc).isoformat()
        end_utc_str = end_date_local.astimezone(timezone.utc).isoformat()

        logger.debug(f"Querying DB for Elia '{forecast_type}' forecasts between {start_utc_str} and {end_utc_str}")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = """
                SELECT timestamp_utc, forecast_type, resolution_minutes,
                       most_recent_forecast_mwh, monitored_capacity_mw, fetched_at_utc
                FROM elia_open_data
                WHERE forecast_type = ?
                  AND timestamp_utc >= ?
                  AND timestamp_utc < ?
                ORDER BY timestamp_utc;
            """
            cursor.execute(sql, (forecast_type, start_utc_str, end_utc_str))
            rows = cursor.fetchall()

            for row in rows:
                record = dict(row)  # Convert to dict
                # Ensure timestamp is a proper datetime object
                record['timestamp_utc'] = datetime.fromisoformat(record['timestamp_utc'])
                record['fetched_at_utc'] = datetime.fromisoformat(record['fetched_at_utc'])
                results.append(record)

            if not results:
                logger.info(f"No Elia '{forecast_type}' records found in DB for the period "
                                f"{start_date_local} - {end_date_local}.")
        except sqlite3.Error as e:
            logger.error(f"Error retrieving Elia '{forecast_type}' forecasts from database: {e}", exc_info=True)

        return results

    def store_inverter_data(self, inverter_data: Dict[str, Any]) -> bool:
        """Stores a single inverter data record into the database."""
        if not inverter_data or 'timestamp_utc_iso' not in inverter_data:
            logger.warning("Inverter Log: Invalid or missing data provided for DB storage.")
            return False

        values = (
            inverter_data.get('timestamp_utc_iso'),
            inverter_data.get('operational_status'),
            inverter_data.get('pv_power_watts'),
            inverter_data.get('daily_yield_wh'),
            inverter_data.get('total_yield_wh'),
            inverter_data.get('active_power_limit_watts')
        )

        sql = """
            INSERT OR REPLACE INTO inverter_log (
                timestamp_utc, operational_status, pv_power_watts, 
                daily_yield_wh, total_yield_wh, active_power_limit_watts
            ) VALUES (?, ?, ?, ?, ?, ?); 
        """

        retries = 5
        for attempt in range(retries):
            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(sql, values)

                    if cursor.rowcount > 0:
                        logger.debug(
                            f"Inverter Log: Successfully stored data for {inverter_data['timestamp_utc_iso']} in DB.")
                        return True

                logger.warning(f"Inverter Log: Data at {inverter_data['timestamp_utc_iso']} not stored (rowcount 0).")
                return False
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    logger.warning(
                        f"Inverter Log: Database is locked. Retrying {retries - attempt - 1} more time(s)...")
                    sleep(1)
                else:
                    logger.error(f"Inverter Log: OperationalError: {e}", exc_info=True)
                    return False
            except sqlite3.Error as e:
                logger.error(f"Inverter Log: Error storing data in database: {e}", exc_info=True)
                return False
            except KeyError as e:
                logger.error(f"Inverter Log: Missing key '{e}' in inverter_data: {inverter_data}", exc_info=True)
                return False
        logger.error("Inverter Log: Failed to store data after multiple attempts.")
        return False

    def get_inverter_data(self, start_date_local: datetime, end_date_local: datetime) -> List[Dict[str, Any]]:
        """
        Retrieves inverter log data for a given UTC ISO datetime period (inclusive start, exclusive end).

        Args:
            start_date_local (datetime): local datetime start date.
            end_date_local (datetime): local datetime end date (exclusive)

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a logged inverter record.
                                  Returns empty list on error or if no data.
        """
        results = []
        start_utc_str = start_date_local.astimezone(timezone.utc).isoformat()
        end_utc_str = end_date_local.astimezone(timezone.utc).isoformat()

        logger.debug(f"Querying Inverter Log DB for data between {start_utc_str} and {end_utc_str}")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = """
            SELECT timestamp_utc, operational_status, pv_power_watts, 
                   daily_yield_wh, total_yield_wh, active_power_limit_watts
            FROM inverter_log
            WHERE timestamp_utc >= ? 
              AND timestamp_utc < ? 
            ORDER BY timestamp_utc;
        """
            cursor.execute(sql, (start_utc_str, end_utc_str))
            rows = cursor.fetchall()

            for row_obj in rows:
                results.append(dict(row_obj))

            if results:
                logger.info(f"Retrieved {len(results)} inverter log records from DB for the period.")
            else:
                logger.info("No inverter log records found in DB for the specified period.")
        except sqlite3.Error as e:
            logger.error(f"Error retrieving inverter log data from database: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error retrieving inverter log data: {e}", exc_info=True)

        return results

    def store_battery_data(self, battery_data: Dict[str, Any]) -> bool:
        """
        Stores a single battery data record unconditionally.
        Returns True if the DB insert/update affected rows, False on failure.
        """
        if not battery_data:
            logger.warning("Battery data: missing data for DB storage.")
            return False

        # Map API keys to DB columns
        values = (
            battery_data.get('battery_name'),
            battery_data.get('timestamp_utc_iso'),
            battery_data.get('energy_import_kwh'),
            battery_data.get('energy_export_kwh'),
            battery_data.get('state_of_charge_pct'),
            battery_data.get('battery_mode'),
            battery_data.get('cycles')
        )

        sql = """
            INSERT OR REPLACE INTO battery_log (
                battery_name, timestamp_utc, energy_import_kwh, energy_export_kwh, state_of_charge_pct,
                battery_mode, cycles
            ) VALUES (?, ?, ?, ?, ?, ?, ?); 
        """

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, values)

                if cursor.rowcount > 0:
                    logger.debug(f"Battery data: Successfully stored data in DB.")
                    return True

            logger.warning(f"Battery data: Data was not stored.")
            return False
        except sqlite3.Error as e:
            logger.error(f"Battery data: Error storing data in database: {e}", exc_info=True)
            return False

    def save_setting(self, key: str, value: Any) -> bool:
        """Saves or updates a single application setting in the database."""
        now_utc_str = datetime.now(timezone.utc).isoformat()

        try:
            if isinstance(value, bool):
                value_type_str = "bool"
                value_json_str = json.dumps(value)
            elif isinstance(value, int):
                value_type_str = "int"
                value_json_str = json.dumps(value)
            elif isinstance(value, float):
                value_type_str = "float"
                value_json_str = json.dumps(value)
            elif isinstance(value, str):
                value_type_str = "str"
                value_json_str = json.dumps(value)
            elif isinstance(value, Enum):  # Handle Enum types
                value_type_str = f"enum:{value.__class__.__name__}"  # e.g., "enum:AppStatus"
                value_json_str = json.dumps(value.name)  # Store the Enum member's name
            elif isinstance(value, (dict, list)):
                value_type_str = "dict" if isinstance(value, dict) else "list"
                value_json_str = json.dumps(value)  # Directly serialize dicts/lists
            elif value is None:
                value_type_str = "none"
                value_json_str = json.dumps(None)
            else:
                logger.warning(
                    f"Attempting to save unsupported type for setting '{key}': {type(value)}. Storing as string.")
                value_type_str = "str_fallback"
                value_json_str = json.dumps(str(value))

            with self.transaction() as conn:
                sql = """
                    INSERT OR REPLACE INTO app_settings 
                    (setting_key, setting_value, value_type, last_updated_utc)
                    VALUES (?, ?, ?, ?);
                """
                conn.execute(sql, (key, value_json_str, value_type_str, now_utc_str))

            logger.info(f"Setting '{key}' saved to database with value type '{value_type_str}'.")
            return True
        except sqlite3.Error as e:
            logger.error(f"Error saving setting '{key}' to database: {e}", exc_info=True)
            return False
        except json.JSONDecodeError as e:
            logger.error(f"Error serializing value for setting '{key}': {e}", exc_info=True)
            return False

    def load_setting(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        """Loads a single application setting from the database."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = "SELECT setting_value, value_type FROM app_settings WHERE setting_key = ?;"
            cursor.execute(sql, (key,))
            row = cursor.fetchone()

            if row:
                value_json_str, value_type_str = row["setting_value"], row["value_type"]

                if value_type_str == "bool":
                    return json.loads(value_json_str)
                elif value_type_str == "int":
                    return json.loads(value_json_str)
                elif value_type_str == "float":
                    return json.loads(value_json_str)
                elif value_type_str == "str":
                    return json.loads(value_json_str)
                elif value_type_str.startswith("enum:"):
                    enum_class_name = value_type_str.split(":")[1]
                    enum_member_name = json.loads(value_json_str)
                    # Attempt to find the Enum class in constants module
                    if hasattr(c, enum_class_name):
                        enum_class = getattr(c, enum_class_name)
                        if hasattr(enum_class, enum_member_name):
                            return getattr(enum_class, enum_member_name)
                        else:
                            logger.warning(
                                f"Enum member '{enum_member_name}' not found in Enum class '{enum_class_name}' "
                                f"for setting '{key}'. Returning default.")
                    else:
                        logger.warning(
                            f"Enum class '{enum_class_name}' not found in constants for setting '{key}'. "
                            f"Returning default.")
                    return default  # Fallback to default if enum deserialization fails
                elif value_type_str in ["dict", "list"]:
                    return json.loads(value_json_str)
                elif value_type_str == "none":
                    return None
                elif value_type_str == "str_fallback":
                    # This was stored as str(value), try to load as string
                    return json.loads(value_json_str)
                else:
                    logger.warning(f"Unknown value_type '{value_type_str}' for setting '{key}'. Returning default.")
                    return default
            else:
                # logger.debug(f"Setting '{key}' not found in database. Returning default.")
                return default
        except sqlite3.Error as e:
            logger.error(f"Error loading setting '{key}' from database: {e}", exc_info=True)
            return default
        except json.JSONDecodeError as e:
            logger.error(f"Error deserializing value for setting '{key}': {e}", exc_info=True)
            return default

    def load_all_settings(self) -> Dict[str, Any]:
        """Loads all settings from the database into a dictionary."""
        all_settings_from_db = {}
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = "SELECT setting_key, setting_value, value_type FROM app_settings;"
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                key, value_json_str, value_type_str = row["setting_key"], row["setting_value"], row["value_type"]
                value = None
                try:
                    if value_type_str == "bool":
                        value = json.loads(value_json_str)
                    elif value_type_str == "int":
                        value = json.loads(value_json_str)
                    elif value_type_str == "float":
                        value = json.loads(value_json_str)
                    elif value_type_str == "str":
                        value = json.loads(value_json_str)
                    elif value_type_str.startswith("enum:"):
                        enum_class_name = value_type_str.split(":")[1]
                        enum_member_name = json.loads(value_json_str)
                        if hasattr(c, enum_class_name):
                            enum_class = getattr(c, enum_class_name)
                            if hasattr(enum_class, enum_member_name):
                                value = getattr(enum_class, enum_member_name)
                    elif value_type_str in ["dict", "list"]:
                        value = json.loads(value_json_str)
                    elif value_type_str == "none":
                        value = None
                    elif value_type_str == "str_fallback":
                        value = json.loads(value_json_str)

                    if value is not None or value_type_str == "none":  # Include None if it was explicitly stored
                        all_settings_from_db[key] = value
                    else:  # Deserialization failed for some reason for this type
                        logger.warning(
                            f"Could not deserialize setting '{key}' of type '{value_type_str}' "
                            f"during load_all_settings.")

                except Exception as e_inner:
                    logger.error(
                        f"Error deserializing setting '{key}' (type: {value_type_str}) "
                            f"during load_all_settings: {e_inner}")

            logger.info(f"Loaded {len(all_settings_from_db)} settings from database.")
        except sqlite3.Error as e:
            logger.error(f"Error loading all settings from database: {e}", exc_info=True)
        return all_settings_from_db

    def record_incident(
        self,
        severity: str,
        source: str,
        message: str,
        occurred_at_utc: Optional[datetime] = None,
        notification_type: Optional[str] = None,
        fingerprint_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create or update a persistent incident.

        Identical active incidents update their occurrence count and do not request
        a new notification. A recurrence after acknowledgement/resolution reopens
        the incident and requests notification again.
        """
        normalized_severity = self._normalize_incident_severity(severity)
        normalized_source = str(source or "application").strip() or "application"
        normalized_message = str(message or "").strip() or "(no message)"
        normalized_notification_type = self._normalize_notification_type(
            notification_type or normalized_severity
        )
        now_iso = self._utc_iso(occurred_at_utc)
        fingerprint = self._incident_fingerprint(
            normalized_severity,
            normalized_source,
            fingerprint_key or normalized_message,
        )

        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM app_incidents WHERE fingerprint = ?",
                (fingerprint,),
            ).fetchone()

            if existing is None:
                conn.execute(
                    """
                    INSERT INTO app_incidents (
                        severity, source, message, fingerprint, notification_type,
                        first_seen_utc, last_seen_utc, occurrence_count, status,
                        last_notified_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        normalized_severity,
                        normalized_source,
                        normalized_message,
                        fingerprint,
                        normalized_notification_type,
                        now_iso,
                        now_iso,
                        INCIDENT_ACTIVE,
                        now_iso,
                    ),
                )
                incident_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                should_notify = True
            else:
                was_active = existing["status"] == INCIDENT_ACTIVE
                should_notify = not was_active
                conn.execute(
                    """
                    UPDATE app_incidents
                    SET severity = ?,
                        source = ?,
                        message = ?,
                        notification_type = ?,
                        last_seen_utc = ?,
                        occurrence_count = occurrence_count + 1,
                        status = ?,
                        acknowledged_at_utc = CASE WHEN ? THEN NULL ELSE acknowledged_at_utc END,
                        acknowledged_by = CASE WHEN ? THEN NULL ELSE acknowledged_by END,
                        resolved_at_utc = CASE WHEN ? THEN NULL ELSE resolved_at_utc END,
                        last_notified_utc = CASE WHEN ? THEN ? ELSE last_notified_utc END
                    WHERE id = ?
                    """,
                    (
                        normalized_severity,
                        normalized_source,
                        normalized_message,
                        normalized_notification_type,
                        now_iso,
                        INCIDENT_ACTIVE,
                        int(should_notify),
                        int(should_notify),
                        int(should_notify),
                        int(should_notify),
                        now_iso,
                        existing["id"],
                    ),
                )
                incident_id = existing["id"]

            row = conn.execute(
                "SELECT * FROM app_incidents WHERE id = ?",
                (incident_id,),
            ).fetchone()

        result = self._row_to_dict(row)
        result["should_notify"] = should_notify
        return result

    def acknowledge_incident(
        self,
        incident_id: int,
        acknowledged_by: str = "dashboard",
        acknowledged_at_utc: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        now_iso = self._utc_iso(acknowledged_at_utc)
        actor = str(acknowledged_by or "dashboard").strip() or "dashboard"
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE app_incidents
                SET status = ?,
                    acknowledged_at_utc = ?,
                    acknowledged_by = ?
                WHERE id = ? AND status != ?
                """,
                (INCIDENT_ACKNOWLEDGED, now_iso, actor, incident_id, INCIDENT_RESOLVED),
            )
            row = conn.execute(
                "SELECT * FROM app_incidents WHERE id = ?",
                (incident_id,),
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def acknowledge_all_active_incidents(
        self,
        acknowledged_by: str = "dashboard",
        acknowledged_at_utc: Optional[datetime] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        now_iso = self._utc_iso(acknowledged_at_utc)
        actor = str(acknowledged_by or "dashboard").strip() or "dashboard"

        with self.transaction() as conn:
            active_rows = conn.execute(
                """
                SELECT id
                FROM app_incidents
                WHERE status = ?
                ORDER BY
                    CASE severity WHEN 'error' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
                    last_seen_utc DESC,
                    id DESC
                LIMIT ?
                """,
                (INCIDENT_ACTIVE, limit),
            ).fetchall()
            incident_ids = [row["id"] for row in active_rows]
            if not incident_ids:
                return []

            conn.executemany(
                """
                UPDATE app_incidents
                SET status = ?,
                    acknowledged_at_utc = ?,
                    acknowledged_by = ?
                WHERE id = ? AND status = ?
                """,
                [
                    (INCIDENT_ACKNOWLEDGED, now_iso, actor, incident_id, INCIDENT_ACTIVE)
                    for incident_id in incident_ids
                ],
            )

            placeholders = ",".join("?" for _ in incident_ids)
            rows = conn.execute(
                f"SELECT * FROM app_incidents WHERE id IN ({placeholders})",
                tuple(incident_ids),
            ).fetchall()

        rows_by_id = {row["id"]: self._row_to_dict(row) for row in rows}
        return [rows_by_id[incident_id] for incident_id in incident_ids if incident_id in rows_by_id]

    def resolve_incident(
        self,
        incident_id: int,
        resolved_at_utc: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        now_iso = self._utc_iso(resolved_at_utc)
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE app_incidents
                SET status = ?,
                    resolved_at_utc = ?
                WHERE id = ?
                """,
                (INCIDENT_RESOLVED, now_iso, incident_id),
            )
            row = conn.execute(
                "SELECT * FROM app_incidents WHERE id = ?",
                (incident_id,),
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_incidents(self, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        params: List[Any] = []
        where_clause = ""
        if status:
            normalized_status = self._normalize_incident_status(status)
            where_clause = "WHERE status = ?"
            params.append(normalized_status)

        sql = f"""
            SELECT *
            FROM app_incidents
            {where_clause}
            ORDER BY
                CASE severity WHEN 'error' THEN 0 WHEN 'warning' THEN 1 ELSE 2 END,
                last_seen_utc DESC,
                id DESC
            LIMIT ?
        """
        params.append(limit)

        with self.connection() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_dashboard_incidents(
        self,
        active_limit: int = 50,
        acknowledged_limit: int = 50,
        resolved_limit: int = 50,
    ) -> Dict[str, List[Dict[str, Any]]]:
        return {
            "active": self.get_incidents(INCIDENT_ACTIVE, active_limit),
            "acknowledged": self.get_incidents(INCIDENT_ACKNOWLEDGED, acknowledged_limit),
            "resolved": self.get_incidents(INCIDENT_RESOLVED, resolved_limit),
        }

    def get_active_incident_summary(self) -> Dict[str, Any]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT severity, COUNT(*) AS count
                FROM app_incidents
                WHERE status = ?
                GROUP BY severity
                """,
                (INCIDENT_ACTIVE,),
            ).fetchall()

        counts = {"warning": 0, "error": 0}
        for row in rows:
            counts[row["severity"]] = row["count"]

        if counts["error"] > 0:
            highest_severity = "error"
        elif counts["warning"] > 0:
            highest_severity = "warning"
        else:
            highest_severity = None

        return {
            "active_warning_count": counts["warning"],
            "active_error_count": counts["error"],
            "highest_severity": highest_severity,
        }

    def register_notification_device(
        self,
        device_token: str,
        label: str,
        notification_types: List[str],
        activated_at_utc: Optional[datetime] = None,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        token = str(device_token or "").strip()
        if not token:
            raise ValueError("device_token is required")

        selected_types = self._normalize_notification_types(notification_types)
        now_iso = self._utc_iso(activated_at_utc)
        device_label = str(label or "Dashboard device").strip() or "Dashboard device"
        values = {
            "warning": int("warning" in selected_types),
            "error": int("error" in selected_types),
            "peak_consumption": int("peak_consumption" in selected_types),
        }

        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO notification_devices (
                    device_token, label, enabled, notify_warning, notify_error,
                    notify_peak_consumption, activated_at_utc, last_seen_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(device_token) DO UPDATE SET
                    label = excluded.label,
                    enabled = excluded.enabled,
                    notify_warning = excluded.notify_warning,
                    notify_error = excluded.notify_error,
                    notify_peak_consumption = excluded.notify_peak_consumption,
                    last_seen_utc = excluded.last_seen_utc
                """,
                (
                    token,
                    device_label,
                    int(bool(enabled)),
                    values["warning"],
                    values["error"],
                    values["peak_consumption"],
                    now_iso,
                    now_iso,
                ),
            )
            row = conn.execute(
                "SELECT * FROM notification_devices WHERE device_token = ?",
                (token,),
            ).fetchone()

        return self._notification_device_row_to_dict(row)

    def queue_notification(
        self,
        notification_type: str,
        title: str,
        message: str,
        incident_id: Optional[int] = None,
        dedupe_key: Optional[str] = None,
        created_at_utc: Optional[datetime] = None,
    ) -> int:
        normalized_type = self._normalize_notification_type(notification_type)
        title_text = str(title or "Home Energy Control").strip() or "Home Energy Control"
        message_text = str(message or "").strip() or "(no message)"
        created_iso = self._utc_iso(created_at_utc)
        key = dedupe_key or self._notification_dedupe_key(normalized_type, title_text, message_text, incident_id)
        enabled_column = {
            "warning": "notify_warning",
            "error": "notify_error",
            "peak_consumption": "notify_peak_consumption",
        }[normalized_type]

        with self.transaction() as conn:
            devices = conn.execute(
                f"""
                SELECT device_token
                FROM notification_devices
                WHERE enabled = 1 AND {enabled_column} = 1
                """
            ).fetchall()

            inserted = 0
            for device in devices:
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO notification_queue (
                        device_token, notification_type, incident_id, title, message,
                        created_at_utc, dedupe_key
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        device["device_token"],
                        normalized_type,
                        incident_id,
                        title_text,
                        message_text,
                        created_iso,
                        key,
                    ),
                )
                inserted += cursor.rowcount

        return inserted

    def take_pending_notifications(self, device_token: str, limit: int = 20) -> List[Dict[str, Any]]:
        token = str(device_token or "").strip()
        if not token:
            return []
        limit = max(1, min(int(limit), 100))
        delivered_iso = self._utc_iso()

        with self.transaction() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM notification_queue
                WHERE device_token = ? AND delivered_at_utc IS NULL
                ORDER BY id ASC
                LIMIT ?
                """,
                (token, limit),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                conn.execute(
                    f"UPDATE notification_queue SET delivered_at_utc = ? WHERE id IN ({placeholders})",
                    (delivered_iso, *ids),
                )
            conn.execute(
                "UPDATE notification_devices SET last_seen_utc = ? WHERE device_token = ?",
                (delivered_iso, token),
            )

        return [self._row_to_dict(row) for row in rows]

    @staticmethod
    def _row_to_dict(row: Optional[sqlite3.Row]) -> Dict[str, Any]:
        return dict(row) if row is not None else {}

    @staticmethod
    def _utc_iso(value: Optional[datetime] = None) -> str:
        dt_value = value or datetime.now(timezone.utc)
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        return dt_value.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _normalize_incident_severity(severity: str) -> str:
        normalized = str(severity or "").strip().lower()
        if normalized in ("critical", "fatal", "alarm"):
            normalized = "error"
        if normalized not in INCIDENT_SEVERITIES:
            raise ValueError(f"Unsupported incident severity: {severity}")
        return normalized

    @staticmethod
    def _normalize_incident_status(status: str) -> str:
        normalized = str(status or "").strip().lower()
        if normalized not in INCIDENT_STATUSES:
            raise ValueError(f"Unsupported incident status: {status}")
        return normalized

    @staticmethod
    def _normalize_notification_type(notification_type: str) -> str:
        normalized = str(notification_type or "").strip().lower()
        if normalized in ("peak", "peak-consumption", "peak consumption"):
            normalized = "peak_consumption"
        if normalized not in NOTIFICATION_TYPES:
            raise ValueError(f"Unsupported notification type: {notification_type}")
        return normalized

    @classmethod
    def _normalize_notification_types(cls, notification_types: List[str]) -> List[str]:
        selected = []
        for raw_type in notification_types or []:
            normalized = cls._normalize_notification_type(raw_type)
            if normalized not in selected:
                selected.append(normalized)
        return selected

    @staticmethod
    def _incident_fingerprint(severity: str, source: str, message: str) -> str:
        raw = f"{severity}\n{source}\n{message}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _notification_dedupe_key(
        notification_type: str,
        title: str,
        message: str,
        incident_id: Optional[int],
    ) -> str:
        raw = f"{notification_type}\n{incident_id or ''}\n{title}\n{message}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _notification_device_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        selected_types = []
        if row["notify_warning"]:
            selected_types.append("warning")
        if row["notify_error"]:
            selected_types.append("error")
        if row["notify_peak_consumption"]:
            selected_types.append("peak_consumption")
        return {
            "device_token": row["device_token"],
            "label": row["label"],
            "enabled": bool(row["enabled"]),
            "notification_types": selected_types,
            "activated_at_utc": row["activated_at_utc"],
            "last_seen_utc": row["last_seen_utc"],
        }

    def get_p1_meter_data_for_period(self, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
        query = """
                SELECT timestamp_utc, \
                       total_power_import_kwh, \
                       total_power_export_kwh
                FROM p1_meter_log
                WHERE timestamp_utc >= ? \
                  AND timestamp_utc < ?
                ORDER BY timestamp_utc ASC \
                """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (start_utc.isoformat(), end_utc.isoformat()))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"DBHandler: Error retrieving p1 meter data: {e}", exc_info=True)
            return []

    def get_battery_data_for_period(self, start_utc: datetime, end_utc: datetime) -> List[Dict[str, Any]]:
        query = """
                SELECT timestamp_utc, \
                       battery_name, \
                       energy_import_kwh, \
                       energy_export_kwh
                FROM battery_log
                WHERE timestamp_utc >= ? \
                  AND timestamp_utc < ?
                ORDER BY timestamp_utc ASC \
                """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (start_utc.isoformat(), end_utc.isoformat()))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"DBHandler: Error retrieving battery data: {e}", exc_info=True)
            return []

    def apply_energy_history_retention(
        self,
        retention_days: Optional[int] = None,
        reference_utc: Optional[datetime] = None
    ) -> Dict[str, int]:
        """Delete old energy/history rows while preserving boundary rows for cumulative calculations."""
        days_to_keep = retention_days if retention_days is not None else self.history_retention_days
        reference = reference_utc or datetime.now(timezone.utc)
        cutoff = reference - timedelta(days=days_to_keep)
        cutoff_str = cutoff.isoformat()
        deleted_by_table: Dict[str, int] = {}

        with self.transaction() as conn:
            for table_name, retention_config in ENERGY_HISTORY_RETENTION_TABLES.items():
                timestamp_column, preserve_boundary, group_column = retention_config
                if preserve_boundary and group_column:
                    sql = f"""
                        DELETE FROM {table_name}
                        WHERE {timestamp_column} < ?
                          AND EXISTS (
                              SELECT 1
                              FROM {table_name} newer
                              WHERE newer.{group_column} = {table_name}.{group_column}
                                AND newer.{timestamp_column} < ?
                                AND newer.{timestamp_column} > {table_name}.{timestamp_column}
                          )
                    """
                    cursor = conn.execute(sql, (cutoff_str, cutoff_str))
                elif preserve_boundary:
                    sql = f"""
                        DELETE FROM {table_name}
                        WHERE {timestamp_column} < ?
                          AND rowid NOT IN (
                              SELECT rowid
                              FROM {table_name}
                              WHERE {timestamp_column} < ?
                              ORDER BY {timestamp_column} DESC
                              LIMIT 1
                          )
                    """
                    cursor = conn.execute(sql, (cutoff_str, cutoff_str))
                else:
                    cursor = conn.execute(
                        f"DELETE FROM {table_name} WHERE {timestamp_column} < ?",
                        (cutoff_str,),
                    )
                deleted_by_table[table_name] = max(cursor.rowcount, 0)

        return deleted_by_table

    def apply_log_retention(
        self,
        log_retention_hours: Optional[int] = None,
        info_debug_retention_hours: Optional[int] = None,
        reference_utc: Optional[datetime] = None
    ) -> Dict[str, int]:
        """Delete old logs independently from energy-history retention."""
        reference = reference_utc or datetime.now(timezone.utc)
        with self.transaction() as conn:
            deleted_count = self._delete_old_logs(
                conn,
                reference,
                log_retention_hours=log_retention_hours,
                info_debug_retention_hours=info_debug_retention_hours,
            )
        return {"logs": deleted_count}

    def _delete_old_logs(
        self,
        conn: sqlite3.Connection,
        reference_utc: datetime,
        log_retention_hours: Optional[int] = None,
        info_debug_retention_hours: Optional[int] = None
    ) -> int:
        all_logs_hours = log_retention_hours if log_retention_hours is not None else self.log_retention_hours
        info_debug_hours = (
            info_debug_retention_hours
            if info_debug_retention_hours is not None
            else self.info_debug_log_retention_hours
        )
        info_debug_cutoff = (reference_utc - timedelta(hours=info_debug_hours)).isoformat()
        all_logs_cutoff = (reference_utc - timedelta(hours=all_logs_hours)).isoformat()
        cursor = conn.execute(
            """
            DELETE FROM logs
            WHERE (level IN ('INFO', 'DEBUG') AND timestamp < ?)
               OR (timestamp < ?)
            """,
            (info_debug_cutoff, all_logs_cutoff),
        )
        return max(cursor.rowcount, 0)

    def store_log(self, log: dict):
        sql_insert = "INSERT INTO logs (timestamp, level, message, module) VALUES (?, ?, ?, ?)"
        try:
            with self.transaction() as conn:
                conn.execute(sql_insert, (log['timestamp'], log['level'], log['message'], log['module']))
                self._delete_old_logs(conn, datetime.now(timezone.utc))
        except Exception as e:
            # Use sys.stderr to avoid infinite logging loops
            import sys
            print(f"CRITICAL: Failed to write log to DB: {e}", file=sys.stderr)

    def get_latest_logs(self, limit=1000):
        """Fetch logs for the API."""
        sql = "SELECT * FROM logs ORDER BY timestamp DESC LIMIT ?"
        conn = self._get_connection()
        return [dict(row) for row in conn.execute(sql, (limit,)).fetchall()]

    def store_predicted_prices(self, df: pd.DataFrame):
        """
        Saves the predicted prices DataFrame to the database and
        deletes any predictions older than 14 days.
        """
        if df is None or df.empty:
            return

        # Convert to list of dicts and ensure ISO strings for SQLite
        records = df.copy()
        if pd.api.types.is_datetime64_any_dtype(records['timestamp_utc']):
            records['timestamp_utc'] = records['timestamp_utc'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        data = records.to_dict('records')

        cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).strftime('%Y-%m-%dT%H:%M:%SZ')

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Insert/Overwrite
                cursor.executemany("""
                                   REPLACE INTO predicted_prices
                                   (timestamp_utc, predicted_gross_price_kwh, solar_factor, wind_factor, grid_load_mwh)
                                   VALUES (:timestamp_utc, :predicted_gross_price_kwh, :solar_factor, :wind_factor,
                                           :grid_load_mwh)
                                   """, data)

                # Cleanup
                cursor.execute("DELETE FROM predicted_prices WHERE timestamp_utc < ?", (cutoff,))
                conn.commit()

            logger.debug(f"Saved {len(data)} predictions and cleared old records.")

        except Exception as e:
            logger.error(f"Error saving predicted prices to DB: {e}", exc_info=True)
            raise

    def get_predicted_prices_for_date(self, target_date_local: date) -> List[Dict[str, Any]]:
        """
        Retrieves predicted prices for a specific target date (UTC).
        Returns an empty DataFrame if no data is found.
        """
        tz = ZoneInfo("Europe/Brussels")

        local_start = datetime.combine(target_date_local, datetime.min.time(), tzinfo=tz)
        local_end = local_start + timedelta(days=1)

        start_str = local_start.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_str = local_end.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        try:
            with self._get_connection() as conn:
                conn.row_factory = sqlite3.Row  # This allows accessing columns by name
                cursor = conn.cursor()
                query = """
                        SELECT * \
                        FROM predicted_prices
                        WHERE timestamp_utc >= ? \
                          AND timestamp_utc < ?
                        ORDER BY timestamp_utc ASC \
                        """
                cursor.execute(query, (start_str, end_str))
                # Convert Row objects to standard dictionaries
                return [dict(row) for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Error retrieving predicted prices for {target_date_local}: {e}", exc_info=True)
            return []

    def store_evcc_session(self, timestamp_utc: datetime, current_session_kwh: float):
        """
        Snaps to 15m intervals. Updates the previous interval's energy_delta.
        Backfills missing rows if a gap > 15m is detected.
        """
        # 1. Snap current timestamp
        minute = (timestamp_utc.minute // 15) * 15
        current_snapped_dt = timestamp_utc.replace(minute=minute, second=0, microsecond=0)
        current_ts_str = current_snapped_dt.strftime('%Y-%m-%d %H:%M:%S')

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # 2. Get the most recent record
                cursor.execute("SELECT timestamp_utc, session_energy FROM evcc_log ORDER BY timestamp_utc DESC LIMIT 1")
                prev_row = cursor.fetchone()

                if prev_row:
                    prev_ts_str, prev_val = prev_row
                    prev_dt = datetime.strptime(prev_ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.UTC)

                    # Calculate the time gap in 15-minute increments
                    time_diff = current_snapped_dt - prev_dt
                    intervals = int(time_diff.total_seconds() // 900)  # 900s = 15m

                    if intervals > 0:
                        # Handle session reset (car unplugged)
                        total_delta = max(0,
                                          current_session_kwh - prev_val) if current_session_kwh >= prev_val else current_session_kwh
                        energy_per_slot = total_delta / intervals

                        # 3. Update the previous row's delta
                        cursor.execute("""
                            UPDATE evcc_log 
                            SET energy_delta = ? 
                            WHERE timestamp_utc = ?
                        """, (energy_per_slot, prev_ts_str))

                        # 4. Backfill missing rows if gap > 15 mins
                        if intervals > 1:
                            # Start at 1, so we skip the prev_row we just updated
                            for i in range(1, intervals):
                                fill_dt = prev_dt + timedelta(minutes=15 * i)
                                fill_ts_str = fill_dt.strftime('%Y-%m-%d %H:%M:%S')

                                # Calculate a logical session_energy for the gap
                                simulated_session = prev_val + (
                                            energy_per_slot * i) if current_session_kwh >= prev_val else current_session_kwh

                                cursor.execute("""
                                    INSERT OR REPLACE INTO evcc_log (timestamp_utc, session_energy, energy_delta)
                                    VALUES (?, ?, ?)
                                """, (fill_ts_str, simulated_session, energy_per_slot))

                # 5. ALWAYS insert the current slot
                cursor.execute("""
                    INSERT OR REPLACE INTO evcc_log (timestamp_utc, session_energy, energy_delta)
                    VALUES (?, ?, 0.0)
                """, (current_ts_str, current_session_kwh))

                conn.commit()

                if prev_row and intervals > 1:
                    logger.info(f"EVCC Log: Backfilled {intervals - 1} missing slots.")

        except Exception as e:
            logger.error(f"Failed to store EVCC database: {e}", exc_info=True)

    def get_evcc_data_for_period(self, start_utc: datetime, end_utc: datetime) -> list[dict]:
        """Fetches EVCC energy delta logs for a specific period."""
        try:
            start_str = start_utc.strftime('%Y-%m-%d %H:%M:%S')
            end_str = end_utc.strftime('%Y-%m-%d %H:%M:%S')
            with self._get_connection() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT timestamp_utc, energy_delta 
                    FROM evcc_log 
                    WHERE timestamp_utc >= ? AND timestamp_utc <= ?
                """, (start_str, end_str))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to fetch EVCC data: {e}")
            return []


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_main = logging.getLogger(__name__)

    # Initialize
    app_config = {"database": {"type": "sqlite", "path": "home_energy.db"}}
    db_handler = DatabaseHandler(app_config['database'])
    # db_handler.initialize_database()
    today_local = datetime.combine(datetime.now(), time.min)
    tomorrow_local = today_local + timedelta(days=1)
    fall_dst = datetime(2024, 10, 27, 0, 0, 0)

    db_handler.store_evcc_session(datetime(2026, 4, 9, 13, 45, 5, tzinfo=pytz.UTC), 4.813)
    # Retrieve solar forecast
    # solar_forecast = db_handler.get_elia_forecasts("solar", fall_dst, fall_dst + timedelta(days=1))
    # print(solar_forecast)
    # exit(0)
    # Inverter data
    # print(db_handler.get_inverter__data(start_date_local=today_local, end_date_local=datetime.combine(today_local, time.max)))

    # Power peak avg
    # print(db_handler.get_avg_monthly_peak_w_last_12m(date(2025, 5, 1), 2500))

    # Energy deltas
    nepis: List[NetElectricityPriceInterval] = \
        [NetElectricityPriceInterval(datetime(2025, 12, 3, 22, 0), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 3, 22, 15), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 3, 22, 30), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 3, 22, 45), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 3, 23, 0), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 3, 23, 15), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 3, 23, 30), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 3, 23, 45), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 0, 0), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 0, 15), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 0, 30), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 0, 45), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 1, 0), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 1, 15), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 1, 30), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 1, 45), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 2, 0), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 2, 15), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 2, 30), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 2, 45), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 3, 0), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 3, 15), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 3, 30), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 3, 45), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 4, 0), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 4, 15), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 4, 30), 15, "dynamic", {}),
         NetElectricityPriceInterval(datetime(2025, 12, 4, 4, 45), 15, "dynamic", {})
         ]
    # print(db_handler.get_battery_deltas_for_intervals(nepis))
    # print(db_handler.get_energy_deltas_for_intervals(nepis))

    # print(db_handler.get_energy_deltas_for_period(date(2025, 5, 1), date(2025, 5, 10)))

    # Retrieve electricity prices
    # prices_today_from_db = db_handler.get_da_prices(today_local)
    # print(prices_today_from_db)
