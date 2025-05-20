# database_ops/db_handler.py
import logging
import sqlite3
from datetime import datetime, timezone, timedelta, time, date
from pathlib import Path
from typing import List, Optional, Dict, Any

from hec.core.models import PricePoint, NetElectricityPriceInterval

logger = logging.getLogger(__name__)


class DatabaseHandler:
    def __init__(self, db_config: dict):
        db_path_str = db_config.get("path")
        if not db_path_str:
            raise ValueError("Database path is not specified in the configuration.")

        project_root = Path(__file__).resolve().parent.parent
        self.db_path = project_root / db_path_str
        self.conn: Optional[sqlite3.Connection] = None
        logger.info(f"Database handler initialized for SQLite DB at: {self.db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """Establishes and returns a database connection."""
        if self.conn is None or self._is_connection_closed():
            try:
                # Ensure parent directory exists
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
                self.conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
                self.conn.row_factory = sqlite3.Row  # Access columns by name
                logger.info(f"Successfully connected to SQLite database: {self.db_path}")
            except sqlite3.Error as e:
                logger.error(f"Error connecting to SQLite database {self.db_path}: {e}", exc_info=True)
                raise
        return self.conn

    def _is_connection_closed(self) -> bool:
        """Checks if the connection is closed or unusable."""
        if self.conn is None:
            return True
        try:
            self.conn.execute("SELECT 1").fetchone()
            return False
        except sqlite3.ProgrammingError:
            return True
        except sqlite3.OperationalError:
            return True

    def close_connection(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("SQLite database connection closed.")

    def initialize_database(self):
        """Creates necessary tables if they don't exist."""
        try:
            conn = self._get_connection()
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

            # --- Inverter Log table ---
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

            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error initializing database tables: {e}", exc_info=True)
            raise
        finally:
            pass

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
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = """
                INSERT OR REPLACE INTO belpex_da_prices 
                (timestamp_utc, price_eur_per_mwh, resolution_minutes, fetched_at_utc)
                VALUES (?, ?, ?, ?);
            """
            cursor.executemany(sql, rows_to_insert)
            conn.commit()
            inserted_count = cursor.rowcount
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
            p1_data.get('montly_power_peak_w'),
            p1_data.get('montly_power_peak_timestamp')
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
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, values)
            conn.commit()
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
            start_imp = start_exp = None
            while read_idx < n and readings[read_idx][0] <= iv_start_utc:
                start_ts, start_imp, start_exp = readings[read_idx]
                read_idx += 1
            # If we never found a ≤ start, use the first reading
            if start_imp is None:
                start_ts, start_imp, start_exp = readings[0]

            # Find end reading: first reading ≥ iv_end_utc
            end_idx = read_idx
            while end_idx < n and readings[end_idx][0] < iv_end_utc:
                end_idx += 1
            if end_idx < n:
                _, end_imp, end_exp = readings[end_idx]
            else:
                # No reading ≥ end—use last available
                _, end_imp, end_exp = readings[-1]

            # Compute deltas (clamped at 0)
            imported = max(0.0, end_imp - start_imp)
            exported = max(0.0, end_exp - start_exp)

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
        conn = self._get_connection()
        cursor = conn.cursor()

        peaks_kw = []

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
            logger.debug(f"Peak w: {max(peak_w, minimum_peak_w)} for {mon}/{year}")
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
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = """
                INSERT OR REPLACE INTO elia_open_data (
                    timestamp_utc, forecast_type, resolution_minutes,
                    most_recent_forecast_mwh, monitored_capacity_mw,
                    fetched_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?);
            """
            cursor.executemany(sql, rows_to_insert)
            conn.commit()
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

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, values)
            conn.commit()
            if cursor.rowcount > 0:
                logger.debug(
                    f"Inverter Log: Successfully stored data for timestamp {inverter_data['timestamp_utc_iso']} in DB.")
                return True
            else:
                logger.warning(f"Inverter Log: Data for timestamp {inverter_data['timestamp_utc_iso']} was not stored.")
                return False
        except sqlite3.Error as e:
            logger.error(f"Inverter Log: Error storing data in database: {e}", exc_info=True)
            return False
        except KeyError as e:
            logger.error(f"Inverter Log: Missing key '{e}' in inverter_data: {inverter_data}", exc_info=True)
            return False

    def get_inverter__data(self, start_date_local: datetime, end_date_local: datetime) -> List[Dict[str, Any]]:
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
                ORDER BY timestamp_utc ASC;
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


# if __name__ == '__main__':
#     logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     logger_main = logging.getLogger(__name__)
#
#     # Initialize
#     app_config = {"database": {"type": "sqlite", "path": "home_energy.db"}}
#     db_handler = DatabaseHandler(app_config['database'])
#     db_handler.initialize_database()
#     today_local = datetime.combine(datetime.now(), time.min)
#     tomorrow_local = today_local + timedelta(days=1)
#     fall_dst = datetime(2024, 10, 27, 0, 0, 0)
#
#     # Retrieve solar forecast
#     solar_forecast = db_handler.get_elia_forecasts("solar", fall_dst, fall_dst + timedelta(days=1))
#     print(solar_forecast)
#     exit(0)
#     # Inverter data
#     print(db_handler.get_inverter__data(start_date_local=today_local,
#                                         end_date_local=datetime.combine(today_local, time.max)))
#
#     # Power peak avg
#     print(db_handler.get_avg_monthly_peak_w_last_12m(date(2025, 5, 1), 2500))
#
#     # Energy deltas
#     nepis: List[NetElectricityPriceInterval] = \
#         [NetElectricityPriceInterval(datetime(2025, 5, 14, 9, 0), 60, "dynamic", {}),
#          NetElectricityPriceInterval(datetime(2025, 5, 14, 10, 0), 60, "dynamic", {})]
#     print(db_handler.get_energy_deltas_for_intervals(nepis))
#
#     print(db_handler.get_energy_deltas_for_period(date(2025, 5, 1), date(2025, 5, 10)))
#
#     # Retrieve electricity prices
#     prices_today_from_db = db_handler.get_da_prices(today_local)
#     print(prices_today_from_db)
