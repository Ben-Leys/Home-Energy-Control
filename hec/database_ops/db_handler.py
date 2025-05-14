# database_ops/db_handler.py
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from pathlib import Path
from hec.data_sources.day_ahead_price_api import PricePoint

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
            # Timestamps as TEXT in ISO 8601 format (UTC)
            # Prices in EUR/MWh as fetched
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS belpex_da_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    forecast_timestamp_utc TEXT NOT NULL,    -- Start of the price interval (ISO 8601 UTC string)
                    price_eur_per_mwh REAL NOT NULL,
                    resolution_minutes INTEGER NOT NULL,     -- e.g., 15, 30, 60
                    fetched_at_utc TEXT NOT NULL,          -- When this data was retrieved (ISO 8601 UTC string)
                    source_api TEXT DEFAULT 'ENTSO-E',
                    UNIQUE (forecast_timestamp_utc, resolution_minutes) -- Ensure no duplicate entries for same interval
                );
            """)
            logger.info("Table 'belpex_da_prices' checked/created.")

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_forecast_timestamp_utc 
                ON belpex_da_prices (forecast_timestamp_utc);
            """)
            logger.info("Index 'idx_price_forecast_timestamp_utc' checked/created.")

            # --- P1 Meter Log Table ---
            cursor.execute("""
                            CREATE TABLE IF NOT EXISTS p1_meter_log (
                                timestamp_utc TEXT PRIMARY KEY,    -- ISO 8601 UTC string from the data dict
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
            logger.info("Table 'p1_meter_log' checked/created.")
            # No separate index needed for timestamp_utc as it's PRIMARY KEY

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
                (forecast_timestamp_utc, price_eur_per_mwh, resolution_minutes, fetched_at_utc)
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

    def get_da_prices(self, target_day_local: datetime) -> List[PricePoint]:
        """
        Retrieves price forecasts for a specific local day from the database.
        Returns a list of PricePoint objects.
        """
        results = []
        local_tz = target_day_local.tzinfo if target_day_local.tzinfo else datetime.now().astimezone().tzinfo

        day_start_local = target_day_local.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
        day_end_local = (day_start_local + timedelta(days=1))

        # Convert local day boundaries to UTC strings for querying
        day_start_utc_str = day_start_local.astimezone(timezone.utc).isoformat()
        day_end_utc_str = day_end_local.astimezone(timezone.utc).isoformat()

        logger.debug(f"Querying DB for prices between {day_start_utc_str} and {day_end_utc_str}")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = """
                SELECT forecast_timestamp_utc, price_eur_per_mwh, resolution_minutes
                FROM belpex_da_prices
                WHERE forecast_timestamp_utc >= ? 
                  AND forecast_timestamp_utc < ? 
                ORDER BY forecast_timestamp_utc;
            """
            cursor.execute(sql, (day_start_utc_str, day_end_utc_str))
            rows = cursor.fetchall()

            for row in rows:
                # forecast_timestamp_utc is stored as TEXT, convert back to datetime
                ts_utc = datetime.fromisoformat(row["forecast_timestamp_utc"])
                results.append(PricePoint(
                    timestamp_utc=ts_utc,
                    price_eur_per_mwh=row["price_eur_per_mwh"],
                    position=0,  # Has no added value when retrieving
                    resolution_minutes=row["resolution_minutes"]
                ))
            logger.info(f"Retrieved {len(results)} price points from DB for {target_day_local.strftime('%Y-%m-%d')}.")
        except sqlite3.Error as e:
            logger.error(f"Error retrieving price forecasts from database: {e}", exc_info=True)

        return results

    def store_p1_meter_data(self, p1_data: Dict[str, Any], app_state, boundary: int = 5) -> bool:
        """
        Stores a single P1 meter data record into the database within n-minute boundary.
        Assumes p1_data dictionary contains all necessary fields including 'timestamp_utc_iso'.

        Args:
            p1_data (Dict[str, Any]): The P1 data including 'timestamp_utc_iso'.
            app_state (AppState): The global application state instance.
            boundary (int): The minute interval for storing (example: every 5 min).

        Returns:
            bool: True if data was stored, False otherwise.
        """
        if not p1_data or 'timestamp_utc_iso' not in p1_data:
            logger.warning("P1 Meter: Missing key field for DB storage.")
            return False

        # Check if data needs to be stored
        p1_data_ts_utc = datetime.fromisoformat(p1_data['timestamp_utc_iso'])
        if p1_data_ts_utc.minute % boundary != 0:
            logger.debug(f"P1 Meter: Current minute ({p1_data_ts_utc.minute}) is not a {boundary}-min boundary. "
                         f"Skipping DB store.")
            return False
        last_boundary_minute = (p1_data_ts_utc.minute // boundary) * boundary
        boundary_slot = p1_data_ts_utc.replace(minute=last_boundary_minute, second=0, microsecond=0)
        boundary_slot_iso = boundary_slot.isoformat()
        last_db_slot_iso = app_state.get("p1_meter_last_stored_boundary_slot_utc_iso")
        if boundary_slot_iso == last_db_slot_iso:
            logger.debug(f"P1 Meter: Already stored data for boundary slot {boundary_slot_iso}. Skipping DB store.")
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
                # Update AppState with the boundary slot that was just successfully written
                app_state.set("p1_meter_last_stored_boundary_slot_utc_iso", boundary_slot_iso)
                return True
            else:
                logger.warning(
                    f"P1 Meter: Data for timestamp {p1_data['timestamp_utc_iso']} was not stored (no rows affected).")
                return False
        except sqlite3.Error as e:
            logger.error(f"P1 Meter: Error storing data in database: {e}", exc_info=True)
            return False


if __name__ == '__main__':
    from hec.core.config_loader import load_app_config

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger_main = logging.getLogger(__name__)

    # Initialize
    APP_CONFIG = load_app_config()
    db_handler = DatabaseHandler(APP_CONFIG['database'])
    db_handler.initialize_database()

    # Retrieve electricity prices
    today_local = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    prices_today_from_db = db_handler.get_da_prices(today_local)
    print(prices_today_from_db)
