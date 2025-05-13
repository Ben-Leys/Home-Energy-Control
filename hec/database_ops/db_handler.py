# database_ops/db_handler.py
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional
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

            # Day-Ahead Price Forecasts Table
            # Storing timestamps as TEXT in ISO 8601 format (UTC)
            # Storing prices in EUR/MWh as fetched
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS electricity_price_forecasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    forecast_timestamp_utc TEXT NOT NULL,    -- Start of the price interval (ISO 8601 UTC string)
                    price_eur_per_mwh REAL NOT NULL,
                    resolution_minutes INTEGER NOT NULL,     -- e.g., 15, 30, 60
                    fetched_at_utc TEXT NOT NULL,          -- When this data was retrieved (ISO 8601 UTC string)
                    source_api TEXT DEFAULT 'ENTSO-E',
                    UNIQUE (forecast_timestamp_utc, resolution_minutes) -- Ensure no duplicate entries for same interval
                );
            """)
            logger.info("Table 'electricity_price_forecasts' checked/created.")

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_price_forecast_timestamp_utc 
                ON electricity_price_forecasts (forecast_timestamp_utc);
            """)
            logger.info("Index 'idx_price_forecast_timestamp_utc' checked/created.")

            # Add other tables here later (P1 logs, Inverter logs, etc.)
            # Example P1 log table:
            # cursor.execute("""
            # CREATE TABLE IF NOT EXISTS p1_meter_log (
            #     timestamp_utc TEXT PRIMARY KEY, -- ISO 8601 UTC
            #     -- Add your P1 fields here, e.g.:
            #     active_power_import_w REAL,
            #     active_power_export_w REAL,
            #     gas_m3 REAL
            # );
            # """)
            # logger.info("Table 'p1_meter_log' checked/created.")

            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error initializing database tables: {e}", exc_info=True)
            raise
        finally:
            pass

    def store_price_forecasts(self, price_points: List[PricePoint]) -> int:
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
                INSERT OR REPLACE INTO electricity_price_forecasts 
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

    def get_price_forecasts_for_day(self, target_day_local: datetime) -> List[PricePoint]:
        """
        Retrieves price forecasts for a specific local day from the database.
        Returns a list of PricePoint objects.
        """
        results = []
        local_tz = target_day_local.tzinfo if target_day_local.tzinfo else datetime.now().astimezone().tzinfo

        day_start_local = target_day_local.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=local_tz)
        day_end_local = (day_start_local + timedelta(days=1))  # Exclusive end

        # Convert local day boundaries to UTC strings for querying
        day_start_utc_str = day_start_local.astimezone(timezone.utc).isoformat()
        day_end_utc_str = day_end_local.astimezone(timezone.utc).isoformat()

        logger.debug(f"Querying DB for prices between {day_start_utc_str} and {day_end_utc_str}")

        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            sql = """
                SELECT forecast_timestamp_utc, price_eur_per_mwh, resolution_minutes
                FROM electricity_price_forecasts
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
            logger.info(
                f"Retrieved {len(results)} price points from DB for {target_day_local.strftime('%Y-%m-%d')}.")
        except sqlite3.Error as e:
            logger.error(f"Error retrieving price forecasts from database: {e}", exc_info=True)

        return results


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
    prices_today_from_db = db_handler.get_price_forecasts_for_day(today_local)
    print(prices_today_from_db)
