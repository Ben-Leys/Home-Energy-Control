import io
import logging
import sqlite3
import threading
import time
import unittest
from contextlib import redirect_stderr
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from hec.core.app_logging import GlobalStateHandler
from hec.database_ops.db_handler import DatabaseHandler


class TestDatabasePhase2Reliability(unittest.TestCase):
    def setUp(self):
        self.scratch_root = Path.cwd() / "_scratch"
        self.scratch_root.mkdir(exist_ok=True)
        safe_test_name = self._testMethodName.replace("test_", "")
        self.db_path = self.scratch_root / f"phase2-{safe_test_name}.sqlite"
        self._remove_sqlite_artifacts()
        self.handlers = []

    def tearDown(self):
        for handler in self.handlers:
            handler.close_connection()
        self._remove_sqlite_artifacts()

    def _remove_sqlite_artifacts(self):
        paths = [
            self.db_path,
            self.db_path.with_name(f"{self.db_path.name}-journal"),
            self.db_path.with_name(f"{self.db_path.name}-wal"),
            self.db_path.with_name(f"{self.db_path.name}-shm"),
        ]
        for attempt in range(10):
            try:
                for path in paths:
                    path.unlink(missing_ok=True)
                return
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.05)

    def make_handler(self, **config_overrides):
        config = {
            "path": str(self.db_path),
            "busy_timeout_ms": 750,
            **config_overrides,
        }
        handler = DatabaseHandler(config)
        handler.initialize_database()
        self.handlers.append(handler)
        return handler

    def test_connection_policy_sets_wal_busy_timeout_and_foreign_keys(self):
        handler = self.make_handler(busy_timeout_ms=1234)

        with handler.connection() as conn:
            self.assertEqual("wal", conn.execute("PRAGMA journal_mode").fetchone()[0].lower())
            self.assertEqual(1234, conn.execute("PRAGMA busy_timeout").fetchone()[0])
            self.assertEqual(1, conn.execute("PRAGMA foreign_keys").fetchone()[0])
            self.assertIs(sqlite3.Row, conn.row_factory)

    def test_legacy_connection_accessor_does_not_share_one_connection_across_threads(self):
        handler = self.make_handler()
        main_connection = handler._get_connection()
        thread_connection_holder = []

        def get_thread_connection():
            thread_connection_holder.append(handler._get_connection())

        thread = threading.Thread(target=get_thread_connection)
        thread.start()
        thread.join(timeout=5)

        self.assertEqual(1, len(thread_connection_holder))
        self.assertIsNot(main_connection, thread_connection_holder[0])

    def test_close_current_thread_connection_removes_cached_connection(self):
        handler = self.make_handler()
        connection = handler._get_connection()
        thread_id = threading.get_ident()

        self.assertIn(thread_id, handler._thread_connections)

        handler.close_current_thread_connection()

        self.assertNotIn(thread_id, handler._thread_connections)
        with self.assertRaises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")

    def test_transaction_rolls_back_failed_write(self):
        handler = self.make_handler()

        with self.assertRaises(RuntimeError):
            with handler.transaction() as conn:
                conn.execute(
                    "INSERT INTO logs (timestamp, level, module, message) VALUES (?, ?, ?, ?)",
                    (datetime.now(timezone.utc).isoformat(), "INFO", "test", "should rollback"),
                )
                raise RuntimeError("force rollback")

        with handler.connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]

        self.assertEqual(0, count)

    def test_store_log_uses_transaction_helper_and_waits_out_short_write_lock(self):
        handler = self.make_handler(busy_timeout_ms=1000)
        lock_conn = sqlite3.connect(self.db_path, timeout=1, isolation_level=None)
        writer_finished = threading.Event()

        try:
            lock_conn.execute("BEGIN IMMEDIATE")
            lock_conn.execute(
                "INSERT INTO logs (timestamp, level, module, message) VALUES (?, ?, ?, ?)",
                (datetime.now(timezone.utc).isoformat(), "INFO", "lock-holder", "holding lock"),
            )

            with patch.object(handler, "transaction", wraps=handler.transaction) as transaction_spy:
                writer = threading.Thread(
                    target=lambda: (
                        handler.store_log(
                            {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "level": "INFO",
                                "module": "writer",
                                "message": "waited for lock",
                            }
                        ),
                        writer_finished.set(),
                    ),
                )
                writer.start()
                time.sleep(0.05)
                self.assertFalse(writer_finished.is_set())
                lock_conn.commit()
                writer.join(timeout=5)

                self.assertTrue(writer_finished.is_set())
                self.assertTrue(transaction_spy.called)
        finally:
            try:
                lock_conn.rollback()
            except sqlite3.Error:
                pass
            lock_conn.close()

        with handler.connection() as conn:
            messages = [
                row["message"]
                for row in conn.execute("SELECT message FROM logs ORDER BY id").fetchall()
            ]
        self.assertIn("waited for lock", messages)

    def test_save_setting_logs_after_setting_transaction_commits(self):
        handler = self.make_handler(busy_timeout_ms=50)
        db_logger = logging.getLogger("hec.database_ops.db_handler")
        original_level = db_logger.level
        log_handler = GlobalStateHandler(global_app_state=object())
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(logging.Formatter("%(message)s"))
        log_handler.set_db_handler(handler)
        stderr = io.StringIO()

        db_logger.setLevel(logging.INFO)
        db_logger.addHandler(log_handler)
        try:
            with redirect_stderr(stderr):
                self.assertTrue(handler.save_setting("empty_since", None))
        finally:
            db_logger.removeHandler(log_handler)
            db_logger.setLevel(original_level)

        self.assertNotIn("database is locked", stderr.getvalue().lower())

        with handler.connection() as conn:
            messages = [
                row["message"]
                for row in conn.execute("SELECT message FROM logs ORDER BY id").fetchall()
            ]

        self.assertTrue(
            any("Setting 'empty_since' saved to database" in message for message in messages),
            messages,
        )

    def test_schema_migration_table_is_idempotent(self):
        handler = self.make_handler()

        with handler.connection() as conn:
            first_rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT version, description FROM schema_version ORDER BY version"
                ).fetchall()
            ]

        handler.initialize_database()

        with handler.connection() as conn:
            second_rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT version, description FROM schema_version ORDER BY version"
                ).fetchall()
            ]

        self.assertEqual(first_rows, second_rows)
        self.assertEqual([{"version": 1, "description": "001_noop_initial_schema"}], second_rows)

    def test_predicted_price_lookup_exception_logs_target_date_without_crashing(self):
        handler = DatabaseHandler({"path": str(self.db_path)})

        with (
            patch.object(handler, "_get_connection", side_effect=RuntimeError("connection failed")),
            self.assertLogs("hec.database_ops.db_handler", level="ERROR") as captured,
        ):
            result = handler.get_predicted_prices_for_date(date(2026, 6, 29))

        self.assertEqual([], result)
        self.assertIn("2026-06-29", "\n".join(captured.output))

    def test_energy_history_retention_preserves_boundary_rows_and_leaves_logs_alone(self):
        handler = self.make_handler()
        reference = datetime(2026, 6, 29, tzinfo=timezone.utc)
        cutoff = reference - timedelta(days=365 * 3)
        very_old = cutoff - timedelta(days=400)
        boundary_old = cutoff - timedelta(days=1)
        recent = cutoff + timedelta(days=1)

        with handler.transaction() as conn:
            for timestamp in (very_old, boundary_old, recent):
                conn.execute(
                    "INSERT INTO p1_meter_log (timestamp_utc, total_power_import_kwh, total_power_export_kwh) "
                    "VALUES (?, ?, ?)",
                    (timestamp.isoformat(), 10.0, 5.0),
                )
            conn.execute(
                "INSERT INTO logs (timestamp, level, module, message) VALUES (?, ?, ?, ?)",
                (very_old.isoformat(), "INFO", "retention", "energy retention must not purge this"),
            )

        result = handler.apply_energy_history_retention(reference_utc=reference)

        with handler.connection() as conn:
            p1_timestamps = [
                row["timestamp_utc"]
                for row in conn.execute("SELECT timestamp_utc FROM p1_meter_log ORDER BY timestamp_utc").fetchall()
            ]
            log_count = conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0]

        self.assertEqual([boundary_old.isoformat(), recent.isoformat()], p1_timestamps)
        self.assertEqual(1, log_count)
        self.assertEqual(1, result["p1_meter_log"])

    def test_log_retention_is_separate_from_energy_history_retention(self):
        handler = self.make_handler()
        reference = datetime(2026, 6, 29, 12, 0, tzinfo=timezone.utc)

        with handler.transaction() as conn:
            rows = [
                (reference - timedelta(hours=13), "INFO", "old info"),
                (reference - timedelta(hours=13), "DEBUG", "old debug"),
                (reference - timedelta(hours=13), "WARNING", "kept warning"),
                (reference - timedelta(hours=73), "ERROR", "old error"),
                (reference - timedelta(hours=1), "INFO", "recent info"),
            ]
            for timestamp, level, message in rows:
                conn.execute(
                    "INSERT INTO logs (timestamp, level, module, message) VALUES (?, ?, ?, ?)",
                    (timestamp.isoformat(), level, "retention", message),
                )

        result = handler.apply_log_retention(reference_utc=reference)

        with handler.connection() as conn:
            remaining = [
                row["message"]
                for row in conn.execute("SELECT message FROM logs ORDER BY id").fetchall()
            ]

        self.assertEqual(["kept warning", "recent info"], remaining)
        self.assertEqual(3, result["logs"])


if __name__ == "__main__":
    unittest.main()
