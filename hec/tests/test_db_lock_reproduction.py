import time
import unittest
from pathlib import Path

from tools.reproduce_db_lock import run_db_lock_reproduction


class TestDbLockReproduction(unittest.TestCase):
    def test_reproduction_reports_locked_writes_against_isolated_database(self):
        scratch_dir = Path.cwd() / "_scratch"  # / "hec" / "tests" / "_scratch"
        scratch_dir.mkdir(exist_ok=True)
        db_path = scratch_dir / "lock-test.sqlite"
        _remove_sqlite_artifacts(db_path)

        try:
            result = run_db_lock_reproduction(
                db_path=db_path,
                workers=2,
                writes_per_worker=2,
                hold_lock_seconds=0.2,
                write_timeout_seconds=0.001,
            )
        finally:
            _remove_sqlite_artifacts(db_path)

        self.assertEqual(result.attempted_writes, 4)
        self.assertGreater(result.locked_errors, 0)
        self.assertEqual(
            result.attempted_writes,
            result.successful_writes + result.locked_errors + result.other_errors,
        )
        self.assertIn("database is locked", result.error_examples[0].lower())


def _sqlite_artifacts(db_path):
    return [
        db_path,
        db_path.with_name(f"{db_path.name}-journal"),
        db_path.with_name(f"{db_path.name}-wal"),
        db_path.with_name(f"{db_path.name}-shm"),
    ]


def _remove_sqlite_artifacts(db_path):
    paths = _sqlite_artifacts(db_path)
    for attempt in range(10):
        try:
            for path in paths:
                path.unlink(missing_ok=True)
            return
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(0.05)


if __name__ == "__main__":
    unittest.main()
