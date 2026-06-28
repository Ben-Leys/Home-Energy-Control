from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class DbLockReproductionResult:
    db_path: str
    workers: int
    writes_per_worker: int
    hold_lock_seconds: float
    write_timeout_seconds: float
    attempted_writes: int
    successful_writes: int
    locked_errors: int
    other_errors: int
    elapsed_seconds: float
    error_examples: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _initialize_logs_schema(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                level TEXT NOT NULL,
                module TEXT NOT NULL,
                message TEXT NOT NULL
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs (timestamp);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_level ON logs (level);")
        conn.commit()
    finally:
        conn.close()


def _insert_log(conn: sqlite3.Connection, module: str, message: str) -> None:
    conn.execute(
        "INSERT INTO logs (timestamp, level, module, message) VALUES (?, ?, ?, ?)",
        (datetime.now(timezone.utc).isoformat(), "INFO", module, message),
    )


def run_db_lock_reproduction(
    db_path: Path | str,
    workers: int = 2,
    writes_per_worker: int = 5,
    hold_lock_seconds: float = 2.0,
    write_timeout_seconds: float = 0.05,
) -> DbLockReproductionResult:
    """Hold a SQLite write lock while concurrent writer threads insert log rows."""

    resolved_db_path = Path(db_path).resolve()
    _initialize_logs_schema(resolved_db_path)

    lock_acquired = threading.Event()
    counters_lock = threading.Lock()
    attempted_writes = 0
    successful_writes = 0
    locked_errors = 0
    other_errors = 0
    error_examples: List[str] = []

    def record_attempt() -> None:
        nonlocal attempted_writes
        with counters_lock:
            attempted_writes += 1

    def record_success() -> None:
        nonlocal successful_writes
        with counters_lock:
            successful_writes += 1

    def record_error(exc: Exception) -> None:
        nonlocal locked_errors, other_errors
        message = str(exc)
        with counters_lock:
            if "database is locked" in message.lower():
                locked_errors += 1
            else:
                other_errors += 1
            if len(error_examples) < 5:
                error_examples.append(message)

    def hold_write_lock() -> None:
        conn = sqlite3.connect(
            resolved_db_path,
            timeout=write_timeout_seconds,
            isolation_level=None,
        )
        try:
            conn.execute("BEGIN IMMEDIATE")
            _insert_log(conn, "db_lock_reproduction", "holding write lock")
            lock_acquired.set()
            time.sleep(hold_lock_seconds)
            conn.commit()
        except Exception as exc:
            lock_acquired.set()
            record_error(exc)
        finally:
            conn.close()

    def writer(worker_index: int) -> None:
        if not lock_acquired.wait(timeout=5):
            record_error(TimeoutError("lock holder did not acquire the SQLite write lock"))
            return

        conn = sqlite3.connect(
            resolved_db_path,
            timeout=write_timeout_seconds,
            isolation_level=None,
        )
        try:
            for write_index in range(writes_per_worker):
                record_attempt()
                try:
                    _insert_log(
                        conn,
                        f"writer_{worker_index}",
                        f"concurrent write {write_index}",
                    )
                    record_success()
                except sqlite3.Error as exc:
                    record_error(exc)
        finally:
            conn.close()

    started_at = time.perf_counter()
    lock_thread = threading.Thread(target=hold_write_lock, name="sqlite-lock-holder")
    lock_thread.start()

    writer_threads = [
        threading.Thread(target=writer, args=(worker_index,), name=f"sqlite-writer-{worker_index}")
        for worker_index in range(workers)
    ]
    for thread in writer_threads:
        thread.start()
    for thread in writer_threads:
        thread.join()
    lock_thread.join()
    elapsed_seconds = time.perf_counter() - started_at

    return DbLockReproductionResult(
        db_path=str(resolved_db_path),
        workers=workers,
        writes_per_worker=writes_per_worker,
        hold_lock_seconds=hold_lock_seconds,
        write_timeout_seconds=write_timeout_seconds,
        attempted_writes=attempted_writes,
        successful_writes=successful_writes,
        locked_errors=locked_errors,
        other_errors=other_errors,
        elapsed_seconds=elapsed_seconds,
        error_examples=error_examples,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reproduce SQLite write-lock contention with concurrent log writes."
    )
    parser.add_argument("--db-path", type=Path, help="SQLite DB path. Defaults to a temporary DB.")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--writes-per-worker", type=int, default=5)
    parser.add_argument("--hold-lock-seconds", type=float, default=2.0)
    parser.add_argument("--write-timeout-seconds", type=float, default=0.05)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.db_path:
        result = run_db_lock_reproduction(
            db_path=args.db_path,
            workers=args.workers,
            writes_per_worker=args.writes_per_worker,
            hold_lock_seconds=args.hold_lock_seconds,
            write_timeout_seconds=args.write_timeout_seconds,
        )
        print(json.dumps(result.to_dict(), indent=2))
        return 0

    with tempfile.TemporaryDirectory() as temp_dir:
        result = run_db_lock_reproduction(
            db_path=Path(temp_dir) / "db-lock-reproduction.sqlite",
            workers=args.workers,
            writes_per_worker=args.writes_per_worker,
            hold_lock_seconds=args.hold_lock_seconds,
            write_timeout_seconds=args.write_timeout_seconds,
        )
        print(json.dumps(result.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
