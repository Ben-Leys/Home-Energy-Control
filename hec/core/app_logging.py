# core/app_logging.py
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from hec.core import constants as c
from hec.core.notifications import NotificationDispatcher


def sync_app_status_from_incidents(global_app_state, db_handler):
    """Derive top-level warning/alarm state from unacknowledged active incidents."""
    if not global_app_state or not db_handler:
        return

    summary = db_handler.get_active_incident_summary()
    highest_severity = summary.get("highest_severity")
    current_status = global_app_state.get("app_state")

    if highest_severity == "error":
        global_app_state.set("app_state", c.AppStatus.ALARM)
    elif highest_severity == "warning":
        global_app_state.set("app_state", c.AppStatus.WARNING)
    elif current_status in (c.AppStatus.WARNING, c.AppStatus.ALARM):
        global_app_state.set("app_state", c.AppStatus.NORMAL)


class GlobalStateHandler(logging.Handler):
    """Custom logging handler to update global application state."""
    def __init__(self, global_app_state):
        super().__init__()
        self.global_app_state = global_app_state
        self.db = None

    def emit(self, record):
        # Persist to Database unless exception, don't log to avoid infinite loop
        if self.db:
            try:
                message = self.format(record)
                occurred_at_utc = datetime.fromtimestamp(record.created, timezone.utc)
                log_entry = {
                    'timestamp': occurred_at_utc.isoformat(),
                    'level': record.levelname,
                    'message': message,
                    'module': record.module
                }
                self.db.store_log(log_entry)
                if record.levelno >= logging.WARNING:
                    severity = "error" if record.levelno >= logging.ERROR else "warning"
                    incident = self.db.record_incident(
                        severity=severity,
                        source=record.name,
                        message=message,
                        occurred_at_utc=occurred_at_utc,
                    )
                    NotificationDispatcher(self.db).dispatch_incident(incident)
                    sync_app_status_from_incidents(self.global_app_state, self.db)
            except Exception:
                import sys
                print(f"CRITICAL: Logging to DB failed!", file=sys.stderr)
        elif record.levelno == logging.WARNING:
            self.global_app_state.set("app_state", c.AppStatus.WARNING)
        elif record.levelno >= logging.ERROR:
            self.global_app_state.set("app_state", c.AppStatus.ALARM)

    def set_db_handler(self, db):
        self.db = db


def inject_db_to_logging(db_handler):
    """Finds the GlobalStateHandler and injects the database connection."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, GlobalStateHandler):
            handler.set_db_handler(db_handler)
            logging.info("Database handler successfully injected into Logging system.")
            return True
    logging.error("Failed to inject DB handler: GlobalStateHandler not found.")
    return False


def start_logger(config, global_app_state=None):
    """Configures logging for the application based on the config."""
    log_level = config.get('application', {}).get('log_level', 'INFO').upper()
    log_level = getattr(logging, log_level, logging.INFO)

    log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear any existing handlers to avoid duplicate logging
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(log_format, datefmt=date_format)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File Handler
    if config.get('application', {}).get('log_to_file', False):
        log_file_path_str = config.get('application', {}).get('log_file_path', 'logfile.log')
        project_root = Path(__file__).resolve().parent.parent
        log_file_path = project_root / log_file_path_str

        # Ensure log directory exists
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        #  RotatingFileHandler for log management. Settings in config
        max_bytes = config.get('application', {}).get('log_rotation_max_bytes', 5 * 1024 * 1024)
        backup_count = config.get('application', {}).get('log_rotation_backup_count', 5)
        file_handler = RotatingFileHandler(
            log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(log_format, datefmt=date_format)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        root_logger.info(f"Logging to file: {log_file_path}")
    else:
        root_logger.info("File logging is disabled in config.")

    # Add GlobalStateHandler
    if global_app_state is not None:
        global_state_handler = GlobalStateHandler(global_app_state)
        global_state_handler.name = "GlobalStateHandler"
        global_state_handler.setLevel(logging.INFO)  # Only WARNING and ERROR
        root_logger.addHandler(global_state_handler)

    root_logger.info(f"Logger started. Level: {log_level}")
