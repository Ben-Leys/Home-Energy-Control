# core/app_logging.py
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from hec.core import constants as c


class GlobalStateHandler(logging.Handler):
    """Custom logging handler to update global application state."""
    def __init__(self, global_app_state):
        super().__init__()
        self.global_app_state = global_app_state

    def emit(self, record):
        if record.levelno == logging.WARNING:
            self.global_app_state.set("app_state", c.AppStatus.WARNING)
        elif record.levelno >= logging.ERROR:
            self.global_app_state.set("app_state", c.AppStatus.ALARM)


def start_logger(config, global_app_state=None):
    """Configures logging for the application based on the config."""
    log_level_app = config.get('application', {}).get('log_level', 'INFO').upper()
    log_level_file = config.get('application', {}).get('log_level_file', 'DEBUG').upper()
    log_level = getattr(logging, log_level_app, logging.INFO)

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
        file_handler.setLevel(log_level_file)
        file_formatter = logging.Formatter(log_format, datefmt=date_format)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        root_logger.info(f"Logging to file: {log_file_path}")
    else:
        root_logger.info("File logging is disabled in config.")

    # Add GlobalStateHandler
    if global_app_state is not None:
        global_state_handler = GlobalStateHandler(global_app_state)
        global_state_handler.setLevel(logging.WARNING)  # Only WARNING and ERROR
        root_logger.addHandler(global_state_handler)

    root_logger.info(f"Logger started. Level: {log_level_app}")
