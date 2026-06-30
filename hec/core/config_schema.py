from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class ConfigValidationError(ValueError):
    """Raised when config.yaml is syntactically valid but not usable by HEC."""


@dataclass(frozen=True)
class ApplicationConfig:
    log_level: str = "INFO"
    tariffs_file_name: str = "tariffs.yaml"
    log_to_file: bool = False


@dataclass(frozen=True)
class DatabaseConfig:
    path: str
    busy_timeout_ms: int = 10000
    history_retention_days: int = 1095
    log_retention_hours: int = 72


@dataclass(frozen=True)
class SchedulerConfig:
    timezone: str = "Europe/Brussels"
    thread_pool_max_workers: int = 10
    run_in_background: bool = True


@dataclass(frozen=True)
class HistoricDataConfig:
    start_date: str


@dataclass(frozen=True)
class AuthConfig:
    enabled: bool = False
    password_env: str = "HEC_AUTH_PASSWORD"
    password_hash_env: str = "HEC_AUTH_PASSWORD_HASH"
    cookie_secret_env: str = "HEC_AUTH_COOKIE_SECRET"
    cookie_max_age_days: int = 365
    csrf_enabled: bool = True
    same_origin_enabled: bool = True
    secure_cookie: bool = False


@dataclass(frozen=True)
class ApiServerConfig:
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8123
    debug: bool = False
    auth: AuthConfig = AuthConfig()


@dataclass(frozen=True)
class RuntimeConfig:
    restart_strategy: str = "supervised_process"
    restart_exit_code: int = 75
    main_loop_sleep_seconds: float = 1.0


@dataclass(frozen=True)
class HttpConfig:
    default_timeout_seconds: float = 10.0
    retries: int = 2
    backoff_factor: float = 0.2
    verify_tls: bool = True


@dataclass(frozen=True)
class TypedAppConfig:
    application: ApplicationConfig
    database: DatabaseConfig
    scheduler: SchedulerConfig
    historic_data: HistoricDataConfig
    api_server: ApiServerConfig
    runtime: RuntimeConfig
    http: HttpConfig


def validate_app_config(config: Mapping[str, Any]) -> TypedAppConfig:
    """Validate the boot-critical shape of config.yaml and return typed sections."""
    if not isinstance(config, Mapping):
        raise ConfigValidationError("config must be a YAML mapping")

    application = _validate_application(_section(config, "application", required=False))
    database = _validate_database(_section(config, "database"))
    scheduler = _validate_scheduler(_section(config, "scheduler"))
    historic_data = _validate_historic_data(_section(config, "historic_data"))
    api_server = _validate_api_server(_section(config, "api_server", required=False))
    runtime = _validate_runtime(_section(config, "runtime", required=False))
    http = _validate_http(_section(config, "http", required=False))

    return TypedAppConfig(
        application=application,
        database=database,
        scheduler=scheduler,
        historic_data=historic_data,
        api_server=api_server,
        runtime=runtime,
        http=http,
    )


def _section(config: Mapping[str, Any], name: str, required: bool = True) -> Mapping[str, Any]:
    value = config.get(name)
    if value is None:
        if required:
            raise ConfigValidationError(f"{name} section is required")
        return {}
    if not isinstance(value, Mapping):
        raise ConfigValidationError(f"{name} must be a mapping")
    return value


def _validate_application(section: Mapping[str, Any]) -> ApplicationConfig:
    return ApplicationConfig(
        log_level=_string(section, "log_level", default="INFO"),
        tariffs_file_name=_string(section, "tariffs_file_name", default="tariffs.yaml"),
        log_to_file=_bool(section, "log_to_file", default=False),
    )


def _validate_database(section: Mapping[str, Any]) -> DatabaseConfig:
    return DatabaseConfig(
        path=_string(section, "path", "database.path"),
        busy_timeout_ms=_int(section, "busy_timeout_ms", default=10000, minimum=1),
        history_retention_days=_int(section, "history_retention_days", default=1095, minimum=1),
        log_retention_hours=_int(section, "log_retention_hours", default=72, minimum=1),
    )


def _validate_scheduler(section: Mapping[str, Any]) -> SchedulerConfig:
    timezone_name = _timezone(section, "timezone", "scheduler.timezone", default="Europe/Brussels")
    return SchedulerConfig(
        timezone=timezone_name,
        thread_pool_max_workers=_int(section, "thread_pool_max_workers", default=10, minimum=1),
        run_in_background=_bool(section, "run_in_background", default=True),
    )


def _validate_historic_data(section: Mapping[str, Any]) -> HistoricDataConfig:
    start_date = _string(section, "start_date", "historic_data.start_date")
    try:
        date.fromisoformat(start_date)
    except ValueError as exc:
        raise ConfigValidationError("historic_data.start_date must be an ISO date") from exc
    return HistoricDataConfig(start_date=start_date)


def _validate_api_server(section: Mapping[str, Any]) -> ApiServerConfig:
    auth_section = section.get("auth", {})
    if auth_section is None:
        auth_section = {}
    if not isinstance(auth_section, Mapping):
        raise ConfigValidationError("api_server.auth must be a mapping")

    auth = AuthConfig(
        enabled=_bool(auth_section, "enabled", default=False),
        password_env=_string(auth_section, "password_env", default="HEC_AUTH_PASSWORD"),
        password_hash_env=_string(auth_section, "password_hash_env", default="HEC_AUTH_PASSWORD_HASH"),
        cookie_secret_env=_string(auth_section, "cookie_secret_env", default="HEC_AUTH_COOKIE_SECRET"),
        cookie_max_age_days=_int(auth_section, "cookie_max_age_days", default=365, minimum=1),
        csrf_enabled=_bool(auth_section, "csrf_enabled", default=True),
        same_origin_enabled=_bool(auth_section, "same_origin_enabled", default=True),
        secure_cookie=_bool(auth_section, "secure_cookie", default=False),
    )
    return ApiServerConfig(
        enabled=_bool(section, "enabled", default=True),
        host=_string(section, "host", default="0.0.0.0"),
        port=_int(section, "port", default=8123, minimum=1, maximum=65535),
        debug=_bool(section, "debug", default=False),
        auth=auth,
    )


def _validate_runtime(section: Mapping[str, Any]) -> RuntimeConfig:
    return RuntimeConfig(
        restart_strategy=_string(section, "restart_strategy", default="supervised_process"),
        restart_exit_code=_int(section, "restart_exit_code", default=75, minimum=0, maximum=255),
        main_loop_sleep_seconds=_float(
            section,
            "main_loop_sleep_seconds",
            default=1.0,
            minimum=0.01,
        ),
    )


def _validate_http(section: Mapping[str, Any]) -> HttpConfig:
    return HttpConfig(
        default_timeout_seconds=_float(section, "default_timeout_seconds", default=10.0, minimum=0.1),
        retries=_int(section, "retries", default=2, minimum=0),
        backoff_factor=_float(section, "backoff_factor", default=0.2, minimum=0.0),
        verify_tls=_bool(section, "verify_tls", default=True),
    )


def _string(section: Mapping[str, Any], key: str, display_name: str | None = None, default: str | None = None) -> str:
    value = section.get(key, default)
    name = display_name or key
    if value is None or not isinstance(value, str) or not value.strip():
        raise ConfigValidationError(f"{name} must be a non-empty string")
    return value


def _bool(section: Mapping[str, Any], key: str, default: bool) -> bool:
    value = section.get(key, default)
    if not isinstance(value, bool):
        raise ConfigValidationError(f"{key} must be true or false")
    return value


def _int(
        section: Mapping[str, Any],
        key: str,
        default: int | None = None,
        minimum: int | None = None,
        maximum: int | None = None,
) -> int:
    value = section.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigValidationError(f"{key} must be an integer")
    if minimum is not None and value < minimum:
        raise ConfigValidationError(f"{key} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ConfigValidationError(f"{key} must be at most {maximum}")
    return value


def _float(
        section: Mapping[str, Any],
        key: str,
        display_name: str | None = None,
        default: float | None = None,
        minimum: float | None = None,
) -> float:
    value = section.get(key, default)
    name = display_name or key
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigValidationError(f"{name} must be a number")
    final_value = float(value)
    if minimum is not None and final_value < minimum:
        raise ConfigValidationError(f"{name} must be at least {minimum}")
    return final_value


def _timezone(
        section: Mapping[str, Any],
        key: str,
        display_name: str,
        default: str | None = None,
) -> str:
    timezone_name = _string(section, key, display_name, default=default)
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ConfigValidationError(f"{display_name} must be a valid IANA timezone") from exc
    return timezone_name
