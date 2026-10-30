"""Configuración TOML y secretos solo por entorno."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from hikvision_event_bridge.timeutil import parse_configured_time, require_zone

URL_POLICIES = {"", "hold", "use_snapshot", "use_current"}


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class GatewayConfig:
    url: str
    username: str
    password_env: str
    password: str
    verify_tls: bool

    def __repr__(self) -> str:
        return (
            f"GatewayConfig(url={self.url!r}, username={self.username!r}, "
            f"password_env={self.password_env!r}, verify_tls={self.verify_tls!r})"
        )


@dataclass(frozen=True)
class Timeouts:
    connect_seconds: float
    stream_seconds: float
    post_seconds: float


@dataclass(frozen=True)
class StorageConfig:
    sqlite_path: Path
    busy_timeout_ms: int
    lock_ttl_seconds: float


@dataclass(frozen=True)
class RetentionConfig:
    delivered_days: float


@dataclass(frozen=True)
class DeliveryConfig:
    concurrency: int
    initial_backoff_seconds: float
    max_backoff_seconds: float
    auth_hold_seconds: float
    retry_after_cap_seconds: float
    jitter_ratio: float
    lease_seconds: float


@dataclass(frozen=True)
class BackfillConfig:
    enabled: bool
    interval_seconds: float
    overlap_seconds: float
    window_seconds: float
    page_size: int
    min_window_seconds: float
    initial_start: str


@dataclass(frozen=True)
class LimitsConfig:
    max_header_bytes: int
    max_json_part_bytes: int
    max_binary_part_bytes: int


@dataclass(frozen=True)
class FilterConfig:
    major_event_types: tuple[int, ...]
    sub_event_types: tuple[int, ...]
    include_authentication: tuple[str, ...]
    authentication_sub_event_types: tuple[int, ...]
    exclude_door_events: bool
    door_sub_event_types: tuple[int, ...]
    success_sub_event_types: tuple[int, ...]
    failure_sub_event_types: tuple[int, ...]


@dataclass(frozen=True)
class DeviceConfig:
    name: str
    device_uid: str
    enabled: bool
    timezone: str
    destinations: tuple[str, ...]
    filter: FilterConfig


@dataclass(frozen=True)
class DestinationConfig:
    name: str
    url: str
    token_env: str
    token: str
    pending_url_policy: str

    def __repr__(self) -> str:
        return (
            f"DestinationConfig(name={self.name!r}, url={self.url!r}, "
            f"token_env={self.token_env!r}, pending_url_policy={self.pending_url_policy!r})"
        )


@dataclass(frozen=True)
class OrphanConfig:
    name: str
    action: str


@dataclass(frozen=True)
class PrivacyConfig:
    strip_biometric_fields: bool


@dataclass(frozen=True)
class LoggingConfig:
    level: str


@dataclass(frozen=True)
class AppConfig:
    gateway: GatewayConfig
    timeouts: Timeouts
    storage: StorageConfig
    retention: RetentionConfig
    delivery: DeliveryConfig
    backfill: BackfillConfig
    limits: LimitsConfig
    devices: dict[str, DeviceConfig]
    devices_by_uid: dict[str, DeviceConfig]
    destinations: dict[str, DestinationConfig]
    orphans: dict[str, OrphanConfig]
    privacy: PrivacyConfig
    logging: LoggingConfig
    warnings: tuple[str, ...]

    def enabled_devices(self) -> list[DeviceConfig]:
        return [device for device in self.devices.values() if device.enabled]


def load_dotenv(path: Path) -> None:
    """Carga KEY=value sin sustituir variables ya definidas. No registra valores."""
    if not path.is_file():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ConfigError(f"no se pudo leer {path.name}: {exc.strerror}") from exc
    for number, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            raise ConfigError(f"{path.name}:{number} no tiene la forma CLAVE=valor")
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or any(character.isspace() for character in key):
            raise ConfigError(f"{path.name}:{number} tiene una clave inválida")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def load_config(path: Path) -> AppConfig:
    if not path.is_file():
        raise ConfigError(f"no existe el archivo de configuración {path}")
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"TOML inválido en {path.name}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("la configuración debe ser una tabla TOML")
    _require_keys(raw, {"gateway", "timeouts", "storage", "retention", "delivery", "backfill", "limits", "devices", "destinations"}, "raíz")
    _reject_unknown(
        raw,
        {
            "gateway",
            "timeouts",
            "storage",
            "retention",
            "delivery",
            "backfill",
            "limits",
            "devices",
            "destinations",
            "orphaned_destinations",
            "privacy",
            "logging",
        },
        "raíz",
    )
    warnings: list[str] = []
    gateway = _gateway(raw["gateway"])
    timeouts = _timeouts(raw["timeouts"])
    storage = _storage(raw["storage"], path)
    retention = _retention(raw["retention"])
    delivery = _delivery(raw["delivery"], timeouts)
    backfill = _backfill(raw["backfill"])
    limits = _limits(raw["limits"])
    destinations = _destinations(raw["destinations"])
    devices = _devices(raw["devices"], destinations, warnings)
    orphans = _orphans(raw.get("orphaned_destinations", {}), destinations)
    privacy = _privacy(raw.get("privacy", {}))
    logging_config = _logging(raw.get("logging", {}))
    if gateway.verify_tls is False and gateway.url.startswith("https://"):
        warnings.append("verify_tls está desactivado; la conexión al gateway no comprueba el certificado")
    for destination in destinations.values():
        if destination.url.startswith("http://"):
            warnings.append(f"el destino {destination.name} usa HTTP; el token viaja sin TLS")
    return AppConfig(
        gateway=gateway,
        timeouts=timeouts,
        storage=storage,
        retention=retention,
        delivery=delivery,
        backfill=backfill,
        limits=limits,
        devices=devices,
        devices_by_uid={device.device_uid: device for device in devices.values()},
        destinations=destinations,
        orphans=orphans,
        privacy=privacy,
        logging=logging_config,
        warnings=tuple(warnings),
    )


def _gateway(raw: object) -> GatewayConfig:
    table = _table(raw, "gateway")
    _require_keys(table, {"url", "username", "password_env"}, "gateway")
    _reject_unknown(table, {"url", "username", "password_env", "verify_tls"}, "gateway")
    url = _http_url(table["url"], "gateway.url")
    username = _nonempty_str(table["username"], "gateway.username")
    password_env = _env_name(table["password_env"], "gateway.password_env")
    password = os.environ.get(password_env, "")
    if not password:
        raise ConfigError(f"falta el secreto de entorno {password_env} (contraseña del gateway)")
    verify_tls = table.get("verify_tls", True)
    if not isinstance(verify_tls, bool):
        raise ConfigError("gateway.verify_tls debe ser booleano")
    return GatewayConfig(url=url, username=username, password_env=password_env, password=password, verify_tls=verify_tls)


def _timeouts(raw: object) -> Timeouts:
    table = _table(raw, "timeouts")
    _require_keys(table, {"connect_seconds", "stream_seconds", "post_seconds"}, "timeouts")
    _reject_unknown(table, {"connect_seconds", "stream_seconds", "post_seconds"}, "timeouts")
    return Timeouts(
        connect_seconds=_positive(table["connect_seconds"], "timeouts.connect_seconds"),
        stream_seconds=_positive(table["stream_seconds"], "timeouts.stream_seconds"),
        post_seconds=_positive(table["post_seconds"], "timeouts.post_seconds"),
    )


def _storage(raw: object, config_path: Path) -> StorageConfig:
    table = _table(raw, "storage")
    _require_keys(table, {"sqlite_path"}, "storage")
    _reject_unknown(table, {"sqlite_path", "busy_timeout_ms", "lock_ttl_seconds"}, "storage")
    sqlite_value = _nonempty_str(table["sqlite_path"], "storage.sqlite_path")
    sqlite_path = Path(sqlite_value)
    if not sqlite_path.is_absolute():
        sqlite_path = (config_path.parent / sqlite_path).resolve()
    busy = table.get("busy_timeout_ms", 5000)
    lock_ttl = table.get("lock_ttl_seconds", 30)
    return StorageConfig(
        sqlite_path=sqlite_path,
        busy_timeout_ms=int(_positive(busy, "storage.busy_timeout_ms")),
        lock_ttl_seconds=_positive(lock_ttl, "storage.lock_ttl_seconds"),
    )


def _retention(raw: object) -> RetentionConfig:
    table = _table(raw, "retention")
    _require_keys(table, {"delivered_days"}, "retention")
    _reject_unknown(table, {"delivered_days"}, "retention")
    days = table["delivered_days"]
    if isinstance(days, bool) or not isinstance(days, (int, float)) or days < 0:
        raise ConfigError("retention.delivered_days debe ser un número mayor o igual que 0")
    return RetentionConfig(delivered_days=float(days))


def _delivery(raw: object, timeouts: Timeouts) -> DeliveryConfig:
    table = _table(raw, "delivery")
    _require_keys(
        table,
        {"concurrency", "initial_backoff_seconds", "max_backoff_seconds", "auth_hold_seconds", "retry_after_cap_seconds", "jitter_ratio"},
        "delivery",
    )
    _reject_unknown(
        table,
        {
            "concurrency",
            "initial_backoff_seconds",
            "max_backoff_seconds",
            "auth_hold_seconds",
            "retry_after_cap_seconds",
            "jitter_ratio",
            "lease_seconds",
        },
        "delivery",
    )
    initial = _positive(table["initial_backoff_seconds"], "delivery.initial_backoff_seconds")
    maximum = _positive(table["max_backoff_seconds"], "delivery.max_backoff_seconds")
    if maximum < initial:
        raise ConfigError("delivery.max_backoff_seconds debe ser mayor o igual que initial_backoff_seconds")
    jitter = table["jitter_ratio"]
    if isinstance(jitter, bool) or not isinstance(jitter, (int, float)) or not 0 <= float(jitter) <= 1:
        raise ConfigError("delivery.jitter_ratio debe estar entre 0 y 1")
    concurrency = table["concurrency"]
    if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
        raise ConfigError("delivery.concurrency debe ser un entero mayor o igual que 1")
    lease = table.get("lease_seconds", timeouts.post_seconds + 30)
    lease_seconds = _positive(lease, "delivery.lease_seconds")
    if lease_seconds <= timeouts.post_seconds:
        raise ConfigError("delivery.lease_seconds debe ser mayor que timeouts.post_seconds")
    return DeliveryConfig(
        concurrency=concurrency,
        initial_backoff_seconds=initial,
        max_backoff_seconds=maximum,
        auth_hold_seconds=_positive(table["auth_hold_seconds"], "delivery.auth_hold_seconds"),
        retry_after_cap_seconds=_positive(table["retry_after_cap_seconds"], "delivery.retry_after_cap_seconds"),
        jitter_ratio=float(jitter),
        lease_seconds=lease_seconds,
    )


def _backfill(raw: object) -> BackfillConfig:
    table = _table(raw, "backfill")
    _require_keys(table, {"enabled", "interval_seconds", "overlap_seconds", "window_seconds", "page_size", "min_window_seconds", "initial_start"}, "backfill")
    _reject_unknown(
        table,
        {"enabled", "interval_seconds", "overlap_seconds", "window_seconds", "page_size", "min_window_seconds", "initial_start"},
        "backfill",
    )
    enabled = table["enabled"]
    if not isinstance(enabled, bool):
        raise ConfigError("backfill.enabled debe ser booleano")
    overlap = _non_negative(table["overlap_seconds"], "backfill.overlap_seconds")
    window = _positive(table["window_seconds"], "backfill.window_seconds")
    minimum = _positive(table["min_window_seconds"], "backfill.min_window_seconds")
    if overlap >= window:
        raise ConfigError("backfill.overlap_seconds debe ser menor que window_seconds")
    if minimum > window:
        raise ConfigError("backfill.min_window_seconds debe ser menor o igual que window_seconds")
    page_size = table["page_size"]
    if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 100:
        raise ConfigError("backfill.page_size debe ser un entero entre 1 y 100")
    initial = _nonempty_str(table["initial_start"], "backfill.initial_start")
    if enabled:
        try:
            parse_configured_time(initial)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc
    return BackfillConfig(
        enabled=enabled,
        interval_seconds=_positive(table["interval_seconds"], "backfill.interval_seconds"),
        overlap_seconds=overlap,
        window_seconds=window,
        page_size=page_size,
        min_window_seconds=minimum,
        initial_start=initial,
    )


def _limits(raw: object) -> LimitsConfig:
    table = _table(raw, "limits")
    _require_keys(table, {"max_header_bytes", "max_json_part_bytes", "max_binary_part_bytes"}, "limits")
    _reject_unknown(table, {"max_header_bytes", "max_json_part_bytes", "max_binary_part_bytes"}, "limits")
    return LimitsConfig(
        max_header_bytes=_positive_int(table["max_header_bytes"], "limits.max_header_bytes"),
        max_json_part_bytes=_positive_int(table["max_json_part_bytes"], "limits.max_json_part_bytes"),
        max_binary_part_bytes=_positive_int(table["max_binary_part_bytes"], "limits.max_binary_part_bytes"),
    )


def _destinations(raw: object) -> dict[str, DestinationConfig]:
    table = _table(raw, "destinations")
    if not table:
        raise ConfigError("debe existir al menos un destino")
    found: dict[str, DestinationConfig] = {}
    for name, value in table.items():
        item = _table(value, f"destinations.{name}")
        _require_keys(item, {"url", "token_env"}, f"destinations.{name}")
        _reject_unknown(item, {"url", "token_env", "pending_url_policy"}, f"destinations.{name}")
        url = _http_url(item["url"], f"destinations.{name}.url")
        token_env = _env_name(item["token_env"], f"destinations.{name}.token_env")
        token = os.environ.get(token_env, "")
        if not token:
            raise ConfigError(f"falta el secreto de entorno {token_env} (token del destino {name})")
        policy = item.get("pending_url_policy", "")
        if not isinstance(policy, str) or policy not in URL_POLICIES:
            raise ConfigError(
                f"destinations.{name}.pending_url_policy debe ser hold, use_snapshot, use_current o vacío"
            )
        found[name] = DestinationConfig(name=name, url=url, token_env=token_env, token=token, pending_url_policy=policy)
    return found


def _devices(raw: object, destinations: dict[str, DestinationConfig], warnings: list[str]) -> dict[str, DeviceConfig]:
    table = _table(raw, "devices")
    if not table:
        raise ConfigError("debe existir al menos un dispositivo")
    found: dict[str, DeviceConfig] = {}
    seen_uids: dict[str, str] = {}
    enabled = 0
    for name, value in table.items():
        item = _table(value, f"devices.{name}")
        _require_keys(item, {"device_uid", "enabled", "timezone", "destinations", "filter"}, f"devices.{name}")
        _reject_unknown(item, {"device_uid", "enabled", "timezone", "destinations", "filter"}, f"devices.{name}")
        device_uid = _nonempty_str(item["device_uid"], f"devices.{name}.device_uid")
        if device_uid in seen_uids:
            raise ConfigError(f"device_uid repetido en {seen_uids[device_uid]} y {name}")
        seen_uids[device_uid] = name
        if not isinstance(item["enabled"], bool):
            raise ConfigError(f"devices.{name}.enabled debe ser booleano")
        timezone_name = _nonempty_str(item["timezone"], f"devices.{name}.timezone")
        try:
            require_zone(timezone_name)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc
        dest_names = _str_tuple(item["destinations"], f"devices.{name}.destinations")
        if not dest_names:
            raise ConfigError(f"devices.{name}.destinations no puede estar vacío")
        for dest_name in dest_names:
            if dest_name not in destinations:
                raise ConfigError(f"devices.{name} referencia el destino inexistente {dest_name}")
        filt = _filter(item["filter"], f"devices.{name}.filter")
        if item["enabled"]:
            enabled += 1
            if not _filter_is_constrained(filt):
                warnings.append(
                    f"el dispositivo {name} no tiene filtros; reenviará todo AccessControllerEvent, incluidas puertas si el equipo las emite"
                )
        found[name] = DeviceConfig(
            name=name,
            device_uid=device_uid,
            enabled=bool(item["enabled"]),
            timezone=timezone_name,
            destinations=dest_names,
            filter=filt,
        )
    if enabled < 1:
        raise ConfigError("debe haber al menos un dispositivo habilitado")
    return found


def _filter(raw: object, label: str) -> FilterConfig:
    table = _table(raw, label)
    _reject_unknown(
        table,
        {
            "major_event_types",
            "sub_event_types",
            "include_authentication",
            "authentication_sub_event_types",
            "exclude_door_events",
            "door_sub_event_types",
            "success_sub_event_types",
            "failure_sub_event_types",
        },
        label,
    )
    exclude_door = table.get("exclude_door_events", False)
    if not isinstance(exclude_door, bool):
        raise ConfigError(f"{label}.exclude_door_events debe ser booleano")
    door_codes = _int_tuple(table.get("door_sub_event_types", []), f"{label}.door_sub_event_types")
    if exclude_door and not door_codes:
        raise ConfigError(
            f"{label}.exclude_door_events está activo pero door_sub_event_types está vacío. "
            "El bridge no trae códigos de puerta predeterminados: hay que confirmarlos en el equipo."
        )
    success = _int_tuple(table.get("success_sub_event_types", []), f"{label}.success_sub_event_types")
    failure = _int_tuple(table.get("failure_sub_event_types", []), f"{label}.failure_sub_event_types")
    overlap = set(success) & set(failure)
    if overlap:
        raise ConfigError(f"{label} repite códigos en éxito y fallo: {sorted(overlap)}")
    return FilterConfig(
        major_event_types=_int_tuple(table.get("major_event_types", []), f"{label}.major_event_types"),
        sub_event_types=_int_tuple(table.get("sub_event_types", []), f"{label}.sub_event_types"),
        include_authentication=_str_tuple(table.get("include_authentication", []), f"{label}.include_authentication"),
        authentication_sub_event_types=_int_tuple(
            table.get("authentication_sub_event_types", []),
            f"{label}.authentication_sub_event_types",
        ),
        exclude_door_events=exclude_door,
        door_sub_event_types=door_codes,
        success_sub_event_types=success,
        failure_sub_event_types=failure,
    )


def _filter_is_constrained(filt: FilterConfig) -> bool:
    return bool(
        filt.major_event_types
        or filt.sub_event_types
        or filt.include_authentication
        or filt.authentication_sub_event_types
        or filt.exclude_door_events
    )


def _orphans(raw: object, destinations: dict[str, DestinationConfig]) -> dict[str, OrphanConfig]:
    if raw in ({}, None):
        return {}
    table = _table(raw, "orphaned_destinations")
    found: dict[str, OrphanConfig] = {}
    for name, value in table.items():
        if name in destinations:
            raise ConfigError(f"orphaned_destinations.{name} sigue definido como destino activo")
        item = _table(value, f"orphaned_destinations.{name}")
        _require_keys(item, {"action"}, f"orphaned_destinations.{name}")
        _reject_unknown(item, {"action"}, f"orphaned_destinations.{name}")
        action = item["action"]
        if action != "hold":
            raise ConfigError(
                f"orphaned_destinations.{name}.action solo admite hold. "
                "No se reasignan pendientes a otra URL."
            )
        found[name] = OrphanConfig(name=name, action="hold")
    return found


def _privacy(raw: object) -> PrivacyConfig:
    if raw in ({}, None):
        return PrivacyConfig(strip_biometric_fields=True)
    table = _table(raw, "privacy")
    _reject_unknown(table, {"strip_biometric_fields"}, "privacy")
    enabled = table.get("strip_biometric_fields", True)
    if not isinstance(enabled, bool):
        raise ConfigError("privacy.strip_biometric_fields debe ser booleano")
    return PrivacyConfig(strip_biometric_fields=enabled)


def _logging(raw: object) -> LoggingConfig:
    if raw in ({}, None):
        return LoggingConfig(level="INFO")
    table = _table(raw, "logging")
    _reject_unknown(table, {"level"}, "logging")
    level = str(table.get("level", "INFO")).upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        raise ConfigError("logging.level debe ser DEBUG, INFO, WARNING o ERROR")
    return LoggingConfig(level=level)


def _table(raw: object, label: str) -> dict[str, object]:
    if not isinstance(raw, dict):
        raise ConfigError(f"{label} debe ser una tabla")
    return raw


def _require_keys(table: dict[str, object], required: set[str], label: str) -> None:
    missing = sorted(required - set(table))
    if missing:
        raise ConfigError(f"faltan claves en {label}: {', '.join(missing)}")


def _reject_unknown(table: dict[str, object], allowed: set[str], label: str) -> None:
    unknown = sorted(set(table) - allowed)
    if unknown:
        raise ConfigError(f"claves no reconocidas en {label}: {', '.join(unknown)}")


def _nonempty_str(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{label} debe ser texto no vacío")
    return value.strip()


def _env_name(value: object, label: str) -> str:
    name = _nonempty_str(value, label)
    if not name.replace("_", "").isalnum() or name[:1].isdigit():
        raise ConfigError(f"{label} no es un nombre de variable de entorno válido")
    return name


def _http_url(value: object, label: str) -> str:
    url = _nonempty_str(value, label)
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ConfigError(f"{label} debe ser una URL http o https")
    if parts.username or parts.password:
        raise ConfigError(f"{label} no debe incluir usuario ni contraseña")
    return url.rstrip("/")


def _positive(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) <= 0:
        raise ConfigError(f"{label} debe ser un número mayor que 0")
    return float(value)


def _non_negative(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) < 0:
        raise ConfigError(f"{label} debe ser un número mayor o igual que 0")
    return float(value)


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConfigError(f"{label} debe ser un entero mayor que 0")
    return value


def _int_tuple(value: object, label: str) -> tuple[int, ...]:
    if not isinstance(value, list):
        raise ConfigError(f"{label} debe ser una lista")
    numbers: list[int] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int):
            raise ConfigError(f"{label} solo admite enteros")
        numbers.append(item)
    return tuple(numbers)


def _str_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ConfigError(f"{label} debe ser una lista")
    texts: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ConfigError(f"{label} solo admite texto no vacío")
        texts.append(item.strip())
    return tuple(texts)
