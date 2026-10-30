"""Diagnóstico de solo lectura. No guarda eventos ni llama a Rancherito."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from hikvision_event_bridge.backfill import condition_body, parse_acs_response
from hikvision_event_bridge.config import AppConfig, ConfigError, DeviceConfig
from hikvision_event_bridge.gateway import GatewayError
from hikvision_event_bridge.normalization import loads_gateway_json, parse_history_record, parse_live_part
from hikvision_event_bridge.timeutil import parse_utc

MAX_HISTORY_PAGES = 100
COMPARE_FIELDS = (
    "device_index",
    "serial_no",
    "occurred_at",
    "occurred_at_utc",
    "employee_no",
    "card_no",
    "major_event_type",
    "sub_event_type",
    "attendance_status",
    "label",
    "event_id",
)


@dataclass(frozen=True)
class CompareReport:
    text: str
    searched: bool


def access_device_uid(payload: Any, allowed: set[str] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    body = payload.get("EventNotificationAlert", payload)
    if not isinstance(body, dict) or str(body.get("eventType") or "") != "AccessControllerEvent":
        return None
    device = body.get("devIndex")
    if not isinstance(device, str) or not device.strip():
        return None
    device = device.strip()
    if allowed is not None and device not in allowed:
        return None
    return device


def capture_bodies(raw: bytes) -> list[bytes]:
    raw = raw.replace(b"-- part\r\n", b"-- part\n")
    marker = b"-- part\n"
    if not raw.startswith(marker):
        return [raw]
    bodies = []
    for piece in raw.split(marker):
        if not piece:
            continue
        bodies.append(piece[:-1] if piece.endswith(b"\n") else piece)
    return bodies


def compare_capture(
    path: Path,
    config: AppConfig,
    source: Any,
    *,
    window_seconds: float,
    device_name: str = "",
) -> CompareReport:
    live, device = _live_event(path, config, device_name)
    moment = parse_utc(str(live["occurred_at_utc"]))
    margin = timedelta(seconds=window_seconds)
    page_size = max(1, int(config.backfill.page_size))
    position = 0
    pages = 0
    historical: list[dict[str, Any]] = []
    positions: list[int] = []
    while pages < MAX_HISTORY_PAGES:
        positions.append(position)
        payload = source.search_acs_events(
            device.device_uid,
            condition_body(
                search_id=f"compare-{pages + 1}",
                position=position,
                max_results=page_size,
                start=moment - margin,
                end=moment + margin,
                timezone_name=device.timezone,
            ),
        )
        page = parse_acs_response(payload)
        pages += 1
        if page.error:
            raise GatewayError(page.error)
        for record in page.records:
            parsed = parse_history_record(
                record,
                device_uid=device.device_uid,
                timezone_name=device.timezone,
                result_filter=device.filter,
                strip_biometric=config.privacy.strip_biometric_fields,
            )
            if parsed.event is not None:
                historical.append(parsed.event)
        if page.status != "MORE" or page.num_of_matches <= 0:
            break
        position += page.num_of_matches
    return CompareReport(_render(live, historical, positions, page_size), True)


def _live_event(path: Path, config: AppConfig, device_name: str) -> tuple[dict[str, Any], DeviceConfig]:
    if not path.is_file():
        raise ValueError(f"no existe la captura {path}")
    chosen: dict[str, Any] | None = None
    device: DeviceConfig | None = None
    for body in capture_bodies(path.read_bytes()):
        try:
            payload = loads_gateway_json(body)
        except ValueError:
            continue
        if not isinstance(payload, dict):
            continue
        uid = access_device_uid(payload, None)
        if uid is None:
            continue
        match = _device_for(config, uid, device_name)
        if match is None:
            continue
        parsed = parse_live_part(
            payload,
            timezone_name=match.timezone,
            result_filter=match.filter,
            strip_biometric=config.privacy.strip_biometric_fields,
        )
        if parsed.event is None:
            continue
        chosen = parsed.event
        device = match
        break
    if chosen is None or device is None:
        raise ValueError("la captura no tiene un AccessControllerEvent de un dispositivo configurado")
    return chosen, device


def _device_for(config: AppConfig, device_uid: str, device_name: str) -> DeviceConfig | None:
    if device_name:
        named = config.devices.get(device_name)
        if named is None or not named.enabled:
            raise ConfigError(f"dispositivo desconocido o deshabilitado: {device_name}")
        if named.device_uid != device_uid:
            return None
        return named
    for device in config.devices.values():
        if device.enabled and device.device_uid == device_uid:
            return device
    return None


def _render(live: dict[str, Any], historical: list[dict[str, Any]], positions: list[int], page_size: int) -> str:
    lines = [
        "Comparación de solo lectura. Una muestra no vuelve estable serial_no ni el índice UNIQUE.",
        f"páginas consultadas: {len(positions)}; searchResultPosition={positions}; maxResults={page_size}",
        "vivo:",
        _event_block(live),
    ]
    same = [item for item in historical if item.get("event_id") == live.get("event_id")]
    if same:
        lines.append("historial con el mismo event_id:")
        lines.append(_event_block(same[0]))
        lines.append("event_id igual. Revisa aun así los campos que solo trae una fuente.")
        lines.extend(_exclusive_fields(live, same[0]))
        return "\n".join(lines)
    if not historical:
        lines.append("el historial no devolvió marcaciones en la ventana. No se usa maxResults=1.")
        return "\n".join(lines)
    serial_hits = [item for item in historical if item.get("serial_no") == live.get("serial_no")]
    candidate = serial_hits[0] if len(serial_hits) == 1 else historical[0]
    lines.append("ningún historial produjo el mismo event_id. Candidato más cercano:")
    lines.append(_event_block(candidate))
    lines.append("diferencias de identidad o de texto:")
    for field in COMPARE_FIELDS:
        left = candidate.get(field)
        right = live.get(field)
        if left != right:
            lines.append(f"- {field}: historial={left!r} vivo={right!r}")
    lines.extend(_exclusive_fields(live, candidate))
    lines.append("event_id distinto. No se cambia la clave ni el índice UNIQUE con esta muestra.")
    return "\n".join(lines)


def _event_block(event: dict[str, Any]) -> str:
    shown = {field: event.get(field) for field in COMPARE_FIELDS}
    return json.dumps(shown, ensure_ascii=False, sort_keys=True)


def _exclusive_fields(live: dict[str, Any], historical: dict[str, Any]) -> list[str]:
    live_raw = live.get("raw") if isinstance(live.get("raw"), dict) else {}
    history_raw = historical.get("raw") if isinstance(historical.get("raw"), dict) else {}
    live_keys = _leaf_keys(live_raw)
    history_keys = _leaf_keys(history_raw)
    only_live = sorted(live_keys - history_keys)
    only_history = sorted(history_keys - live_keys)
    lines = []
    if only_live:
        lines.append("claves solo en vivo: " + ", ".join(only_live))
    if only_history:
        lines.append("claves solo en historial: " + ", ".join(only_history))
    return lines


def _leaf_keys(value: Any, prefix: str = "") -> set[str]:
    if not isinstance(value, dict):
        return set()
    keys: set[str] = set()
    for key, item in value.items():
        name = f"{prefix}.{key}" if prefix else str(key)
        keys.add(name)
        keys.update(_leaf_keys(item, name))
    return keys
