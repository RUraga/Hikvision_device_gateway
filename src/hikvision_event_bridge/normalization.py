"""Normalización de AccessControllerEvent en vivo y de AcsEvent histórico.

Las rutas salen de la guía del Device Gateway V1.6.1:
- vivo: JSON_EventNotificationAlert_AccessControlEventMsg (A.33)
- historial: JSON_AcsEvent (A.2), consultado por devIndex

No se traducen labels ni se inventan entradas o salidas.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from hikvision_event_bridge.config import FilterConfig
from hikvision_event_bridge.identity import IDENTITY_VERSION, canonical_document, event_id_for
from hikvision_event_bridge.timeutil import format_utc, parse_event_time

SCHEMA_VERSION = 1
NORMALIZATION_VERSION = 1

# URLs, archivos y coordenadas de imagen. No hacen falta para la marcación.
# El bridge no descarga pictureURL ni conserva plantillas faciales.
BIOMETRIC_KEYS = {
    "pictureurl",
    "visiblelighturl",
    "visiblelightpicurl",
    "thermalurl",
    "thermalpicurl",
    "faceurl",
    "facepicurl",
    "humanfaceurl",
    "filename",
    "facedata",
    "modeldata",
    "regioncoordinates",
    "currtemperature",
    "isabnomaltemperature",
    "isabnormaltemperature",
}

_TRAILING_COMMA = re.compile(r",(\s*[}\]])")
_TOKEN_SPLIT = re.compile(r"And|Or")


@dataclass
class ParsedPart:
    kind: str
    event: dict[str, Any] | None = None
    device_uid: str | None = None
    reason: str | None = None


def loads_gateway_json(data: bytes) -> Any:
    """JSON estricto y, si falla, una sola tolerancia: comas finales.

    Los ejemplos de la guía incluyen comas finales, que no son JSON válido.
    No se reparan comillas simples, comentarios ni otros formatos.
    """
    text = _decode_json_prefix(data).strip("\x00 \r\n\t")
    decoder = json.JSONDecoder()
    try:
        value, _end = decoder.raw_decode(text)
        return value
    except json.JSONDecodeError:
        fixed = _TRAILING_COMMA.sub(r"\1", text)
        if fixed == text:
            raise
        value, _end = decoder.raw_decode(fixed)
        return value


def _decode_json_prefix(data: bytes) -> str:
    """El ejemplo con foto binaria pone JPEG detrás del JSON, en la misma parte."""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        return data[: exc.start].decode("utf-8", "strict")


def authentication_tokens(verify_mode: str | None) -> set[str]:
    if not verify_mode:
        return set()
    return {token.casefold() for token in _TOKEN_SPLIT.split(verify_mode) if token}


def classify_result(sub_event_type: int, filt: FilterConfig) -> str:
    success = sub_event_type in filt.success_sub_event_types
    failure = sub_event_type in filt.failure_sub_event_types
    if success and failure:
        return "unknown"
    if success:
        return "success"
    if failure:
        return "failure"
    return "unknown"


def parse_live_part(
    payload: Any,
    *,
    timezone_name: str | None,
    result_filter: FilterConfig | None,
    strip_biometric: bool,
) -> ParsedPart:
    if not isinstance(payload, dict):
        return ParsedPart(kind="invalid", reason="json_no_objeto")
    body = payload.get("EventNotificationAlert", payload)
    if not isinstance(body, dict):
        return ParsedPart(kind="invalid", reason="alerta_no_objeto")
    event_type = str(body.get("eventType") or "")
    device_uid = _text(body.get("devIndex"))
    if event_type.casefold() == "heartbeat":
        return ParsedPart(kind="heartbeat", device_uid=device_uid)
    if event_type != "AccessControllerEvent":
        return ParsedPart(kind="ignored", device_uid=device_uid, reason=event_type or "sin_tipo")
    access = body.get("AccessControllerEvent")
    if not isinstance(access, dict):
        return ParsedPart(kind="invalid", device_uid=device_uid, reason="sin_access_controller_event")
    if result_filter is None or timezone_name is None:
        return ParsedPart(kind="event_unrouted", device_uid=device_uid)
    try:
        event = _build_event(
            source_container=access,
            occurred_raw=body.get("dateTime"),
            device_uid=device_uid or "",
            timezone_name=timezone_name,
            source="live",
            filt=result_filter,
            raw=_prepare_raw(payload, strip_biometric),
        )
    except ValueError as exc:
        return ParsedPart(kind="invalid", device_uid=device_uid, reason=str(exc))
    return ParsedPart(kind="event", event=event, device_uid=device_uid)


def parse_history_record(
    record: dict[str, Any],
    *,
    device_uid: str,
    timezone_name: str,
    result_filter: FilterConfig,
    strip_biometric: bool,
) -> ParsedPart:
    try:
        event = _build_event(
            source_container=record,
            occurred_raw=record.get("time"),
            device_uid=device_uid,
            timezone_name=timezone_name,
            source="backfill",
            filt=result_filter,
            raw=_prepare_raw(record, strip_biometric),
            historical=True,
        )
    except ValueError as exc:
        return ParsedPart(kind="invalid", device_uid=device_uid, reason=str(exc))
    return ParsedPart(kind="event", event=event, device_uid=device_uid)


def _build_event(
    *,
    source_container: dict[str, Any],
    occurred_raw: Any,
    device_uid: str,
    timezone_name: str,
    source: str,
    filt: FilterConfig,
    raw: dict[str, Any],
    historical: bool = False,
) -> dict[str, Any]:
    if not device_uid:
        raise ValueError("sin_dev_index")
    if not isinstance(occurred_raw, str):
        raise ValueError("sin_fecha")
    contract_time, occurred_utc = parse_event_time(occurred_raw, timezone_name)
    if historical:
        major = _as_int(source_container.get("major"))
        sub = _as_int(source_container.get("minor"))
    else:
        major = _as_int(source_container.get("majorEventType"))
        sub = _as_int(source_container.get("subEventType"))
    serial = _as_int(source_container.get("serialNo"))
    if major is None or sub is None:
        raise ValueError("sin_tipo_major_minor")
    if serial is None:
        raise ValueError("sin_serial")
    employee = _employee(source_container)
    card = _token(source_container.get("cardNo"))
    attendance = _token(source_container.get("attendanceStatus"))
    label = _token(source_container.get("label"))
    verify_mode = _token(source_container.get("currentVerifyMode"))
    name = _token(source_container.get("name")) or _token(source_container.get("employeeName"))
    document = canonical_document(
        device_uid=device_uid,
        serial_no=serial,
        occurred_at_utc=occurred_utc,
        employee_no=employee,
        card_no=card,
        major_event_type=major,
        sub_event_type=sub,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id_for(document),
        "identity_version": IDENTITY_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "device_index": device_uid,
        "serial_no": serial,
        "occurred_at": contract_time,
        "occurred_at_utc": format_utc(occurred_utc),
        "employee_no": employee,
        "card_no": card,
        "attendance_status": attendance,
        "label": label,
        "major_event_type": major,
        "sub_event_type": sub,
        "result": classify_result(sub, filt),
        "verify_mode": verify_mode,
        "name": name,
        "source": source,
        "raw": raw,
    }


def public_payload(event: dict[str, Any]) -> dict[str, Any]:
    """Cuerpo HTTP. El event_id canónico no se sustituye por el id de Laravel."""
    return {
        "schema_version": event["schema_version"],
        "event_id": event["event_id"],
        "device_index": event["device_index"],
        "serial_no": event["serial_no"],
        "occurred_at": event["occurred_at"],
        "employee_no": event["employee_no"] if isinstance(event.get("employee_no"), str) else "",
        "card_no": event["card_no"] if isinstance(event.get("card_no"), str) else "",
        "attendance_status": event["attendance_status"],
        "label": event["label"],
        "major_event_type": event["major_event_type"],
        "sub_event_type": event["sub_event_type"],
        "result": event["result"],
        "verify_mode": event["verify_mode"],
        "name": event["name"],
        "source": event["source"],
        "raw": event["raw"],
    }


def strip_biometric(value: Any) -> Any:
    if isinstance(value, dict):
        clean = {}
        for key, item in value.items():
            if str(key).casefold() in BIOMETRIC_KEYS:
                continue
            clean[key] = strip_biometric(item)
        return clean
    if isinstance(value, list):
        return [strip_biometric(item) for item in value]
    return value


def _prepare_raw(payload: Any, enabled: bool) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if enabled:
        stripped = strip_biometric(payload)
        return stripped if isinstance(stripped, dict) else {}
    return payload


def _employee(container: dict[str, Any]) -> str | None:
    string_form = container.get("employeeNoString")
    if string_form not in (None, ""):
        return _token(string_form)
    return _token(container.get("employeeNo"))


def _token(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return None
    if isinstance(value, str):
        if value == "":
            return None
        return value
    return None


def _text(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"-?\d+", value):
        return int(value)
    return None
