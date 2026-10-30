"""Identidad provisional de una marcación.

La guía del Device Gateway (A.33) define ``serialNo`` como número de serie del
evento y ``frontSerialNo`` para detectar pérdidas cuando el serial de la
suscripción no coincide con el anterior. El historial (A.2) trae ``serialNo``
y no documenta ``frontSerialNo``. Por eso la clave no puede usar solo
``device_uid + serial_no`` ni mezclar ``frontSerialNo``, que existe en un
solo canal.

Estrategia provisional (identity_version = 1), SHA-256 sobre JSON canónico:

- device_uid
- serial_no
- occurred_at en UTC, truncado al segundo
- employee_no (texto; vacío si falta)
- card_no (texto; vacío si falta)
- major_event_type
- sub_event_type

No entran: UUID de recepción, received_at, source, intentos, JSON raw,
nombre, label, attendance_status, verify_mode ni result. Esos últimos pueden
omitirse en una de las dos fuentes. Si ambos traen un valor distinto en
nombre, label, attendance_status o verify_mode, se registra una incidencia y
no se sobrescribe el evento ya guardado.

Limitación: si el vivo y el historial traen distinto ``serialNo`` para la
misma marcación, no se deduplican. Si el dispositivo reutiliza un serial con
el mismo segundo, persona, tarjeta y tipos, se considerarían el mismo evento.
Hay que contrastarlo con el terminal de Rancherito antes de dar la clave por
definitiva.

Rancherito deriva sha256("hikvision-event-v1|" + event_id) y todavía impone
UNIQUE(device_uid, serial_no). Ninguna de las dos sustituye esta clave. No se
cambia el event_id entre reintentos, en vivo o en historial, ni para esquivar
un 409.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

IDENTITY_VERSION = 1

SOFT_FIELDS = ("attendance_status", "label", "verify_mode", "name")


def canonical_document(
    *,
    device_uid: str,
    serial_no: int,
    occurred_at_utc: datetime,
    employee_no: str | None,
    card_no: str | None,
    major_event_type: int,
    sub_event_type: int,
) -> dict[str, object]:
    from hikvision_event_bridge.timeutil import format_utc

    return {
        "v": IDENTITY_VERSION,
        "device_uid": device_uid,
        "serial_no": serial_no,
        "occurred_at": format_utc(occurred_at_utc),
        "employee_no": employee_no or "",
        "card_no": card_no or "",
        "major_event_type": major_event_type,
        "sub_event_type": sub_event_type,
    }


def canonical_bytes(document: dict[str, object]) -> bytes:
    return json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def event_id_for(document: dict[str, object]) -> str:
    return hashlib.sha256(canonical_bytes(document)).hexdigest()


def soft_conflict(stored: dict[str, object], incoming: dict[str, object]) -> str | None:
    """Conflicto solo si ambos lados traen un valor no vacío y distinto.

    Un lado vacío no contradice al otro: el historial documentado puede omitir
    el nombre. No se actualiza el registro existente.
    """
    for field in SOFT_FIELDS:
        old = _text(stored.get(field))
        new = _text(incoming.get(field))
        if old and new and old != new:
            return field
    return None


def _text(value: object) -> str:
    if value is None:
        return ""
    return str(value)
