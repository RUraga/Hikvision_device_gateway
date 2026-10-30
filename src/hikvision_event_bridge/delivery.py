"""Entrega HTTP a destinos como Rancherito. No sigue redirecciones.

Rancherito guarda una clave derivada sha256("hikvision-event-v1|" + event_id).
Esa clave no se envía. Idempotency-Key es el mismo event_id del cuerpo, en
todos los reintentos y tanto si el evento llegó en vivo como por historial.
Un 409 no autoriza a cambiar esa clave.
"""

from __future__ import annotations

import hashlib
import json
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from hikvision_event_bridge.config import DeliveryConfig, DestinationConfig
from hikvision_event_bridge.timeutil import add_seconds

RETRY_STATUSES = {408, 425, 429, 500, 502, 503, 504}
REJECT_STATUSES = {400, 404, 405, 406, 410, 411, 412, 413, 414, 415, 416, 417, 428, 431}
SUCCESS_STATUSES = {200, 201}
MAX_BODY = 65536
RANCHERITO_STORAGE_PREFIX = "hikvision-event-v1|"
# Informativos. No deciden la entrega: accepted=true basta, aunque el
# procesamiento laboral quede en error o ignored.
RANCHERITO_PROCESSING_STATUSES = frozenset(
    {
        "pending",
        "processed",
        "unresolved_identity",
        "unmapped_action",
        "ignored",
        "error",
        "pending_classification",
    }
)
_OFFSET = re.compile(r"(?:Z|[+-]\d{2}:\d{2})$")
SERIAL_MAX = 4_294_967_295
TEXT_LIMITS = {
    "event_id": 191,
    "device_index": 191,
    "employee_no": 64,
    "card_no": 64,
    "attendance_status": 32,
    "label": 100,
    "result": 32,
    "verify_mode": 32,
    "name": 100,
}


@dataclass(frozen=True)
class AttemptResult:
    outcome: str
    http_status: int | None
    error: str | None
    retry_after: float | None = None


def backoff_delay(
    attempt: int,
    *,
    initial: float,
    maximum: float,
    jitter_ratio: float,
    rng: random.Random,
) -> float:
    step = max(1, attempt)
    base = min(maximum, initial * (2 ** (step - 1)))
    if jitter_ratio:
        delta = base * jitter_ratio
        base += rng.uniform(-delta, delta)
    return max(0.0, base)


def parse_retry_after(value: str | None, now: datetime) -> float | None:
    if not value:
        return None
    text = value.strip()
    if text.isdigit():
        return float(text)
    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return max(0.0, (moment - now).total_seconds())


def rancherito_storage_key(event_id: str) -> str:
    """Clave interna de Rancherito. El bridge no la manda ni la usa como identidad."""
    return hashlib.sha256(f"{RANCHERITO_STORAGE_PREFIX}{event_id}".encode("utf-8")).hexdigest()


def interpret_response(status: int, headers: Any, body: bytes, now: datetime) -> AttemptResult:
    retry_after = parse_retry_after(headers.get("Retry-After") if headers is not None else None, now)
    if 300 <= status < 400:
        return AttemptResult("rejected", status, "redirect_not_followed", retry_after)
    if status == 401:
        return AttemptResult("auth_blocked", status, "token_invalido", retry_after)
    if status == 403:
        return AttemptResult("auth_blocked", status, "dispositivo_no_registrado", retry_after)
    if status == 409:
        return AttemptResult("conflict", status, "conflicto_identidad", retry_after)
    if status == 422:
        return AttemptResult("rejected", status, "contrato_invalido", retry_after)
    if status in REJECT_STATUSES:
        return AttemptResult("rejected", status, f"http_{status}", retry_after)
    if status in RETRY_STATUSES:
        return AttemptResult("retry", status, f"http_{status}", retry_after)
    if status in SUCCESS_STATUSES:
        if _accepted(body):
            return AttemptResult("delivered", status, None, retry_after)
        return AttemptResult("retry", status, "confirmacion_invalida", retry_after)
    if 400 <= status < 500:
        return AttemptResult("rejected", status, f"http_{status}", retry_after)
    return AttemptResult("retry", status, f"http_{status}", retry_after)


def next_delay(result: AttemptResult, *, attempt: int, delivery: DeliveryConfig, rng: random.Random) -> float:
    if result.outcome == "auth_blocked":
        requested = result.retry_after if result.retry_after is not None else delivery.auth_hold_seconds
        return min(max(requested, delivery.auth_hold_seconds), max(delivery.auth_hold_seconds, delivery.retry_after_cap_seconds))
    delay = backoff_delay(
        attempt,
        initial=delivery.initial_backoff_seconds,
        maximum=delivery.max_backoff_seconds,
        jitter_ratio=delivery.jitter_ratio,
        rng=rng,
    )
    if result.retry_after is not None:
        delay = max(delay, min(result.retry_after, delivery.retry_after_cap_seconds))
    return delay


def schedule_after(now: datetime, delay: float) -> datetime:
    return add_seconds(now, delay)


class DestinationPoster:
    def __init__(self, connect_timeout: float, post_timeout: float, verify_tls: bool = True) -> None:
        self.connect_timeout = connect_timeout
        self.post_timeout = post_timeout
        self.verify_tls = verify_tls

    def post(self, destination: DestinationConfig, url: str, payload: dict[str, Any], event_id: str) -> AttemptResult:
        problem = _contract_problem(payload, event_id)
        if problem is not None:
            return AttemptResult("rejected", None, problem)
        session = requests.Session()
        session.trust_env = False
        session.verify = self.verify_tls
        headers = {
            "Authorization": f"Bearer {destination.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Idempotency-Key": event_id,
        }
        try:
            response = session.post(
                url,
                json=payload,
                headers=headers,
                timeout=(self.connect_timeout, self.post_timeout),
                allow_redirects=False,
                stream=True,
            )
        except requests.Timeout:
            return AttemptResult("retry", None, "timeout")
        except requests.ConnectionError:
            return AttemptResult("retry", None, "connection_error")
        except requests.RequestException:
            return AttemptResult("retry", None, "request_error")
        try:
            raw = response.raw.read(MAX_BODY + 1, decode_content=True) or b""
            if len(raw) > MAX_BODY:
                raw = raw[:MAX_BODY]
            return interpret_response(response.status_code, response.headers, raw, datetime.now(timezone.utc))
        finally:
            response.close()
            session.close()


def _contract_problem(payload: dict[str, Any], event_id: str) -> str | None:
    """Rechaza el envío sin alterar event_id ni sustituir tipos por cero."""
    body_id = payload.get("event_id")
    if not isinstance(body_id, str) or body_id == "" or body_id != event_id:
        return "event_id_no_coincide"
    occurred = payload.get("occurred_at")
    if not isinstance(occurred, str) or _OFFSET.search(occurred) is None:
        return "occurred_at_sin_offset"
    for field in ("employee_no", "card_no"):
        if not isinstance(payload.get(field), str):
            return f"{field}_no_es_texto"
    for field, limit in TEXT_LIMITS.items():
        value = payload.get(field)
        if value is None:
            continue
        if not isinstance(value, str) or len(value) > limit:
            return f"{field}_fuera_de_contrato"
    serial = payload.get("serial_no")
    if isinstance(serial, bool) or not isinstance(serial, int) or serial < 0 or serial > SERIAL_MAX:
        return "serial_no_fuera_de_contrato"
    source = payload.get("source")
    if source not in {"live", "backfill"}:
        return "source_fuera_de_contrato"
    if payload.get("schema_version") != 1:
        return "schema_version_fuera_de_contrato"
    for field in ("major_event_type", "sub_event_type"):
        value = payload.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            return f"{field}_ausente"
    return None


def _accepted(body: bytes) -> bool:
    if not body or body.lstrip().startswith(b"<"):
        return False
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and payload.get("accepted") is True
