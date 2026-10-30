from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hikvision_event_bridge.config import DeliveryConfig
from hikvision_event_bridge.delivery import AttemptResult, backoff_delay, interpret_response, next_delay
import random


def _delivery() -> DeliveryConfig:
    return DeliveryConfig(
        concurrency=1,
        initial_backoff_seconds=2,
        max_backoff_seconds=300,
        auth_hold_seconds=120,
        retry_after_cap_seconds=30,
        jitter_ratio=0,
        lease_seconds=5,
    )


def test_codigos_http_y_confirmacion() -> None:
    now = datetime(2026, 10, 29, tzinfo=timezone.utc)
    assert interpret_response(200, {}, b'{"accepted":true,"duplicate":false,"event_id":9,"processing_status":"pending"}', now).outcome == "delivered"
    assert interpret_response(201, {}, b'{"accepted":true}', now).outcome == "delivered"
    assert interpret_response(200, {}, b"<html>ok</html>", now).error == "confirmacion_invalida"
    assert interpret_response(200, {}, b'{"accepted":false}', now).outcome == "retry"
    assert interpret_response(200, {}, b"{", now).outcome == "retry"
    assert interpret_response(429, {"Retry-After": "100"}, b"", now).outcome == "retry"
    blocked = interpret_response(401, {}, b"", now)
    assert blocked.outcome == "auth_blocked"
    assert blocked.error == "token_invalido"
    inactive = interpret_response(403, {}, b"", now)
    assert inactive.outcome == "auth_blocked"
    assert inactive.error == "dispositivo_no_registrado"
    conflict = interpret_response(409, {}, b"", now)
    assert conflict.outcome == "conflict"
    assert conflict.error == "conflicto_identidad"
    invalid = interpret_response(422, {}, b"", now)
    assert invalid.outcome == "rejected"
    assert invalid.error == "contrato_invalido"
    assert interpret_response(400, {}, b"", now).outcome == "rejected"
    assert interpret_response(413, {}, b"", now).outcome == "rejected"
    assert interpret_response(415, {}, b"", now).outcome == "rejected"
    assert interpret_response(500, {}, b"", now).outcome == "retry"
    assert interpret_response(302, {"Location": "https://otro.example"}, b"", now).outcome == "rejected"
    assert interpret_response(404, {}, b"", now).outcome == "rejected"


def test_retry_after_tiene_tope_y_401_no_reintenta_en_seguida() -> None:
    now = datetime(2026, 10, 29, tzinfo=timezone.utc)
    delivery = _delivery()
    limited = interpret_response(429, {"Retry-After": "100"}, b"", now)
    assert next_delay(limited, attempt=1, delivery=delivery, rng=random.Random(1)) == 30
    blocked = interpret_response(401, {"Retry-After": "1"}, b"", now)
    assert next_delay(blocked, attempt=4, delivery=delivery, rng=random.Random(1)) == 120
    assert backoff_delay(5, initial=2, maximum=10, jitter_ratio=0, rng=random.Random(0)) == 10


def test_no_hay_descartes_por_numero_de_intentos() -> None:
    now = datetime(2026, 10, 29, tzinfo=timezone.utc)
    result = interpret_response(503, {}, b"", now)
    delay = next_delay(result, attempt=50, delivery=_delivery(), rng=random.Random(0))
    assert result.outcome == "retry"
    assert delay == 300
    assert timedelta(seconds=delay).total_seconds() > 0
