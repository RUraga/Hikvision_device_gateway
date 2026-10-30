"""Cliente HTTP contra las respuestas publicadas por Rancherito.

Los códigos 5 y 75 del cuerpo son datos de la muestra, no un catálogo del
terminal ni una traducción de entrada, salida, comida o descanso.
"""

from __future__ import annotations

import json

from hikvision_event_bridge.config import DestinationConfig
from hikvision_event_bridge.delivery import DestinationPoster, rancherito_storage_key
from hikvision_event_bridge.normalization import public_payload
from support import DestinationHarness

EVENT_ID = "ab" * 32
DEVICE = "9CE552AA-7ACD-7145-B340-A8618DA1B946"
TOKEN = "token-dedicado"
PATH = "/api/integrations/hikvision/events"
PROCESSING = (
    "pending",
    "processed",
    "unresolved_identity",
    "unmapped_action",
    "ignored",
    "error",
    "pending_classification",
)


def _payload(**overrides: object) -> dict:
    body = {
        "schema_version": 1,
        "event_id": EVENT_ID,
        "device_index": DEVICE,
        "serial_no": 12345,
        "occurred_at": "2026-10-29T09:00:00-06:00",
        "employee_no": "0002",
        "card_no": "03322275427",
        "attendance_status": "checkIn",
        "label": "Entrada",
        "major_event_type": 5,
        "sub_event_type": 75,
        "result": "unknown",
        "verify_mode": "face",
        "name": "Ana",
        "source": "live",
        "raw": {"serialNo": 12345, "major": 5, "minor": 75},
    }
    body.update(overrides)
    return body


def _destination(url: str) -> DestinationConfig:
    return DestinationConfig(
        name="rancherito",
        url=url,
        token_env="RANCHERITO_EVENTS_TOKEN",
        token=TOKEN,
        pending_url_policy="",
    )


def _post(script: list[dict], payloads: list[dict]) -> tuple[list, list[dict]]:
    harness = DestinationHarness(script)
    url = harness.start() + PATH
    poster = DestinationPoster(connect_timeout=2, post_timeout=2, verify_tls=False)
    try:
        results = [poster.post(_destination(url), url, payload, str(payload["event_id"])) for payload in payloads]
    finally:
        harness.stop()
    return results, harness.seen


def _accepted(status: int, processing: str, duplicate: bool) -> dict:
    return {
        "status": status,
        "body": {
            "accepted": True,
            "duplicate": duplicate,
            "event_id": 123,
            "processing_status": processing,
        },
    }


def test_la_clave_de_almacenamiento_no_reemplaza_al_event_id() -> None:
    derived = rancherito_storage_key(EVENT_ID)
    assert derived != EVENT_ID
    assert len(derived) == 64
    assert derived == rancherito_storage_key(EVENT_ID)


def test_cliente_confirma_201_nuevo_y_200_duplicado() -> None:
    body = _payload()
    duplicate = _payload(source="backfill")
    results, seen = _post(
        [_accepted(201, "pending", False), _accepted(200, "pending", True)],
        [body, duplicate],
    )
    assert [item.outcome for item in results] == ["delivered", "delivered"]
    assert [item.http_status for item in results] == [201, 200]
    assert body["event_id"] == EVENT_ID
    assert duplicate["event_id"] == EVENT_ID
    for record, source in zip(seen, ("live", "backfill"), strict=True):
        sent = json.loads(record["body"])
        assert record["path"] == PATH
        assert record["authorization"] == f"Bearer {TOKEN}"
        assert record["idempotency"] == EVENT_ID
        assert record["idempotency"] != rancherito_storage_key(EVENT_ID)
        assert sent["event_id"] == EVENT_ID
        assert sent["event_id"] != 123
        assert sent["source"] == source
        assert sent["employee_no"] == "0002"
        assert sent["card_no"] == "03322275427"
        assert sent["occurred_at"].endswith("-06:00")
        assert sent["major_event_type"] == 5
        assert sent["sub_event_type"] == 75
        assert "hikvision-event-v1" not in record["idempotency"]


def test_accepted_true_entrega_aunque_el_procesamiento_falle() -> None:
    script = [_accepted(201, status, False) for status in PROCESSING]
    script.extend(_accepted(200, status, True) for status in PROCESSING)
    payloads = [_payload() for _ in script]
    results, seen = _post(script, payloads)
    assert [item.outcome for item in results] == ["delivered"] * len(script)
    assert {record["idempotency"] for record in seen} == {EVENT_ID}
    assert all(json.loads(record["body"])["event_id"] == EVENT_ID for record in seen)


def test_sin_accepted_true_no_hay_entrega() -> None:
    script = [
        {"status": 201, "body": {"accepted": False, "duplicate": False, "event_id": 123, "processing_status": "pending"}},
        {"status": 200, "body": {"duplicate": True, "event_id": 123, "processing_status": "processed"}},
        {"status": 201, "body": {"accepted": "true", "event_id": 123, "processing_status": "pending"}},
    ]
    results, seen = _post(script, [_payload(), _payload(), _payload()])
    assert [item.outcome for item in results] == ["retry", "retry", "retry"]
    assert [item.error for item in results] == ["confirmacion_invalida"] * 3
    assert [record["idempotency"] for record in seen] == [EVENT_ID, EVENT_ID, EVENT_ID]


def test_errores_conservan_el_event_id() -> None:
    script = [
        {"status": 401, "body": {"accepted": False}},
        {"status": 403, "body": {"accepted": False}},
        {"status": 409, "body": {"accepted": False}},
        {"status": 409, "body": {"accepted": False}},
        {"status": 422, "body": {"accepted": False}},
    ]
    original = _payload()
    results, seen = _post(script, [original for _ in script])
    assert [(item.http_status, item.outcome, item.error) for item in results] == [
        (401, "auth_blocked", "token_invalido"),
        (403, "auth_blocked", "dispositivo_no_registrado"),
        (409, "conflict", "conflicto_identidad"),
        (409, "conflict", "conflicto_identidad"),
        (422, "rejected", "contrato_invalido"),
    ]
    assert original["event_id"] == EVENT_ID
    assert [record["idempotency"] for record in seen] == [EVENT_ID] * 5
    assert all(json.loads(record["body"])["major_event_type"] == 5 for record in seen)
    assert all(json.loads(record["body"])["sub_event_type"] == 75 for record in seen)


def test_no_inventa_ceros_ni_cambia_la_clave_para_poder_enviar() -> None:
    harness = DestinationHarness([_accepted(201, "pending", False)])
    url = harness.start() + PATH
    poster = DestinationPoster(connect_timeout=2, post_timeout=2, verify_tls=False)
    destination = _destination(url)
    try:
        missing_major = _payload()
        missing_major.pop("major_event_type")
        missing = poster.post(destination, url, missing_major, EVENT_ID)
        naive = poster.post(destination, url, _payload(occurred_at="2026-10-29T09:00:00"), EVENT_ID)
        text_major = poster.post(destination, url, _payload(major_event_type="5"), EVENT_ID)
        null_card = poster.post(destination, url, _payload(card_no=None), EVENT_ID)
        other_key = poster.post(destination, url, _payload(), EVENT_ID + "ff")
        long_label = poster.post(destination, url, _payload(label="x" * 101), EVENT_ID)
        huge_serial = poster.post(destination, url, _payload(serial_no=4_294_967_296), EVENT_ID)
    finally:
        harness.stop()
    assert missing.error == "major_event_type_ausente"
    assert naive.error == "occurred_at_sin_offset"
    assert text_major.error == "major_event_type_ausente"
    assert null_card.error == "card_no_no_es_texto"
    assert other_key.error == "event_id_no_coincide"
    assert long_label.error == "label_fuera_de_contrato"
    assert huge_serial.error == "serial_no_fuera_de_contrato"
    assert harness.seen == []
    assert missing_major["event_id"] == EVENT_ID


def test_occurred_at_con_z_y_cadenas_vacias_salen_en_el_post() -> None:
    stored = _payload(employee_no=None, card_no=None, occurred_at="2026-10-29T15:00:00Z")
    sent = public_payload(stored)
    assert sent["employee_no"] == ""
    assert sent["card_no"] == ""
    assert sent["event_id"] == EVENT_ID
    assert sent["major_event_type"] == 5
    assert sent["sub_event_type"] == 75
    results, seen = _post([_accepted(201, "pending", False)], [sent])
    body = json.loads(seen[0]["body"])
    assert results[0].outcome == "delivered"
    assert body["employee_no"] == ""
    assert body["card_no"] == ""
    assert body["occurred_at"] == "2026-10-29T15:00:00Z"
    assert seen[0]["idempotency"] == EVENT_ID
