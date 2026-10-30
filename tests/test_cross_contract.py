"""El normalizador y el cliente HTTP producen el contrato que Rancherito guarda."""

from __future__ import annotations

import hashlib
import json

from hikvision_event_bridge.config import DestinationConfig
from hikvision_event_bridge.delivery import DestinationPoster, rancherito_storage_key
from hikvision_event_bridge.identity import canonical_document, event_id_for
from hikvision_event_bridge.timeutil import parse_event_time
from contract_payloads import (
    CONTRACT_FIXTURE,
    DEVICE,
    MEAL_FIXTURE,
    RANCHERITO_CONTRACT,
    RANCHERITO_MEAL,
    bridge_contract_event,
    history_event_id,
    meal_sequence,
    missing_punch_sequence,
)
from support import DestinationHarness


def test_el_fixture_compartido_no_diverge_del_normalizador() -> None:
    current = bridge_contract_event()
    stored = json.loads(CONTRACT_FIXTURE.read_text(encoding="utf-8"))
    rancherito = json.loads(RANCHERITO_CONTRACT.read_text(encoding="utf-8"))
    assert current == stored == rancherito
    assert current["employee_no"] == "0002"
    assert current["card_no"] == "03322275427"
    assert current["occurred_at"].endswith("-06:00")
    assert current["major_event_type"] == 5
    assert current["sub_event_type"] == 75
    assert current["result"] == "unknown"
    assert current["attendance_status"] == "checkIn"
    assert current["label"] == "Entrada"
    assert "pictureURL" not in json.dumps(current["raw"])
    assert history_event_id() == current["event_id"]
    assert rancherito_storage_key(current["event_id"]) != current["event_id"]
    derived = hashlib.sha256(f"hikvision-event-v1|{current['event_id']}".encode()).hexdigest()
    assert derived == rancherito_storage_key(current["event_id"])
    assert hashlib.sha256(f"hikvision-event-v1|{derived}".encode()).hexdigest() != derived


def test_la_misma_hora_con_otro_offset_conserva_el_event_id() -> None:
    contract, moment = parse_event_time("2026-10-29T15:00:00Z", "America/Mexico_City")
    local, local_moment = parse_event_time("2026-10-29T09:00:00-06:00", "America/Mexico_City")
    assert moment == local_moment
    assert contract != local
    left = event_id_for(
        canonical_document(
            device_uid=DEVICE,
            serial_no=12345,
            occurred_at_utc=moment,
            employee_no="0002",
            card_no="03322275427",
            major_event_type=5,
            sub_event_type=75,
        )
    )
    right = event_id_for(
        canonical_document(
            device_uid=DEVICE,
            serial_no=12345,
            occurred_at_utc=local_moment,
            employee_no="0002",
            card_no="03322275427",
            major_event_type=5,
            sub_event_type=75,
        )
    )
    assert left == right == bridge_contract_event()["event_id"]


def test_la_secuencia_de_comida_es_identica_en_los_dos_repositorios() -> None:
    document = {"meal": meal_sequence(), "missing_punch": missing_punch_sequence()}
    assert json.loads(MEAL_FIXTURE.read_text(encoding="utf-8")) == document
    assert json.loads(RANCHERITO_MEAL.read_text(encoding="utf-8")) == document
    identifiers = [item["event_id"] for item in document["meal"] + document["missing_punch"]]
    assert len(identifiers) == len(set(identifiers))
    assert {item["result"] for item in document["meal"]} == {"unknown", "failure", "success"}


def test_el_cliente_envia_el_fixture_y_no_cambia_la_clave_al_reintentar() -> None:
    payload = bridge_contract_event()
    harness = DestinationHarness(
        [
            {
                "status": 201,
                "body": {
                    "accepted": True,
                    "duplicate": False,
                    "event_id": 123,
                    "processing_status": "processed",
                },
            },
            {
                "status": 200,
                "body": {
                    "accepted": True,
                    "duplicate": True,
                    "event_id": 123,
                    "processing_status": "processed",
                },
            },
        ]
    )
    url = harness.start() + "/api/integrations/hikvision/events"
    poster = DestinationPoster(2, 2, verify_tls=False)
    destination = DestinationConfig("rancherito", url, "RANCHERITO_EVENTS_TOKEN", "token-prueba", "")
    try:
        first = poster.post(destination, url, payload, payload["event_id"])
        second = poster.post(destination, url, payload, payload["event_id"])
    finally:
        harness.stop()
    assert first.outcome == "delivered"
    assert first.http_status == 201
    assert second.outcome == "delivered"
    assert second.http_status == 200
    assert payload["event_id"] != 123
    sent = [json.loads(item["body"]) for item in harness.seen]
    assert sent[0] == sent[1] == payload
    assert [item["idempotency"] for item in harness.seen] == [payload["event_id"], payload["event_id"]]
