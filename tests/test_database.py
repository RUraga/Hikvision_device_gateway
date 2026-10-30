from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from hikvision_event_bridge.config import FilterConfig
from hikvision_event_bridge.database import INSERT_CONFLICT, INSERT_DUPLICATE, INSERT_STORED, Database, LockBusy
from hikvision_event_bridge.normalization import parse_live_part
from hikvision_event_bridge.timeutil import utc_now

UID = "9CE552AA-7ACD-7145-B340-A8618DA1B946"
ROOT = Path(__file__).resolve().parents[1]
FILT = FilterConfig((), (), ("face",), (), False, (), (), ())


def _database(tmp_path: Path) -> Database:
    database = Database(tmp_path / "events.sqlite", busy_timeout_ms=1000)
    database.migrate()
    return database


def _event(name: str = "Ana") -> dict:
    payload = json.loads((ROOT / "tests" / "fixtures" / "live_face.json").read_text(encoding="utf-8"))
    payload["EventNotificationAlert"]["AccessControllerEvent"]["name"] = name
    parsed = parse_live_part(payload, timezone_name="America/Mexico_City", result_filter=FILT, strip_biometric=True)
    assert parsed.event is not None
    return parsed.event


def _store(database: Database, event: dict, destinations: list[tuple[str, str]] | None = None) -> str:
    return database.insert_event_and_deliveries(
        event_id=event["event_id"],
        identity_version=event["identity_version"],
        normalization_version=event["normalization_version"],
        device_uid=UID,
        occurred_at_utc=datetime.fromisoformat(event["occurred_at_utc"].replace("Z", "+00:00")),
        received_at=utc_now(),
        normalized=event,
        raw=event["raw"],
        source=event["source"],
        destinations=destinations or [("rancherito", "https://example.test/events")],
    )


def test_persistir_antes_de_enviar_y_duplicado(tmp_path: Path) -> None:
    database = _database(tmp_path)
    event = _event()
    assert _store(database, event) == INSERT_STORED
    assert _store(database, event) == INSERT_DUPLICATE
    status = database.status()
    assert status["deliveries"]["pending"] == 1


def test_el_historial_completa_el_label_vacio_y_conserva_el_raw(tmp_path: Path) -> None:
    database = _database(tmp_path)
    live = _event()
    live["label"] = None
    live["raw"] = {"serialNo": live["serial_no"], "attendanceStatus": "checkIn"}
    assert _store(database, live) == INSERT_STORED
    connection = database.connect()
    try:
        connection.execute("UPDATE deliveries SET state = 'delivered'")
        connection.commit()
    finally:
        connection.close()
    history = dict(live)
    history["label"] = "Entrada"
    history["source"] = "backfill"
    history["raw"] = {"label": "Entrada", "serialNo": live["serial_no"]}
    assert _store(database, history) == INSERT_DUPLICATE
    connection = database.connect()
    try:
        event = connection.execute("SELECT normalized_json, raw_json, source FROM events").fetchone()
        normalized = json.loads(event["normalized_json"])
        raw = json.loads(event["raw_json"])
        assert normalized["label"] == "Entrada"
        assert normalized["event_id"] == live["event_id"]
        assert "label" not in raw
        assert event["source"] == "live"
        evidence = connection.execute("SELECT source, raw_json FROM event_evidence ORDER BY id").fetchall()
        assert [row["source"] for row in evidence] == ["live", "backfill"]
        assert json.loads(evidence[1]["raw_json"])["label"] == "Entrada"
        assert connection.execute("SELECT state FROM deliveries").fetchone()["state"] == "pending"
        assert connection.execute("SELECT COUNT(*) AS total FROM deliveries").fetchone()["total"] == 1
    finally:
        connection.close()
    contradicted = dict(live)
    contradicted["label"] = "Salida"
    contradicted["source"] = "backfill"
    assert _store(database, contradicted) == INSERT_CONFLICT
    connection = database.connect()
    try:
        assert json.loads(connection.execute("SELECT normalized_json FROM events").fetchone()["normalized_json"])["label"] == "Entrada"
    finally:
        connection.close()


def test_la_base_publica_no_aparece_sin_la_tabla_deliveries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = Database(tmp_path / "events.sqlite")
    real_replace = os.replace
    ready = {}

    def spy(src: str, dst: str) -> None:
        connection = sqlite3.connect(src)
        try:
            connection.execute("SELECT COUNT(*) FROM deliveries")
            ready["schema"] = True
        finally:
            connection.close()
        assert Path(dst).exists() is False
        real_replace(src, dst)

    monkeypatch.setattr("hikvision_event_bridge.database.os.replace", spy)
    database.migrate()
    assert ready["schema"] is True
    assert database.status()["deliveries"] == {}


def test_conflicto_no_sobrescribe(tmp_path: Path) -> None:
    database = _database(tmp_path)
    original = _event("Ana")
    changed = _event("Bruno")
    assert _store(database, original) == INSERT_STORED
    assert _store(database, changed) == INSERT_CONFLICT
    connection = database.connect()
    try:
        row = connection.execute("SELECT normalized_json FROM events").fetchone()
        assert json.loads(row["normalized_json"])["name"] == "Ana"
        assert connection.execute("SELECT COUNT(*) AS total FROM deliveries").fetchone()["total"] == 1
        assert connection.execute("SELECT COUNT(*) AS total FROM incidents").fetchone()["total"] == 1
    finally:
        connection.close()


def test_lease_expirado_vuelve_a_pendiente(tmp_path: Path) -> None:
    database = _database(tmp_path)
    event = _event()
    _store(database, event)
    claimed = database.claim_one("worker", timedelta(seconds=5), utc_now())
    assert claimed is not None
    assert database.release_all_inflight(utc_now()) == 1
    again = database.claim_one("worker-2", timedelta(seconds=5), utc_now())
    assert again is not None
    assert again.delivery_id == claimed.delivery_id
    assert again.attempts == 0
    assert again.event_id == event["event_id"]


def test_retencion_conserva_pendientes_y_borra_entregados(tmp_path: Path) -> None:
    database = _database(tmp_path)
    old = _event("Ana")
    pending = json.loads(json.dumps(old))
    pending["serial_no"] = 99
    pending["event_id"] = "otro-id-estable-de-prueba-0001"
    _store(database, old, [("uno", "https://a.test"), ("dos", "https://b.test")])
    _store(database, pending, [("uno", "https://a.test")])
    now = utc_now()
    first = database.claim_one("w", timedelta(seconds=30), now)
    second = database.claim_one("w", timedelta(seconds=30), now)
    assert first and second
    database.finish_delivery(first.delivery_id, outcome="delivered", now=now, error=None, http_status=200, next_attempt_at=None)
    database.finish_delivery(second.delivery_id, outcome="delivered", now=now, error=None, http_status=200, next_attempt_at=None)
    connection = database.connect()
    try:
        connection.execute("UPDATE deliveries SET delivered_at = ? WHERE state = 'delivered'", ("2020-01-01T00:00:00Z",))
        connection.commit()
    finally:
        connection.close()
    removed = database.purge_delivered(timedelta(days=1), now)
    assert removed == 1
    status = database.status()
    assert status["deliveries"].get("pending") == 1
    assert status["deliveries"].get("delivered") is None


def test_dos_instancias_no_comparten_cerrojo(tmp_path: Path) -> None:
    database = _database(tmp_path)
    now = utc_now()
    first = database.connect()
    second = database.connect()
    try:
        database.acquire_instance_lock(first, "instancia-a", timedelta(seconds=30), now)
        with pytest.raises(LockBusy):
            database.acquire_instance_lock(second, "instancia-b", timedelta(seconds=30), now)
        first.execute("UPDATE instance_lock SET heartbeat_at = ?", ("2020-01-01T00:00:00Z",))
        first.commit()
        database.acquire_instance_lock(second, "instancia-b", timedelta(seconds=30), now)
    finally:
        first.close()
        second.close()


def test_lectura_no_escribe(tmp_path: Path) -> None:
    database = _database(tmp_path)
    connection = database.connect_readonly()
    try:
        with pytest.raises(Exception):
            connection.execute("INSERT INTO counters(name, value) VALUES ('x', 1)")
    finally:
        connection.close()
