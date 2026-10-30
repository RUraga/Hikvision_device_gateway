from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from hikvision_event_bridge.backfill import parse_acs_response, reconcile_device
from hikvision_event_bridge.config import load_config
from hikvision_event_bridge.database import Database
from hikvision_event_bridge.gateway import GatewayError

from support import write_config

UID = "9CE552AA-7ACD-7145-B340-A8618DA1B946"


class ScriptedHistory:
    def __init__(self, pages: list[object]) -> None:
        self.pages = list(pages)
        self.calls: list[dict] = []

    def search_acs_events(self, device_uid: str, body: dict) -> dict:
        self.calls.append({"device_uid": device_uid, "body": body})
        if not self.pages:
            return {"AcsEvent": {"responseStatusStrg": "NO MATCH", "numOfMatches": 0, "totalMatches": 0}}
        item = self.pages.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _setup(tmp_path: Path, monkeypatch) -> tuple[object, Database]:
    monkeypatch.setenv("HIKVISION_GATEWAY_PASSWORD", "gateway-secret")
    monkeypatch.setenv("RANCHERITO_EVENTS_TOKEN", "dest-token")
    path = tmp_path / "config.toml"
    write_config(path, "http://127.0.0.1", "http://127.0.0.1/events", backfill_enabled=True)
    text = path.read_text(encoding="utf-8")
    text = text.replace("window_seconds = 3600", "window_seconds = 600")
    text = text.replace("min_window_seconds = 60", "min_window_seconds = 600")
    text = text.replace('initial_start = "2026-10-01T00:00:00-06:00"', 'initial_start = "2026-10-29T09:00:00-06:00"')
    path.write_text(text, encoding="utf-8")
    config = load_config(path)
    database = Database(config.storage.sqlite_path)
    database.migrate()
    return config, database


def test_paginacion_avanza_checkpoint_y_guarda(tmp_path: Path, monkeypatch) -> None:
    config, database = _setup(tmp_path, monkeypatch)
    record = {
        "major": 5,
        "minor": 1,
        "time": "2026-10-29T09:00:00-06:00",
        "serialNo": 10,
        "employeeNoString": "0002",
        "cardNo": "0332",
        "currentVerifyMode": "card",
        "attendanceStatus": "checkIn",
        "label": "Entrada",
    }
    source = ScriptedHistory(
        [
            {"AcsEvent": {"responseStatusStrg": "MORE", "numOfMatches": 1, "totalMatches": 2, "MatchList": [record]}},
            {
                "AcsEvent": {
                    "responseStatusStrg": "OK",
                    "numOfMatches": 1,
                    "totalMatches": 2,
                    "MatchList": [{**record, "serialNo": 11}],
                }
            },
        ]
    )
    saved: list[dict] = []
    now = datetime(2026, 10, 29, 16, 0, tzinfo=timezone.utc)
    result = reconcile_device(
        config.devices["rancherito"],
        database,
        source,
        config,
        now,
        saved.append,
        "test",
    )
    assert result.completed is True
    assert [call["body"]["AcsEventCond"]["searchResultPosition"] for call in source.calls[:2]] == [0, 1]
    assert len(saved) == 2
    sync = database.get_sync(UID)
    assert sync is not None and sync.checkpoint_until == "2026-10-29T16:00:00Z"


def test_pagina_fallida_no_avanza_checkpoint(tmp_path: Path, monkeypatch) -> None:
    config, database = _setup(tmp_path, monkeypatch)
    record = {
        "major": 5,
        "minor": 1,
        "time": "2026-10-29T09:00:00-06:00",
        "serialNo": 10,
        "employeeNoString": "0002",
        "currentVerifyMode": "face",
    }
    source = ScriptedHistory(
        [
            {"AcsEvent": {"responseStatusStrg": "MORE", "numOfMatches": 1, "totalMatches": 3, "MatchList": [record]}},
            {"statusCode": 4, "statusString": "Invalid Operation", "subStatusCode": "badParameters"},
        ]
    )
    saved: list[dict] = []
    now = datetime(2026, 10, 29, 16, 0, tzinfo=timezone.utc)
    result = reconcile_device(config.devices["rancherito"], database, source, config, now, saved.append, "test")
    assert result.completed is False
    assert saved == [record]
    sync = database.get_sync(UID)
    assert sync is None or sync.checkpoint_until is None


def test_pagina_repetida_no_avanza(tmp_path: Path, monkeypatch) -> None:
    config, database = _setup(tmp_path, monkeypatch)
    record = {"major": 5, "minor": 1, "time": "2026-10-29T09:00:00-06:00", "serialNo": 10, "currentVerifyMode": "card"}
    page = {"AcsEvent": {"responseStatusStrg": "MORE", "numOfMatches": 2, "totalMatches": 2, "MatchList": [record, record]}}
    source = ScriptedHistory([page, page, page, page])
    now = datetime(2026, 10, 29, 16, 0, tzinfo=timezone.utc)
    result = reconcile_device(config.devices["rancherito"], database, source, config, now, lambda _record: None, "test")
    assert result.completed is False
    sync = database.get_sync(UID)
    assert sync is None or sync.checkpoint_until is None
    assert database.status()["incidents"] >= 1


def test_respuesta_vacia_y_error_de_autenticacion(tmp_path: Path, monkeypatch) -> None:
    config, database = _setup(tmp_path, monkeypatch)
    empty = ScriptedHistory([{"AcsEvent": {"responseStatusStrg": "NO MATCH", "numOfMatches": 0, "totalMatches": 0}}])
    now = datetime(2026, 10, 29, 16, 0, tzinfo=timezone.utc)
    result = reconcile_device(config.devices["rancherito"], database, empty, config, now, lambda _record: None, "test")
    assert result.completed is True
    denied = ScriptedHistory([GatewayError("autenticación del gateway rechazada", status=401, auth_failed=True)])
    later = reconcile_device(
        config.devices["rancherito"],
        database,
        denied,
        config,
        now + timedelta(hours=1),
        lambda _record: None,
        "test-2",
    )
    assert later.auth_failed is True
    assert parse_acs_response({"statusCode": 4, "subStatusCode": "badParameters"}).status == "ERROR"


def test_infolist_se_lee_y_una_lista_ausente_no_avanza_el_checkpoint(tmp_path: Path, monkeypatch) -> None:
    config, database = _setup(tmp_path, monkeypatch)
    record = {
        "major": 5,
        "minor": 1,
        "time": "2026-10-29T18:47:13-06:00",
        "serialNo": 173,
        "employeeNoString": "1005",
        "attendanceStatus": "checkIn",
        "label": "Entrada",
        "cardNo": "fixture-card",
    }
    page = parse_acs_response(
        {
            "AcsEvent": {
                "responseStatusStrg": "OK",
                "numOfMatches": 1,
                "totalMatches": 1,
                "InfoList": [record],
            }
        }
    )
    assert page.error is None
    assert page.records == [record]
    assert page.status == "OK"
    missing = {
        "AcsEvent": {
            "responseStatusStrg": "OK",
            "numOfMatches": 3,
            "totalMatches": 3,
        }
    }
    assert parse_acs_response(missing).error == "lista_de_eventos_ausente"
    source = ScriptedHistory([missing])
    now = datetime(2026, 10, 30, 1, 0, tzinfo=timezone.utc)
    result = reconcile_device(config.devices["rancherito"], database, source, config, now, lambda _record: None, "test")
    assert result.completed is False
    sync = database.get_sync(UID)
    assert sync is None or sync.checkpoint_until is None
    sent = source.calls[0]["body"]["AcsEventCond"]
    assert sent["major"] == 5
    assert sent["minor"] == 0
    assert sent["startTime"].endswith("-06:00")
    assert sent["endTime"].endswith("-06:00")
