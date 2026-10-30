"""Captura y comparación de solo lectura contra un gateway simulado."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from hikvision_event_bridge.cli import main
from support import GatewayHarness, multipart_message, write_config

DEVICE = "9CE552AA-7ACD-7145-B340-A8618DA1B946"
LIVE = Path(__file__).resolve().parent / "fixtures" / "live_face.json"
HISTORY = Path(__file__).resolve().parent / "fixtures" / "historical_face.json"


def _config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, gateway_url: str) -> Path:
    monkeypatch.setenv("HIKVISION_GATEWAY_PASSWORD", "gateway-secret")
    monkeypatch.setenv("RANCHERITO_EVENTS_TOKEN", "token-de-prueba")
    path = tmp_path / "config.toml"
    write_config(path, gateway_url, "https://admin.rancherito.com.mx/api/integrations/hikvision/events")
    return path


def _part(body: dict) -> tuple[dict[str, str], bytes]:
    raw = json.dumps(body).encode()
    return ({"Content-Type": "application/json", "Content-Length": str(len(raw))}, raw)


def test_la_captura_espera_la_marcacion_y_no_corta_en_heartbeats(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    gateway = GatewayHarness()
    live = json.loads(LIVE.read_text(encoding="utf-8"))
    heartbeat = {"EventNotificationAlert": {"eventType": "heartBeat", "devIndex": DEVICE}}
    gateway.subscribe_body = multipart_message(
        [
            _part({"SubscribeDeviceMgmtRsp": {"id": 41}}),
            _part(heartbeat),
            _part(heartbeat),
            _part(heartbeat),
            _part(live),
        ]
    )
    url = gateway.start()
    capture = tmp_path / "muestra.bin"
    try:
        code = main([
            "--config", str(_config(tmp_path, monkeypatch, url)),
            "--env-file", str(tmp_path / "no.env"),
            "diagnostics",
            "--subscribe-sample",
            "--timeout", "30",
            "--capture", str(capture),
        ])
    finally:
        gateway.stop()
    output = capsys.readouterr().out
    assert code == 0
    assert "marcación encontrada" in output
    assert "parte 5" in output
    assert "parte 3" in output
    assert any("unSubscribeDeviceMgmt/41" in path for _method, path in gateway.authorized)
    assert all("integrations" not in path for _method, path in gateway.authorized)
    saved = capture.read_bytes()
    assert b"AccessControllerEvent" in saved
    assert saved.startswith(b"-- part\n")
    if os.name != "nt":
        assert stat.S_IMODE(capture.stat().st_mode) == 0o600


def test_compare_pagina_y_separa_un_event_id_distinto(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    live = json.loads(LIVE.read_text(encoding="utf-8"))
    history = json.loads(HISTORY.read_text(encoding="utf-8"))
    other = dict(history)
    other["serialNo"] = 1
    other["time"] = "2026-10-29T09:00:05-06:00"
    second = dict(history)
    second["serialNo"] = 2
    gateway = GatewayHarness()
    gateway.history_pages = [
        {
            "AcsEvent": {
                "responseStatusStrg": "MORE",
                "numOfMatches": 2,
                "totalMatches": 3,
                "MatchList": [other, second],
            }
        },
        {
            "AcsEvent": {
                "responseStatusStrg": "OK",
                "numOfMatches": 1,
                "totalMatches": 3,
                "MatchList": [history],
            }
        },
    ]
    url = gateway.start()
    capture = tmp_path / "muestra.bin"
    capture.write_bytes(b"-- part\n" + json.dumps(live).encode() + b"\n")
    try:
        code = main([
            "--config", str(_config(tmp_path, monkeypatch, url)),
            "--env-file", str(tmp_path / "no.env"),
            "compare-sample",
            "--capture", str(capture),
            "--window-seconds", "180",
        ])
        output = capsys.readouterr().out
        assert code == 0
        assert "searchResultPosition=[0, 2]" in output
        assert "maxResults=2" in output
        assert "event_id igual" in output
        assert all("AcsEvent" in path for _method, path in gateway.authorized)
        assert gateway.unsubscribed == []

        without_card = dict(history)
        without_card.pop("cardNo")
        without_card.pop("employeeNoString")
        gateway.history_pages = [
            {
                "AcsEvent": {
                    "responseStatusStrg": "OK",
                    "numOfMatches": 1,
                    "totalMatches": 1,
                    "MatchList": [without_card],
                }
            }
        ]
        code = main([
            "--config", str(_config(tmp_path, monkeypatch, url)),
            "--env-file", str(tmp_path / "no.env"),
            "compare-sample",
            "--capture", str(capture),
            "--window-seconds", "180",
        ])
    finally:
        gateway.stop()
    mismatch = capsys.readouterr().out
    assert code == 0
    assert "event_id distinto" in mismatch
    assert "employee_no" in mismatch
    assert "card_no" in mismatch
    assert "UNIQUE" in mismatch
