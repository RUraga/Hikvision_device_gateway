from __future__ import annotations

import logging
from pathlib import Path

import pytest

from hikvision_event_bridge.cli import main
from hikvision_event_bridge.database import Database

from support import write_config


def _prepare(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HIKVISION_GATEWAY_PASSWORD", "clave-que-no-debe-imprimirse")
    monkeypatch.setenv("RANCHERITO_EVENTS_TOKEN", "token-que-no-debe-imprimirse")
    path = tmp_path / "config.toml"
    write_config(path, "http://127.0.0.1", "https://admin.rancherito.com.mx/api/integrations/hikvision/events")
    return path


def test_check_config_no_imprime_secretos(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    path = _prepare(tmp_path, monkeypatch)
    assert main(["--config", str(path), "--env-file", str(tmp_path / "no.env"), "check-config"]) == 0
    output = capsys.readouterr().out
    assert "clave-que-no-debe-imprimirse" not in output
    assert "token-que-no-debe-imprimirse" not in output
    assert "presente" in output


def test_dry_run_sin_base_distinta(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _prepare(tmp_path, monkeypatch)
    assert main(["--config", str(path), "--env-file", str(tmp_path / "no.env"), "run", "--dry-run"]) == 2


def test_status_y_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    path = _prepare(tmp_path, monkeypatch)
    from hikvision_event_bridge.config import load_config

    config = load_config(path)
    database = Database(config.storage.sqlite_path)
    database.migrate()
    connection = database.connect()
    try:
        connection.execute("BEGIN IMMEDIATE")
        cursor = connection.execute(
            """
            INSERT INTO events (
                event_id, identity_version, normalization_version, device_uid,
                occurred_at, received_at, normalized_json, raw_json, source
            ) VALUES ('evt-1', 1, 1, '9CE552AA-7ACD-7145-B340-A8618DA1B946',
                      '2026-10-29T15:00:00Z', '2026-10-29T15:00:01Z', '{}', '{}', 'live')
            """
        )
        connection.execute(
            """
            INSERT INTO deliveries (
                event_row_id, destination_id, destination_url, state, attempts, created_at, last_error
            ) VALUES (?, 'rancherito', 'https://admin.rancherito.com.mx/api/integrations/hikvision/events',
                      'rejected', 1, '2026-10-29T15:00:01Z', 'http_422')
            """,
            (cursor.lastrowid,),
        )
        connection.commit()
    finally:
        connection.close()
    assert main(["--config", str(path), "--env-file", str(tmp_path / "no.env"), "status"]) == 0
    assert "rejected: 1" in capsys.readouterr().out
    assert main(["--config", str(path), "--env-file", str(tmp_path / "no.env"), "retry-rejected", "--id", "1"]) == 0
    assert database.status()["deliveries"].get("pending") == 1


def test_log_redacta_datos_personales() -> None:
    from hikvision_event_bridge.logging_config import JsonFormatter

    record = logging.LogRecord("test.redact", logging.INFO, __file__, 1, "muestra", (), None)
    record.fields = {"card_no": "0332", "name": "Ana", "event_id": "abc"}
    formatted = JsonFormatter().format(record)
    assert "0332" not in formatted
    assert "Ana" not in formatted
    assert "abc" in formatted
