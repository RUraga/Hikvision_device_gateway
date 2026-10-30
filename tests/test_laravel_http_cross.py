"""Normalizador y cliente del bridge contra el endpoint real de Laravel.

Usa la base rancherito_test. No toca la base de la aplicación.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
import requests

from hikvision_event_bridge.config import DestinationConfig
from hikvision_event_bridge.delivery import DestinationPoster, rancherito_storage_key
from contract_payloads import meal_sequence, missing_punch_sequence, rancherito_root, synthetic_event


def _php_binary() -> str | None:
    candidates: list[str] = []
    configured = os.environ.get("PHP_BIN", "").strip()
    if configured:
        candidates.append(configured)
    found = shutil.which("php")
    if found:
        candidates.append(found)
    fallback = Path(r"C:\laragon\bin\php\php-8.3.30-Win32-vs16-x64\php.exe")
    if fallback.is_file():
        candidates.append(str(fallback))
    for candidate in candidates:
        if _php_supports(candidate):
            return candidate
    return None


def _php_supports(binary: str) -> bool:
    try:
        completed = subprocess.run(
            [binary, "-r", "echo PHP_MAJOR_VERSION, '.', PHP_MINOR_VERSION;"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    parts = completed.stdout.strip().split(".")
    if completed.returncode != 0 or len(parts) < 2:
        return False
    try:
        major = int(parts[0])
        minor = int(parts[1])
    except ValueError:
        return False
    return major > 8 or (major == 8 and minor >= 1)


def _require_cross() -> tuple[Path, str]:
    root = rancherito_root()
    script = None if root is None else root / "tests" / "HikvisionCrossDb.php"
    if root is None or script is None or not script.is_file():
        pytest.skip(
            "Rancherito no está disponible. Exporta RANCHERITO_ROOT para ejecutar la prueba cruzada."
        )
    binary = _php_binary()
    if binary is None:
        pytest.skip(
            "Hace falta PHP 8.1 o posterior. Exporta PHP_BIN para ejecutar la prueba cruzada."
        )
    return root, binary


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _env(action_map: dict | None = None) -> dict[str, str]:
    env = os.environ.copy()
    env["APP_ENV"] = "testing"
    env["DB_DATABASE"] = "rancherito_test"
    env["CACHE_DRIVER"] = "array"
    env["QUEUE_CONNECTION"] = "sync"
    env["TELESCOPE_ENABLED"] = "false"
    if action_map is not None:
        env["HIKVISION_ATTENDANCE_ACTION_MAP_JSON"] = json.dumps(action_map)
    return env


def _php(action: str, suffix: str, action_map: dict | None = None) -> dict:
    root, binary = _require_cross()
    completed = subprocess.run(
        [binary, str(root / "tests" / "HikvisionCrossDb.php"), action, suffix],
        cwd=root,
        env=_env(action_map),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode == 2:
        pytest.skip(completed.stderr.strip() or "la base de pruebas no está disponible")
    if completed.returncode != 0:
        raise AssertionError(completed.stderr.strip() or completed.stdout)
    line = [item for item in completed.stdout.splitlines() if item.startswith("{")][-1]
    return json.loads(line)


def _wait_port(port: int, server: subprocess.Popen) -> None:
    deadline = time.time() + 25
    while time.time() < deadline:
        if server.poll() is not None:
            raise AssertionError("el servidor local de Laravel terminó antes de aceptar conexiones")
        try:
            with socket.create_connection(("127.0.0.1", port), 0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise AssertionError("el servidor local de Laravel no abrió el puerto")


def test_el_cliente_del_bridge_entra_en_laravel_y_calcula_sesiones() -> None:
    root, binary = _require_cross()
    suffix = "c" + format(time.time_ns() % 0xFFFFFFF, "07x")
    rules = {
        "attendance": [
            {"attendance_status": "fixture-work-start", "label": "fixture-entrada", "event_action": "work_start"},
            {"attendance_status": "fixture-work-end", "label": "fixture-salida", "event_action": "work_end"},
            {
                "attendance_status": "fixture-meal-out",
                "label": "fixture-comida",
                "event_action": "break_start",
                "break_type": "meal",
            },
            {
                "attendance_status": "fixture-meal-in",
                "label": "fixture-regreso",
                "event_action": "break_end",
                "break_type": "meal",
            },
        ]
    }
    seeded = None
    server = None
    try:
        seeded = _php("seed", suffix)
        action_map = {seeded["device_uid"]: rules["attendance"]}
        port = _free_port()
        server = subprocess.Popen(
            [binary, "artisan", "serve", "--host=127.0.0.1", f"--port={port}"],
            cwd=root,
            env=_env(action_map),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        _wait_port(port, server)
        url = f"http://127.0.0.1:{port}/api/integrations/hikvision/events"
        poster = DestinationPoster(3, 20, verify_tls=False)
        destination = DestinationConfig("rancherito", url, "RANCHERITO_EVENTS_TOKEN", seeded["token"], "")

        def send(payload: dict, token: str | None = None):
            current = destination if token is None else DestinationConfig(
                "rancherito", url, "RANCHERITO_EVENTS_TOKEN", token, ""
            )
            body = dict(payload)
            body["device_index"] = seeded["device_uid"]
            return poster.post(current, url, body, body["event_id"]), body

        meal = meal_sequence()
        naive_payload = dict(meal[0])
        naive_payload["occurred_at"] = "2026-10-29T09:00:00"
        rejected = poster.post(destination, url, naive_payload, naive_payload["event_id"])
        assert rejected.error == "occurred_at_sin_offset"

        wrong = send(meal[0], "token-incorrecto")[0]
        assert wrong.http_status == 401
        assert wrong.outcome == "auth_blocked"

        naive = requests.post(
            url,
            json={**meal[0], "device_index": seeded["device_uid"], "occurred_at": "2026-10-29 09:00:00"},
            headers={"Authorization": f"Bearer {seeded['token']}", "Accept": "application/json"},
            timeout=20,
        )
        assert naive.status_code == 422

        first, first_body = send(meal[0])
        assert first.http_status == 201
        assert first.outcome == "delivered"
        again, _ = send(meal[0])
        assert again.http_status == 200
        assert again.outcome == "delivered"

        conflict_body = dict(meal[0])
        conflict_body["label"] = "otra-etiqueta"
        conflict, _ = send(conflict_body)
        assert conflict.http_status == 409
        assert conflict.outcome == "conflict"

        for payload in meal[1:]:
            result, _ = send(payload)
            assert result.outcome == "delivered"
            assert result.http_status == 201

        unknown = synthetic_event(
            serial_no=2999,
            occurred_at="2026-10-28T08:00:00-06:00",
            attendance_status="fixture-work-start",
            label="fixture-entrada",
            result="unknown",
        )
        unknown["employee_no"] = "9999"
        unknown["event_id"] = "ab" * 32
        unknown_result, _ = send(unknown)
        assert unknown_result.http_status == 201
        assert unknown_result.outcome == "delivered"

        for payload in missing_punch_sequence():
            result, _ = send(payload)
            assert result.outcome == "delivered"

        stored = _php("query", suffix)
    finally:
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
        if seeded is not None or server is not None:
            _php("cleanup", suffix)

    statuses = {item["serial_no"]: item["processing_status"] for item in stored["events"]}
    assert statuses[2001] == "pending_classification"
    assert statuses[2003] == "ignored"
    assert statuses[2004] == "processed"
    assert statuses[2005] == "pending_classification"
    assert statuses[2999] == "pending_classification"
    bridge_ids = {item["bridge_event_id"] for item in stored["events"]}
    assert first_body["event_id"] in bridge_ids
    assert all(item["laravel_event_id"] != item["bridge_event_id"] for item in stored["events"])
    original = next(item for item in stored["events"] if item["serial_no"] == 2001)
    assert original["idempotency_key"] == rancherito_storage_key(original["bridge_event_id"])
    assert original["idempotency_key"] != rancherito_storage_key(original["idempotency_key"])
    assert original["time"] == "2026-10-29 15:00:00"
    assert original["received_at"]
    assert [item for item in stored["sessions"] if item["worked_seconds"] is not None] == []
    opened = [item for item in stored["sessions"] if item["status"] == "open"]
    assert len(opened) == 1
    assert opened[0]["started_at"] == "2026-10-29 20:40:00"
