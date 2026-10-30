from __future__ import annotations

import json
import sqlite3
import threading
from datetime import timedelta
from pathlib import Path

import pytest

from hikvision_event_bridge.config import ConfigError, load_config
from hikvision_event_bridge.database import Database
from hikvision_event_bridge.delivery import AttemptResult
from hikvision_event_bridge.gateway import GatewayError
from hikvision_event_bridge.multipart import Part
from hikvision_event_bridge.service import BridgeService
from hikvision_event_bridge.timeutil import utc_now

from support import DestinationHarness, GatewayHarness, multipart_message, write_config

UID = "9CE552AA-7ACD-7145-B340-A8618DA1B946"
ROOT = Path(__file__).resolve().parents[1]


def _secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIKVISION_GATEWAY_PASSWORD", "gateway-secret")
    monkeypatch.setenv("RANCHERITO_EVENTS_TOKEN", "dest-token")


def _live_bytes() -> bytes:
    return (ROOT / "tests" / "fixtures" / "live_face.json").read_bytes()


class FakeGateway:
    def __init__(self, parts: list[Part]) -> None:
        self.parts = parts
        self.closed = threading.Event()
        self.unsubscribed: list[str] = []

    def subscribe(self):
        parts = self.parts

        def generate():
            yield from parts
            self.closed.wait(3)

        return "multipart/mixed; boundary=boundary", generate()

    def close_stream(self) -> None:
        self.closed.set()

    def unsubscribe(self, subscription_id: str) -> None:
        self.unsubscribed.append(subscription_id)

    def search_acs_events(self, device_uid: str, body: dict) -> dict:
        return {"AcsEvent": {"responseStatusStrg": "NO MATCH", "numOfMatches": 0, "totalMatches": 0}}


class RecordingPoster:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.payloads: list[dict] = []
        self.started = threading.Event()

    def post(self, destination, url, payload, event_id):
        self.calls.append((destination.name, event_id))
        self.payloads.append(payload)
        self.started.set()
        if destination.name == "broken":
            return AttemptResult("retry", 500, "http_500")
        return AttemptResult("delivered", 200, None)


def _parts() -> list[Part]:
    return [
        Part({"content-type": "application/json"}, b'{"SubscribeDeviceMgmtRsp":{"id":41}}'),
        Part({"content-type": "application/json"}, b"{esto no es json"),
        Part({"content-type": "image/jpeg"}, b"\xff\xd8\xff"),
        Part({"content-type": "application/json"}, b'{"EventNotificationAlert":{"eventType":"heartBeat"}}'),
        Part({"content-type": "application/json"}, _live_bytes()),
    ]


def _service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, **overrides):
    _secrets(monkeypatch)
    path = tmp_path / "config.toml"
    write_config(path, overrides.pop("gateway_url", "http://127.0.0.1"), overrides.pop("destination_url", "http://127.0.0.1/events"), **overrides)
    config = load_config(path)
    return config


def _wait_until(predicate, timeout: float = 3) -> None:
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (FileNotFoundError, sqlite3.OperationalError):
            pass
        time.sleep(0.02)
    raise AssertionError("la condición no se cumplió a tiempo")


class ServiceRun:
    """Arranca el servicio y lo detiene aunque la prueba falle."""

    def __init__(self, service: BridgeService) -> None:
        self.service = service
        self.stop = threading.Event()
        self.thread = threading.Thread(target=service.run, args=(self.stop,), name="heb-test-service")

    def __enter__(self) -> "ServiceRun":
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop.set()
        close_stream = getattr(self.service.gateway, "close_stream", None)
        if close_stream is not None:
            try:
                close_stream()
            except Exception:
                pass
        self.thread.join(5)


def test_evento_persistido_antes_del_envio_y_apagado(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _service(tmp_path, monkeypatch)
    gateway = FakeGateway(_parts())
    poster = RecordingPoster()
    seen = threading.Event()

    def post(destination, url, payload, event_id):
        counts = Database(config.storage.sqlite_path).status()["deliveries"]
        assert counts.get("pending", 0) + counts.get("in_flight", 0) >= 1
        seen.set()
        poster.calls.append((destination.name, event_id))
        poster.payloads.append(payload)
        return AttemptResult("delivered", 200, None)

    poster.post = post
    service = BridgeService(config, gateway=gateway, poster=poster)
    with ServiceRun(service) as running:
        assert seen.wait(3)
    assert not running.thread.is_alive()
    assert gateway.unsubscribed == ["41"]
    assert poster.calls
    assert poster.payloads[0]["employee_no"] == "0002"
    assert poster.payloads[0]["source"] == "live"
    assert "pictureURL" not in json.dumps(poster.payloads[0]["raw"])
    status = Database(config.storage.sqlite_path).status()
    assert status["deliveries"].get("delivered") == 1
    assert status["counters"].get("invalid_json") == 1
    assert status["counters"].get("binary_parts") == 1
    assert status["counters"].get("heartbeats") == 1


def test_dry_run_no_envia_ni_marca_delivered(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _service(tmp_path, monkeypatch, sqlite_name="dry.sqlite")
    gateway = FakeGateway(_parts())
    poster = RecordingPoster()
    service = BridgeService(config, gateway=gateway, poster=poster, dry_run=True)
    database = Database(config.storage.sqlite_path)

    def stored() -> bool:
        if not config.storage.sqlite_path.exists():
            return False
        return database.status()["counters"].get("events_stored", 0) >= 1

    with ServiceRun(service):
        _wait_until(stored)
    assert poster.calls == []
    assert database.status()["deliveries"].get("pending") == 1
    assert database.status()["deliveries"].get("delivered") is None


def test_destino_lento_no_bloquea_al_otro(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _secrets(monkeypatch)
    path = tmp_path / "config.toml"
    write_config(
        path,
        "http://127.0.0.1",
        "http://127.0.0.1/ok",
        destination_names='["rancherito", "broken"]',
        extra_destinations='[destinations.broken]\nurl = "http://127.0.0.1/broken"\ntoken_env = "RANCHERITO_EVENTS_TOKEN"\npending_url_policy = ""\n',
    )
    config = load_config(path)
    gateway = FakeGateway(_parts())
    poster = RecordingPoster()
    service = BridgeService(config, gateway=gateway, poster=poster)

    def both() -> bool:
        names = {name for name, _event in poster.calls}
        return names == {"rancherito", "broken"}

    with ServiceRun(service):
        _wait_until(both)
    status = Database(config.storage.sqlite_path).status()
    assert status["deliveries"].get("delivered") == 1
    assert status["deliveries"].get("pending") == 1


def test_auth_del_gateway_espera(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _service(tmp_path, monkeypatch, auth_hold=80, jitter=0)
    delays: list[float] = []

    class Denied:
        def subscribe(self):
            raise GatewayError("autenticación del gateway rechazada", status=401, auth_failed=True)

        def close_stream(self) -> None:
            return None

        def unsubscribe(self, _subscription_id: str) -> None:
            return None

    def sleeper(seconds: float) -> None:
        delays.append(seconds)
        stop.set()

    service = BridgeService(config, gateway=Denied(), sleep=sleeper)
    stop = threading.Event()
    service.run(stop)
    assert delays
    assert delays[0] >= 80


def test_timeout_despues_de_aceptar_reintenta_la_misma_clave(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    destination = DestinationHarness(
        [
            {
                "status": 200,
                "delay": 1.2,
                "body": {"accepted": True, "duplicate": False, "event_id": 7, "processing_status": "pending"},
            },
            {
                "status": 200,
                "body": {"accepted": True, "duplicate": True, "event_id": 7, "processing_status": "pending"},
            },
        ]
    )
    destination.start()
    try:
        config = _service(tmp_path, monkeypatch, destination_url=destination.url, post_timeout=0.3, initial_backoff=0.01, max_backoff=0.05, jitter=0)
        gateway = FakeGateway(_parts())
        service = BridgeService(config, gateway=gateway)
        database = Database(config.storage.sqlite_path)

        def delivered() -> bool:
            if not config.storage.sqlite_path.exists():
                return False
            return database.status()["deliveries"].get("delivered") == 1

        with ServiceRun(service):
            _wait_until(delivered, timeout=4)
    finally:
        destination.stop()
    assert len(destination.seen) >= 2
    assert destination.seen[0]["idempotency"] == destination.seen[1]["idempotency"]
    assert destination.seen[0]["idempotency"]
    assert "dest-token" == destination.seen[0]["authorization"].removeprefix("Bearer ")


def test_respuestas_http_del_destino(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cases = [
        (401, "auth_blocked"),
        (403, "auth_blocked"),
        (409, "conflict"),
        (422, "rejected"),
        (429, "pending"),
        (500, "pending"),
    ]
    for status, expected in cases:
        destination = DestinationHarness([{"status": status, "body": {"accepted": False}, "headers": {"Retry-After": "5"}}])
        destination.start()
        case_dir = tmp_path / str(status)
        case_dir.mkdir()
        try:
            config = _service(
                case_dir,
                monkeypatch,
                destination_url=destination.url,
                auth_hold=30,
                initial_backoff=30,
                max_backoff=30,
                jitter=0,
            )
            service = BridgeService(config, gateway=FakeGateway(_parts()))
            database = Database(config.storage.sqlite_path)

            def seen() -> bool:
                if not destination.seen or not config.storage.sqlite_path.exists():
                    return False
                counts = database.status()["deliveries"]
                return expected in counts

            with ServiceRun(service):
                _wait_until(seen)
            assert database.status()["deliveries"].get(expected) == 1
        finally:
            destination.stop()


def test_confirmacion_invalida_queda_pendiente(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    destination = DestinationHarness([{"status": 200, "body": b"<html>no</html>", "content_type": "text/html"}])
    destination.start()
    try:
        config = _service(tmp_path, monkeypatch, destination_url=destination.url, initial_backoff=30, max_backoff=30, jitter=0)
        service = BridgeService(config, gateway=FakeGateway(_parts()))
        database = Database(config.storage.sqlite_path)
        with ServiceRun(service):
            _wait_until(lambda: bool(destination.seen) and config.storage.sqlite_path.exists() and database.status()["deliveries"].get("pending") == 1)
    finally:
        destination.stop()
    assert Database(config.storage.sqlite_path).status()["deliveries"].get("delivered") is None


def test_cambio_de_url_exige_decision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _service(tmp_path, monkeypatch, destination_url="http://127.0.0.1/nueva")
    database = Database(config.storage.sqlite_path)
    database.migrate()
    event = json.loads((ROOT / "tests" / "fixtures" / "live_face.json").read_text(encoding="utf-8"))
    from hikvision_event_bridge.config import FilterConfig
    from hikvision_event_bridge.normalization import parse_live_part

    parsed = parse_live_part(
        event,
        timezone_name="America/Mexico_City",
        result_filter=FilterConfig((), (), ("face",), (), False, (), (), ()),
        strip_biometric=True,
    )
    assert parsed.event is not None
    database.insert_event_and_deliveries(
        event_id=parsed.event["event_id"],
        identity_version=1,
        normalization_version=1,
        device_uid=UID,
        occurred_at_utc=utc_now(),
        received_at=utc_now(),
        normalized=parsed.event,
        raw={},
        source="live",
        destinations=[("rancherito", "http://127.0.0.1/vieja")],
    )
    service = BridgeService(config, database=database, gateway=FakeGateway([]))
    with pytest.raises(ConfigError, match="pending_url_policy"):
        service.resolve_routes()


def test_use_current_actualiza_la_url_guardada(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _service(tmp_path, monkeypatch, destination_url="http://127.0.0.1/nueva", pending_policy="use_current")
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
            ) VALUES ('evt-url', 1, 1, ?, '2026-10-29T15:00:00Z', '2026-10-29T15:00:00Z', '{}', '{}', 'live')
            """,
            (UID,),
        )
        connection.execute(
            """
            INSERT INTO deliveries (event_row_id, destination_id, destination_url, state, attempts, created_at)
            VALUES (?, 'rancherito', 'http://127.0.0.1/vieja', 'pending', 0, '2026-10-29T15:00:00Z')
            """,
            (cursor.lastrowid,),
        )
        connection.commit()
    finally:
        connection.close()
    BridgeService(config, database=database, gateway=FakeGateway([])).resolve_routes()
    connection = database.connect()
    try:
        row = connection.execute("SELECT destination_url, state FROM deliveries").fetchone()
    finally:
        connection.close()
    assert row["destination_url"] == "http://127.0.0.1/nueva"
    assert row["state"] == "pending"


def test_huérfano_requiere_hold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = _service(tmp_path, monkeypatch)
    database = Database(config.storage.sqlite_path)
    database.migrate()
    now = utc_now()
    connection = database.connect()
    try:
        connection.execute("BEGIN IMMEDIATE")
        cursor = connection.execute(
            """
            INSERT INTO events (
                event_id, identity_version, normalization_version, device_uid,
                occurred_at, received_at, normalized_json, raw_json, source
            ) VALUES ('abc', 1, 1, ?, ?, ?, '{}', '{}', 'live')
            """,
            (UID, "2026-10-29T15:00:00Z", "2026-10-29T15:00:00Z"),
        )
        connection.execute(
            """
            INSERT INTO deliveries (event_row_id, destination_id, destination_url, state, attempts, created_at)
            VALUES (?, 'viejo', 'http://127.0.0.1/viejo', 'pending', 0, ?)
            """,
            (cursor.lastrowid, "2026-10-29T15:00:00Z"),
        )
        connection.commit()
    finally:
        connection.close()
    service = BridgeService(config, database=database, gateway=FakeGateway([]))
    with pytest.raises(ConfigError, match="orphaned_destinations.viejo"):
        service.resolve_routes()
    del now


def test_digest_reconexion_y_dispositivo_no_configurado(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GatewayHarness()
    unknown = b'{"EventNotificationAlert":{"eventType":"AccessControllerEvent","devIndex":"OTRO","AccessControllerEvent":{"majorEventType":5,"subEventType":1,"serialNo":1,"dateTime":"x"}}}'
    good = _live_bytes()
    gateway.subscribe_body = multipart_message(
        [
            ({"Content-Type": "application/json"}, b'{"SubscribeDeviceMgmtRsp":{"id":77}}'),
            ({"Content-Type": "application/json"}, unknown),
            ({"Content-Type": "application/json"}, good),
        ]
    )
    gateway.start()
    destination = DestinationHarness(
        [{"status": 200, "body": {"accepted": True, "duplicate": False, "event_id": 3, "processing_status": "pending"}}]
    )
    destination.start()
    try:
        config = _service(
            tmp_path,
            monkeypatch,
            gateway_url=gateway.url,
            destination_url=destination.url,
            initial_backoff=0.01,
            max_backoff=0.05,
            jitter=0,
        )
        service = BridgeService(config)
        database = Database(config.storage.sqlite_path)

        def ready() -> bool:
            posts = [item for item in gateway.authorized if item[0] == "POST" and "subscribe" in item[1]]
            stored = config.storage.sqlite_path.exists() and database.status()["counters"].get("events_stored", 0) >= 1
            return len(posts) >= 2 and stored

        with ServiceRun(service):
            _wait_until(ready, timeout=4)
    finally:
        gateway.stop()
        destination.stop()
    status = database.status()
    assert status["counters"].get("ignored_unconfigured", 0) >= 1
    assert any(path.startswith("/ISAPI/Event/notification/unSubscribeDeviceMgmt/77") for _method, path in gateway.authorized) or gateway.unsubscribed


def test_un_fallo_detiene_el_servicio_y_el_gateway(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = GatewayHarness()
    gateway.subscribe_body = multipart_message(
        [({"Content-Type": "application/json"}, b'{"SubscribeDeviceMgmtRsp":{"id":9}}')]
    )
    gateway.start()
    running = None
    try:
        config = _service(tmp_path, monkeypatch, gateway_url=gateway.url)
        running = ServiceRun(BridgeService(config))
        with pytest.raises(AssertionError):
            with running:
                assert False
    finally:
        gateway.stop()
    assert running is not None
    assert not running.thread.is_alive()
    assert gateway._thread is None
