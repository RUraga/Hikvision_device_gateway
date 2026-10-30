from __future__ import annotations

import json
from pathlib import Path

import pytest

from hikvision_event_bridge.config import ConfigError, load_config
from hikvision_event_bridge.identity import event_id_for, soft_conflict
from hikvision_event_bridge.normalization import (
    classify_result,
    loads_gateway_json,
    parse_history_record,
    parse_live_part,
)
from hikvision_event_bridge.routing import rejection_reason

from support import write_config

UID = "9CE552AA-7ACD-7145-B340-A8618DA1B946"
ROOT = Path(__file__).resolve().parents[1]


def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIKVISION_GATEWAY_PASSWORD", "gateway-secret")
    monkeypatch.setenv("RANCHERITO_EVENTS_TOKEN", "dest-token")


def test_config_example_valida(monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    config = load_config(ROOT / "config.example.toml")
    assert config.devices["rancherito"].device_uid == UID
    assert config.destinations["rancherito"].url.endswith("/api/integrations/hikvision/events")
    assert "gateway-secret" not in repr(config.gateway)
    assert "dest-token" not in repr(config.destinations["rancherito"])


def test_config_exige_secreto_y_rechaza_destino_desconocido(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HIKVISION_GATEWAY_PASSWORD", raising=False)
    monkeypatch.setenv("RANCHERITO_EVENTS_TOKEN", "dest-token")
    path = tmp_path / "config.toml"
    write_config(path, "http://127.0.0.1", "http://127.0.0.1/events")
    with pytest.raises(ConfigError, match="HIKVISION_GATEWAY_PASSWORD"):
        load_config(path)

    _env(monkeypatch)
    path.write_text(path.read_text(encoding="utf-8").replace('["rancherito"]', '["no-existe"]'), encoding="utf-8")
    with pytest.raises(ConfigError, match="destino inexistente"):
        load_config(path)


def test_config_puerta_sin_codigos_y_aviso_sin_filtro(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _env(monkeypatch)
    path = tmp_path / "config.toml"
    write_config(path, "http://127.0.0.1", "http://127.0.0.1/events")
    text = path.read_text(encoding="utf-8").replace("exclude_door_events = false", "exclude_door_events = true")
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError, match="door_sub_event_types"):
        load_config(path)

    write_config(path, "http://127.0.0.1", "http://127.0.0.1/events")
    text = path.read_text(encoding="utf-8").replace('include_authentication = ["card", "face"]', "include_authentication = []")
    path.write_text(text, encoding="utf-8")
    config = load_config(path)
    assert any("no tiene filtros" in warning for warning in config.warnings)


def test_live_e_historico_comparten_event_id_y_conservan_texto() -> None:
    from hikvision_event_bridge.config import FilterConfig

    live = json.loads((ROOT / "tests" / "fixtures" / "live_face.json").read_text(encoding="utf-8"))
    historical = json.loads((ROOT / "tests" / "fixtures" / "historical_face.json").read_text(encoding="utf-8"))

    filt = FilterConfig((), (), ("card", "face"), (), False, (), (), ())
    parsed_live = parse_live_part(live, timezone_name="America/Mexico_City", result_filter=filt, strip_biometric=True)
    parsed_history = parse_history_record(
        historical,
        device_uid=UID,
        timezone_name="America/Mexico_City",
        result_filter=filt,
        strip_biometric=True,
    )
    assert parsed_live.event is not None and parsed_history.event is not None
    assert parsed_live.event["event_id"] == parsed_history.event["event_id"]
    assert parsed_live.event["employee_no"] == "0002"
    assert parsed_live.event["card_no"] == "03322275427"
    assert parsed_live.event["attendance_status"] == "checkIn"
    assert parsed_live.event["label"] == "Entrada"
    assert parsed_live.event["result"] == "unknown"
    assert parsed_live.event["verify_mode"] == "face"
    assert "pictureURL" not in json.dumps(parsed_live.event["raw"])
    assert soft_conflict(parsed_live.event, parsed_history.event) is None
    assert event_id_for({"v": 1}) != parsed_live.event["event_id"]


def test_serial_distinto_no_deduplica_y_nombre_contradictorio_si() -> None:
    from hikvision_event_bridge.config import FilterConfig

    filt = FilterConfig((), (), ("face",), (), False, (), (75,), ())
    base = json.loads((ROOT / "tests" / "fixtures" / "live_face.json").read_text(encoding="utf-8"))
    other = json.loads(json.dumps(base))
    other["EventNotificationAlert"]["AccessControllerEvent"]["serialNo"] = 12346
    first = parse_live_part(base, timezone_name="America/Mexico_City", result_filter=filt, strip_biometric=True)
    second = parse_live_part(other, timezone_name="America/Mexico_City", result_filter=filt, strip_biometric=True)
    assert first.event is not None and second.event is not None
    assert first.event["event_id"] != second.event["event_id"]
    assert first.event["result"] == "success"
    renamed = json.loads(json.dumps(base))
    renamed["EventNotificationAlert"]["AccessControllerEvent"]["name"] = "Bruno"
    third = parse_live_part(renamed, timezone_name="America/Mexico_City", result_filter=filt, strip_biometric=True)
    assert third.event is not None
    assert third.event["event_id"] == first.event["event_id"]
    assert soft_conflict(first.event, third.event) == "name"


def test_filtros_de_tarjeta_rostro_y_puerta() -> None:
    from hikvision_event_bridge.config import FilterConfig

    filt = FilterConfig((), (), ("card", "face"), (), True, (21, 22), (), ())
    face = _event(verify_mode="cardOrFace", sub=75)
    door = _event(verify_mode="", sub=21)
    fingerprint = _event(verify_mode="fp", sub=38)
    assert rejection_reason(face, filt) is None
    assert rejection_reason(door, filt) == "door"
    assert rejection_reason(fingerprint, filt) == "authentication"
    assert classify_result(75, filt) == "unknown"


def test_json_solo_tolera_comas_finales() -> None:
    payload = loads_gateway_json(b'{"EventNotificationAlert":{"eventType":"heartBeat",}}')
    assert payload["EventNotificationAlert"]["eventType"] == "heartBeat"
    mixed = b'{"ok":true}' + b"\xff\xd8\xff"
    assert loads_gateway_json(mixed) == {"ok": True}
    with pytest.raises(json.JSONDecodeError):
        loads_gateway_json(b"{'a': 1}")


def test_fecha_sin_offset_usa_zona_del_dispositivo() -> None:
    from hikvision_event_bridge.config import FilterConfig

    filt = FilterConfig((), (), ("face",), (), False, (), (), ())
    record = json.loads((ROOT / "tests" / "fixtures" / "historical_face.json").read_text(encoding="utf-8"))
    record["time"] = "2026-10-29T09:00:00"
    parsed = parse_history_record(record, device_uid=UID, timezone_name="America/Mexico_City", result_filter=filt, strip_biometric=True)
    assert parsed.event is not None
    assert parsed.event["occurred_at_utc"] == "2026-10-29T15:00:00Z"
    assert parsed.event["occurred_at"].endswith("-06:00")


def _event(*, verify_mode: str, sub: int) -> dict:
    return {
        "major_event_type": 5,
        "sub_event_type": sub,
        "verify_mode": verify_mode or None,
    }
