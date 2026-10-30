"""Cuerpos que el bridge realmente construye para Rancherito.

Los textos fixture-* y checkIn/Entrada de las muestras locales no son códigos
confirmados del reloj. Sirven para que los dos repositorios comparen los
mismos bytes.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from hikvision_event_bridge.config import FilterConfig
from hikvision_event_bridge.identity import canonical_document, event_id_for
from hikvision_event_bridge.normalization import parse_history_record, parse_live_part, public_payload
from hikvision_event_bridge.timeutil import parse_event_time

DEVICE = "9CE552AA-7ACD-7145-B340-A8618DA1B946"
ROOT = Path(__file__).resolve().parents[1]
LIVE_FIXTURE = ROOT / "tests" / "fixtures" / "live_face.json"
HISTORY_FIXTURE = ROOT / "tests" / "fixtures" / "historical_face.json"
CONTRACT_FIXTURE = ROOT / "tests" / "fixtures" / "rancherito_contract_event.json"
MEAL_FIXTURE = ROOT / "tests" / "fixtures" / "rancherito_meal_sequence.json"


def rancherito_root() -> Path | None:
    """Checkout de Rancherito, si está disponible.

    RANCHERITO_ROOT tiene prioridad. Si no, se usa un directorio hermano que
    contenga artisan. Sin eso, la suite del bridge sigue y las pruebas
    cruzadas se omiten.
    """
    configured = os.environ.get("RANCHERITO_ROOT", "").strip()
    candidates = []
    if configured:
        candidates.append(Path(configured))
    candidates.append(ROOT.parent / "rancherito")
    for candidate in candidates:
        if (candidate / "artisan").is_file():
            return candidate
    return None


def rancherito_contract_path() -> Path | None:
    root = rancherito_root()
    if root is None:
        return None
    return root / "tests" / "fixtures" / "hikvision" / "rancherito_contract_event.json"


def rancherito_meal_path() -> Path | None:
    root = rancherito_root()
    if root is None:
        return None
    return root / "tests" / "fixtures" / "hikvision" / "rancherito_meal_sequence.json"

EMPTY_FILTER = FilterConfig(
    major_event_types=(),
    sub_event_types=(),
    include_authentication=(),
    authentication_sub_event_types=(),
    exclude_door_events=False,
    door_sub_event_types=(),
    success_sub_event_types=(),
    failure_sub_event_types=(),
)


def bridge_contract_event() -> dict:
    live = json.loads(LIVE_FIXTURE.read_text(encoding="utf-8"))
    parsed = parse_live_part(
        live,
        timezone_name="America/Mexico_City",
        result_filter=EMPTY_FILTER,
        strip_biometric=True,
    )
    if parsed.event is None:
        raise RuntimeError(parsed.reason or "la muestra en vivo no produjo evento")
    return public_payload(parsed.event)


def history_event_id() -> str:
    record = json.loads(HISTORY_FIXTURE.read_text(encoding="utf-8"))
    parsed = parse_history_record(
        record,
        device_uid=DEVICE,
        timezone_name="America/Mexico_City",
        result_filter=EMPTY_FILTER,
        strip_biometric=True,
    )
    if parsed.event is None:
        raise RuntimeError(parsed.reason or "la muestra histórica no produjo evento")
    return str(parsed.event["event_id"])


def synthetic_event(
    *,
    serial_no: int,
    occurred_at: str,
    attendance_status: str,
    label: str,
    result: str,
    source: str = "live",
) -> dict:
    contract, moment = parse_event_time(occurred_at, "America/Mexico_City")
    document = canonical_document(
        device_uid=DEVICE,
        serial_no=serial_no,
        occurred_at_utc=moment,
        employee_no="0002",
        card_no="03322275427",
        major_event_type=5,
        sub_event_type=75,
    )
    return public_payload(
        {
            "schema_version": 1,
            "event_id": event_id_for(document),
            "device_index": DEVICE,
            "serial_no": serial_no,
            "occurred_at": contract,
            "employee_no": "0002",
            "card_no": "03322275427",
            "attendance_status": attendance_status,
            "label": label,
            "major_event_type": 5,
            "sub_event_type": 75,
            "result": result,
            "verify_mode": "face",
            "name": "Ana",
            "source": source,
            "raw": {"fixture": source, "serial_no": serial_no},
        }
    )


def meal_sequence() -> list[dict]:
    """Secuencia sintética. Los textos no salen de una captura del reloj."""
    return [
        synthetic_event(
            serial_no=2001,
            occurred_at="2026-10-29T09:00:00-06:00",
            attendance_status="fixture-work-start",
            label="fixture-entrada",
            result="unknown",
        ),
        synthetic_event(
            serial_no=2002,
            occurred_at="2026-10-29T14:00:00-06:00",
            attendance_status="fixture-meal-out",
            label="fixture-comida",
            result="unknown",
        ),
        synthetic_event(
            serial_no=2003,
            occurred_at="2026-10-29T14:10:00-06:00",
            attendance_status="fixture-work-end",
            label="fixture-salida",
            result="failure",
        ),
        synthetic_event(
            serial_no=2004,
            occurred_at="2026-10-29T14:40:00-06:00",
            attendance_status="fixture-meal-in",
            label="fixture-regreso",
            result="success",
        ),
        synthetic_event(
            serial_no=2005,
            occurred_at="2026-10-29T15:00:00-06:00",
            attendance_status="codigo-sin-muestra",
            label="texto-sin-muestra",
            result="unknown",
        ),
        synthetic_event(
            serial_no=2006,
            occurred_at="2026-10-29T18:00:00-06:00",
            attendance_status="fixture-work-end",
            label="fixture-salida",
            result="unknown",
        ),
    ]


def missing_punch_sequence() -> list[dict]:
    return [
        synthetic_event(
            serial_no=2101,
            occurred_at="2026-10-30T09:00:00-06:00",
            attendance_status="fixture-work-start",
            label="fixture-entrada",
            result="unknown",
        ),
        synthetic_event(
            serial_no=2102,
            occurred_at="2026-10-30T14:40:00-06:00",
            attendance_status="fixture-meal-in",
            label="fixture-regreso",
            result="unknown",
        ),
        synthetic_event(
            serial_no=2103,
            occurred_at="2026-10-30T18:00:00-06:00",
            attendance_status="fixture-work-end",
            label="fixture-salida",
            result="unknown",
        ),
    ]


def dump(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    dump(CONTRACT_FIXTURE, bridge_contract_event())
    dump(RANCHERITO_CONTRACT, bridge_contract_event())
    meal = {"meal": meal_sequence(), "missing_punch": missing_punch_sequence()}
    dump(MEAL_FIXTURE, meal)
    dump(RANCHERITO_MEAL, meal)
