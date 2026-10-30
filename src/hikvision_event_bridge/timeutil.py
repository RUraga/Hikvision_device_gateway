"""Fechas internas en UTC. El texto con offset se conserva para el contrato."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("la fecha no tiene zona horaria")
    return value.astimezone(UTC)


def format_utc(value: datetime) -> str:
    """Formato fijo que ordena bien como texto: 2026-10-29T15:00:00Z."""
    moment = ensure_utc(value).replace(microsecond=0)
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    moment = datetime.fromisoformat(text)
    return ensure_utc(moment)


def require_zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"zona horaria IANA desconocida: {name}") from exc


def parse_event_time(value: str, timezone_name: str | None) -> tuple[str, datetime]:
    """Devuelve el texto para el contrato y el instante UTC truncado a segundos.

    Si el texto trae offset, se conserva. Si no lo trae, se interpreta con la
    zona del dispositivo. El manual del gateway muestra horas con offset
    (por ejemplo 2016-12-12T17:30:08+08:00) y las llama «UTC time».
    """
    text = value.strip()
    if not text:
        raise ValueError("fecha vacía")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        moment = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("fecha de evento ilegible") from exc
    if moment.tzinfo is None:
        if not timezone_name:
            raise ValueError("la fecha no tiene offset y el dispositivo no tiene zona IANA")
        moment = moment.replace(tzinfo=require_zone(timezone_name))
        contract = moment.isoformat(timespec="seconds")
    else:
        contract = text
    return contract, ensure_utc(moment).replace(microsecond=0)


def format_gateway_time(value: datetime, timezone_name: str) -> str:
    """Hora ISO 8601 con el offset del dispositivo, para AcsEventCond."""
    local = ensure_utc(value).astimezone(require_zone(timezone_name))
    return local.replace(microsecond=0).isoformat(timespec="seconds")


def parse_configured_time(value: str) -> datetime:
    """Punto inicial explícito. Exige offset para no asumir «ahora» ni UTC implícito."""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"fecha inválida: {value}") from exc
    if moment.tzinfo is None:
        raise ValueError(f"la fecha {value} debe incluir offset, por ejemplo 2026-10-01T00:00:00-06:00")
    return ensure_utc(moment)


def add_seconds(value: datetime, seconds: float) -> datetime:
    return ensure_utc(value) + timedelta(seconds=seconds)
