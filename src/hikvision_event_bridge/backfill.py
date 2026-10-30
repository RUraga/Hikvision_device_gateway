"""Recuperación histórica documentada en la guía del Device Gateway, sección 5.4.9.

POST /ISAPI/AccessControl/AcsEvent?format=json&devIndex=<uuid>
con JSON_AcsEventCond. La respuesta es JSON_AcsEvent.

searchResultPosition se envía como desplazamiento inicial: 0 y después la suma
de numOfMatches. La guía lo llama posición final y dice que la siguiente
búsqueda continúa después de esa marca. Esa lectura no está verificada en el
terminal de Rancherito.

No se envía picEnable. La guía no dice si filtra eventos o solo pide fotos.
No se descargan imágenes.

El checkpoint solo avanza cuando una ventana completa, incluidas sus páginas
y subdivisiones, quedó persistida. Una página fallida no salta el hueco.
Los eventos ya guardados permanecen; la ventana se reintenta y la identidad
canónica deduplica.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from hikvision_event_bridge.config import AppConfig, DeviceConfig
from hikvision_event_bridge.database import Database
from hikvision_event_bridge.gateway import GatewayError
from hikvision_event_bridge.timeutil import format_gateway_time, parse_configured_time, parse_utc

MAX_PAGES = 10000


class HistorySource(Protocol):
    def search_acs_events(self, device_uid: str, body: dict[str, Any]) -> dict[str, Any]:
        ...


@dataclass
class HistoryPage:
    status: str
    records: list[dict[str, Any]]
    num_of_matches: int
    total_matches: int | None = None
    error: str | None = None


@dataclass
class RecoverResult:
    completed: bool
    auth_failed: bool = False
    message: str | None = None


def parse_acs_response(payload: dict[str, Any]) -> HistoryPage:
    if "AcsEvent" not in payload:
        code = payload.get("statusCode")
        sub = payload.get("subStatusCode")
        return HistoryPage("ERROR", [], 0, error=f"statusCode={code};subStatusCode={sub}")
    acs = payload.get("AcsEvent")
    if not isinstance(acs, dict):
        return HistoryPage("ERROR", [], 0, error="AcsEvent_no_objeto")
    status = str(acs.get("responseStatusStrg") or "").strip()
    raw_records = acs.get("MatchList") or []
    if not isinstance(raw_records, list):
        return HistoryPage("ERROR", [], 0, error="MatchList_invalida")
    records = [item for item in raw_records if isinstance(item, dict)]
    number = acs.get("numOfMatches")
    if isinstance(number, bool) or not isinstance(number, int):
        number = len(records)
    total = acs.get("totalMatches")
    if isinstance(total, bool) or not isinstance(total, int):
        total = None
    return HistoryPage(status, records, number, total)


def condition_body(
    *,
    search_id: str,
    position: int,
    max_results: int,
    start: datetime,
    end: datetime,
    timezone_name: str,
) -> dict[str, Any]:
    return {
        "AcsEventCond": {
            "searchID": search_id,
            "searchResultPosition": position,
            "maxResults": max_results,
            "startTime": format_gateway_time(start, timezone_name),
            "endTime": format_gateway_time(end, timezone_name),
        }
    }


def iter_windows(start: datetime, end: datetime, window_seconds: float) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    cursor = start
    window = timedelta(seconds=window_seconds)
    while cursor < end:
        window_end = min(cursor + window, end)
        windows.append((cursor, window_end))
        if window_end >= end:
            break
        overlapped = window_end - timedelta(seconds=1)
        cursor = overlapped if overlapped > cursor else window_end
    return windows


def reconcile_device(
    device: DeviceConfig,
    database: Database,
    source: HistorySource,
    config: AppConfig,
    now: datetime,
    persist: Callable[[dict[str, Any]], None],
    owner: str,
) -> RecoverResult:
    ttl = timedelta(seconds=max(config.backfill.window_seconds, config.storage.lock_ttl_seconds))
    if not database.try_acquire_backfill(device.device_uid, owner, ttl, now):
        return RecoverResult(False, message="recuperación ya en curso")
    try:
        sync = database.get_sync(device.device_uid)
        if sync is not None and sync.checkpoint_until:
            start = parse_utc(sync.checkpoint_until) - timedelta(seconds=config.backfill.overlap_seconds)
        else:
            start = parse_configured_time(config.backfill.initial_start)
        if start >= now:
            return RecoverResult(True)
        for window_start, window_end in iter_windows(start, now, config.backfill.window_seconds):
            if not database.try_acquire_backfill(device.device_uid, owner, ttl, now):
                return RecoverResult(False, message="recuperación ya en curso")
            result = recover_window(
                device,
                database,
                source,
                config,
                window_start,
                window_end,
                persist,
                advance_checkpoint=True,
                now=now,
            )
            if not result.completed:
                return result
        return RecoverResult(True)
    finally:
        database.release_backfill(device.device_uid, owner)


def recover_window(
    device: DeviceConfig,
    database: Database,
    source: HistorySource,
    config: AppConfig,
    start: datetime,
    end: datetime,
    persist: Callable[[dict[str, Any]], None],
    *,
    advance_checkpoint: bool,
    now: datetime,
    checkpoint_floor: datetime | None = None,
) -> RecoverResult:
    completed, auth_failed, message = _recover_range(device, source, config, start, end, persist, database, now)
    if not completed:
        database.set_backfill_error(device.device_uid, message or "ventana incompleta", now)
        return RecoverResult(False, auth_failed=auth_failed, message=message)
    if advance_checkpoint and _checkpoint_can_advance(checkpoint_floor, start, end):
        database.set_checkpoint(device.device_uid, end, now)
    return RecoverResult(True)


def _checkpoint_can_advance(checkpoint: datetime | None, start: datetime, end: datetime) -> bool:
    if checkpoint is None:
        return True
    return start <= checkpoint


def _recover_range(
    device: DeviceConfig,
    source: HistorySource,
    config: AppConfig,
    start: datetime,
    end: datetime,
    persist: Callable[[dict[str, Any]], None],
    database: Database,
    now: datetime,
) -> tuple[bool, bool, str | None]:
    outcome, auth_failed, message = _read_pages(device, source, config, start, end, persist)
    if outcome == "complete":
        return True, False, None
    if outcome == "auth":
        return False, True, message
    span = (end - start).total_seconds()
    if outcome == "split" and span > config.backfill.min_window_seconds:
        midpoint = start + (end - start) / 2
        left = _recover_range(device, source, config, start, midpoint, persist, database, now)
        if not left[0]:
            return left
        return _recover_range(device, source, config, midpoint, end, persist, database, now)
    if outcome == "split":
        database.add_incident(
            "backfill_result_cap",
            device_uid=device.device_uid,
            details={"window_seconds": int(span)},
            now=now,
        )
        return False, False, "la ventana sigue saturada y no se puede subdividir más"
    return False, auth_failed, message or "página histórica fallida"


def _read_pages(
    device: DeviceConfig,
    source: HistorySource,
    config: AppConfig,
    start: datetime,
    end: datetime,
    persist: Callable[[dict[str, Any]], None],
) -> tuple[str, bool, str | None]:
    search_id = f"heb-{int(start.timestamp())}-{int(end.timestamp())}"
    position = 0
    previous: tuple[object, ...] | None = None
    for _page in range(MAX_PAGES):
        body = condition_body(
            search_id=search_id,
            position=position,
            max_results=config.backfill.page_size,
            start=start,
            end=end,
            timezone_name=device.timezone,
        )
        try:
            payload = source.search_acs_events(device.device_uid, body)
        except GatewayError as exc:
            return ("auth" if exc.auth_failed else "failed"), exc.auth_failed, str(exc)
        page = parse_acs_response(payload)
        status = page.status.casefold()
        if page.error or status not in {"ok", "more", "no match"}:
            return "failed", False, page.error or f"estado_desconocido:{page.status}"
        if status == "no match":
            return "complete", False, None
        signature = tuple((item.get("serialNo"), item.get("time")) for item in page.records)
        if status == "more" and page.num_of_matches <= 0:
            return "split", False, "paginación sin progreso"
        if signature and signature == previous:
            return "split", False, "página repetida"
        previous = signature
        for record in page.records:
            persist(record)
        step = page.num_of_matches if page.num_of_matches > 0 else len(page.records)
        if step <= 0:
            return "split", False, "página vacía"
        position += step
        if status == "ok":
            if page.num_of_matches >= config.backfill.page_size:
                return "split", False, "posible tope de resultados"
            return "complete", False, None
    return "failed", False, "demasiadas páginas"
