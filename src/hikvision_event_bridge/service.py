"""Orquesta suscripción, recuperación, entregas y retención dentro del proceso."""

from __future__ import annotations

import logging
import random
import threading
from datetime import datetime, timedelta

from hikvision_event_bridge.backfill import reconcile_device, recover_window
from hikvision_event_bridge.config import AppConfig, ConfigError, DeviceConfig
from hikvision_event_bridge.database import INSERT_CONFLICT, INSERT_DUPLICATE, INSERT_STORED, ClaimedDelivery, Database
from hikvision_event_bridge.delivery import AttemptResult, DestinationPoster, next_delay, schedule_after
from hikvision_event_bridge.gateway import GatewayClient, GatewayError, subscription_id_from_payload
from hikvision_event_bridge.logging_config import log
from hikvision_event_bridge.multipart import MultipartError, Part
from hikvision_event_bridge.normalization import loads_gateway_json, parse_history_record, parse_live_part, public_payload
from hikvision_event_bridge.routing import rejection_reason
from hikvision_event_bridge.timeutil import parse_utc, utc_now

LOGGER = logging.getLogger("hikvision_event_bridge.service")


class BridgeService:
    def __init__(
        self,
        config: AppConfig,
        *,
        database: Database | None = None,
        gateway: GatewayClient | None = None,
        poster: DestinationPoster | None = None,
        sleep=None,
        clock=None,
        rng: random.Random | None = None,
        dry_run: bool = False,
    ) -> None:
        self.config = config
        self.database = database or Database(config.storage.sqlite_path, busy_timeout_ms=config.storage.busy_timeout_ms)
        self.gateway = gateway
        self.poster = poster or DestinationPoster(
            config.timeouts.connect_seconds,
            config.timeouts.post_seconds,
            verify_tls=config.gateway.verify_tls,
        )
        self._sleep_override = sleep
        self.clock = clock or utc_now
        self.rng = rng or random.Random()
        self.dry_run = dry_run
        self._stop = threading.Event()
        self._subscription_id: str | None = None
        self._subscription_lock = threading.Lock()

    def run(self, stop: threading.Event) -> None:
        self._stop = stop
        self.database.migrate()
        self.resolve_routes()
        now = self.clock()
        released = self.database.release_all_inflight(now)
        if released:
            log(LOGGER, logging.INFO, "entregas_en_vuelo_recuperadas", total=released)
        owner = self.database.owner_name()
        lock_connection = self.database.connect()
        self.database.acquire_instance_lock(
            lock_connection,
            owner,
            timedelta(seconds=self.config.storage.lock_ttl_seconds),
            now,
        )
        threads: list[threading.Thread] = []
        try:
            threads.append(threading.Thread(target=self._lock_heartbeat, args=(lock_connection, owner), name="heb-lock", daemon=True))
            threads.append(threading.Thread(target=self._subscribe_loop, name="heb-subscribe", daemon=True))
            threads.append(threading.Thread(target=self._backfill_loop, name="heb-backfill", daemon=True))
            threads.append(threading.Thread(target=self._retention_loop, name="heb-retention", daemon=True))
            if not self.dry_run:
                for index in range(self.config.delivery.concurrency):
                    threads.append(threading.Thread(target=self._delivery_loop, name=f"heb-delivery-{index}", daemon=True))
            for thread in threads:
                thread.start()
            while not stop.is_set():
                stop.wait(0.2)
        finally:
            stop.set()
            if self.gateway is not None:
                self.gateway.close_stream()
            subscription_id = self._subscription_id
            if subscription_id and self.gateway is not None:
                self.gateway.unsubscribe(subscription_id)
                log(LOGGER, logging.INFO, "suscripcion_cancelada", subscription_id=subscription_id)
            for thread in threads:
                thread.join(timeout=2)
            self.database.release_instance_lock(lock_connection, owner)
            lock_connection.close()
            closer = getattr(self.gateway, "close", None)
            if closer is not None:
                closer()

    def resolve_routes(self) -> None:
        grouped: dict[str, set[str]] = {}
        for row in self.database.open_delivery_routes():
            grouped.setdefault(str(row["destination_id"]), set()).add(str(row["destination_url"]))
        for destination_id, urls in grouped.items():
            destination = self.config.destinations.get(destination_id)
            if destination is None:
                orphan = self.config.orphans.get(destination_id)
                if orphan is None:
                    raise ConfigError(
                        f"hay entregas abiertas del destino '{destination_id}', que ya no está en la configuración. "
                        f"Añada [orphaned_destinations.{destination_id}] action = \"hold\" o restaure ese destino. "
                        "No se reenvían a otra URL."
                    )
                self.database.hold_destination(destination_id, "destino_eliminado")
                log(LOGGER, logging.WARNING, "destino_en_espera", destination=destination_id)
                continue
            mismatched = sorted(url for url in urls if url != destination.url)
            if not mismatched:
                continue
            policy = destination.pending_url_policy
            if policy == "":
                raise ConfigError(
                    f"el destino '{destination_id}' cambió de URL y quedan entregas con la URL anterior. "
                    "Defina pending_url_policy = \"hold\", \"use_snapshot\" o \"use_current\". "
                    "Sin esa decisión el servicio no arranca y no usa la URL nueva."
                )
            if policy == "hold":
                self.database.hold_destination(destination_id, "url_en_revision")
                log(LOGGER, logging.WARNING, "url_en_espera", destination=destination_id)
            elif policy == "use_current":
                self.database.rewrite_snapshot(destination_id, destination.url)
                log(LOGGER, logging.WARNING, "url_actualizada_por_politica", destination=destination_id)
            else:
                log(LOGGER, logging.WARNING, "se_conserva_la_url_original", destination=destination_id)

    def _lock_heartbeat(self, connection, owner: str) -> None:
        interval = max(1.0, self.config.storage.lock_ttl_seconds / 3)
        while not self._stop.wait(interval):
            if not self.database.heartbeat_lock(connection, owner, self.clock()):
                log(LOGGER, logging.ERROR, "cerrojo_perdido")
                self._stop.set()
                return

    def _subscribe_loop(self) -> None:
        delay = self.config.delivery.initial_backoff_seconds
        while not self._stop.is_set():
            auth_failed = False
            saw_part = False
            try:
                self.gateway = self.gateway or GatewayClient(self.config)
                _content_type, parts = self.gateway.subscribe()
                for part in parts:
                    if self._stop.is_set():
                        break
                    saw_part = True
                    self._handle_part(part)
                delay = self.config.delivery.initial_backoff_seconds if saw_part else self._grow(delay)
            except GatewayError as exc:
                self.database.increment("reconnects")
                auth_failed = exc.auth_failed
                delay = self.config.delivery.auth_hold_seconds if exc.auth_failed else self._grow(delay)
                if exc.auth_failed:
                    self.database.add_incident("gateway_auth", details={"status": exc.status or 0}, now=self.clock())
                    log(LOGGER, logging.ERROR, "autenticacion_gateway_rechazada", status=exc.status or 0)
                else:
                    log(LOGGER, logging.WARNING, "suscripcion_interrumpida", error=type(exc).__name__, status=exc.status or 0)
            except MultipartError:
                self.database.increment("reconnects")
                delay = self._grow(delay)
                log(LOGGER, logging.WARNING, "multipart_invalido")
            except Exception as exc:
                self.database.increment("reconnects")
                delay = self._grow(delay)
                log(LOGGER, logging.ERROR, "suscripcion_error", error=type(exc).__name__)
            finally:
                if self.gateway is not None:
                    self.gateway.close_stream()
            if self._stop.is_set():
                return
            floor = 0.8 if auth_failed else 0.0
            self._pause(self._with_jitter(delay, floor_ratio=floor))

    def _handle_part(self, part: Part) -> None:
        content_type = part.content_type.lower()
        if part.too_large:
            self.database.increment("part_too_large")
            self.database.add_incident("part_too_large", details={"content_type": content_type[:60]}, now=self.clock())
            return
        if content_type and "json" not in content_type and not content_type.startswith("text/"):
            self.database.increment("binary_parts")
            return
        try:
            payload = loads_gateway_json(part.body)
        except (UnicodeDecodeError, ValueError):
            self.database.increment("invalid_json")
            self.database.add_incident("invalid_json", details={"size": len(part.body)}, now=self.clock())
            return
        subscription_id = subscription_id_from_payload(payload)
        if subscription_id:
            with self._subscription_lock:
                self._subscription_id = subscription_id
            if not (isinstance(payload, dict) and "EventNotificationAlert" in payload):
                return
        self._ingest_live(payload)

    def _ingest_live(self, payload: object) -> None:
        preview = parse_live_part(payload, timezone_name=None, result_filter=None, strip_biometric=True)
        if preview.kind == "heartbeat":
            self.database.increment("heartbeats")
            if preview.device_uid:
                self.database.touch_live(preview.device_uid, self.clock())
            return
        if preview.kind == "ignored":
            self.database.increment("ignored_event_type")
            return
        if preview.kind == "invalid":
            self.database.increment("invalid_event")
            self.database.add_incident(
                "invalid_event",
                device_uid=preview.device_uid,
                details={"reason": preview.reason or "invalid"},
                now=self.clock(),
            )
            return
        device = self.config.devices_by_uid.get(preview.device_uid or "")
        if device is None or not device.enabled:
            self.database.increment("ignored_disabled" if device is not None else "ignored_unconfigured")
            if preview.device_uid:
                self.database.increment("ignored_uid:" + preview.device_uid)
            return
        parsed = parse_live_part(
            payload,
            timezone_name=device.timezone,
            result_filter=device.filter,
            strip_biometric=self.config.privacy.strip_biometric_fields,
        )
        self._store_parsed(parsed, device)

    def _store_parsed(self, parsed, device: DeviceConfig) -> None:
        if parsed.kind != "event" or parsed.event is None:
            self.database.increment("invalid_event")
            self.database.add_incident(
                "invalid_event",
                device_uid=device.device_uid,
                details={"reason": parsed.reason or "invalid"},
                now=self.clock(),
            )
            return
        reason = rejection_reason(parsed.event, device.filter)
        if reason is not None:
            self.database.increment("filtered_" + reason)
            return
        event = parsed.event
        destinations = [(name, self.config.destinations[name].url) for name in device.destinations]
        outcome = self.database.insert_event_and_deliveries(
            event_id=str(event["event_id"]),
            identity_version=int(event["identity_version"]),
            normalization_version=int(event["normalization_version"]),
            device_uid=device.device_uid,
            occurred_at_utc=parse_utc(str(event["occurred_at_utc"])),
            received_at=self.clock(),
            normalized=event,
            raw=event["raw"] if isinstance(event["raw"], dict) else {},
            source=str(event["source"]),
            destinations=destinations,
        )
        if outcome == INSERT_STORED:
            self.database.increment("events_stored")
            log(LOGGER, logging.INFO, "evento_guardado", event_id=event["event_id"], device_uid=device.device_uid, source=event["source"])
        elif outcome == INSERT_DUPLICATE:
            self.database.increment("events_duplicate")
        elif outcome == INSERT_CONFLICT:
            self.database.increment("events_conflict")
            log(LOGGER, logging.WARNING, "conflicto_canonico", event_id=event["event_id"], device_uid=device.device_uid)

    def _backfill_loop(self) -> None:
        if not self.config.backfill.enabled:
            return
        while not self._stop.is_set():
            auth_failed = False
            try:
                auth_failed = self._reconcile_enabled_devices()
            except Exception as exc:
                log(LOGGER, logging.ERROR, "backfill_error", error=type(exc).__name__)
            pause = self.config.delivery.auth_hold_seconds if auth_failed else self.config.backfill.interval_seconds
            if self._pause(pause):
                return

    def _reconcile_enabled_devices(self) -> bool:
        source = self.gateway or GatewayClient(self.config)
        self.gateway = source
        auth_failed = False
        for device in self.config.enabled_devices():
            if self._stop.is_set():
                break
            result = reconcile_device(
                device,
                self.database,
                source,
                self.config,
                self.clock(),
                lambda record, current=device: self._persist_history(current, record),
                self.database.owner_name() + ":backfill",
            )
            if result.auth_failed:
                auth_failed = True
            if result.message and not result.completed:
                log(LOGGER, logging.WARNING, "backfill_incompleto", device_uid=device.device_uid, reason=result.message)
        return auth_failed

    def recover_explicit_window(self, device: DeviceConfig, start: datetime, end: datetime) -> None:
        source = self.gateway or GatewayClient(self.config)
        self.gateway = source
        sync = self.database.get_sync(device.device_uid)
        floor = parse_utc(sync.checkpoint_until) if sync and sync.checkpoint_until else None
        owner = self.database.owner_name() + ":cli-backfill"
        if not self.database.try_acquire_backfill(device.device_uid, owner, timedelta(seconds=self.config.storage.lock_ttl_seconds), self.clock()):
            raise ConfigError(f"la recuperación de {device.name} ya está en curso")
        try:
            result = recover_window(
                device,
                self.database,
                source,
                self.config,
                start,
                end,
                lambda record: self._persist_history(device, record),
                advance_checkpoint=floor is not None and start <= floor,
                now=self.clock(),
                checkpoint_floor=floor,
            )
        finally:
            self.database.release_backfill(device.device_uid, owner)
        if not result.completed:
            raise ConfigError(result.message or "la ventana histórica no se completó")

    def _persist_history(self, device: DeviceConfig, record: dict) -> None:
        parsed = parse_history_record(
            record,
            device_uid=device.device_uid,
            timezone_name=device.timezone,
            result_filter=device.filter,
            strip_biometric=self.config.privacy.strip_biometric_fields,
        )
        self._store_parsed(parsed, device)

    def _delivery_loop(self) -> None:
        worker = f"{self.database.owner_name()}:{threading.get_ident()}"
        lease = timedelta(seconds=self.config.delivery.lease_seconds)
        while not self._stop.is_set():
            claimed = self.database.claim_one(worker, lease, self.clock())
            if claimed is None:
                if self._stop.wait(0.2):
                    return
                continue
            self._deliver(claimed)

    def _deliver(self, claimed: ClaimedDelivery) -> None:
        destination = self.config.destinations.get(claimed.destination_id)
        if destination is None:
            self.database.finish_delivery(
                claimed.delivery_id,
                outcome="held",
                now=self.clock(),
                error="destino_ausente",
                http_status=None,
                next_attempt_at=None,
            )
            return
        url = claimed.destination_url if destination.pending_url_policy == "use_snapshot" else destination.url
        result = self.poster.post(destination, url, public_payload(claimed.payload), claimed.event_id)
        self._apply_result(claimed, result)

    def _apply_result(self, claimed: ClaimedDelivery, result: AttemptResult) -> None:
        now = self.clock()
        attempt = claimed.attempts + 1
        if result.outcome == "delivered":
            self.database.finish_delivery(
                claimed.delivery_id,
                outcome="delivered",
                now=now,
                error=None,
                http_status=result.http_status,
                next_attempt_at=None,
            )
            log(LOGGER, logging.INFO, "entrega_confirmada", event_id=claimed.event_id, destination=claimed.destination_id, status=result.http_status or 0)
            return
        if result.outcome == "retry":
            delay = next_delay(result, attempt=attempt, delivery=self.config.delivery, rng=self.rng)
            self.database.finish_delivery(
                claimed.delivery_id,
                outcome="pending",
                now=now,
                error=result.error,
                http_status=result.http_status,
                next_attempt_at=schedule_after(now, delay),
            )
            return
        if result.outcome == "auth_blocked":
            delay = next_delay(result, attempt=attempt, delivery=self.config.delivery, rng=self.rng)
            self.database.finish_delivery(
                claimed.delivery_id,
                outcome="auth_blocked",
                now=now,
                error=result.error,
                http_status=result.http_status,
                next_attempt_at=schedule_after(now, delay),
            )
            self.database.add_incident(
                "destination_auth",
                event_id=claimed.event_id,
                details={"destination": claimed.destination_id, "status": result.http_status or 0},
                now=now,
            )
            log(LOGGER, logging.ERROR, "destino_rechazo_credenciales", destination=claimed.destination_id, status=result.http_status or 0)
            return
        outcome = result.outcome if result.outcome in {"rejected", "conflict", "held"} else "pending"
        self.database.finish_delivery(
            claimed.delivery_id,
            outcome=outcome,
            now=now,
            error=result.error,
            http_status=result.http_status,
            next_attempt_at=None,
        )
        self.database.add_incident(
            "delivery_" + outcome,
            event_id=claimed.event_id,
            details={"destination": claimed.destination_id, "status": result.http_status or 0, "error": result.error or ""},
            now=now,
        )

    def _retention_loop(self) -> None:
        while not self._stop.is_set():
            removed = self.database.purge_delivered(timedelta(days=self.config.retention.delivered_days), self.clock())
            if removed:
                log(LOGGER, logging.INFO, "retencion_aplicada", removed=removed)
            if self._stop.wait(3600):
                return

    def _grow(self, delay: float) -> float:
        return min(self.config.delivery.max_backoff_seconds, max(self.config.delivery.initial_backoff_seconds, delay * 2))

    def _with_jitter(self, delay: float, *, floor_ratio: float) -> float:
        ratio = self.config.delivery.jitter_ratio
        if ratio <= 0:
            return delay
        value = delay + self.rng.uniform(-delay * ratio, delay * ratio)
        return max(delay * floor_ratio, value)

    def _pause(self, seconds: float) -> bool:
        if self._sleep_override is not None:
            self._sleep_override(seconds)
            return self._stop.is_set()
        return self._stop.wait(max(0.0, seconds))
