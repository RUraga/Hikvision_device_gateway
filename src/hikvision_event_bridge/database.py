"""SQLite durable. Las llamadas HTTP no deben ejecutarse dentro de una transacción."""

from __future__ import annotations

import json
import os
import socket
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from hikvision_event_bridge.identity import SOFT_FIELDS, soft_conflict
from hikvision_event_bridge.timeutil import format_utc, parse_utc, utc_now

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    identity_version INTEGER NOT NULL,
    normalization_version INTEGER NOT NULL,
    device_uid TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    normalized_json TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY,
    event_row_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    destination_id TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    last_error TEXT,
    last_http_status INTEGER,
    delivered_at TEXT,
    lease_owner TEXT,
    lease_until TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(event_row_id, destination_id)
);

CREATE TABLE IF NOT EXISTS sync_state (
    device_uid TEXT PRIMARY KEY,
    checkpoint_until TEXT,
    last_attempt_at TEXT,
    last_success_at TEXT,
    last_error TEXT,
    last_live_at TEXT,
    backfill_owner TEXT,
    backfill_until TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS incidents (
    id INTEGER PRIMARY KEY,
    incident_type TEXT NOT NULL,
    created_at TEXT NOT NULL,
    device_uid TEXT,
    event_id TEXT,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_evidence (
    id INTEGER PRIMARY KEY,
    event_row_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(event_row_id, source)
);

CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS instance_lock (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    owner TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_deliveries_claim
    ON deliveries(state, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_events_device_time
    ON events(device_uid, occurred_at);
"""

DELIVERY_STATES = {
    "pending",
    "in_flight",
    "delivered",
    "rejected",
    "conflict",
    "auth_blocked",
    "held",
}

INSERT_STORED = "stored"
INSERT_DUPLICATE = "duplicate"
INSERT_CONFLICT = "conflict"


class LockBusy(Exception):
    pass


@dataclass
class StoredEvent:
    row_id: int
    event_id: str
    normalized: dict[str, object]


@dataclass
class ClaimedDelivery:
    delivery_id: int
    event_row_id: int
    event_id: str
    destination_id: str
    destination_url: str
    payload: dict[str, object]
    attempts: int


@dataclass
class SyncSnapshot:
    device_uid: str
    checkpoint_until: str | None
    last_live_at: str | None
    last_success_at: str | None
    last_error: str | None


class Database:
    def __init__(self, path: Path, *, busy_timeout_ms: int = 5000) -> None:
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms

    def connect(self) -> sqlite3.Connection:
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        connection = sqlite3.connect(self.path, timeout=self.busy_timeout_ms / 1000, isolation_level=None)
        connection.row_factory = sqlite3.Row
        self._prepare(connection)
        return connection

    def connect_readonly(self) -> sqlite3.Connection:
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        uri = self.path.resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=self.busy_timeout_ms / 1000)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        return connection

    @contextmanager
    def session(self):
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    def migrate(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._publish_new_database()
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.executescript(SCHEMA)
            row = connection.execute("SELECT value FROM schema_meta WHERE key = 'version'").fetchone()
            if row is None:
                connection.execute("INSERT INTO schema_meta(key, value) VALUES ('version', '1')")
            elif row["value"] != "1":
                connection.rollback()
                raise RuntimeError(f"versión de esquema no soportada: {row['value']}")
            connection.commit()

    def _publish_new_database(self) -> None:
        """El archivo público aparece solo cuando deliveries ya existe.

        sqlite3.connect crea el archivo antes del esquema. Una consulta de
        estado que vea ese archivo a medias recibe 'no such table'.
        """
        partial = self.path.with_name(f".{self.path.name}.{os.getpid()}.partial")
        published = False
        try:
            connection = sqlite3.connect(partial, timeout=self.busy_timeout_ms / 1000, isolation_level=None)
            try:
                connection.executescript(SCHEMA)
                connection.execute("INSERT INTO schema_meta(key, value) VALUES ('version', '1')")
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                connection.close()
            for extra in (Path(str(partial) + "-wal"), Path(str(partial) + "-shm")):
                if extra.exists():
                    extra.unlink()
            os.replace(partial, self.path)
            published = True
        finally:
            if not published:
                for candidate in (partial, Path(str(partial) + "-wal"), Path(str(partial) + "-shm")):
                    if candidate.exists():
                        candidate.unlink()

    def owner_name(self) -> str:
        return f"{socket.gethostname()}:{os.getpid()}"

    def acquire_instance_lock(self, connection: sqlite3.Connection, owner: str, ttl: timedelta, now: datetime) -> None:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute("SELECT owner, heartbeat_at FROM instance_lock WHERE id = 1").fetchone()
        if row is not None and row["owner"] != owner:
            heartbeat = parse_utc(row["heartbeat_at"])
            if now - heartbeat < ttl:
                connection.rollback()
                raise LockBusy(f"otra instancia está activa ({row['owner']})")
        connection.execute(
            """
            INSERT INTO instance_lock(id, owner, heartbeat_at) VALUES (1, ?, ?)
            ON CONFLICT(id) DO UPDATE SET owner = excluded.owner, heartbeat_at = excluded.heartbeat_at
            """,
            (owner, format_utc(now)),
        )
        connection.commit()

    def heartbeat_lock(self, connection: sqlite3.Connection, owner: str, now: datetime) -> bool:
        connection.execute("BEGIN IMMEDIATE")
        cursor = connection.execute(
            "UPDATE instance_lock SET heartbeat_at = ? WHERE id = 1 AND owner = ?",
            (format_utc(now), owner),
        )
        connection.commit()
        return cursor.rowcount == 1

    def release_instance_lock(self, connection: sqlite3.Connection, owner: str) -> None:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("DELETE FROM instance_lock WHERE id = 1 AND owner = ?", (owner,))
        connection.commit()

    def insert_event_and_deliveries(
        self,
        *,
        event_id: str,
        identity_version: int,
        normalization_version: int,
        device_uid: str,
        occurred_at_utc: datetime,
        received_at: datetime,
        normalized: dict[str, object],
        raw: dict[str, object],
        source: str,
        destinations: list[tuple[str, str]],
    ) -> str:
        """Persiste el evento y sus entregas en una sola transacción.

        destinations es una lista de (destination_id, url_en_el_momento).
        """
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
            if existing is not None:
                stored = json.loads(existing["normalized_json"])
                conflict = soft_conflict(stored, normalized)
                if conflict is not None:
                    self._incident(
                        connection,
                        "canonical_conflict",
                        device_uid=device_uid,
                        event_id=event_id,
                        details={"field": conflict, "source": source},
                        now=received_at,
                    )
                    connection.commit()
                    return INSERT_CONFLICT
                self._keep_evidence(connection, int(existing["id"]), source, raw, received_at)
                completed = _blank_completions(stored, normalized)
                if completed:
                    stored.update(completed)
                    connection.execute(
                        "UPDATE events SET normalized_json = ? WHERE id = ?",
                        (json.dumps(stored, ensure_ascii=False, sort_keys=True), existing["id"]),
                    )
                    connection.execute(
                        """
                        UPDATE deliveries
                        SET state = 'pending',
                            next_attempt_at = ?,
                            lease_owner = NULL,
                            lease_until = NULL
                        WHERE event_row_id = ? AND state IN ('pending', 'in_flight', 'delivered')
                        """,
                        (format_utc(received_at), existing["id"]),
                    )
                connection.commit()
                return INSERT_DUPLICATE
            cursor = connection.execute(
                """
                INSERT INTO events (
                    event_id, identity_version, normalization_version, device_uid,
                    occurred_at, received_at, normalized_json, raw_json, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    identity_version,
                    normalization_version,
                    device_uid,
                    format_utc(occurred_at_utc),
                    format_utc(received_at),
                    json.dumps(normalized, ensure_ascii=False, sort_keys=True),
                    json.dumps(raw, ensure_ascii=False, sort_keys=True),
                    source,
                ),
            )
            event_row_id = int(cursor.lastrowid)
            self._keep_evidence(connection, event_row_id, source, raw, received_at)
            created = format_utc(received_at)
            for destination_id, url in destinations:
                connection.execute(
                    """
                    INSERT INTO deliveries (
                        event_row_id, destination_id, destination_url, state,
                        attempts, next_attempt_at, created_at
                    ) VALUES (?, ?, ?, 'pending', 0, ?, ?)
                    """,
                    (event_row_id, destination_id, url, created, created),
                )
            connection.commit()
            return INSERT_STORED

    def claim_one(self, worker: str, lease: timedelta, now: datetime) -> ClaimedDelivery | None:
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            stamp = format_utc(now)
            row = connection.execute(
                """
                SELECT id FROM deliveries
                WHERE state IN ('pending', 'auth_blocked')
                  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                ORDER BY COALESCE(next_attempt_at, created_at), id
                LIMIT 1
                """,
                (stamp,),
            ).fetchone()
            if row is None:
                row = connection.execute(
                    """
                    SELECT id FROM deliveries
                    WHERE state = 'in_flight' AND lease_until IS NOT NULL AND lease_until <= ?
                    ORDER BY lease_until, id
                    LIMIT 1
                    """,
                    (stamp,),
                ).fetchone()
            if row is None:
                connection.commit()
                return None
            lease_until = format_utc(now + lease)
            connection.execute(
                """
                UPDATE deliveries
                SET state = 'in_flight', lease_owner = ?, lease_until = ?
                WHERE id = ?
                """,
                (worker, lease_until, row["id"]),
            )
            claimed = self._load_claim(connection, int(row["id"]))
            connection.commit()
            return claimed

    def finish_delivery(
        self,
        delivery_id: int,
        *,
        outcome: str,
        now: datetime,
        error: str | None,
        http_status: int | None,
        next_attempt_at: datetime | None,
    ) -> bool:
        if outcome not in DELIVERY_STATES or outcome == "in_flight":
            raise ValueError(f"resultado de entrega inválido: {outcome}")
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            delivered_at = format_utc(now) if outcome == "delivered" else None
            cursor = connection.execute(
                """
                UPDATE deliveries
                SET state = ?,
                    attempts = attempts + 1,
                    next_attempt_at = ?,
                    last_error = ?,
                    last_http_status = ?,
                    delivered_at = COALESCE(?, delivered_at),
                    lease_owner = NULL,
                    lease_until = NULL
                WHERE id = ? AND state = 'in_flight'
                """,
                (
                    outcome,
                    format_utc(next_attempt_at) if next_attempt_at else None,
                    _clip(error),
                    http_status,
                    delivered_at,
                    delivery_id,
                ),
            )
            connection.commit()
            return cursor.rowcount == 1

    def release_all_inflight(self, now: datetime) -> int:
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE deliveries
                SET state = 'pending', lease_owner = NULL, lease_until = NULL, next_attempt_at = ?
                WHERE state = 'in_flight'
                """,
                (format_utc(now),),
            )
            connection.commit()
            return int(cursor.rowcount)

    def hold_destination(self, destination_id: str, reason: str) -> int:
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE deliveries
                SET state = 'held', last_error = ?, lease_owner = NULL, lease_until = NULL
                WHERE destination_id = ? AND state IN ('pending', 'in_flight', 'auth_blocked')
                """,
                (_clip(reason), destination_id),
            )
            connection.commit()
            return int(cursor.rowcount)

    def rewrite_snapshot(self, destination_id: str, url: str) -> int:
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE deliveries
                SET destination_url = ?
                WHERE destination_id = ? AND state != 'delivered'
                """,
                (url, destination_id),
            )
            connection.commit()
            return int(cursor.rowcount)

    def open_delivery_routes(self) -> list[sqlite3.Row]:
        with self.session() as connection:
            return list(
                connection.execute(
                    """
                    SELECT destination_id, destination_url, state, COUNT(*) AS total
                    FROM deliveries
                    WHERE state != 'delivered'
                    GROUP BY destination_id, destination_url, state
                    """
                )
            )

    def retry_rejected(self, *, delivery_id: int | None, event_id: str | None, destination_id: str | None, include_conflict: bool) -> int:
        states = ["rejected"]
        if include_conflict:
            states.append("conflict")
        clauses = [f"deliveries.state IN ({', '.join('?' for _ in states)})"]
        params: list[object] = list(states)
        if delivery_id is not None:
            clauses.append("deliveries.id = ?")
            params.append(delivery_id)
        if event_id is not None:
            clauses.append("events.event_id = ?")
            params.append(event_id)
        if destination_id is not None:
            clauses.append("deliveries.destination_id = ?")
            params.append(destination_id)
        if delivery_id is None and event_id is None:
            raise ValueError("hay que indicar delivery_id o event_id")
        sql = f"""
            UPDATE deliveries
            SET state = 'pending', next_attempt_at = NULL, lease_owner = NULL, lease_until = NULL
            WHERE id IN (
                SELECT deliveries.id
                FROM deliveries
                JOIN events ON events.id = deliveries.event_row_id
                WHERE {' AND '.join(clauses)}
            )
        """
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(sql, params)
            connection.commit()
            return int(cursor.rowcount)

    def add_incident(
        self,
        incident_type: str,
        *,
        device_uid: str | None = None,
        event_id: str | None = None,
        details: dict[str, object] | None = None,
        now: datetime | None = None,
    ) -> None:
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._incident(
                connection,
                incident_type,
                device_uid=device_uid,
                event_id=event_id,
                details=details or {},
                now=now or utc_now(),
            )
            connection.commit()

    def increment(self, name: str, amount: int = 1) -> None:
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO counters(name, value) VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET value = value + excluded.value
                """,
                (name, amount),
            )
            connection.commit()

    def touch_live(self, device_uid: str | None, now: datetime) -> None:
        if not device_uid:
            return
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO sync_state(device_uid, last_live_at, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(device_uid) DO UPDATE SET
                    last_live_at = excluded.last_live_at,
                    updated_at = excluded.updated_at
                """,
                (device_uid, format_utc(now), format_utc(now)),
            )
            connection.commit()

    def get_sync(self, device_uid: str) -> SyncSnapshot | None:
        with self.session() as connection:
            row = connection.execute("SELECT * FROM sync_state WHERE device_uid = ?", (device_uid,)).fetchone()
        if row is None:
            return None
        return SyncSnapshot(
            device_uid=row["device_uid"],
            checkpoint_until=row["checkpoint_until"],
            last_live_at=row["last_live_at"],
            last_success_at=row["last_success_at"],
            last_error=row["last_error"],
        )

    def set_checkpoint(self, device_uid: str, checkpoint: datetime, now: datetime) -> None:
        stamp = format_utc(checkpoint)
        current = format_utc(now)
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO sync_state(device_uid, checkpoint_until, last_success_at, last_error, updated_at)
                VALUES (?, ?, ?, NULL, ?)
                ON CONFLICT(device_uid) DO UPDATE SET
                    checkpoint_until = CASE
                        WHEN sync_state.checkpoint_until IS NULL OR sync_state.checkpoint_until < excluded.checkpoint_until
                        THEN excluded.checkpoint_until
                        ELSE sync_state.checkpoint_until
                    END,
                    last_success_at = excluded.last_success_at,
                    last_error = NULL,
                    updated_at = excluded.updated_at
                """,
                (device_uid, stamp, current, current),
            )
            connection.commit()

    def set_backfill_error(self, device_uid: str, message: str, now: datetime) -> None:
        current = format_utc(now)
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT INTO sync_state(device_uid, last_attempt_at, last_error, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(device_uid) DO UPDATE SET
                    last_attempt_at = excluded.last_attempt_at,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (device_uid, current, _clip(message), current),
            )
            connection.commit()

    def try_acquire_backfill(self, device_uid: str, owner: str, ttl: timedelta, now: datetime) -> bool:
        until = format_utc(now + ttl)
        current = format_utc(now)
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT backfill_owner, backfill_until FROM sync_state WHERE device_uid = ?",
                (device_uid,),
            ).fetchone()
            if row is not None and row["backfill_owner"] and row["backfill_owner"] != owner and row["backfill_until"]:
                if parse_utc(row["backfill_until"]) > now:
                    connection.commit()
                    return False
            connection.execute(
                """
                INSERT INTO sync_state(device_uid, backfill_owner, backfill_until, last_attempt_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(device_uid) DO UPDATE SET
                    backfill_owner = excluded.backfill_owner,
                    backfill_until = excluded.backfill_until,
                    last_attempt_at = excluded.last_attempt_at,
                    updated_at = excluded.updated_at
                """,
                (device_uid, owner, until, current, current),
            )
            connection.commit()
            return True

    def release_backfill(self, device_uid: str, owner: str) -> None:
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE sync_state
                SET backfill_owner = NULL, backfill_until = NULL, updated_at = ?
                WHERE device_uid = ? AND backfill_owner = ?
                """,
                (format_utc(utc_now()), device_uid, owner),
            )
            connection.commit()

    def purge_delivered(self, retention: timedelta, now: datetime) -> int:
        cutoff = format_utc(now - retention)
        with self.session() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT events.id
                FROM events
                WHERE NOT EXISTS (
                    SELECT 1 FROM deliveries
                    WHERE deliveries.event_row_id = events.id
                      AND deliveries.state != 'delivered'
                )
                AND EXISTS (
                    SELECT 1 FROM deliveries WHERE deliveries.event_row_id = events.id
                )
                AND (
                    SELECT MAX(delivered_at) FROM deliveries WHERE deliveries.event_row_id = events.id
                ) < ?
                """,
                (cutoff,),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if ids:
                connection.executemany("DELETE FROM events WHERE id = ?", [(item,) for item in ids])
            connection.commit()
            return len(ids)

    def status(self) -> dict[str, object]:
        connection = self.connect_readonly()
        try:
            counts = {
                row["state"]: row["total"]
                for row in connection.execute("SELECT state, COUNT(*) AS total FROM deliveries GROUP BY state")
            }
            oldest = connection.execute(
                """
                SELECT MIN(deliveries.created_at) AS oldest
                FROM deliveries
                WHERE state IN ('pending', 'auth_blocked', 'rejected', 'conflict', 'held', 'in_flight')
                """
            ).fetchone()
            devices = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT device_uid, checkpoint_until, last_live_at, last_success_at, last_error
                    FROM sync_state
                    ORDER BY device_uid
                    """
                )
            ]
            counters = {row["name"]: row["value"] for row in connection.execute("SELECT name, value FROM counters ORDER BY name")}
            incidents = connection.execute("SELECT COUNT(*) AS total FROM incidents").fetchone()["total"]
        finally:
            connection.close()
        return {
            "deliveries": counts,
            "oldest_open": oldest["oldest"] if oldest else None,
            "devices": devices,
            "counters": counters,
            "incidents": incidents,
        }

    def _prepare(self, connection: sqlite3.Connection) -> None:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=" + str(int(self.busy_timeout_ms)))
        journal = connection.execute("PRAGMA journal_mode=WAL").fetchone()
        if journal and str(journal[0]).lower() != "wal":
            connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")

    def _load_claim(self, connection: sqlite3.Connection, delivery_id: int) -> ClaimedDelivery:
        row = connection.execute(
            """
            SELECT deliveries.*, events.event_id, events.normalized_json
            FROM deliveries
            JOIN events ON events.id = deliveries.event_row_id
            WHERE deliveries.id = ?
            """,
            (delivery_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("la entrega reclamada desapareció")
        return ClaimedDelivery(
            delivery_id=row["id"],
            event_row_id=row["event_row_id"],
            event_id=row["event_id"],
            destination_id=row["destination_id"],
            destination_url=row["destination_url"],
            payload=json.loads(row["normalized_json"]),
            attempts=row["attempts"],
        )

    def _keep_evidence(
        self,
        connection: sqlite3.Connection,
        event_row_id: int,
        source: str,
        raw: dict[str, object],
        now: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO event_evidence(event_row_id, source, raw_json, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(event_row_id, source) DO NOTHING
            """,
            (
                event_row_id,
                source,
                json.dumps(raw, ensure_ascii=False, sort_keys=True),
                format_utc(now),
            ),
        )

    def _incident(
        self,
        connection: sqlite3.Connection,
        incident_type: str,
        *,
        device_uid: str | None,
        event_id: str | None,
        details: dict[str, object],
        now: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO incidents(incident_type, created_at, device_uid, event_id, details_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                incident_type,
                format_utc(now),
                device_uid,
                event_id,
                json.dumps(_sanitize(details), ensure_ascii=False, sort_keys=True),
            ),
        )


def _blank_completions(stored: dict[str, object], incoming: dict[str, object]) -> dict[str, object]:
    """Completa solo campos vacíos. Un valor ya guardado no se sustituye."""
    completed: dict[str, object] = {}
    for field in SOFT_FIELDS:
        if _present(stored.get(field)) or not _present(incoming.get(field)):
            continue
        completed[field] = incoming.get(field)
    return completed


def _present(value: object) -> bool:
    return value is not None and value != ""


def _clip(value: str | None, limit: int = 300) -> str | None:
    if value is None:
        return None
    text = " ".join(value.split())
    return text[:limit]


def _sanitize(details: dict[str, object]) -> dict[str, object]:
    blocked = {"password", "token", "authorization", "card_no", "employee_no", "name", "label", "raw"}
    clean: dict[str, object] = {}
    for key, value in details.items():
        if key.casefold() in blocked:
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            clean[key] = value
    return clean

