"""Servidores HTTP locales para la suite. No hablan con equipos reales."""

from __future__ import annotations

import hashlib
import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

AUTH_RE = re.compile(r'(\w+)=(".*?"|[^,]+)')


def digest_matches(header: str, method: str, username: str, password: str, realm: str, nonce: str) -> bool:
    if not header or not header.startswith("Digest "):
        return False
    fields = {key: value.strip().strip('"') for key, value in AUTH_RE.findall(header[len("Digest ") :])}
    if fields.get("username") != username or "response" not in fields or "uri" not in fields:
        return False
    ha1 = hashlib.md5(f"{username}:{realm}:{password}".encode()).hexdigest()
    ha2 = hashlib.md5(f"{method}:{fields['uri']}".encode()).hexdigest()
    if fields.get("qop"):
        expected = hashlib.md5(
            f"{ha1}:{nonce}:{fields.get('nc', '')}:{fields.get('cnonce', '')}:{fields['qop']}:{ha2}".encode()
        ).hexdigest()
    else:
        expected = hashlib.md5(f"{ha1}:{nonce}:{ha2}".encode()).hexdigest()
    return expected == fields["response"]


class GatewayHarness:
    def __init__(self) -> None:
        self.username = "admin"
        self.password = "gateway-secret"
        self.realm = "gateway-test"
        self.nonce = "nonce-fijo"
        self.authorized: list[tuple[str, str]] = []
        self.unsubscribed: list[str] = []
        self.subscribe_body = b""
        self.history_pages: list[dict] = []
        self.fail_auth = False
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        assert self._httpd is not None
        host, port = self._httpd.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> str:
        harness = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt: str, *args) -> None:
                return

            def _read_body(self) -> bytes:
                length = int(self.headers.get("Content-Length", "0") or 0)
                return self.rfile.read(length) if length else b""

            def _unauthorized(self) -> None:
                self.send_response(401)
                self.send_header(
                    "WWW-Authenticate",
                    f'Digest realm="{harness.realm}", nonce="{harness.nonce}", qop="auth", algorithm=MD5',
                )
                self.send_header("Content-Length", "0")
                self.end_headers()

            def _authed(self, method: str) -> bool:
                if harness.fail_auth:
                    self._unauthorized()
                    return False
                header = self.headers.get("Authorization", "")
                if not digest_matches(header, method, harness.username, harness.password, harness.realm, harness.nonce):
                    self._unauthorized()
                    return False
                harness.authorized.append((method, self.path))
                return True

            def handle_error(self, request, client_address) -> None:
                return

            def _send(self, status: int, body: bytes, content_type: str) -> None:
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    if body:
                        self.wfile.write(body)
                except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError, ValueError):
                    return

            def do_GET(self) -> None:
                if not self._authed("GET"):
                    return
                if self.path.startswith("/ISAPI/System/deviceInfo"):
                    self._send(200, b'{"DeviceInfo":{"deviceName":"lab"}}', "application/json")
                    return
                self._send(404, b"{}", "application/json")

            def do_DELETE(self) -> None:
                if not self._authed("DELETE"):
                    return
                harness.unsubscribed.append(self.path)
                self._send(200, b'{"statusCode":1,"statusString":"OK","subStatusCode":"ok"}', "application/json")

            def do_POST(self) -> None:
                self._read_body()
                if not self._authed("POST"):
                    return
                if "AcsEvent" in self.path:
                    page = harness.history_pages.pop(0) if harness.history_pages else {
                        "AcsEvent": {"responseStatusStrg": "NO MATCH", "numOfMatches": 0, "totalMatches": 0}
                    }
                    body = json.dumps(page).encode()
                    self._send(200, body, "application/json")
                    return
                if "subscribeDeviceMgmt" in self.path:
                    self._send(200, harness.subscribe_body, 'multipart/mixed; boundary=boundary')
                    return
                self._send(404, b"{}", "application/json")

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._httpd.daemon_threads = True
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="gateway-harness")
        self._thread.start()
        return self.url

    def stop(self) -> None:
        httpd = self._httpd
        thread = self._thread
        self._httpd = None
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()
        if thread is not None:
            thread.join(3)
        self._thread = None


class DestinationHarness:
    def __init__(self, script: list[dict]) -> None:
        self.script = list(script)
        self.seen: list[dict] = []
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        assert self._httpd is not None
        host, port = self._httpd.server_address[:2]
        return f"http://{host}:{port}"

    def start(self) -> str:
        harness = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, fmt: str, *args) -> None:
                return

            def handle_error(self, request, client_address) -> None:
                return

            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0") or 0)
                raw = self.rfile.read(length) if length else b""
                step = harness.script.pop(0) if harness.script else {"status": 500, "body": b"{}"}
                harness.seen.append(
                    {
                        "path": self.path,
                        "authorization": self.headers.get("Authorization", ""),
                        "idempotency": self.headers.get("Idempotency-Key", ""),
                        "body": raw,
                        "status": step["status"],
                    }
                )
                if step.get("delay"):
                    threading.Event().wait(step["delay"])
                body = step.get("body", b"")
                if isinstance(body, dict):
                    body = json.dumps(body).encode()
                try:
                    self.send_response(step["status"])
                    for key, value in step.get("headers", {}).items():
                        self.send_header(key, value)
                    if step["status"] in {301, 302, 303, 307, 308}:
                        self.send_header("Location", step.get("location", "/otro-origen"))
                    self.send_header("Content-Type", step.get("content_type", "application/json"))
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    if body:
                        self.wfile.write(body)
                except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError, ValueError):
                    return

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._httpd.daemon_threads = True
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="destination-harness")
        self._thread.start()
        return self.url

    def stop(self) -> None:
        httpd = self._httpd
        thread = self._thread
        self._httpd = None
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()
        if thread is not None:
            thread.join(3)
        self._thread = None


def write_config(path: Path, gateway_url: str, destination_url: str, **overrides: object) -> None:
    backfill_enabled = overrides.get("backfill_enabled", False)
    if isinstance(backfill_enabled, bool):
        backfill_enabled = "true" if backfill_enabled else "false"
    auth_hold = overrides.get("auth_hold", 120)
    initial = overrides.get("initial_backoff", 0.01)
    maximum = overrides.get("max_backoff", 0.05)
    jitter = overrides.get("jitter", 0)
    concurrency = overrides.get("concurrency", 2)
    post_timeout = overrides.get("post_timeout", 2)
    stream_timeout = overrides.get("stream_timeout", 2)
    sqlite_name = overrides.get("sqlite_name", "events.sqlite")
    extra_destinations = overrides.get("extra_destinations", "")
    destination_names = overrides.get("destination_names", '["rancherito"]')
    pending_policy = overrides.get("pending_policy", "")
    orphans = overrides.get("orphans", "")
    text = f"""
[gateway]
url = "{gateway_url}"
username = "admin"
password_env = "HIKVISION_GATEWAY_PASSWORD"
verify_tls = true

[timeouts]
connect_seconds = 2
stream_seconds = {stream_timeout}
post_seconds = {post_timeout}

[storage]
sqlite_path = "{sqlite_name}"
busy_timeout_ms = 2000
lock_ttl_seconds = 30

[retention]
delivered_days = 30

[delivery]
concurrency = {concurrency}
initial_backoff_seconds = {initial}
max_backoff_seconds = {maximum}
auth_hold_seconds = {auth_hold}
retry_after_cap_seconds = 30
jitter_ratio = {jitter}
lease_seconds = 5

[backfill]
enabled = {backfill_enabled}
interval_seconds = 30
overlap_seconds = 5
window_seconds = 3600
page_size = 2
min_window_seconds = 60
initial_start = "2026-10-01T00:00:00-06:00"

[limits]
max_header_bytes = 4096
max_json_part_bytes = 65536
max_binary_part_bytes = 65536

[privacy]
strip_biometric_fields = true

[devices.rancherito]
device_uid = "9CE552AA-7ACD-7145-B340-A8618DA1B946"
enabled = true
timezone = "America/Mexico_City"
destinations = {destination_names}

[devices.rancherito.filter]
major_event_types = []
sub_event_types = []
include_authentication = ["card", "face"]
authentication_sub_event_types = []
exclude_door_events = false
door_sub_event_types = []
success_sub_event_types = []
failure_sub_event_types = []

[destinations.rancherito]
url = "{destination_url}"
token_env = "RANCHERITO_EVENTS_TOKEN"
pending_url_policy = "{pending_policy}"
{extra_destinations}
{orphans}
"""
    path.write_text(text.strip() + "\n", encoding="utf-8")


def multipart_message(parts: list[tuple[dict[str, str], bytes]], boundary: str = "boundary") -> bytes:
    marker = boundary.encode()
    chunks = []
    for headers, body in parts:
        chunks.append(b"--" + marker + b"\r\n")
        for key, value in headers.items():
            chunks.append(f"{key}: {value}\r\n".encode())
        chunks.append(b"\r\n")
        chunks.append(body)
        chunks.append(b"\r\n")
    chunks.append(b"--" + marker + b"--\r\n")
    return b"".join(chunks)
