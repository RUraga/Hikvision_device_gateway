"""Lectura incremental de multipart/mixed. No carga el stream completo."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from email.message import EmailMessage


class MultipartError(Exception):
    """El framing del stream no se puede seguir. Conviene reconectar."""


@dataclass
class Part:
    header_map: dict[str, str]
    body: bytes
    too_large: bool = False

    @property
    def content_type(self) -> str:
        return self.header_map.get("content-type", "")


def boundary_from_content_type(content_type: str) -> bytes:
    message = EmailMessage()
    message["Content-Type"] = content_type
    value = message.get_param("boundary", header="Content-Type")
    if not isinstance(value, str) or value == "":
        raise MultipartError("la respuesta no incluye boundary multipart")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise MultipartError("el boundary no es ASCII") from exc
    if len(encoded) > 200:
        raise MultipartError("el boundary excede el límite")
    return encoded


class MultipartParser:
    """Parser de un solo uso. ``feed`` puede llamarse con fragmentos arbitrarios."""

    def __init__(self, boundary: bytes, *, max_header_bytes: int, max_part_bytes: int) -> None:
        self._boundary = boundary
        self._marker = b"--" + boundary
        self._max_header_bytes = max_header_bytes
        self._max_part_bytes = max_part_bytes
        self._buffer = bytearray()
        self._phase = "boundary"
        self._headers: dict[str, str] = {}
        self._fixed_remaining = 0
        self._fixed_body = bytearray()
        self._fixed_too_large = False
        self._scan_body = bytearray()
        self._scan_too_large = False
        self._done = False

    @property
    def done(self) -> bool:
        return self._done

    def feed(self, chunk: bytes) -> list[Part]:
        if self._done or not chunk:
            return []
        self._buffer.extend(chunk)
        return self._pump()

    def finish(self) -> list[Part]:
        parts = self._pump()
        if self._done:
            return parts
        if self._phase == "boundary" and not self._buffer.strip(b"\r\n- \t"):
            self._done = True
            return parts
        raise MultipartError("el stream multipart terminó dentro de una parte")

    def _pump(self) -> list[Part]:
        parts: list[Part] = []
        while not self._done:
            if self._phase == "boundary":
                if not self._consume_boundary():
                    break
            elif self._phase == "headers":
                if not self._consume_headers():
                    break
            elif self._phase == "fixed":
                part = self._consume_fixed()
                if part is None:
                    break
                parts.append(part)
            elif self._phase == "scan":
                part = self._consume_scan()
                if part is None:
                    break
                parts.append(part)
            else:
                break
        return parts

    def _consume_boundary(self) -> bool:
        self._drop_one_linebreak()
        if self._buffer.startswith(self._marker + b"--"):
            self._done = True
            self._buffer.clear()
            return False
        if self._buffer.startswith(self._marker):
            line_end = _line_end(self._buffer, len(self._marker))
            if line_end is None:
                if len(self._buffer) > self._max_header_bytes:
                    raise MultipartError("línea de boundary demasiado larga")
                return False
            del self._buffer[:line_end]
            self._phase = "headers"
            return True
        index = self._buffer.find(self._marker)
        if index == -1:
            keep = len(self._marker) + 4
            if len(self._buffer) > max(keep, self._max_part_bytes):
                raise MultipartError("no se encontró el boundary")
            if len(self._buffer) > keep:
                del self._buffer[:-keep]
            return False
        if index > self._max_part_bytes:
            raise MultipartError("preámbulo multipart demasiado grande")
        del self._buffer[:index]
        return True

    def _consume_headers(self) -> bool:
        match = _header_end(self._buffer)
        if match is None:
            if len(self._buffer) > self._max_header_bytes:
                raise MultipartError("cabeceras multipart demasiado grandes")
            return False
        index, separator_length = match
        if index > self._max_header_bytes:
            raise MultipartError("cabeceras multipart demasiado grandes")
        raw_headers = bytes(self._buffer[:index])
        del self._buffer[: index + separator_length]
        self._headers = _parse_headers(raw_headers)
        length = _content_length(self._headers)
        if length is None:
            self._scan_body = bytearray()
            self._scan_too_large = False
            self._phase = "scan"
        else:
            self._fixed_remaining = length
            self._fixed_body = bytearray()
            self._fixed_too_large = length > self._max_part_bytes
            self._phase = "fixed"
        return True

    def _consume_fixed(self) -> Part | None:
        if self._fixed_remaining > 0:
            take = min(self._fixed_remaining, len(self._buffer))
            if take == 0:
                return None
            chunk = self._buffer[:take]
            del self._buffer[:take]
            self._fixed_remaining -= take
            if not self._fixed_too_large:
                self._fixed_body.extend(chunk)
        if self._fixed_remaining > 0:
            return None
        part = Part(header_map=self._headers, body=bytes(self._fixed_body), too_large=self._fixed_too_large)
        self._headers = {}
        self._fixed_body = bytearray()
        self._phase = "boundary"
        return part

    def _consume_scan(self) -> Part | None:
        if self._buffer.startswith(self._marker):
            part = Part(header_map=self._headers, body=bytes(self._scan_body), too_large=self._scan_too_large)
            self._headers = {}
            self._scan_body = bytearray()
            self._phase = "boundary"
            return part
        token = b"\r\n" + self._marker
        index = self._buffer.find(token)
        newline_token = b"\n" + self._marker
        index_nl = self._buffer.find(newline_token)
        if index == -1 or (index_nl != -1 and index_nl < index):
            if index_nl != -1:
                index = index_nl
                token = newline_token
        if index == -1:
            keep = len(self._marker) + 4
            if len(self._buffer) > keep:
                emit = self._buffer[:-keep]
                del self._buffer[:-keep]
                self._append_scan(emit)
            return None
        self._append_scan(self._buffer[:index])
        del self._buffer[: index + (2 if token.startswith(b"\r\n") else 1)]
        part = Part(header_map=self._headers, body=bytes(self._scan_body), too_large=self._scan_too_large)
        self._headers = {}
        self._scan_body = bytearray()
        self._phase = "boundary"
        return part

    def _append_scan(self, data: bytes | bytearray) -> None:
        if self._scan_too_large or not data:
            return
        room = self._max_part_bytes - len(self._scan_body)
        if len(data) > room:
            self._scan_too_large = True
            self._scan_body.clear()
            return
        self._scan_body.extend(data)

    def _drop_one_linebreak(self) -> None:
        if self._buffer.startswith(b"\r\n"):
            del self._buffer[:2]
        elif self._buffer.startswith(b"\n"):
            del self._buffer[:1]


def iter_parts(chunks: Iterator[bytes], content_type: str, *, max_header_bytes: int, max_part_bytes: int) -> Iterator[Part]:
    parser = MultipartParser(
        boundary_from_content_type(content_type),
        max_header_bytes=max_header_bytes,
        max_part_bytes=max_part_bytes,
    )
    for chunk in chunks:
        yield from parser.feed(chunk)
    yield from parser.finish()


def _header_end(buffer: bytearray) -> tuple[int, int] | None:
    windows = []
    crlf = buffer.find(b"\r\n\r\n")
    lf = buffer.find(b"\n\n")
    if crlf != -1:
        windows.append((crlf, 4))
    if lf != -1:
        windows.append((lf, 2))
    if not windows:
        return None
    return min(windows, key=lambda item: item[0])


def _line_end(buffer: bytearray, start: int) -> int | None:
    crlf = buffer.find(b"\r\n", start)
    lf = buffer.find(b"\n", start)
    if crlf == -1 and lf == -1:
        return None
    if crlf == -1:
        return lf + 1
    if lf == -1 or crlf <= lf:
        return crlf + 2
    return lf + 1


def _parse_headers(raw: bytes) -> dict[str, str]:
    headers: dict[str, str] = {}
    text = raw.decode("utf-8", "replace")
    for line in text.splitlines():
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        headers[name.strip().lower()] = value.strip()
    return headers


def _content_length(headers: dict[str, str]) -> int | None:
    raw = headers.get("content-length")
    if raw is None:
        return None
    try:
        length = int(raw)
    except ValueError as exc:
        raise MultipartError("Content-Length multipart inválido") from exc
    if length < 0:
        raise MultipartError("Content-Length multipart negativo")
    return length
