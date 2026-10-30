from __future__ import annotations

from hikvision_event_bridge.multipart import MultipartError, MultipartParser, boundary_from_content_type

from support import multipart_message


def _parse(blob: bytes, *, max_header: int = 4096, max_part: int = 1024, chunk: int = 3) -> list:
    parser = MultipartParser(b"boundary", max_header_bytes=max_header, max_part_bytes=max_part)
    parts = []
    for offset in range(0, len(blob), chunk):
        parts.extend(parser.feed(blob[offset : offset + chunk]))
    parts.extend(parser.finish())
    return parts


def test_partes_con_y_sin_content_length_y_binaria() -> None:
    json_body = b'{"EventNotificationAlert":{"eventType":"heartBeat"}}'
    binary = b"\xff\xd8\xff\x00jpeg"
    blob = multipart_message(
        [
            ({"Content-Type": "application/json", "Content-Length": str(len(json_body))}, json_body),
            ({"Content-Type": "application/json"}, b'{"n":1}'),
            ({"Content-Type": "image/jpeg"}, binary),
        ]
    )
    parts = _parse(blob, chunk=1)
    assert parts[0].body == json_body
    assert parts[1].body == b'{"n":1}'
    assert parts[2].body == binary
    assert "jpeg" in parts[2].content_type


def test_parte_demasiado_grande_no_impide_la_siguiente() -> None:
    blob = multipart_message(
        [
            ({"Content-Type": "application/json"}, b"12345678901234567890"),
            ({"Content-Type": "application/json"}, b'{"ok":1}'),
        ]
    )
    parts = _parse(blob, max_part=12, chunk=2)
    assert parts[0].too_large is True
    assert parts[0].body == b""
    assert parts[1].too_large is False
    assert parts[1].body == b'{"ok":1}'


def test_cabecera_demasiado_grande() -> None:
    blob = multipart_message([({"Content-Type": "application/json", "X-Big": "a" * 300}, b"{}")])
    parser = MultipartParser(boundary_from_content_type("multipart/mixed; boundary=boundary"), max_header_bytes=80, max_part_bytes=1000)
    try:
        parser.feed(blob)
        parser.finish()
    except MultipartError as exc:
        assert "cabeceras" in str(exc)
    else:
        raise AssertionError("se esperaba un límite de cabeceras")


def test_cierre_del_stream_dentro_de_una_parte() -> None:
    parser = MultipartParser(b"boundary", max_header_bytes=1000, max_part_bytes=1000)
    parser.feed(b"--boundary\r\nContent-Type: application/json\r\n\r\n{")
    try:
        parser.finish()
    except MultipartError as exc:
        assert "terminó" in str(exc)
    else:
        raise AssertionError("el stream incompleto debe fallar")
