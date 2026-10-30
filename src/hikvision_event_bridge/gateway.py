"""Cliente HTTP del Device Gateway. Digest, stream y consulta histórica.

Contratos tomados de la guía UD29329B V1.6.1. No están probados contra el
gateway de Rancherito: la suite usa un servidor simulado.
"""

from __future__ import annotations

from collections.abc import Iterator
from urllib.parse import quote
from dataclasses import dataclass
from typing import Any

import requests
from requests.auth import HTTPDigestAuth

from hikvision_event_bridge.config import AppConfig
from hikvision_event_bridge.multipart import Part, iter_parts

SUBSCRIBE_PATH = "/ISAPI/Event/notification/subscribeDeviceMgmt?format=json"
UNSUBSCRIBE_PATH = "/ISAPI/Event/notification/unSubscribeDeviceMgmt/{subscription_id}?format=json"
ACS_EVENT_PATH = "/ISAPI/AccessControl/AcsEvent?format=json&devIndex={device_uid}"
DEVICE_INFO_PATH = "/ISAPI/System/deviceInfo?format=json"

SUBSCRIBE_BODY = {"SubscribeDeviceMgmt": {"eventMode": "all", "defenceMode": "all"}}


class GatewayError(Exception):
    def __init__(self, message: str, *, status: int | None = None, auth_failed: bool = False) -> None:
        super().__init__(message)
        self.status = status
        self.auth_failed = auth_failed


@dataclass
class DeviceInfoResult:
    status: int
    content_type: str


class GatewayClient:
    def __init__(self, config: AppConfig, session: requests.Session | None = None) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.session.auth = HTTPDigestAuth(config.gateway.username, config.gateway.password)
        self.session.verify = config.gateway.verify_tls
        self.session.trust_env = False
        self._response: requests.Response | None = None

    def close_stream(self) -> None:
        response = self._response
        self._response = None
        if response is not None:
            response.close()

    def close(self) -> None:
        self.close_stream()
        self.session.close()

    def subscribe(self) -> tuple[str, Iterator[Part]]:
        """Abre el stream. El primer valor útil puede ser el id de suscripción.

        eventMode "all" suscribe todos los dispositivos del gateway. La guía
        dice que en ese modo no se puede limitar a un solo dispositivo.
        """
        url = self.config.gateway.url + SUBSCRIBE_PATH
        try:
            response = self.session.post(
                url,
                json=SUBSCRIBE_BODY,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "multipart/mixed, application/json",
                    "Accept-Encoding": "identity",
                    "Connection": "keep-alive",
                },
                stream=True,
                timeout=(self.config.timeouts.connect_seconds, self.config.timeouts.stream_seconds),
                allow_redirects=False,
            )
        except requests.Timeout as exc:
            raise GatewayError("tiempo de espera al suscribir") from exc
        except requests.RequestException as exc:
            raise GatewayError(type(exc).__name__) from exc
        self._response = response
        if response.status_code in {301, 302, 303, 307, 308}:
            response.close()
            raise GatewayError("el gateway respondió una redirección y no se siguió", status=response.status_code)
        if response.status_code in {401, 403}:
            response.close()
            raise GatewayError("autenticación del gateway rechazada", status=response.status_code, auth_failed=True)
        if response.status_code != 200:
            response.close()
            raise GatewayError("suscripción rechazada", status=response.status_code)
        content_type = response.headers.get("Content-Type", "")
        if "multipart/" not in content_type.lower():
            response.close()
            raise GatewayError("la suscripción no devolvió multipart", status=response.status_code)
        parts = iter_parts(
            response.iter_content(chunk_size=4096),
            content_type,
            max_header_bytes=self.config.limits.max_header_bytes,
            max_part_bytes=max(self.config.limits.max_json_part_bytes, self.config.limits.max_binary_part_bytes),
        )
        return content_type, parts

    def unsubscribe(self, subscription_id: str) -> None:
        url = self.config.gateway.url + UNSUBSCRIBE_PATH.format(subscription_id=quote(str(subscription_id), safe=""))
        try:
            response = self.session.delete(
                url,
                timeout=(self.config.timeouts.connect_seconds, self.config.timeouts.post_seconds),
                allow_redirects=False,
            )
            response.close()
        except requests.RequestException:
            return

    def search_acs_events(self, device_uid: str, body: dict[str, Any]) -> dict[str, Any]:
        url = self.config.gateway.url + ACS_EVENT_PATH.format(device_uid=quote(device_uid, safe=""))
        try:
            response = self.session.post(
                url,
                json=body,
                headers={"Content-Type": "application/json", "Accept": "application/json", "Accept-Encoding": "identity"},
                timeout=(self.config.timeouts.connect_seconds, self.config.timeouts.post_seconds),
                allow_redirects=False,
            )
        except requests.Timeout as exc:
            raise GatewayError("tiempo de espera en la consulta histórica") from exc
        except requests.RequestException as exc:
            raise GatewayError(type(exc).__name__) from exc
        try:
            if response.status_code in {401, 403}:
                raise GatewayError("autenticación del gateway rechazada", status=response.status_code, auth_failed=True)
            if response.status_code != 200:
                raise GatewayError("consulta histórica rechazada", status=response.status_code)
            try:
                payload = response.json()
            except ValueError as exc:
                raise GatewayError("la consulta histórica no devolvió JSON") from exc
            if not isinstance(payload, dict):
                raise GatewayError("la consulta histórica no devolvió un objeto JSON")
            return payload
        finally:
            response.close()

    def device_info(self) -> DeviceInfoResult:
        url = self.config.gateway.url + DEVICE_INFO_PATH
        try:
            response = self.session.get(
                url,
                timeout=(self.config.timeouts.connect_seconds, self.config.timeouts.post_seconds),
                allow_redirects=False,
                headers={"Accept": "application/json", "Accept-Encoding": "identity"},
            )
        except requests.Timeout as exc:
            raise GatewayError("tiempo de espera al consultar el gateway") from exc
        except requests.RequestException as exc:
            raise GatewayError(type(exc).__name__) from exc
        try:
            if response.status_code in {401, 403}:
                raise GatewayError("autenticación del gateway rechazada", status=response.status_code, auth_failed=True)
            return DeviceInfoResult(status=response.status_code, content_type=response.headers.get("Content-Type", ""))
        finally:
            response.close()


def subscription_id_from_payload(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    body = payload.get("SubscribeDeviceMgmtRsp")
    if not isinstance(body, dict) or "id" not in body:
        return None
    value = body["id"]
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None

