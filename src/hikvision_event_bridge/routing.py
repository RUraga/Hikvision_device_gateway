"""Admisión de eventos. No convierte códigos no configurados en marcaciones."""

from __future__ import annotations

from hikvision_event_bridge.config import FilterConfig
from hikvision_event_bridge.normalization import authentication_tokens


def rejection_reason(event: dict[str, object], filt: FilterConfig) -> str | None:
    major = event["major_event_type"]
    sub = event["sub_event_type"]
    if filt.major_event_types and major not in filt.major_event_types:
        return "major"
    if filt.sub_event_types and sub not in filt.sub_event_types:
        return "sub_event"
    if filt.exclude_door_events and sub in filt.door_sub_event_types:
        return "door"
    if filt.include_authentication:
        tokens = authentication_tokens(event.get("verify_mode") if isinstance(event.get("verify_mode"), str) else None)
        wanted = {item.casefold() for item in filt.include_authentication}
        allowed_codes = sub in filt.authentication_sub_event_types
        if tokens.isdisjoint(wanted) and not allowed_codes:
            return "authentication"
    return None
