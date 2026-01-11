from __future__ import annotations

import base64
import hashlib
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class EventHints:
    id_short: Optional[str] = None
    id_short_path: Optional[str] = None
    submodel_id: Optional[str] = None


def decode_base64url(value: str) -> str:
    if not value:
        return value
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.urlsafe_b64decode(value + padding)
        return decoded.decode("utf-8")
    except Exception:
        return value


def parse_basyx_topic(topic: Optional[str]) -> EventHints:
    if not topic:
        return EventHints()
    parts = [part for part in topic.split("/") if part]
    submodel_id = None
    id_short_path = None

    if "submodels" in parts:
        index = parts.index("submodels")
        if index + 1 < len(parts):
            submodel_id = decode_base64url(parts[index + 1])

    if "submodelElements" in parts:
        index = parts.index("submodelElements")
        tail = parts[index + 1 :]
        if tail and tail[-1].lower() in {"updated", "patched", "patch", "value", "$value"}:
            tail = tail[:-1]
        if tail:
            id_short_path = "/".join(tail)

    id_short = id_short_path.split("/")[-1] if id_short_path else None
    return EventHints(id_short=id_short, id_short_path=id_short_path, submodel_id=submodel_id)


def _hash_value(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        payload = base64.b64encode(value).decode("ascii")
    else:
        try:
            payload = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
        except Exception:
            payload = repr(value)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class RecentWriteCache:
    def __init__(
        self,
        ttl_seconds: float,
        max_entries: int,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        self._ttl = max(ttl_seconds, 0.0)
        self._max_entries = max(max_entries, 1)
        self._entries: OrderedDict[str, tuple[float, str]] = OrderedDict()
        self._clock = clock or time.monotonic

    def remember(self, key: str, value: Any) -> None:
        if self._ttl <= 0:
            return
        now = self._clock()
        self._entries[key] = (now, _hash_value(value))
        self._entries.move_to_end(key)
        self._prune(now)

    def matches(self, key: str, value: Any) -> bool:
        if self._ttl <= 0:
            return False
        now = self._clock()
        self._prune(now)
        entry = self._entries.get(key)
        if not entry:
            return False
        timestamp, value_hash = entry
        if now - timestamp > self._ttl:
            self._entries.pop(key, None)
            return False
        return value_hash == _hash_value(value)

    def _prune(self, now: float) -> None:
        if self._ttl > 0:
            expired = [key for key, (ts, _) in self._entries.items() if now - ts > self._ttl]
            for key in expired:
                self._entries.pop(key, None)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
