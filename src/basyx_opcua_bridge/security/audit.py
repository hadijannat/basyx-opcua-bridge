from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import structlog

logger = structlog.get_logger(__name__)

class AuditLogger:
    def __init__(self, log_file: Optional[Path] = None, emit_to_stdout: bool = True) -> None:
        self._log_file = log_file
        self._emit_to_stdout = emit_to_stdout
        if log_file:
            log_file.parent.mkdir(parents=True, exist_ok=True)

    async def log_write(self, node_id: str, user_id: Optional[str], previous_value: Any, new_value: Any, correlation_id: Optional[str] = None) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_type": "WRITE",
            "node_id": node_id,
            "user_id": user_id or "system",
            "previous_value": self._serialize_value(previous_value),
            "new_value": self._serialize_value(new_value),
            "correlation_id": correlation_id,
        }
        await self._write_entry(entry)

    def _serialize_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, bytes):
            return f"<bytes:{len(value)}>"
        return str(value)

    async def _write_entry(self, entry: dict[str, Any]) -> None:
        if self._emit_to_stdout:
            logger.info("audit_entry", **entry)
        if self._log_file:
            with open(self._log_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
