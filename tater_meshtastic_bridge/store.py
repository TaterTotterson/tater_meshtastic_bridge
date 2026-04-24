from __future__ import annotations

from collections import deque
from threading import Lock
from typing import Any, Dict, List, Optional


class EventBuffer:
    def __init__(self, capacity: int = 500, *, next_id: int = 1) -> None:
        self.capacity = max(1, int(capacity))
        self._events: deque[Dict[str, Any]] = deque(maxlen=self.capacity)
        self._lock = Lock()
        self._next_id = max(1, int(next_id))

    def add(self, event: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(event or {})
        with self._lock:
            payload["event_id"] = self._next_id
            self._next_id += 1
            self._events.append(payload)
            return dict(payload)

    def latest_event_id(self) -> int:
        with self._lock:
            if not self._events:
                return 0
            return int(self._events[-1].get("event_id") or 0)

    def list(
        self,
        *,
        since_id: int = 0,
        since_ts: Optional[str] = None,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._events)

        filtered: List[Dict[str, Any]] = []
        min_id = max(0, int(since_id))
        since_ts = str(since_ts or "").strip()
        wanted_type = str(event_type or "").strip()

        for item in items:
            item_id = int(item.get("event_id") or 0)
            if item_id <= min_id:
                continue
            if since_ts:
                item_ts = str(item.get("timestamp") or "").strip()
                if item_ts and item_ts <= since_ts:
                    continue
            if wanted_type and str(item.get("event_type") or "").strip() != wanted_type:
                continue
            filtered.append(dict(item))

        if limit > 0 and len(filtered) > int(limit):
            filtered = filtered[-int(limit):]
        return filtered
