from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional


def _json_dump(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _json_load(raw: Any, fallback: Any) -> Any:
    if raw in (None, ""):
        return fallback
    try:
        return json.loads(str(raw))
    except Exception:
        return fallback


def _row_value(row: Optional[sqlite3.Row], key: str, default: Any = None) -> Any:
    if row is None:
        return default
    value = row[key]
    return default if value is None else value


class BridgeDatabase:
    def __init__(self, path: str) -> None:
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    direction TEXT,
                    delivery TEXT,
                    channel INTEGER,
                    message_id TEXT,
                    from_node_id TEXT,
                    to_node_id TEXT,
                    portnum TEXT,
                    text TEXT,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_events_message ON events(event_type, direction, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_events_from_node ON events(from_node_id, timestamp DESC);

                CREATE TABLE IF NOT EXISTS node_registry (
                    node_id TEXT PRIMARY KEY,
                    num INTEGER,
                    long_name TEXT,
                    short_name TEXT,
                    first_seen TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    sighting_count INTEGER NOT NULL DEFAULT 0,
                    last_event_type TEXT,
                    last_payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS node_sightings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    num INTEGER,
                    long_name TEXT,
                    short_name TEXT,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_node_sightings_node ON node_sightings(node_id, timestamp DESC);

                CREATE TABLE IF NOT EXISTS snapshots (
                    kind TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS snapshot_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_snapshot_history_kind ON snapshot_history(kind, timestamp DESC);

                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp DESC);
                """
            )

    def next_event_id(self) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT COALESCE(MAX(event_id), 0) AS value FROM events").fetchone()
        return int(row["value"] if row is not None else 0) + 1

    def latest_event_id(self) -> int:
        return self.next_event_id() - 1

    def record_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(event or {})
        timestamp = str(payload.get("timestamp") or "")
        from_ref = payload.get("from") or {}
        to_ref = payload.get("to") or {}
        event_id = int(payload.get("event_id") or 0)
        row = (
            event_id,
            timestamp,
            str(payload.get("event_type") or "event"),
            str(payload.get("direction") or ""),
            str(payload.get("delivery") or ""),
            int(payload.get("channel") or 0),
            str(payload.get("message_id") or ""),
            str(from_ref.get("node_id") or ""),
            str(to_ref.get("node_id") or ""),
            str(payload.get("portnum") or ""),
            str(payload.get("text") or ""),
            _json_dump(payload),
        )
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO events (
                    event_id, timestamp, event_type, direction, delivery, channel,
                    message_id, from_node_id, to_node_id, portnum, text, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        self._record_nodes_from_event(payload)
        return payload

    def _record_nodes_from_event(self, event: Dict[str, Any]) -> None:
        timestamp = str(event.get("timestamp") or "")
        payload = event
        event_type = str(event.get("event_type") or "event")
        from_ref = event.get("from") or {}
        to_ref = event.get("to") or {}
        if str(from_ref.get("node_id") or "").strip():
            self.record_node_ref(from_ref, timestamp=timestamp, event_type=event_type, payload=payload)
        if event_type == "node_updated":
            node_ref = event.get("node") or {}
            if str(node_ref.get("node_id") or "").strip():
                self.record_node_ref(node_ref, timestamp=timestamp, event_type=event_type, payload=payload)
        if str(to_ref.get("node_id") or "").strip() not in {"", "^all", "broadcast"}:
            self.record_node_ref(to_ref, timestamp=timestamp, event_type=event_type, payload=payload)

    def record_node_ref(
        self,
        node: Dict[str, Any],
        *,
        timestamp: str,
        event_type: str,
        payload: Dict[str, Any],
    ) -> None:
        node_id = str(node.get("node_id") or "").strip()
        if not node_id:
            return
        num = int(node.get("num") or 0) if str(node.get("num") or "").strip() else 0
        long_name = str(node.get("long_name") or node.get("longName") or "").strip()
        short_name = str(node.get("short_name") or node.get("shortName") or "").strip()
        payload_json = _json_dump(payload)

        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO node_registry (
                    node_id, num, long_name, short_name, first_seen, last_seen,
                    sighting_count, last_event_type, last_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    num=excluded.num,
                    long_name=CASE WHEN excluded.long_name != '' THEN excluded.long_name ELSE node_registry.long_name END,
                    short_name=CASE WHEN excluded.short_name != '' THEN excluded.short_name ELSE node_registry.short_name END,
                    last_seen=excluded.last_seen,
                    sighting_count=node_registry.sighting_count + 1,
                    last_event_type=excluded.last_event_type,
                    last_payload_json=excluded.last_payload_json
                """,
                (
                    node_id,
                    num,
                    long_name,
                    short_name,
                    timestamp,
                    timestamp,
                    event_type,
                    payload_json,
                ),
            )
            conn.execute(
                """
                INSERT INTO node_sightings (
                    node_id, timestamp, event_type, num, long_name, short_name, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    node_id,
                    timestamp,
                    event_type,
                    num,
                    long_name,
                    short_name,
                    payload_json,
                ),
            )

    def list_events(
        self,
        *,
        since_id: int = 0,
        since_ts: Optional[str] = None,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses = ["event_id > ?"]
        params: List[Any] = [max(0, int(since_id))]
        if since_ts:
            clauses.append("timestamp > ?")
            params.append(str(since_ts))
        if event_type:
            clauses.append("event_type = ?")
            params.append(str(event_type))
        params.append(max(1, int(limit)))
        if since_id > 0 or since_ts:
            query = (
                f"SELECT payload_json FROM events WHERE {' AND '.join(clauses)} "
                "ORDER BY event_id ASC LIMIT ?"
            )
        else:
            query = (
                "SELECT payload_json FROM ("
                f"SELECT event_id, payload_json FROM events WHERE {' AND '.join(clauses)} "
                "ORDER BY event_id DESC LIMIT ?"
                ") ORDER BY event_id ASC"
            )
        with self._lock, self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [_json_load(row["payload_json"], {}) for row in rows]

    def list_messages(self, *, since_id: int = 0, since_ts: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return self.list_events(since_id=since_id, since_ts=since_ts, limit=limit, event_type="message")

    def list_known_nodes(self, *, limit: int = 500) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT node_id, num, long_name, short_name, first_seen, last_seen,
                       sighting_count, last_event_type, last_payload_json
                FROM node_registry
                ORDER BY last_seen DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [
            {
                "node_id": str(row["node_id"] or ""),
                "num": int(row["num"] or 0),
                "long_name": str(row["long_name"] or ""),
                "short_name": str(row["short_name"] or ""),
                "first_seen": str(row["first_seen"] or ""),
                "last_seen": str(row["last_seen"] or ""),
                "sighting_count": int(row["sighting_count"] or 0),
                "last_event_type": str(row["last_event_type"] or ""),
                "last_payload": _json_load(row["last_payload_json"], {}),
            }
            for row in rows
        ]

    def get_node_history(self, node_id: str, *, limit: int = 100) -> List[Dict[str, Any]]:
        token = str(node_id or "").strip()
        if not token:
            return []
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT timestamp, event_type, num, long_name, short_name, payload_json
                FROM node_sightings
                WHERE node_id = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (token, max(1, int(limit))),
            ).fetchall()
        return [
            {
                "timestamp": str(row["timestamp"] or ""),
                "event_type": str(row["event_type"] or ""),
                "num": int(row["num"] or 0),
                "long_name": str(row["long_name"] or ""),
                "short_name": str(row["short_name"] or ""),
                "payload": _json_load(row["payload_json"], {}),
            }
            for row in rows
        ]

    def save_snapshot(self, kind: str, payload: Dict[str, Any], *, timestamp: str) -> None:
        token = str(kind or "").strip()
        if not token:
            return
        payload_json = _json_dump(payload)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO snapshots (kind, timestamp, payload_json)
                VALUES (?, ?, ?)
                ON CONFLICT(kind) DO UPDATE SET
                    timestamp=excluded.timestamp,
                    payload_json=excluded.payload_json
                """,
                (token, timestamp, payload_json),
            )
            conn.execute(
                """
                INSERT INTO snapshot_history (kind, timestamp, payload_json)
                VALUES (?, ?, ?)
                """,
                (token, timestamp, payload_json),
            )

    def get_snapshot(self, kind: str) -> Dict[str, Any]:
        token = str(kind or "").strip()
        if not token:
            return {}
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT timestamp, payload_json FROM snapshots WHERE kind = ?",
                (token,),
            ).fetchone()
        if not row:
            return {}
        return {
            "kind": token,
            "timestamp": str(row["timestamp"] or ""),
            "payload": _json_load(row["payload_json"], {}),
        }

    def list_snapshot_history(self, kind: str, *, limit: int = 20) -> List[Dict[str, Any]]:
        token = str(kind or "").strip()
        if not token:
            return []
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT timestamp, payload_json
                FROM snapshot_history
                WHERE kind = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (token, max(1, int(limit))),
            ).fetchall()
        return [
            {
                "kind": token,
                "timestamp": str(row["timestamp"] or ""),
                "payload": _json_load(row["payload_json"], {}),
            }
            for row in rows
        ]

    def record_audit(self, *, timestamp: str, action: str, target: str, status: str, details: Dict[str, Any]) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO audit_log (timestamp, action, target, status, details_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    timestamp,
                    str(action or "").strip() or "action",
                    str(target or "").strip() or "bridge",
                    str(status or "").strip() or "ok",
                    _json_dump(details),
                ),
            )

    def list_audit(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, timestamp, action, target, status, details_json
                FROM audit_log
                ORDER BY id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [
            {
                "id": int(row["id"] or 0),
                "timestamp": str(row["timestamp"] or ""),
                "action": str(row["action"] or ""),
                "target": str(row["target"] or ""),
                "status": str(row["status"] or ""),
                "details": _json_load(row["details_json"], {}),
            }
            for row in rows
        ]

    def stats_summary(self, *, window_hours: int = 24) -> Dict[str, Any]:
        hours = max(1, int(window_hours))
        with self._lock, self._connect() as conn:
            totals = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_events,
                    SUM(CASE WHEN event_type = 'message' THEN 1 ELSE 0 END) AS total_messages,
                    SUM(CASE WHEN event_type = 'message' AND direction = 'inbound' THEN 1 ELSE 0 END) AS inbound_messages,
                    SUM(CASE WHEN event_type = 'message' AND direction = 'outbound' THEN 1 ELSE 0 END) AS outbound_messages,
                    SUM(CASE WHEN event_type = 'connection_state' THEN 1 ELSE 0 END) AS connection_events,
                    COALESCE(MAX(timestamp), '') AS last_event_at
                FROM events
                """
            ).fetchone()
            recent = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS recent_events,
                    SUM(CASE WHEN event_type = 'message' THEN 1 ELSE 0 END) AS recent_messages,
                    SUM(CASE WHEN event_type = 'message' AND direction = 'inbound' THEN 1 ELSE 0 END) AS recent_inbound_messages,
                    SUM(CASE WHEN event_type = 'message' AND direction = 'outbound' THEN 1 ELSE 0 END) AS recent_outbound_messages
                FROM events
                WHERE timestamp >= datetime('now', '-{hours} hours')
                """
            ).fetchone()
            known_nodes = conn.execute("SELECT COUNT(*) AS value FROM node_registry").fetchone()
            top_nodes_rows = conn.execute(
                f"""
                SELECT from_node_id, COUNT(*) AS count
                FROM events
                WHERE event_type = 'message'
                  AND direction = 'inbound'
                  AND from_node_id != ''
                  AND timestamp >= datetime('now', '-{hours} hours')
                GROUP BY from_node_id
                ORDER BY count DESC, from_node_id ASC
                LIMIT 8
                """
            ).fetchall()
            type_rows = conn.execute(
                f"""
                SELECT event_type, COUNT(*) AS count
                FROM events
                WHERE timestamp >= datetime('now', '-{hours} hours')
                GROUP BY event_type
                ORDER BY count DESC, event_type ASC
                """
            ).fetchall()
        return {
            "window_hours": hours,
            "total_events": int(_row_value(totals, "total_events", 0)),
            "total_messages": int(_row_value(totals, "total_messages", 0)),
            "inbound_messages": int(_row_value(totals, "inbound_messages", 0)),
            "outbound_messages": int(_row_value(totals, "outbound_messages", 0)),
            "connection_events": int(_row_value(totals, "connection_events", 0)),
            "last_event_at": str(_row_value(totals, "last_event_at", "")),
            "recent_events": int(_row_value(recent, "recent_events", 0)),
            "recent_messages": int(_row_value(recent, "recent_messages", 0)),
            "recent_inbound_messages": int(_row_value(recent, "recent_inbound_messages", 0)),
            "recent_outbound_messages": int(_row_value(recent, "recent_outbound_messages", 0)),
            "known_nodes": int(_row_value(known_nodes, "value", 0)),
            "top_nodes": [
                {"node_id": str(row["from_node_id"] or ""), "count": int(row["count"] or 0)}
                for row in top_nodes_rows
            ],
            "event_types": [
                {"event_type": str(row["event_type"] or ""), "count": int(row["count"] or 0)}
                for row in type_rows
            ],
        }
