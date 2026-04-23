from __future__ import annotations

import importlib
import logging
import time
from datetime import datetime, timezone
from threading import Event, Lock, Thread
from typing import Any, Dict, List, Optional, Tuple

from .config import Settings
from .store import EventBuffer


logger = logging.getLogger("tater_meshtastic_bridge")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iso_from_unix(raw: Any) -> str:
    try:
        return datetime.fromtimestamp(float(raw), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return _utc_now_iso()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return int(default)


def _plain_data(value: Any, depth: int = 3) -> Any:
    if depth <= 0:
        return repr(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _plain_data(v, depth - 1) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain_data(item, depth - 1) for item in value]
    if hasattr(value, "items"):
        try:
            return {str(k): _plain_data(v, depth - 1) for k, v in value.items()}
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        return _plain_data(vars(value), depth - 1)
    for attr_name in ("to_dict", "as_dict"):
        func = getattr(value, attr_name, None)
        if callable(func):
            try:
                return _plain_data(func(), depth - 1)
            except Exception:
                continue
    return repr(value)


class MeshtasticBridgeService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.transport = "ble"
        self.started_at = time.time()
        self.last_seen: Optional[str] = None
        self.last_error = ""
        self.connected = False
        self.connection_state = "starting"
        self.reconnect_attempt = 0
        self.local_node_id = "^local"
        self.local_long_name = ""
        self.local_short_name = ""
        self.local_node_num = 0
        self.device_name = settings.device_name or settings.device_address or "Meshtastic"
        self.events = EventBuffer(settings.event_buffer_size)

        self._lock = Lock()
        self._stop_event = Event()
        self._disconnect_event = Event()
        self._worker: Optional[Thread] = None
        self._interface: Any = None
        self._pub: Any = None
        self._subscriptions: List[Tuple[Any, str]] = []
        self._meshtastic_module: Any = None
        self._broadcast_addr = "^all"
        self._broadcast_num = 0xFFFFFFFF

    def start(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stop_event.clear()
        self._disconnect_event.clear()
        self._worker = Thread(target=self._run, name="meshtastic-bridge", daemon=True)
        self._worker.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._disconnect_event.set()
        self._close_interface()
        if self._worker:
            timeout = max(0.1, float(self.settings.shutdown_timeout_seconds))
            self._worker.join(timeout=timeout)
            if self._worker.is_alive():
                logger.warning(
                    "Meshtastic bridge worker did not exit within %.1fs; continuing shutdown",
                    timeout,
                )
            else:
                self._worker = None

    def health_snapshot(self) -> Dict[str, Any]:
        status = self.status_snapshot()
        return {
            "ok": True,
            "connected": bool(status["connected"]),
            "transport": status["transport"],
            "device_name": status["device_name"],
            "last_seen": status["last_seen"],
            "uptime_seconds": status["uptime_seconds"],
        }

    def status_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "ok": True,
                "connected": bool(self.connected),
                "transport": self.transport,
                "device_name": self.device_name,
                "device_identifier": self.settings.device_identifier,
                "started_at": _iso_from_unix(self.started_at),
                "uptime_seconds": int(max(0, time.time() - self.started_at)),
                "last_seen": self.last_seen,
                "reconnect_state": self.connection_state,
                "reconnect_attempt": int(self.reconnect_attempt),
                "last_error": self.last_error,
                "latest_event_id": self.events.latest_event_id(),
                "local_node": {
                    "node_id": self.local_node_id,
                    "long_name": self.local_long_name,
                    "short_name": self.local_short_name,
                    "num": self.local_node_num,
                },
            }

    def list_events(self, *, since_id: int = 0, since_ts: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return self.events.list(since_id=since_id, since_ts=since_ts, limit=limit, event_type=None)

    def list_messages(self, *, since_id: int = 0, since_ts: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return self.events.list(since_id=since_id, since_ts=since_ts, limit=limit, event_type="message")

    def list_nodes(self) -> List[Dict[str, Any]]:
        interface = self._interface
        if interface is None:
            return []
        raw_nodes = getattr(interface, "nodes", None) or {}
        nodes: List[Dict[str, Any]] = []
        for node_id, node in sorted(raw_nodes.items()):
            user = dict((node or {}).get("user") or {})
            nodes.append(
                {
                    "node_id": str(user.get("id") or node_id or "").strip(),
                    "long_name": str(user.get("longName") or "").strip(),
                    "short_name": str(user.get("shortName") or "").strip(),
                    "num": _safe_int((node or {}).get("num"), 0),
                    "last_heard": _iso_from_unix((node or {}).get("lastHeard") or time.time()),
                    "raw": _plain_data(node, depth=3),
                }
            )
        return nodes

    def list_channels(self) -> List[Dict[str, Any]]:
        interface = self._interface
        if interface is None:
            return []
        local_node = getattr(interface, "localNode", None)
        raw_channels = getattr(local_node, "channels", None)
        if raw_channels is None:
            return []

        if isinstance(raw_channels, dict):
            iterable = raw_channels.items()
        else:
            iterable = enumerate(raw_channels)

        channels: List[Dict[str, Any]] = []
        for index, channel in iterable:
            plain = _plain_data(channel, depth=3)
            settings = plain.get("settings") if isinstance(plain, dict) else {}
            settings = settings if isinstance(settings, dict) else {}
            role = str(plain.get("role") or settings.get("role") or "").strip() if isinstance(plain, dict) else ""
            name = str(settings.get("name") or plain.get("name") or f"Channel {index}").strip() if isinstance(plain, dict) else f"Channel {index}"
            channels.append(
                {
                    "index": int(index),
                    "name": name,
                    "role": role,
                    "raw": plain,
                }
            )
        return channels

    def device_snapshot(self) -> Dict[str, Any]:
        interface = self._interface
        my_info = getattr(interface, "myInfo", None) if interface is not None else None
        metadata = getattr(interface, "metadata", None) if interface is not None else None
        return {
            **self.status_snapshot(),
            "my_info": _plain_data(my_info, depth=4),
            "metadata": _plain_data(metadata, depth=4),
        }

    def send_text(self, *, text: str, channel: int = 0, destination: str = "broadcast") -> Dict[str, Any]:
        body = str(text or "").strip()
        if not body:
            raise ValueError("Message text is required.")

        interface = self._interface
        if interface is None or not self.connected:
            raise RuntimeError("Meshtastic radio is not connected.")

        destination_id = self._resolve_destination(destination)
        kwargs: Dict[str, Any] = {
            "destinationId": destination_id,
            "channelIndex": int(channel),
            "wantAck": bool(self.settings.want_ack),
        }
        if self.settings.default_hop_limit is not None:
            kwargs["hopLimit"] = int(self.settings.default_hop_limit)

        packet = interface.sendText(body, **kwargs)
        normalized = self._normalize_message_event(packet, direction="outbound", fallback_text=body, channel=int(channel), destination=destination_id)
        if self.settings.include_outbound_events:
            normalized = self.events.add(normalized)
        self._note_seen()
        return {
            "ok": True,
            "connected": True,
            "message": normalized,
        }

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._set_state(connected=False, state="connecting", error="")
                self._connect_once()
                self.reconnect_attempt = 0
                while not self._stop_event.is_set():
                    if self._disconnect_event.wait(0.5):
                        break
            except Exception as exc:
                logger.exception("Meshtastic bridge connection loop failed")
                self._set_state(connected=False, state="reconnecting", error=str(exc))
            finally:
                self._close_interface()
                if self._stop_event.is_set():
                    break
                self.reconnect_attempt += 1
                self._set_state(connected=False, state="reconnecting", error=self.last_error)
                delay = self.settings.reconnect_seconds * min(6, max(1, self.reconnect_attempt))
                if self._stop_event.wait(delay):
                    break

    def _connect_once(self) -> None:
        meshtastic_module, pub = self._load_meshtastic_dependencies()
        ble_module = importlib.import_module("meshtastic.ble_interface")
        interface_cls = getattr(ble_module, "BLEInterface")

        self._disconnect_event.clear()
        self._unsubscribe_all()
        self._subscribe(pub, self._on_connection_established, "meshtastic.connection.established")
        self._subscribe(pub, self._on_connection_lost, "meshtastic.connection.lost")
        self._subscribe(pub, self._on_receive_text, "meshtastic.receive.text")
        self._subscribe(pub, self._on_node_updated, "meshtastic.node.updated")

        identifier = self.settings.device_identifier
        logger.info("Connecting to Meshtastic BLE node %s", identifier or "(first available)")
        self._interface = interface_cls(
            identifier,
            timeout=int(self.settings.connect_timeout_seconds),
            noNodes=bool(self.settings.no_nodes),
        )
        self._refresh_local_identity()
        self._broadcast_addr = str(getattr(meshtastic_module, "BROADCAST_ADDR", "^all") or "^all").strip() or "^all"
        self._broadcast_num = _safe_int(getattr(meshtastic_module, "BROADCAST_NUM", 0xFFFFFFFF), 0xFFFFFFFF)
        self._note_seen()
        self._set_state(connected=True, state="connected", error="")
        self.events.add(
            {
                "event_type": "connection_state",
                "timestamp": _utc_now_iso(),
                "connected": True,
                "transport": self.transport,
                "device_name": self.device_name,
                "local_node": {
                    "node_id": self.local_node_id,
                    "long_name": self.local_long_name,
                    "short_name": self.local_short_name,
                },
            }
        )
        logger.info("Meshtastic BLE connection established")

    def _load_meshtastic_dependencies(self) -> Tuple[Any, Any]:
        if self._meshtastic_module is not None and self._pub is not None:
            return self._meshtastic_module, self._pub

        self._meshtastic_module = importlib.import_module("meshtastic")
        self._pub = importlib.import_module("pubsub").pub
        return self._meshtastic_module, self._pub

    def _subscribe(self, pub: Any, callback: Any, topic: str) -> None:
        pub.subscribe(callback, topic)
        self._subscriptions.append((callback, topic))

    def _unsubscribe_all(self) -> None:
        if self._pub is None:
            self._subscriptions.clear()
            return
        for callback, topic in self._subscriptions:
            try:
                self._pub.unsubscribe(callback, topic)
            except Exception:
                continue
        self._subscriptions.clear()

    def _close_interface(self) -> None:
        interface = self._interface
        self._interface = None
        self._unsubscribe_all()
        if interface is not None:
            closer = Thread(
                target=self._close_interface_blocking,
                args=(interface,),
                name="meshtastic-bridge-close",
                daemon=True,
            )
            closer.start()
            timeout = max(0.1, float(self.settings.shutdown_timeout_seconds))
            closer.join(timeout=timeout)
            if closer.is_alive():
                logger.warning(
                    "Timed out waiting %.1fs for Meshtastic interface close; continuing shutdown",
                    timeout,
                )
        self._set_state(connected=False, state="disconnected", error=self.last_error)

    @staticmethod
    def _close_interface_blocking(interface: Any) -> None:
        try:
            interface.close()
        except Exception:
            logger.exception("Error while closing Meshtastic interface")

    def _set_state(self, *, connected: bool, state: str, error: str) -> None:
        with self._lock:
            self.connected = bool(connected)
            self.connection_state = str(state or "").strip() or "unknown"
            self.last_error = str(error or "").strip()

    def _note_seen(self) -> None:
        with self._lock:
            self.last_seen = _utc_now_iso()

    def _refresh_local_identity(self) -> None:
        interface = self._interface
        if interface is None:
            return

        node_info = None
        try:
            node_info = interface.getMyNodeInfo()
        except Exception:
            node_info = None

        user = dict((node_info or {}).get("user") or {})
        node_id = str(user.get("id") or "").strip()
        long_name = str(user.get("longName") or "").strip()
        short_name = str(user.get("shortName") or "").strip()
        node_num = _safe_int((node_info or {}).get("num"), 0)

        if not node_num:
            my_info = getattr(interface, "myInfo", None)
            node_num = _safe_int(getattr(my_info, "my_node_num", 0), 0)

        if not long_name:
            try:
                long_name = str(interface.getLongName() or "").strip()
            except Exception:
                long_name = ""
        if not short_name:
            try:
                short_name = str(interface.getShortName() or "").strip()
            except Exception:
                short_name = ""

        with self._lock:
            self.local_node_id = node_id or "^local"
            self.local_long_name = long_name
            self.local_short_name = short_name
            self.local_node_num = node_num
            if long_name:
                self.device_name = long_name

    def _resolve_destination(self, destination: str) -> str:
        token = str(destination or "broadcast").strip()
        if not token:
            return self._broadcast_addr
        lowered = token.lower()
        if lowered in {"broadcast", "^all", "all"}:
            return self._broadcast_addr
        if lowered in {"local", "^local", "me"}:
            return self.local_node_id
        return token

    def _lookup_node_by_num(self, node_num: Any) -> Optional[Dict[str, Any]]:
        interface = self._interface
        if interface is None:
            return None
        nodes_by_num = getattr(interface, "nodesByNum", None) or {}
        try:
            return dict(nodes_by_num.get(int(node_num)) or {})
        except Exception:
            return None

    def _lookup_node_by_id(self, node_id: str) -> Optional[Dict[str, Any]]:
        interface = self._interface
        if interface is None:
            return None
        nodes = getattr(interface, "nodes", None) or {}
        return dict(nodes.get(node_id) or {}) if node_id else None

    def _normalize_node_ref(
        self,
        *,
        node_id: str = "",
        node_num: Any = None,
        prefer_local: bool = False,
    ) -> Dict[str, Any]:
        raw_id = str(node_id or "").strip()
        raw_num = _safe_int(node_num, 0) if node_num not in (None, "") else None
        node = self._lookup_node_by_id(raw_id) if raw_id else None
        if not node and raw_num not in (None, 0):
            node = self._lookup_node_by_num(raw_num)

        user = dict((node or {}).get("user") or {})
        resolved_id = str(user.get("id") or raw_id or "").strip()
        long_name = str(user.get("longName") or "").strip()
        short_name = str(user.get("shortName") or "").strip()
        resolved_num = _safe_int((node or {}).get("num") or raw_num, 0)

        if prefer_local and not resolved_id:
            resolved_id = self.local_node_id
        if prefer_local and not long_name:
            long_name = self.local_long_name
        if prefer_local and not short_name:
            short_name = self.local_short_name
        if prefer_local and not resolved_num:
            resolved_num = self.local_node_num

        return {
            "node_id": resolved_id,
            "long_name": long_name,
            "short_name": short_name,
            "num": resolved_num,
        }

    def _normalize_destination_ref(self, packet: Dict[str, Any], explicit_destination: Optional[str]) -> Tuple[Dict[str, Any], str]:
        if explicit_destination is not None:
            token = self._resolve_destination(explicit_destination)
            lowered = token.lower()
            if lowered == self._broadcast_addr.lower():
                return {"node_id": self._broadcast_addr, "long_name": "Broadcast", "short_name": "BCAST", "num": self._broadcast_num}, "broadcast"
            if lowered == str(self.local_node_id).lower():
                return self._normalize_node_ref(node_id=self.local_node_id, prefer_local=True), "direct"
            return self._normalize_node_ref(node_id=token), "direct"

        to_id = str(packet.get("toId") or "").strip()
        to_num = packet.get("to")
        if to_id.lower() == self.local_node_id.lower() or (
            self.local_node_num and _safe_int(to_num, 0) == self.local_node_num
        ):
            return self._normalize_node_ref(node_id=self.local_node_id, prefer_local=True), "direct"
        if _safe_int(to_num, self._broadcast_num) == self._broadcast_num:
            return {"node_id": self._broadcast_addr, "long_name": "Broadcast", "short_name": "BCAST", "num": self._broadcast_num}, "broadcast"
        destination = self._normalize_node_ref(node_id=to_id, node_num=to_num, prefer_local=(to_id.lower() == self.local_node_id.lower()))
        delivery = "direct" if destination.get("node_id") and destination.get("node_id") != self._broadcast_addr else "broadcast"
        if delivery == "direct" and str(destination.get("node_id") or "").lower() == self.local_node_id.lower():
            destination["node_id"] = "^local"
        return destination, delivery

    def _normalize_message_event(
        self,
        packet: Any,
        *,
        direction: str,
        fallback_text: str = "",
        channel: Optional[int] = None,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = dict(packet or {}) if isinstance(packet, dict) else {}
        decoded = dict(payload.get("decoded") or {})
        text = str(decoded.get("text") or fallback_text or "").strip()
        from_ref = self._normalize_node_ref(
            node_id=str(payload.get("fromId") or "").strip(),
            node_num=payload.get("from"),
            prefer_local=(direction == "outbound"),
        )
        to_ref, delivery = self._normalize_destination_ref(payload, explicit_destination=destination)
        normalized_channel = int(channel if channel is not None else _safe_int(payload.get("channel"), 0))

        if direction == "outbound":
            from_ref = self._normalize_node_ref(node_id=self.local_node_id, prefer_local=True)

        event = {
            "event_type": "message",
            "timestamp": _iso_from_unix(payload.get("rxTime") or time.time()),
            "message_id": str(payload.get("id") or payload.get("packetId") or "").strip(),
            "direction": direction,
            "transport": self.transport,
            "delivery": delivery,
            "from": from_ref,
            "to": to_ref,
            "channel": normalized_channel,
            "text": text,
            "portnum": str(decoded.get("portnum") or "TEXT_MESSAGE_APP").strip(),
            "raw": _plain_data(payload, depth=4),
        }
        return event

    def _on_connection_established(self, interface: Any = None, topic: Any = None) -> None:
        self._refresh_local_identity()
        self._note_seen()
        self._set_state(connected=True, state="connected", error="")
        self.events.add(
            {
                "event_type": "connection_state",
                "timestamp": _utc_now_iso(),
                "connected": True,
                "transport": self.transport,
                "device_name": self.device_name,
            }
        )

    def _on_connection_lost(self, interface: Any = None, topic: Any = None) -> None:
        self._set_state(connected=False, state="reconnecting", error=self.last_error)
        self._disconnect_event.set()
        self.events.add(
            {
                "event_type": "connection_state",
                "timestamp": _utc_now_iso(),
                "connected": False,
                "transport": self.transport,
                "device_name": self.device_name,
            }
        )

    def _on_receive_text(self, packet: Optional[Dict[str, Any]] = None, interface: Any = None, topic: Any = None) -> None:
        event = self._normalize_message_event(packet or {}, direction="inbound")
        self.events.add(event)
        self._note_seen()

    def _on_node_updated(
        self,
        node: Optional[Dict[str, Any]] = None,
        interface: Any = None,
        topic: Any = None,
        **_kwargs: Any,
    ) -> None:
        normalized = _plain_data(node or {}, depth=4)
        user = dict((normalized or {}).get("user") or {}) if isinstance(normalized, dict) else {}
        self.events.add(
            {
                "event_type": "node_updated",
                "timestamp": _utc_now_iso(),
                "node": {
                    "node_id": str(user.get("id") or "").strip(),
                    "long_name": str(user.get("longName") or "").strip(),
                    "short_name": str(user.get("shortName") or "").strip(),
                },
                "raw": normalized,
            }
        )
        self._note_seen()
