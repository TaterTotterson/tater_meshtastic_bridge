from __future__ import annotations

import importlib
import logging
import time
from datetime import datetime, timezone
from threading import Event, Lock, RLock, Thread
from typing import Any, Callable, Dict, List, Optional, Tuple

from google.protobuf.json_format import MessageToDict, ParseDict

from .config import Settings
from .database import BridgeDatabase
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


def _proto_to_dict(message: Any) -> Dict[str, Any]:
    try:
        return MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
            use_integers_for_enums=False,
        )
    except Exception:
        return _plain_data(message, depth=5) if isinstance(message, dict) else {}


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
        self.database = BridgeDatabase(settings.database_path)
        self.events = EventBuffer(settings.event_buffer_size, next_id=self.database.next_event_id())

        self._lock = Lock()
        self._radio_lock = RLock()
        self._stop_event = Event()
        self._disconnect_event = Event()
        self._worker: Optional[Thread] = None
        self._interface: Any = None
        self._pub: Any = None
        self._subscriptions: List[Tuple[Any, str]] = []
        self._meshtastic_module: Any = None
        self._broadcast_addr = "^all"
        self._broadcast_num = 0xFFFFFFFF
        self._snapshot_cache: Dict[str, Dict[str, Any]] = {}

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
            "database_path": status["database_path"],
        }

    def status_snapshot(self) -> Dict[str, Any]:
        latest_event_id = max(self.events.latest_event_id(), self.database.latest_event_id())
        with self._lock:
            return {
                "ok": True,
                "connected": bool(self.connected),
                "transport": self.transport,
                "device_name": self.device_name,
                "device_identifier": self.settings.device_identifier,
                "database_path": self.settings.database_path,
                "started_at": _iso_from_unix(self.started_at),
                "uptime_seconds": int(max(0, time.time() - self.started_at)),
                "last_seen": self.last_seen,
                "reconnect_state": self.connection_state,
                "reconnect_attempt": int(self.reconnect_attempt),
                "last_error": self.last_error,
                "latest_event_id": latest_event_id,
                "local_node": {
                    "node_id": self.local_node_id,
                    "long_name": self.local_long_name,
                    "short_name": self.local_short_name,
                    "num": self.local_node_num,
                },
            }

    def list_events(self, *, since_id: int = 0, since_ts: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return self.database.list_events(since_id=since_id, since_ts=since_ts, limit=limit, event_type=None)

    def list_messages(self, *, since_id: int = 0, since_ts: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return self.database.list_messages(since_id=since_id, since_ts=since_ts, limit=limit)

    def list_live_nodes(self) -> List[Dict[str, Any]]:
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
                    "position": _plain_data((node or {}).get("position") or {}, depth=4),
                    "snr": _plain_data((node or {}).get("snr") or "", depth=2),
                    "hops_away": _safe_int((node or {}).get("hopsAway"), 0),
                    "raw": _plain_data(node, depth=4),
                    "live": True,
                }
            )
        return nodes

    def list_nodes(self) -> List[Dict[str, Any]]:
        live_nodes = {str(item.get("node_id") or "").strip(): item for item in self.list_live_nodes() if str(item.get("node_id") or "").strip()}
        known = self.database.list_known_nodes(limit=1000)
        merged: Dict[str, Dict[str, Any]] = {}
        for item in known:
            node_id = str(item.get("node_id") or "").strip()
            merged[node_id] = {
                **item,
                "live": node_id in live_nodes,
            }
        for node_id, live in live_nodes.items():
            existing = merged.get(node_id, {})
            merged[node_id] = {
                **existing,
                **live,
                "first_seen": existing.get("first_seen") or live.get("last_heard") or "",
                "last_seen": existing.get("last_seen") or live.get("last_heard") or "",
                "sighting_count": existing.get("sighting_count") or 0,
                "live": True,
            }
        return sorted(
            merged.values(),
            key=lambda item: (
                0 if item.get("live") else 1,
                str(item.get("last_seen") or item.get("last_heard") or ""),
                str(item.get("node_id") or ""),
            ),
            reverse=False,
        )

    def get_node_history(self, node_id: str, *, limit: int = 100) -> List[Dict[str, Any]]:
        return self.database.get_node_history(node_id, limit=limit)

    def list_channels(self, *, refresh: bool = False) -> List[Dict[str, Any]]:
        if refresh:
            self.snapshot_state()
        interface = self._interface
        if interface is None:
            cached = self.database.get_snapshot("channels")
            return list((cached.get("payload") or {}).get("channels") or [])
        local_node = getattr(interface, "localNode", None)
        raw_channels = getattr(local_node, "channels", None)
        if raw_channels is None:
            cached = self.database.get_snapshot("channels")
            return list((cached.get("payload") or {}).get("channels") or [])

        if isinstance(raw_channels, dict):
            iterable = raw_channels.items()
        else:
            iterable = enumerate(raw_channels)

        channels: List[Dict[str, Any]] = []
        for index, channel in iterable:
            plain = _proto_to_dict(channel) if hasattr(channel, "DESCRIPTOR") else _plain_data(channel, depth=4)
            settings = plain.get("settings") if isinstance(plain, dict) else {}
            settings = settings if isinstance(settings, dict) else {}
            role = str(plain.get("role") or settings.get("role") or "").strip() if isinstance(plain, dict) else ""
            name = str(settings.get("name") or plain.get("name") or f"Channel {index}").strip() if isinstance(plain, dict) else f"Channel {index}"
            channels.append(
                {
                    "index": int(index),
                    "name": name,
                    "role": role,
                    "settings": settings,
                    "raw": plain,
                }
            )
        return channels

    def channel_urls(self) -> Dict[str, str]:
        interface = self._interface
        if interface is None:
            cached = self.database.get_snapshot("device")
            return dict((cached.get("payload") or {}).get("urls") or {})
        local_node = getattr(interface, "localNode", None)
        if local_node is None:
            return {}
        urls: Dict[str, str] = {}
        try:
            urls["primary"] = str(local_node.getURL(includeAll=False) or "").strip()
        except Exception:
            urls["primary"] = ""
        try:
            urls["all"] = str(local_node.getURL(includeAll=True) or "").strip()
        except Exception:
            urls["all"] = ""
        return urls

    def config_snapshot(self, *, refresh: bool = False) -> Dict[str, Any]:
        if refresh:
            self.snapshot_state()
        cached = self.database.get_snapshot("configs")
        payload = cached.get("payload") or {}
        if payload:
            return payload
        return self._live_config_sections()

    def device_snapshot(self, *, refresh: bool = False) -> Dict[str, Any]:
        if refresh:
            snapshots = self.snapshot_state()
            return snapshots.get("device") or {}
        cached = self.database.get_snapshot("device")
        payload = cached.get("payload") or {}
        if payload:
            return payload
        snapshots = self.snapshot_state()
        return snapshots.get("device") or {}

    def snapshot_state(self) -> Dict[str, Any]:
        timestamp = _utc_now_iso()
        interface = self._interface
        if interface is None:
            status = self.status_snapshot()
            snapshots = {
                "device": {**status},
                "channels": {"channels": self.database.get_snapshot("channels").get("payload", {}).get("channels", [])},
                "configs": self.database.get_snapshot("configs").get("payload", {}),
            }
            self._snapshot_cache = snapshots
            return snapshots

        with self._radio_lock:
            self._refresh_local_identity()
            configs = self._live_config_sections()
            channels = self.list_channels()
            urls = self.channel_urls()
            my_info = getattr(interface, "myInfo", None)
            metadata = getattr(interface, "metadata", None)
            node_info = None
            try:
                node_info = interface.getMyNodeInfo()
            except Exception:
                node_info = None
            device = {
                **self.status_snapshot(),
                "timestamp": timestamp,
                "urls": urls,
                "node_info": _plain_data(node_info, depth=4),
                "my_info": _plain_data(my_info, depth=4),
                "metadata": _plain_data(metadata, depth=4),
            }
            channel_payload = {
                "timestamp": timestamp,
                "channels": channels,
                "urls": urls,
            }
            snapshots = {
                "device": device,
                "channels": channel_payload,
                "configs": configs,
            }

        self.database.save_snapshot("device", device, timestamp=timestamp)
        self.database.save_snapshot("channels", channel_payload, timestamp=timestamp)
        self.database.save_snapshot("configs", configs, timestamp=timestamp)
        self._snapshot_cache = snapshots
        return snapshots

    def stats_summary(self, *, window_hours: int = 24) -> Dict[str, Any]:
        stats = self.database.stats_summary(window_hours=window_hours)
        node_lookup = {row.get("node_id"): row for row in self.database.list_known_nodes(limit=2000)}
        for item in stats.get("top_nodes", []):
            node = node_lookup.get(item.get("node_id")) or {}
            item["long_name"] = node.get("long_name") or ""
            item["short_name"] = node.get("short_name") or ""
        return stats

    def list_audit_log(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        return self.database.list_audit(limit=limit)

    def send_text(self, *, text: str, channel: int = 0, destination: str = "broadcast") -> Dict[str, Any]:
        body = str(text or "").strip()
        if not body:
            raise ValueError("Message text is required.")

        with self._radio_lock:
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
            normalized = self._normalize_message_event(
                packet,
                direction="outbound",
                fallback_text=body,
                channel=int(channel),
                destination=destination_id,
            )
            if self.settings.include_outbound_events:
                normalized = self._add_event(normalized)
            self._note_seen()

        return {
            "ok": True,
            "connected": True,
            "message": normalized,
        }

    def update_owner(
        self,
        *,
        long_name: Optional[str] = None,
        short_name: Optional[str] = None,
        is_licensed: bool = False,
        is_unmessagable: Optional[bool] = None,
    ) -> Dict[str, Any]:
        if long_name is None and short_name is None and is_unmessagable is None:
            raise ValueError("Provide at least one owner field to update.")

        details = {
            "long_name": str(long_name or "").strip(),
            "short_name": str(short_name or "").strip(),
            "is_licensed": bool(is_licensed),
        }
        if is_unmessagable is not None:
            details["is_unmessagable"] = bool(is_unmessagable)

        def _run(node: Any) -> Any:
            return node.setOwner(
                long_name=long_name,
                short_name=short_name,
                is_licensed=is_licensed,
                is_unmessagable=is_unmessagable,
            )

        return self._execute_admin_write(action="update_owner", target="device", details=details, operation=_run)

    def set_channel_url(self, *, url: str, add_only: bool = False) -> Dict[str, Any]:
        token = str(url or "").strip()
        if not token:
            raise ValueError("Channel URL is required.")

        def _run(node: Any) -> Any:
            return node.setURL(token, addOnly=bool(add_only))

        return self._execute_admin_write(
            action="set_channel_url",
            target="channels",
            details={"url": token, "add_only": bool(add_only)},
            operation=_run,
        )

    def update_config_section(self, *, scope: str, section: str, values: Dict[str, Any]) -> Dict[str, Any]:
        scope_token = str(scope or "").strip().lower()
        section_token = str(section or "").strip()
        if scope_token not in {"local", "module"}:
            raise ValueError("Scope must be 'local' or 'module'.")
        if not section_token:
            raise ValueError("Config section is required.")

        def _run(node: Any) -> Any:
            container = node.localConfig if scope_token == "local" else node.moduleConfig
            if not hasattr(container, section_token):
                raise ValueError(f"Unknown {scope_token} config section '{section_token}'.")
            current = getattr(container, section_token)
            merged = current.__class__()
            merged.CopyFrom(current)
            ParseDict(dict(values or {}), merged, ignore_unknown_fields=False)
            current.CopyFrom(merged)
            return node.writeConfig(section_token)

        return self._execute_admin_write(
            action="update_config_section",
            target=f"{scope_token}.{section_token}",
            details={"values": values},
            operation=_run,
        )

    def update_channel(self, *, index: int, channel_data: Dict[str, Any]) -> Dict[str, Any]:
        channel_index = int(index)
        payload = dict(channel_data or {})

        def _run(node: Any) -> Any:
            from meshtastic.protobuf import channel_pb2

            if node.channels is None or channel_index < 0 or channel_index >= len(node.channels):
                raise ValueError(f"Channel index {channel_index} is not available.")
            updated = channel_pb2.Channel()
            updated.CopyFrom(node.channels[channel_index])
            ParseDict(payload, updated, ignore_unknown_fields=False)
            node.channels[channel_index].CopyFrom(updated)
            return node.writeChannel(channel_index)

        return self._execute_admin_write(
            action="update_channel",
            target=f"channel:{channel_index}",
            details={"channel": payload},
            operation=_run,
        )

    def delete_channel(self, *, index: int) -> Dict[str, Any]:
        channel_index = int(index)
        if channel_index <= 0:
            raise ValueError("Only secondary channels can be deleted.")

        def _run(node: Any) -> Any:
            return node.deleteChannel(channel_index)

        return self._execute_admin_write(
            action="delete_channel",
            target=f"channel:{channel_index}",
            details={"index": channel_index},
            operation=_run,
        )

    def set_fixed_position(self, *, latitude: float, longitude: float, altitude: int = 0) -> Dict[str, Any]:
        def _run(node: Any) -> Any:
            return node.setFixedPosition(float(latitude), float(longitude), int(altitude))

        return self._execute_admin_write(
            action="set_fixed_position",
            target="position",
            details={"latitude": float(latitude), "longitude": float(longitude), "altitude": int(altitude)},
            operation=_run,
        )

    def set_canned_message(self, *, text: str) -> Dict[str, Any]:
        body = str(text or "").strip()
        if not body:
            raise ValueError("Canned message text is required.")

        def _run(node: Any) -> Any:
            return node.set_canned_message(body)

        return self._execute_admin_write(
            action="set_canned_message",
            target="module.canned_message",
            details={"text": body},
            operation=_run,
        )

    def set_ringtone(self, *, text: str) -> Dict[str, Any]:
        body = str(text or "").strip()
        if not body:
            raise ValueError("Ringtone text is required.")

        def _run(node: Any) -> Any:
            return node.set_ringtone(body)

        return self._execute_admin_write(
            action="set_ringtone",
            target="module.external_notification",
            details={"text": body},
            operation=_run,
        )

    def perform_device_action(self, *, action: str, seconds: int = 10) -> Dict[str, Any]:
        token = str(action or "").strip().lower()
        wait_seconds = max(0, int(seconds))

        def _run(node: Any) -> Any:
            if token == "reboot":
                return node.reboot(wait_seconds or 10)
            if token == "shutdown":
                return node.shutdown(wait_seconds or 10)
            if token == "reboot_ota":
                return node.rebootOTA(wait_seconds or 10)
            if token == "enter_dfu_mode":
                return node.enterDFUMode()
            if token == "exit_simulator":
                return node.exitSimulator()
            raise ValueError(f"Unsupported device action '{token}'.")

        return self._execute_admin_write(
            action="device_action",
            target=token or "device",
            details={"seconds": wait_seconds},
            operation=_run,
            post_delay=0.2,
        )

    def _execute_admin_write(
        self,
        *,
        action: str,
        target: str,
        details: Dict[str, Any],
        operation: Callable[[Any], Any],
        post_delay: float = 0.35,
    ) -> Dict[str, Any]:
        timestamp = _utc_now_iso()
        try:
            with self._radio_lock:
                interface, node = self._require_local_node()
                result = operation(node)
            if post_delay > 0:
                time.sleep(post_delay)
            snapshots = self.snapshot_state()
            payload = {
                "ok": True,
                "action": action,
                "target": target,
                "details": details,
                "result": _plain_data(result, depth=4),
                "snapshots": snapshots,
            }
            self.database.record_audit(timestamp=timestamp, action=action, target=target, status="ok", details=payload)
            return payload
        except SystemExit as exc:
            message = str(exc) or "Meshtastic rejected the requested change."
            self.database.record_audit(
                timestamp=timestamp,
                action=action,
                target=target,
                status="error",
                details={"error": message, "details": details},
            )
            raise RuntimeError(message) from exc
        except Exception as exc:
            self.database.record_audit(
                timestamp=timestamp,
                action=action,
                target=target,
                status="error",
                details={"error": str(exc), "details": details},
            )
            raise

    def _require_local_node(self) -> Tuple[Any, Any]:
        interface = self._interface
        if interface is None or not self.connected:
            raise RuntimeError("Meshtastic radio is not connected.")
        node = getattr(interface, "localNode", None)
        if node is None:
            raise RuntimeError("Meshtastic local node is not ready.")
        return interface, node

    def _live_config_sections(self) -> Dict[str, Any]:
        interface = self._interface
        if interface is None:
            cached = self.database.get_snapshot("configs")
            return cached.get("payload") or {"local": {}, "module": {}, "timestamp": _utc_now_iso()}

        local_node = getattr(interface, "localNode", None)
        if local_node is None:
            return {"local": {}, "module": {}, "timestamp": _utc_now_iso()}

        local_sections: Dict[str, Any] = {}
        for field in getattr(local_node.localConfig.DESCRIPTOR, "fields", []) or []:
            try:
                local_sections[field.name] = _proto_to_dict(getattr(local_node.localConfig, field.name))
            except Exception:
                local_sections[field.name] = {}

        module_sections: Dict[str, Any] = {}
        for field in getattr(local_node.moduleConfig.DESCRIPTOR, "fields", []) or []:
            try:
                module_sections[field.name] = _proto_to_dict(getattr(local_node.moduleConfig, field.name))
            except Exception:
                module_sections[field.name] = {}

        return {
            "timestamp": _utc_now_iso(),
            "local": local_sections,
            "module": module_sections,
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
        self._subscribe(pub, self._on_receive_position, "meshtastic.receive.position")
        self._subscribe(pub, self._on_receive_user, "meshtastic.receive.user")
        self._subscribe(pub, self._on_receive_data, "meshtastic.receive.data")
        self._subscribe(pub, self._on_node_updated, "meshtastic.node.updated")
        self._subscribe(pub, self._on_log_line, "meshtastic.log.line")

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
        self._add_event(
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
        self.snapshot_state()
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

        return {
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

    def _normalize_packet_event(self, packet: Any, *, event_type: str) -> Dict[str, Any]:
        payload = dict(packet or {}) if isinstance(packet, dict) else {}
        decoded = dict(payload.get("decoded") or {})
        from_ref = self._normalize_node_ref(
            node_id=str(payload.get("fromId") or "").strip(),
            node_num=payload.get("from"),
        )
        to_ref, delivery = self._normalize_destination_ref(payload, explicit_destination=None)
        event = {
            "event_type": event_type,
            "timestamp": _iso_from_unix(payload.get("rxTime") or time.time()),
            "message_id": str(payload.get("id") or payload.get("packetId") or "").strip(),
            "direction": "inbound",
            "transport": self.transport,
            "delivery": delivery,
            "from": from_ref,
            "to": to_ref,
            "channel": _safe_int(payload.get("channel"), 0),
            "portnum": str(decoded.get("portnum") or "").strip(),
            "raw": _plain_data(payload, depth=4),
        }
        if event_type == "position":
            event["position"] = _plain_data(decoded.get("position") or {}, depth=4)
        elif event_type == "user":
            event["user"] = _plain_data(decoded.get("user") or {}, depth=4)
        elif event_type == "data":
            event["data"] = _plain_data(decoded, depth=4)
        return event

    def _add_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        stored = self.events.add(event)
        self.database.record_event(stored)
        return stored

    def _on_connection_established(self, interface: Any = None, topic: Any = None) -> None:
        self._refresh_local_identity()
        self._note_seen()
        self._set_state(connected=True, state="connected", error="")
        self._add_event(
            {
                "event_type": "connection_state",
                "timestamp": _utc_now_iso(),
                "connected": True,
                "transport": self.transport,
                "device_name": self.device_name,
            }
        )
        self.snapshot_state()

    def _on_connection_lost(self, interface: Any = None, topic: Any = None) -> None:
        self._set_state(connected=False, state="reconnecting", error=self.last_error)
        self._disconnect_event.set()
        self._add_event(
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
        self._add_event(event)
        self._note_seen()

    def _on_receive_position(self, packet: Optional[Dict[str, Any]] = None, interface: Any = None, topic: Any = None) -> None:
        self._add_event(self._normalize_packet_event(packet or {}, event_type="position"))
        self._note_seen()

    def _on_receive_user(self, packet: Optional[Dict[str, Any]] = None, interface: Any = None, topic: Any = None) -> None:
        self._add_event(self._normalize_packet_event(packet or {}, event_type="user"))
        self._note_seen()

    def _on_receive_data(self, packet: Optional[Dict[str, Any]] = None, interface: Any = None, topic: Any = None) -> None:
        self._add_event(self._normalize_packet_event(packet or {}, event_type="data"))
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
        self._add_event(
            {
                "event_type": "node_updated",
                "timestamp": _utc_now_iso(),
                "node": {
                    "node_id": str(user.get("id") or "").strip(),
                    "long_name": str(user.get("longName") or "").strip(),
                    "short_name": str(user.get("shortName") or "").strip(),
                    "num": _safe_int((normalized or {}).get("num"), 0),
                },
                "raw": normalized,
            }
        )
        self._note_seen()

    def _on_log_line(self, line: Any = None, interface: Any = None, topic: Any = None) -> None:
        text = str(line or "").strip()
        if not text:
            return
        self._add_event(
            {
                "event_type": "radio_log",
                "timestamp": _utc_now_iso(),
                "transport": self.transport,
                "text": text,
            }
        )
