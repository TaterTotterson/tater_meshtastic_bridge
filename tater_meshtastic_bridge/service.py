from __future__ import annotations

import importlib
import json
import logging
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock, RLock, Thread
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.json_format import MessageToDict, ParseDict

from .config import (
    BRIDGE_SETTING_KEYS,
    DEFAULT_BRIDGE_SETTINGS,
    DEFAULT_BRIDGE_PORT,
    RESTART_REQUIRED_SETTINGS,
    Settings,
    _coerce_bool,
    _coerce_float,
    _coerce_int,
    _coerce_optional_int,
    _normalize_log_level,
)
from .database import BridgeDatabase
from .store import EventBuffer


logger = logging.getLogger("tater_meshtastic_bridge")
_STATIC_CONFIG_SCHEMA_CACHE: Optional[Dict[str, Dict[str, Any]]] = None
_STATIC_CHANNEL_SCHEMA_CACHE: Optional[Dict[str, Any]] = None


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


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _slugify_firmware_token(value: Any) -> str:
    token = str(value or "").strip().lower()
    token = token.replace("+", "-plus").replace(".", "-").replace("_", "-")
    token = re.sub(r"[^a-z0-9]+", "-", token)
    return token.strip("-")


def _safe_filename(value: Any, fallback: str = "firmware.bin") -> str:
    name = Path(str(value or fallback)).name
    name = re.sub(r"[^A-Za-z0-9._+-]+", "_", name).strip("._")
    return name or fallback


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


_PROTO_SCALAR_KINDS = {
    FieldDescriptor.TYPE_BOOL: "bool",
    FieldDescriptor.TYPE_BYTES: "bytes",
    FieldDescriptor.TYPE_DOUBLE: "float",
    FieldDescriptor.TYPE_FIXED32: "int",
    FieldDescriptor.TYPE_FIXED64: "int",
    FieldDescriptor.TYPE_FLOAT: "float",
    FieldDescriptor.TYPE_INT32: "int",
    FieldDescriptor.TYPE_INT64: "int",
    FieldDescriptor.TYPE_SFIXED32: "int",
    FieldDescriptor.TYPE_SFIXED64: "int",
    FieldDescriptor.TYPE_SINT32: "int",
    FieldDescriptor.TYPE_SINT64: "int",
    FieldDescriptor.TYPE_STRING: "string",
    FieldDescriptor.TYPE_UINT32: "int",
    FieldDescriptor.TYPE_UINT64: "int",
}


def _labelize_name(name: str) -> str:
    token = str(name or "").strip().replace("_", " ")
    return " ".join(part[:1].upper() + part[1:] for part in token.split())


def _enum_default_name(field: FieldDescriptor) -> str:
    try:
        default_number = int(field.default_value)
        for value in field.enum_type.values:
            if int(value.number) == default_number:
                return str(value.name)
    except Exception:
        pass
    try:
        return str(field.enum_type.values[0].name)
    except Exception:
        return ""


def _compact_exception_message(exc: Exception) -> str:
    return " ".join(str(exc or exc.__class__.__name__).split())


def _proto_default_value(field: FieldDescriptor, *, repeated: Optional[bool] = None) -> Any:
    is_repeated = field.label == FieldDescriptor.LABEL_REPEATED if repeated is None else bool(repeated)
    if is_repeated:
        if field.type == FieldDescriptor.TYPE_MESSAGE and field.message_type and field.message_type.GetOptions().map_entry:
            return {}
        return []
    if field.type == FieldDescriptor.TYPE_MESSAGE:
        return {}
    if field.type == FieldDescriptor.TYPE_ENUM:
        return _enum_default_name(field)
    if field.type == FieldDescriptor.TYPE_BOOL:
        return bool(field.default_value)
    if field.type == FieldDescriptor.TYPE_BYTES:
        raw = field.default_value or b""
        return raw.decode("utf-8", errors="ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
    if field.type in _PROTO_SCALAR_KINDS:
        return field.default_value
    return None


def _enum_options(field: FieldDescriptor) -> List[Dict[str, Any]]:
    enum_type = getattr(field, "enum_type", None)
    if enum_type is None:
        return []
    return [
        {
            "label": str(value.name),
            "value": str(value.name),
            "number": int(value.number),
        }
        for value in enum_type.values
    ]


def _single_value_field_schema(field: FieldDescriptor) -> Dict[str, Any]:
    base = {
        "name": field.name,
        "label": _labelize_name(field.name),
        "number": int(field.number),
        "default": _proto_default_value(field, repeated=False),
    }

    if field.type == FieldDescriptor.TYPE_MESSAGE and field.message_type and field.message_type.GetOptions().map_entry:
        key_field = field.message_type.fields_by_name.get("key")
        value_field = field.message_type.fields_by_name.get("value")
        return {
            **base,
            "kind": "map",
            "default": {},
            "key_schema": _field_schema(key_field) if key_field is not None else {"kind": "string", "default": ""},
            "value_schema": _field_schema(value_field) if value_field is not None else {"kind": "string", "default": ""},
        }

    if field.type == FieldDescriptor.TYPE_MESSAGE:
        return {
            **base,
            "kind": "message",
            "fields": _descriptor_schema(field.message_type).get("fields", []),
            "default": {},
        }

    if field.type == FieldDescriptor.TYPE_ENUM:
        return {
            **base,
            "kind": "enum",
            "options": _enum_options(field),
        }

    return {
        **base,
        "kind": _PROTO_SCALAR_KINDS.get(field.type, "string"),
    }


def _field_schema(field: FieldDescriptor) -> Dict[str, Any]:
    if field.label == FieldDescriptor.LABEL_REPEATED and not (
        field.type == FieldDescriptor.TYPE_MESSAGE and field.message_type and field.message_type.GetOptions().map_entry
    ):
        base = {
            "name": field.name,
            "label": _labelize_name(field.name),
            "number": int(field.number),
            "kind": "array",
            "default": [],
            "item_schema": _single_value_field_schema(field),
        }
        return base
    return _single_value_field_schema(field)


def _descriptor_schema(descriptor: Optional[Descriptor], *, label: str = "") -> Dict[str, Any]:
    if descriptor is None:
        return {"kind": "message", "label": label, "fields": []}
    return {
        "kind": "message",
        "label": label or _labelize_name(getattr(descriptor, "name", "")),
        "fields": [_field_schema(field) for field in getattr(descriptor, "fields", []) or []],
    }


def _static_config_schemas() -> Dict[str, Dict[str, Any]]:
    global _STATIC_CONFIG_SCHEMA_CACHE
    if _STATIC_CONFIG_SCHEMA_CACHE is not None:
        return {
            "local": dict(_STATIC_CONFIG_SCHEMA_CACHE.get("local") or {}),
            "module": dict(_STATIC_CONFIG_SCHEMA_CACHE.get("module") or {}),
        }

    local_schemas: Dict[str, Any] = {}
    module_schemas: Dict[str, Any] = {}
    try:
        from meshtastic.protobuf import config_pb2, module_config_pb2

        for field in getattr(config_pb2.Config.DESCRIPTOR, "fields", []) or []:
            local_schemas[field.name] = _descriptor_schema(field.message_type, label=_labelize_name(field.name))
        for field in getattr(module_config_pb2.ModuleConfig.DESCRIPTOR, "fields", []) or []:
            module_schemas[field.name] = _descriptor_schema(field.message_type, label=_labelize_name(field.name))
    except Exception:
        logger.debug("Unable to build static Meshtastic config schemas", exc_info=True)

    _STATIC_CONFIG_SCHEMA_CACHE = {
        "local": local_schemas,
        "module": module_schemas,
    }
    return {
        "local": dict(local_schemas),
        "module": dict(module_schemas),
    }


def _static_channel_schema() -> Dict[str, Any]:
    global _STATIC_CHANNEL_SCHEMA_CACHE
    if _STATIC_CHANNEL_SCHEMA_CACHE is not None:
        return dict(_STATIC_CHANNEL_SCHEMA_CACHE)

    schema: Dict[str, Any] = {"kind": "message", "label": "Channel", "fields": []}
    try:
        from meshtastic.protobuf import channel_pb2

        schema = _descriptor_schema(channel_pb2.Channel.DESCRIPTOR, label="Channel")
    except Exception:
        logger.debug("Unable to build static Meshtastic channel schema", exc_info=True)

    _STATIC_CHANNEL_SCHEMA_CACHE = dict(schema)
    return dict(schema)


def _normalize_channel_record(record: Any) -> Dict[str, Any]:
    plain = dict(record or {}) if isinstance(record, dict) else {}
    return {
        **plain,
        "schema": dict(plain.get("schema") or _static_channel_schema()),
    }


def _normalize_config_snapshot(payload: Any) -> Dict[str, Any]:
    plain = dict(payload or {}) if isinstance(payload, dict) else {}
    static_schemas = _static_config_schemas()
    payload_schemas = plain.get("schemas") or {}
    local_schemas = dict(static_schemas.get("local") or {})
    local_schemas.update(dict((payload_schemas.get("local") or {})))
    module_schemas = dict(static_schemas.get("module") or {})
    module_schemas.update(dict((payload_schemas.get("module") or {})))
    return {
        "timestamp": plain.get("timestamp") or _utc_now_iso(),
        "local": dict(plain.get("local") or {}),
        "module": dict(plain.get("module") or {}),
        "schemas": {
            "local": local_schemas,
            "module": module_schemas,
        },
    }


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
        self._desired_port = int(settings.port)
        self.database = BridgeDatabase(settings.database_path)
        self.events = EventBuffer(settings.event_buffer_size, next_id=self.database.next_event_id())

        self._lock = Lock()
        self._radio_lock = RLock()
        self._ble_scan_lock = Lock()
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
            return [_normalize_channel_record(item) for item in list((cached.get("payload") or {}).get("channels") or [])]
        local_node = getattr(interface, "localNode", None)
        raw_channels = getattr(local_node, "channels", None)
        if raw_channels is None:
            cached = self.database.get_snapshot("channels")
            return [_normalize_channel_record(item) for item in list((cached.get("payload") or {}).get("channels") or [])]

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
                _normalize_channel_record(
                    {
                    "index": int(index),
                    "name": name,
                    "role": role,
                    "settings": settings,
                    "raw": plain,
                    }
                )
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

    def _qr_svg_for_text(self, text: str) -> str:
        try:
            pyqrcode = importlib.import_module("pyqrcode")
        except Exception as exc:
            raise RuntimeError("QR generation needs pyqrcode. Reinstall the bridge package to install the latest dependencies.") from exc

        qr = pyqrcode.create(str(text or "").strip(), error="M")
        svg = qr.svg_as_string(scale=5, xmldecl=False, svgns=True)
        if isinstance(svg, bytes):
            return svg.decode("utf-8", errors="replace")
        return str(svg or "")

    def channel_share_snapshot(self, *, refresh: bool = False) -> Dict[str, Any]:
        if refresh:
            self.snapshot_state()
        urls = self.channel_urls()
        shares: List[Dict[str, str]] = []
        for key, label, description in (
            ("primary", "Primary Channel", "Share only the primary channel information."),
            ("all", "All Channels", "Share the complete channel set from this radio."),
        ):
            url = str(urls.get(key) or "").strip()
            if not url:
                continue
            shares.append(
                {
                    "kind": key,
                    "label": label,
                    "description": description,
                    "url": url,
                    "svg": self._qr_svg_for_text(url),
                }
            )
        return {
            "ok": True,
            "urls": urls,
            "shares": shares,
            "count": len(shares),
        }

    def config_snapshot(self, *, refresh: bool = False) -> Dict[str, Any]:
        if refresh:
            self.snapshot_state()
        cached = self.database.get_snapshot("configs")
        payload = cached.get("payload") or {}
        if payload:
            return _normalize_config_snapshot(payload)
        return _normalize_config_snapshot(self._live_config_sections())

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

    def firmware_status(self, *, refresh: bool = False) -> Dict[str, Any]:
        device = self.device_snapshot(refresh=refresh)
        metadata = dict(device.get("metadata") or {}) if isinstance(device.get("metadata"), dict) else {}
        my_info = dict(device.get("my_info") or {}) if isinstance(device.get("my_info"), dict) else {}
        node_info = dict(device.get("node_info") or {}) if isinstance(device.get("node_info"), dict) else {}
        user = dict(node_info.get("user") or {}) if isinstance(node_info.get("user"), dict) else {}

        hardware_model = _first_text(
            metadata.get("hw_model"),
            metadata.get("hwModel"),
            metadata.get("hardware_model"),
            metadata.get("hardwareModel"),
            my_info.get("hw_model"),
            my_info.get("hwModel"),
            user.get("hw_model"),
            user.get("hwModel"),
        )
        firmware_version = _first_text(
            metadata.get("firmware_version"),
            metadata.get("firmwareVersion"),
            metadata.get("sw_version"),
            metadata.get("swVersion"),
            my_info.get("firmware_version"),
            my_info.get("firmwareVersion"),
        )
        device_role = _first_text(metadata.get("role"), my_info.get("role"), user.get("role"))
        candidates = self._firmware_target_candidates(hardware_model)
        return {
            "ok": True,
            "connected": bool(device.get("connected")),
            "transport": self.transport,
            "firmware_version": firmware_version,
            "hardware_model": hardware_model,
            "device_role": device_role,
            "target_candidates": candidates,
            "cache_dir": str(self._firmware_cache_dir()),
            "device": device,
            "notes": [
                "BLE can detect the radio and put some boards into DFU/OTA mode.",
                "ESP32 OTA flashing requires WiFi/TCP and firmware support.",
                "USB/Web Serial or drag-and-drop flashing is still recommended for full erase or bootloader recovery.",
            ],
        }

    def firmware_releases(self, *, include_prerelease: bool = False, limit: int = 8) -> Dict[str, Any]:
        releases = self._github_releases(limit=max(1, min(int(limit or 8), 30)))
        status = self.firmware_status(refresh=False)
        candidates = list(status.get("target_candidates") or [])
        rows: List[Dict[str, Any]] = []
        for release in releases:
            if release.get("draft"):
                continue
            if release.get("prerelease") and not include_prerelease:
                continue
            assets = [self._normalize_firmware_asset(asset, candidates) for asset in release.get("assets") or []]
            assets = [asset for asset in assets if asset.get("firmware_like")]
            rows.append(
                {
                    "id": release.get("id"),
                    "name": release.get("name") or release.get("tag_name"),
                    "tag_name": release.get("tag_name"),
                    "published_at": release.get("published_at"),
                    "prerelease": bool(release.get("prerelease")),
                    "html_url": release.get("html_url"),
                    "assets": sorted(assets, key=lambda item: (-int(item.get("match_score") or 0), str(item.get("name") or ""))),
                }
            )
            if len(rows) >= limit:
                break
        return {
            "ok": True,
            "source": "https://api.github.com/repos/meshtastic/firmware/releases",
            "target_candidates": candidates,
            "releases": rows,
            "count": len(rows),
        }

    def list_firmware_files(self) -> Dict[str, Any]:
        cache_dir = self._firmware_cache_dir()
        files: List[Dict[str, Any]] = []
        if cache_dir.exists():
            for path in sorted(cache_dir.rglob("*")):
                if not path.is_file():
                    continue
                stat = path.stat()
                files.append(
                    {
                        "name": path.name,
                        "path": str(path),
                        "tag_name": path.parent.name if path.parent != cache_dir else "",
                        "size": stat.st_size,
                        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    }
                )
        return {"ok": True, "cache_dir": str(cache_dir), "files": files, "count": len(files)}

    def download_firmware_asset(self, *, asset_url: str, asset_name: str, tag_name: str = "") -> Dict[str, Any]:
        url = str(asset_url or "").strip()
        if not url.startswith("https://github.com/meshtastic/firmware/releases/download/"):
            raise ValueError("Firmware asset URL must be from meshtastic/firmware GitHub releases.")
        safe_name = _safe_filename(asset_name)
        safe_tag = _safe_filename(tag_name or "download", fallback="download")
        target_dir = self._firmware_cache_dir() / safe_tag
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / safe_name
        request = Request(url, headers={"User-Agent": "tater-meshtastic-bridge"})
        try:
            with urlopen(request, timeout=60) as response, target_path.open("wb") as out:
                while True:
                    chunk = response.read(1024 * 512)
                    if not chunk:
                        break
                    out.write(chunk)
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(f"Firmware download failed: {exc}") from exc
        stat = target_path.stat()
        payload = {
            "ok": True,
            "file": {
                "name": target_path.name,
                "path": str(target_path),
                "tag_name": safe_tag,
                "size": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            },
        }
        self.database.record_audit(timestamp=_utc_now_iso(), action="download_firmware", target=target_path.name, status="ok", details=payload)
        return payload

    def ota_update_firmware(self, *, file_path: str, tcp_host: str, timeout_seconds: int = 600) -> Dict[str, Any]:
        host = str(tcp_host or "").strip()
        if not host:
            raise ValueError("A Meshtastic WiFi/TCP host is required for OTA firmware update.")
        path = Path(str(file_path or "")).expanduser().resolve()
        cache_dir = self._firmware_cache_dir().resolve()
        try:
            path.relative_to(cache_dir)
        except ValueError as exc:
            raise ValueError("Firmware OTA can only use files downloaded into the bridge firmware cache.") from exc
        if not path.exists() or not path.is_file():
            raise ValueError("Firmware file does not exist.")
        wait = max(60, min(int(timeout_seconds or 600), 1800))
        command = [
            sys.executable,
            "-m",
            "meshtastic",
            "--host",
            host,
            "--ota-update",
            str(path),
            "--timeout",
            str(wait),
            "--no-nodes",
        ]
        timestamp = _utc_now_iso()
        try:
            result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=wait + 30)
        except subprocess.TimeoutExpired as exc:
            self.database.record_audit(
                timestamp=timestamp,
                action="ota_update_firmware",
                target=host,
                status="error",
                details={"file": str(path), "error": "Timed out waiting for OTA update to finish."},
            )
            raise RuntimeError("Timed out waiting for OTA update to finish.") from exc
        payload = {
            "ok": result.returncode == 0,
            "host": host,
            "file": str(path),
            "returncode": result.returncode,
            "stdout": (result.stdout or "")[-6000:],
            "stderr": (result.stderr or "")[-6000:],
        }
        self.database.record_audit(
            timestamp=timestamp,
            action="ota_update_firmware",
            target=host,
            status="ok" if result.returncode == 0 else "error",
            details=payload,
        )
        if result.returncode != 0:
            raise RuntimeError(payload["stderr"] or payload["stdout"] or f"OTA update failed with exit code {result.returncode}.")
        return payload

    def _firmware_cache_dir(self) -> Path:
        return Path(self.settings.database_path).expanduser().resolve().parent / "firmware"

    def _github_releases(self, *, limit: int = 8) -> List[Dict[str, Any]]:
        per_page = max(1, min(int(limit or 8) * 3, 50))
        url = f"https://api.github.com/repos/meshtastic/firmware/releases?per_page={per_page}"
        request = Request(url, headers={"Accept": "application/vnd.github+json", "User-Agent": "tater-meshtastic-bridge"})
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Unable to fetch Meshtastic firmware releases: {exc}") from exc
        return payload if isinstance(payload, list) else []

    def _firmware_target_candidates(self, hardware_model: str) -> List[str]:
        base = _slugify_firmware_token(hardware_model)
        candidates = [base] if base else []
        aliases = {
            "thinknode-m6": ["thinknode-m6"],
            "thinknode-m5": ["thinknode-m5"],
            "thinknode-m4": ["thinknode-m4"],
            "heltec-v3": ["heltec-v3"],
            "heltec-v4": ["heltec-v4"],
            "heltec-mesh-node-t114": ["heltec-mesh-node-t114", "t114"],
            "heltec-wireless-tracker": ["heltec-wireless-tracker"],
            "heltec-wireless-tracker-v2": ["heltec-wireless-tracker-v2"],
            "t-deck": ["t-deck"],
            "t-deck-pro": ["t-deck-pro"],
            "t-echo": ["t-echo", "techo"],
            "t-echo-lite": ["t-echo-lite"],
            "rak4631": ["rak4631"],
            "rak11200": ["rak11200"],
            "seeed-xiao-s3": ["seeed-xiao-esp32s3", "seeed-xiao-s3"],
            "tracker-t1000-e": ["t1000-e", "tracker-t1000-e"],
            "tbeam": ["tbeam"],
            "lilygo-tbeam-s3-core": ["tbeam-s3-core", "lilygo-tbeam-s3-core"],
        }
        for key, values in aliases.items():
            if base == key or base.endswith(key):
                candidates.extend(values)
        for candidate in list(candidates):
            if candidate.startswith("hardware-model-"):
                candidates.append(candidate.removeprefix("hardware-model-"))
        deduped: List[str] = []
        for candidate in candidates:
            if candidate and candidate not in deduped:
                deduped.append(candidate)
        return deduped

    def _normalize_firmware_asset(self, asset: Dict[str, Any], candidates: List[str]) -> Dict[str, Any]:
        name = str(asset.get("name") or "").strip()
        lowered = name.lower()
        firmware_like = lowered.endswith((".zip", ".bin", ".uf2", ".hex")) or "firmware" in lowered
        score = 0
        for candidate in candidates:
            if candidate and candidate in lowered:
                score += 10
        if lowered.endswith(".zip"):
            score += 2
        if "update" in lowered or "ota" in lowered:
            score += 1
        return {
            "id": asset.get("id"),
            "name": name,
            "size": asset.get("size") or 0,
            "download_count": asset.get("download_count") or 0,
            "content_type": asset.get("content_type") or "",
            "browser_download_url": asset.get("browser_download_url") or "",
            "updated_at": asset.get("updated_at") or asset.get("created_at") or "",
            "firmware_like": firmware_like,
            "match_score": score,
        }

    def snapshot_state(self) -> Dict[str, Any]:
        timestamp = _utc_now_iso()
        interface = self._interface
        if interface is None:
            status = self.status_snapshot()
            snapshots = {
                "device": {**status},
                "channels": {"channels": self.database.get_snapshot("channels").get("payload", {}).get("channels", [])},
                "configs": _normalize_config_snapshot(self.database.get_snapshot("configs").get("payload", {})),
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

    def _runtime_settings_values(self, *, desired: bool = True) -> Dict[str, Any]:
        return {
            "host": self.settings.host,
            "port": int(self._desired_port if desired else self.settings.port),
            "database_path": self.settings.database_path,
            "device_name": self.settings.device_name,
            "device_address": self.settings.device_address,
            "api_token": self.settings.api_token,
            "reconnect_seconds": float(self.settings.reconnect_seconds),
            "connect_timeout_seconds": int(self.settings.connect_timeout_seconds),
            "event_buffer_size": int(self.settings.event_buffer_size),
            "log_level": self.settings.log_level,
            "include_outbound_events": bool(self.settings.include_outbound_events),
            "want_ack": bool(self.settings.want_ack),
            "default_hop_limit": self.settings.default_hop_limit,
            "no_nodes": bool(self.settings.no_nodes),
            "shutdown_timeout_seconds": float(self.settings.shutdown_timeout_seconds),
        }

    def _normalize_runtime_settings(self, values: Dict[str, Any]) -> Dict[str, Any]:
        current = self._runtime_settings_values(desired=True)
        raw = dict(values or {})
        return {
            "port": _coerce_int(raw.get("port", current["port"]), int(current["port"] or DEFAULT_BRIDGE_PORT), minimum=1, maximum=65535),
            "device_name": str(raw.get("device_name", current["device_name"]) or "").strip(),
            "device_address": str(raw.get("device_address", current["device_address"]) or "").strip(),
            "api_token": str(raw.get("api_token", current["api_token"]) or "").strip(),
            "reconnect_seconds": _coerce_float(raw.get("reconnect_seconds", current["reconnect_seconds"]), float(current["reconnect_seconds"] or 10.0), minimum=1.0),
            "connect_timeout_seconds": _coerce_int(raw.get("connect_timeout_seconds", current["connect_timeout_seconds"]), int(current["connect_timeout_seconds"] or 60), minimum=5),
            "event_buffer_size": _coerce_int(raw.get("event_buffer_size", current["event_buffer_size"]), int(current["event_buffer_size"] or 500), minimum=50),
            "log_level": _normalize_log_level(raw.get("log_level", current["log_level"])),
            "include_outbound_events": _coerce_bool(raw.get("include_outbound_events", current["include_outbound_events"]), bool(current["include_outbound_events"])),
            "want_ack": _coerce_bool(raw.get("want_ack", current["want_ack"]), bool(current["want_ack"])),
            "default_hop_limit": _coerce_optional_int(raw.get("default_hop_limit", current["default_hop_limit"])),
            "no_nodes": _coerce_bool(raw.get("no_nodes", current["no_nodes"]), bool(current["no_nodes"])),
            "shutdown_timeout_seconds": _coerce_float(
                raw.get("shutdown_timeout_seconds", current["shutdown_timeout_seconds"]),
                float(current["shutdown_timeout_seconds"] or 2.0),
                minimum=0.1,
            ),
        }

    def runtime_settings_snapshot(self) -> Dict[str, Any]:
        saved = self.database.get_bridge_settings()
        sources = dict(self.settings.settings_sources or {})
        values = self._runtime_settings_values(desired=True)
        active = self._runtime_settings_values(desired=False)

        if sources.get("port") != "env" and "port" in saved:
            values["port"] = _coerce_int(saved.get("port"), int(values.get("port") or DEFAULT_BRIDGE_PORT), minimum=1, maximum=65535)
        for key in BRIDGE_SETTING_KEYS:
            if key in saved and sources.get(key) != "env" and key != "port":
                values[key] = self._normalize_runtime_settings({key: saved.get(key)}).get(key, values.get(key))

        return {
            "ok": True,
            "values": values,
            "active": active,
            "sources": sources,
            "env_overrides": {
                "port": sources.get("port") == "env",
                "database_path": sources.get("database_path") == "env",
            },
            "restart_required_keys": sorted(RESTART_REQUIRED_SETTINGS),
            "restart_required": values.get("port") != active.get("port"),
            "notes": {
                "host": "The bridge always binds to 0.0.0.0 so other machines on the LAN can reach it.",
                "port": "MESHTASTIC_BRIDGE_PORT is only treated as a hard override when it is set to a non-default port.",
                "database_path": "The database path is a startup-only setting and still comes from MESHTASTIC_DATABASE_PATH or the platform default.",
            },
        }

    def update_runtime_settings(self, *, values: Dict[str, Any]) -> Dict[str, Any]:
        before = self.runtime_settings_snapshot()
        normalized = self._normalize_runtime_settings(values)
        sources = dict(self.settings.settings_sources or {})

        if sources.get("port") == "env":
            normalized["port"] = int(before.get("values", {}).get("port") or self.settings.port)

        timestamp = _utc_now_iso()
        self.database.save_bridge_settings(normalized, timestamp=timestamp)

        self._desired_port = int(normalized["port"])
        for key, value in normalized.items():
            if key == "port":
                sources[key] = "env" if sources.get("port") == "env" else "ui"
                continue
            if hasattr(self.settings, key):
                setattr(self.settings, key, value)
                sources[key] = "ui"

        self.settings.settings_sources = sources
        self.device_name = self.local_long_name or self.settings.device_name or self.settings.device_address or "Meshtastic"
        logging.getLogger().setLevel(getattr(logging, str(self.settings.log_level or "INFO").upper(), logging.INFO))
        logger.setLevel(getattr(logging, str(self.settings.log_level or "INFO").upper(), logging.INFO))

        after = self.runtime_settings_snapshot()
        changed = [
            key
            for key in sorted(set((before.get("values") or {}).keys()) | set((after.get("values") or {}).keys()))
            if (before.get("values") or {}).get(key) != (after.get("values") or {}).get(key)
        ]
        restart_keys = sorted(key for key in changed if key in RESTART_REQUIRED_SETTINGS)
        after["changed_keys"] = changed
        after["restart_required_keys"] = sorted(set(after.get("restart_required_keys") or []) | set(restart_keys))
        after["restart_required"] = bool(restart_keys or after.get("restart_required"))

        self.database.record_audit(
            timestamp=timestamp,
            action="update_bridge_settings",
            target="bridge",
            status="ok",
            details={
                "changed_keys": changed,
                "restart_required_keys": restart_keys,
                "env_overrides": after.get("env_overrides") or {},
            },
        )
        return after

    def clear_runtime_settings(self) -> Dict[str, Any]:
        before = self.runtime_settings_snapshot()
        previous_saved = self.database.clear_bridge_settings()
        sources = dict(self.settings.settings_sources or {})
        normalized = self._normalize_runtime_settings(dict(DEFAULT_BRIDGE_SETTINGS))

        if sources.get("port") == "env":
            normalized["port"] = int(before.get("values", {}).get("port") or self.settings.port)

        self._desired_port = int(normalized["port"])
        for key, value in normalized.items():
            if key == "port":
                sources[key] = "env" if sources.get("port") == "env" else "default"
                continue
            if hasattr(self.settings, key):
                setattr(self.settings, key, value)
                sources[key] = "default"

        sources["host"] = "fixed"
        sources["database_path"] = sources.get("database_path") or "default"
        self.settings.settings_sources = sources
        self.device_name = self.local_long_name or self.settings.device_name or self.settings.device_address or "Meshtastic"
        logging.getLogger().setLevel(getattr(logging, str(self.settings.log_level or "INFO").upper(), logging.INFO))
        logger.setLevel(getattr(logging, str(self.settings.log_level or "INFO").upper(), logging.INFO))

        after = self.runtime_settings_snapshot()
        changed = [
            key
            for key in sorted(set((before.get("values") or {}).keys()) | set((after.get("values") or {}).keys()))
            if (before.get("values") or {}).get(key) != (after.get("values") or {}).get(key)
        ]
        restart_keys = sorted(key for key in changed if key in RESTART_REQUIRED_SETTINGS)
        after["changed_keys"] = changed
        after["restart_required_keys"] = sorted(set(after.get("restart_required_keys") or []) | set(restart_keys))
        after["restart_required"] = bool(restart_keys or after.get("restart_required"))
        after["cleared_settings"] = previous_saved

        self.database.record_audit(
            timestamp=_utc_now_iso(),
            action="clear_bridge_settings",
            target="bridge",
            status="ok",
            details={
                "changed_keys": changed,
                "restart_required_keys": restart_keys,
                "cleared_settings": previous_saved,
            },
        )
        return after

    def _apply_default_runtime_settings(self, *, before: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        before = before or self.runtime_settings_snapshot()
        sources = dict(self.settings.settings_sources or {})
        normalized = self._normalize_runtime_settings(dict(DEFAULT_BRIDGE_SETTINGS))

        if sources.get("port") == "env":
            normalized["port"] = int(before.get("values", {}).get("port") or self.settings.port)

        self._desired_port = int(normalized["port"])
        for key, value in normalized.items():
            if key == "port":
                sources[key] = "env" if sources.get("port") == "env" else "default"
                continue
            if hasattr(self.settings, key):
                setattr(self.settings, key, value)
                sources[key] = "default"

        sources["host"] = "fixed"
        sources["database_path"] = sources.get("database_path") or "default"
        self.settings.settings_sources = sources
        self.device_name = self.local_long_name or self.settings.device_name or self.settings.device_address or "Meshtastic"
        logging.getLogger().setLevel(getattr(logging, str(self.settings.log_level or "INFO").upper(), logging.INFO))
        logger.setLevel(getattr(logging, str(self.settings.log_level or "INFO").upper(), logging.INFO))
        return normalized

    def clear_all_data(self) -> Dict[str, Any]:
        before = self.runtime_settings_snapshot()
        cleared = self.database.clear_all_data()
        self.events.clear(next_id=1)
        self._snapshot_cache = {}
        self._apply_default_runtime_settings(before=before)
        after = self.runtime_settings_snapshot()
        changed = [
            key
            for key in sorted(set((before.get("values") or {}).keys()) | set((after.get("values") or {}).keys()))
            if (before.get("values") or {}).get(key) != (after.get("values") or {}).get(key)
        ]
        restart_keys = sorted(key for key in changed if key in RESTART_REQUIRED_SETTINGS)
        after["changed_keys"] = changed
        after["restart_required_keys"] = sorted(set(after.get("restart_required_keys") or []) | set(restart_keys))
        after["restart_required"] = bool(restart_keys or after.get("restart_required"))
        after["cleared"] = cleared
        logger.warning("Cleared all Meshtastic bridge data and saved settings")
        return after

    def scan_ble_devices(self) -> Dict[str, Any]:
        if not self._ble_scan_lock.acquire(blocking=False):
            raise RuntimeError("A BLE scan is already running.")

        started_at = _utc_now_iso()
        try:
            ble_module = importlib.import_module("meshtastic.ble_interface")
            interface_cls = getattr(ble_module, "BLEInterface")
            logger.info("Scanning for Meshtastic BLE devices from bridge API")
            devices = interface_cls.scan()
            configured_name = str(self.settings.device_name or "").strip()
            configured_address = str(self.settings.device_address or "").strip()
            normalized: List[Dict[str, Any]] = []
            for index, device in enumerate(devices):
                name = str(getattr(device, "name", "") or "").strip()
                address = str(getattr(device, "address", "") or "").strip()
                rssi = getattr(device, "rssi", None)
                normalized.append(
                    {
                        "index": index,
                        "name": name,
                        "address": address,
                        "identifier": address or name,
                        "rssi": rssi if isinstance(rssi, (int, float)) else None,
                        "matches_config": bool((configured_address and address == configured_address) or (configured_name and name == configured_name)),
                    }
                )
            return {
                "ok": True,
                "transport": "ble",
                "started_at": started_at,
                "finished_at": _utc_now_iso(),
                "devices": normalized,
                "count": len(normalized),
            }
        except RuntimeError:
            raise
        except Exception as exc:
            logger.warning("Meshtastic BLE scan failed: %s", exc)
            raise RuntimeError(f"BLE scan failed: {exc}") from exc
        finally:
            self._ble_scan_lock.release()

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
            payload = cached.get("payload") or {}
            if payload:
                return _normalize_config_snapshot(payload)
            return _normalize_config_snapshot({})

        local_node = getattr(interface, "localNode", None)
        if local_node is None:
            return _normalize_config_snapshot({})

        local_sections: Dict[str, Any] = {}
        local_schemas: Dict[str, Any] = {}
        for field in getattr(local_node.localConfig.DESCRIPTOR, "fields", []) or []:
            try:
                local_sections[field.name] = _proto_to_dict(getattr(local_node.localConfig, field.name))
            except Exception:
                local_sections[field.name] = {}
            local_schemas[field.name] = _descriptor_schema(getattr(getattr(local_node.localConfig, field.name), "DESCRIPTOR", None), label=_labelize_name(field.name))

        module_sections: Dict[str, Any] = {}
        module_schemas: Dict[str, Any] = {}
        for field in getattr(local_node.moduleConfig.DESCRIPTOR, "fields", []) or []:
            try:
                module_sections[field.name] = _proto_to_dict(getattr(local_node.moduleConfig, field.name))
            except Exception:
                module_sections[field.name] = {}
            module_schemas[field.name] = _descriptor_schema(getattr(getattr(local_node.moduleConfig, field.name), "DESCRIPTOR", None), label=_labelize_name(field.name))

        return _normalize_config_snapshot(
            {
                "timestamp": _utc_now_iso(),
                "local": local_sections,
                "module": module_sections,
                "schemas": {
                    "local": local_schemas,
                    "module": module_schemas,
                },
            }
        )

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
                friendly_error = self._friendly_connection_error(exc)
                logger.warning("Meshtastic BLE connection failed: %s", friendly_error)
                logger.debug("Meshtastic bridge connection loop failed", exc_info=True)
                self._set_state(connected=False, state="reconnecting", error=friendly_error)
            finally:
                self._close_interface()
                if self._stop_event.is_set():
                    break
                self.reconnect_attempt += 1
                self._set_state(connected=False, state="reconnecting", error=self.last_error)
                delay = self.settings.reconnect_seconds * min(6, max(1, self.reconnect_attempt))
                if self._stop_event.wait(delay):
                    break

    def _friendly_connection_error(self, exc: Exception) -> str:
        raw = _compact_exception_message(exc)
        lowered = raw.lower()
        identifier = self.settings.device_identifier or "the configured Meshtastic device"

        if "peer removed pairing information" in lowered:
            return (
                "Bluetooth pairing is stale for "
                f"{identifier}. macOS says the radio removed pairing information. "
                "Forget/remove this device in macOS Bluetooth settings, restart the radio, then use Settings -> Scan for Devices and save the fresh address."
            )
        if "no meshtastic ble peripheral" in lowered or "no device found" in lowered:
            return (
                f"No Meshtastic BLE device was found for {identifier}. "
                "Use Settings -> Scan for Devices and save the current name or address."
            )
        if "more than one meshtastic ble peripheral" in lowered:
            return (
                f"More than one Meshtastic BLE device matched {identifier}. "
                "Use Settings -> Scan for Devices and save the exact BLE address instead of the name."
            )
        if "failed to connect" in lowered or "connection failed" in lowered or "esp_err_http_connect" in lowered:
            return (
                f"Could not connect to {identifier}. "
                "Make sure the radio is nearby, powered on, not already connected to another app, and re-scan if the address changed."
            )
        if "timed out" in lowered or "timeout" in lowered:
            return (
                f"Timed out connecting to {identifier}. "
                "Move the radio closer, wake/restart it, then scan again if needed."
            )
        return raw or exc.__class__.__name__

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
