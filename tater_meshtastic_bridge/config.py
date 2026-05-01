from __future__ import annotations

import os
import platform
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv


load_dotenv()


DEFAULT_BRIDGE_HOST = "0.0.0.0"
DEFAULT_BRIDGE_PORT = 8433

BRIDGE_SETTING_KEYS = (
    "port",
    "device_name",
    "device_address",
    "api_token",
    "reconnect_seconds",
    "connect_timeout_seconds",
    "event_buffer_size",
    "log_level",
    "include_outbound_events",
    "want_ack",
    "default_hop_limit",
    "no_nodes",
    "shutdown_timeout_seconds",
)

RESTART_REQUIRED_SETTINGS = {
    "port",
    "device_name",
    "device_address",
    "connect_timeout_seconds",
    "event_buffer_size",
    "no_nodes",
}

ENV_NAMES = {
    "port": "MESHTASTIC_BRIDGE_PORT",
    "database_path": "MESHTASTIC_DATABASE_PATH",
    "device_name": "MESHTASTIC_DEVICE_NAME",
    "device_address": "MESHTASTIC_DEVICE_ADDRESS",
    "api_token": "MESHTASTIC_API_TOKEN",
    "reconnect_seconds": "MESHTASTIC_RECONNECT_SECONDS",
    "connect_timeout_seconds": "MESHTASTIC_CONNECT_TIMEOUT_SECONDS",
    "event_buffer_size": "MESHTASTIC_EVENT_BUFFER_SIZE",
    "log_level": "MESHTASTIC_LOG_LEVEL",
    "include_outbound_events": "MESHTASTIC_INCLUDE_OUTBOUND_EVENTS",
    "want_ack": "MESHTASTIC_WANT_ACK",
    "default_hop_limit": "MESHTASTIC_DEFAULT_HOP_LIMIT",
    "no_nodes": "MESHTASTIC_NO_NODES",
    "shutdown_timeout_seconds": "MESHTASTIC_SHUTDOWN_TIMEOUT_SECONDS",
}

DEFAULT_BRIDGE_SETTINGS: Dict[str, Any] = {
    "port": DEFAULT_BRIDGE_PORT,
    "device_name": "",
    "device_address": "",
    "api_token": "",
    "reconnect_seconds": 10.0,
    "connect_timeout_seconds": 60,
    "event_buffer_size": 500,
    "log_level": "INFO",
    "include_outbound_events": True,
    "want_ack": False,
    "default_hop_limit": None,
    "no_nodes": False,
    "shutdown_timeout_seconds": 2.0,
}


def _env_has_value(name: str) -> bool:
    raw = os.getenv(name)
    return raw is not None and str(raw).strip() != ""


def _coerce_bool(raw: Any, default: bool) -> bool:
    if raw is None:
        return default
    token = str(raw).strip().lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _coerce_int(raw: Any, default: int, minimum: int = 0, maximum: Optional[int] = None) -> int:
    try:
        value = int(float(str(raw).strip())) if raw is not None and str(raw).strip() else int(default)
    except Exception:
        value = int(default)
    value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def _coerce_float(raw: Any, default: float, minimum: float = 0.0, maximum: Optional[float] = None) -> float:
    try:
        value = float(str(raw).strip()) if raw is not None and str(raw).strip() else float(default)
    except Exception:
        value = float(default)
    value = max(float(minimum), value)
    if maximum is not None:
        value = min(float(maximum), value)
    return value


def _coerce_optional_int(raw: Any) -> Optional[int]:
    if raw is None or not str(raw).strip():
        return None
    try:
        return int(float(str(raw).strip()))
    except Exception:
        return None


def _bool_env(name: str, default: bool) -> bool:
    return _coerce_bool(os.getenv(name), default)


def _int_env(name: str, default: int, minimum: int = 0) -> int:
    return _coerce_int(os.getenv(name), default, minimum=minimum)


def _float_env(name: str, default: float, minimum: float = 0.0) -> float:
    return _coerce_float(os.getenv(name), default, minimum=minimum)


def _optional_int_env(name: str) -> Optional[int]:
    return _coerce_optional_int(os.getenv(name))


@dataclass(slots=True)
class Settings:
    host: str
    port: int
    database_path: str
    device_name: str
    device_address: str
    api_token: str
    reconnect_seconds: float
    connect_timeout_seconds: int
    event_buffer_size: int
    log_level: str
    include_outbound_events: bool
    want_ack: bool
    default_hop_limit: Optional[int]
    no_nodes: bool
    shutdown_timeout_seconds: float
    settings_sources: Dict[str, str] = field(default_factory=dict)

    @property
    def device_identifier(self) -> Optional[str]:
        if self.device_address.strip():
            return self.device_address.strip()
        if self.device_name.strip():
            return self.device_name.strip()
        return None


def _default_database_path() -> str:
    home = Path.home()
    system = platform.system().lower()
    if system == "darwin":
        base = home / "Library" / "Application Support" / "Tater Meshtastic Bridge"
    elif system == "windows":
        base = Path(os.getenv("APPDATA", home)) / "Tater Meshtastic Bridge"
    else:
        base = Path(os.getenv("XDG_DATA_HOME", home / ".local" / "share")) / "tater_meshtastic_bridge"
    return str((base / "bridge.sqlite3").expanduser())


def _load_persisted_settings(database_path: str) -> Dict[str, Any]:
    try:
        from .database import BridgeDatabase

        return BridgeDatabase(database_path).get_bridge_settings()
    except Exception:
        return {}


def _setting_value(persisted: Dict[str, Any], key: str, default: Any) -> tuple[Any, str]:
    if key in persisted:
        return persisted.get(key), "ui"
    env_name = ENV_NAMES.get(key, "")
    if env_name and _env_has_value(env_name):
        return os.getenv(env_name), "env"
    return default, "default"


def _normalize_log_level(raw: Any) -> str:
    token = str(raw or "INFO").strip().upper() or "INFO"
    return token if token in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"} else "INFO"


def load_settings() -> Settings:
    database_path = os.getenv("MESHTASTIC_DATABASE_PATH", _default_database_path()).strip() or _default_database_path()
    persisted = _load_persisted_settings(database_path)
    sources: Dict[str, str] = {"host": "fixed", "database_path": "env" if _env_has_value("MESHTASTIC_DATABASE_PATH") else "default"}

    raw_values: Dict[str, Any] = {}
    for key in BRIDGE_SETTING_KEYS:
        if key == "port":
            if key in persisted:
                raw_values[key] = persisted.get(key)
                sources[key] = "ui"
            else:
                raw_values[key] = DEFAULT_BRIDGE_SETTINGS.get(key)
                sources[key] = "default"
            continue
        default = DEFAULT_BRIDGE_SETTINGS.get(key)
        raw, source = _setting_value(persisted, key, default)
        raw_values[key] = raw
        sources[key] = source

    if _env_has_value("MESHTASTIC_BRIDGE_PORT"):
        env_port = _coerce_int(os.getenv("MESHTASTIC_BRIDGE_PORT"), DEFAULT_BRIDGE_PORT, minimum=1, maximum=65535)
        if env_port != DEFAULT_BRIDGE_PORT:
            raw_values["port"] = env_port
            sources["port"] = "env"

    return Settings(
        host=DEFAULT_BRIDGE_HOST,
        port=_coerce_int(raw_values.get("port"), DEFAULT_BRIDGE_PORT, minimum=1, maximum=65535),
        database_path=database_path,
        device_name=str(raw_values.get("device_name") or "").strip(),
        device_address=str(raw_values.get("device_address") or "").strip(),
        api_token=str(raw_values.get("api_token") or "").strip(),
        reconnect_seconds=_coerce_float(raw_values.get("reconnect_seconds"), 10.0, minimum=1.0),
        connect_timeout_seconds=_coerce_int(raw_values.get("connect_timeout_seconds"), 60, minimum=5),
        event_buffer_size=_coerce_int(raw_values.get("event_buffer_size"), 500, minimum=50),
        log_level=_normalize_log_level(raw_values.get("log_level")),
        include_outbound_events=_coerce_bool(raw_values.get("include_outbound_events"), True),
        want_ack=_coerce_bool(raw_values.get("want_ack"), False),
        default_hop_limit=_coerce_optional_int(raw_values.get("default_hop_limit")),
        no_nodes=_coerce_bool(raw_values.get("no_nodes"), False),
        shutdown_timeout_seconds=_coerce_float(raw_values.get("shutdown_timeout_seconds"), 2.0, minimum=0.1),
        settings_sources=sources,
    )
