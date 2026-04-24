from __future__ import annotations

import os
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    token = raw.strip().lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _int_env(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    try:
        value = int(float(raw.strip())) if raw and raw.strip() else int(default)
    except Exception:
        value = int(default)
    return max(int(minimum), value)


def _float_env(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw.strip()) if raw and raw.strip() else float(default)
    except Exception:
        value = float(default)
    return max(float(minimum), value)


def _optional_int_env(name: str) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        return int(float(raw.strip()))
    except Exception:
        return None


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


def load_settings() -> Settings:
    return Settings(
        host=os.getenv("MESHTASTIC_BRIDGE_HOST", "127.0.0.1").strip() or "127.0.0.1",
        port=_int_env("MESHTASTIC_BRIDGE_PORT", 8433, minimum=1),
        database_path=os.getenv("MESHTASTIC_DATABASE_PATH", _default_database_path()).strip() or _default_database_path(),
        device_name=os.getenv("MESHTASTIC_DEVICE_NAME", "").strip(),
        device_address=os.getenv("MESHTASTIC_DEVICE_ADDRESS", "").strip(),
        api_token=os.getenv("MESHTASTIC_API_TOKEN", "").strip(),
        reconnect_seconds=_float_env("MESHTASTIC_RECONNECT_SECONDS", 10.0, minimum=1.0),
        connect_timeout_seconds=_int_env("MESHTASTIC_CONNECT_TIMEOUT_SECONDS", 60, minimum=5),
        event_buffer_size=_int_env("MESHTASTIC_EVENT_BUFFER_SIZE", 500, minimum=50),
        log_level=(os.getenv("MESHTASTIC_LOG_LEVEL", "INFO").strip() or "INFO").upper(),
        include_outbound_events=_bool_env("MESHTASTIC_INCLUDE_OUTBOUND_EVENTS", True),
        want_ack=_bool_env("MESHTASTIC_WANT_ACK", False),
        default_hop_limit=_optional_int_env("MESHTASTIC_DEFAULT_HOP_LIMIT"),
        no_nodes=_bool_env("MESHTASTIC_NO_NODES", False),
        shutdown_timeout_seconds=_float_env("MESHTASTIC_SHUTDOWN_TIMEOUT_SECONDS", 2.0, minimum=0.1),
    )
