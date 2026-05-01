from __future__ import annotations

import logging
import ipaddress
import socket
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import __version__
from .config import load_settings
from .service import MeshtasticBridgeService


class SendMessageRequest(BaseModel):
    text: str = Field(..., description="Plain text to send into the mesh.")
    channel: int = Field(0, ge=0, description="Meshtastic channel index.")
    destination: str = Field("broadcast", description="broadcast, ^local, or a direct node id such as !abcd1234.")


class OwnerUpdateRequest(BaseModel):
    long_name: Optional[str] = None
    short_name: Optional[str] = None
    is_licensed: bool = False
    is_unmessagable: Optional[bool] = None


class ChannelUrlRequest(BaseModel):
    url: str
    add_only: bool = False


class ConfigSectionRequest(BaseModel):
    values: Dict[str, Any] = Field(default_factory=dict)


class ChannelUpdateRequest(BaseModel):
    channel: Dict[str, Any] = Field(default_factory=dict)


class FixedPositionRequest(BaseModel):
    latitude: float
    longitude: float
    altitude: int = 0


class TextBlobRequest(BaseModel):
    text: str


class DeviceActionRequest(BaseModel):
    seconds: int = Field(10, ge=0, le=3600)


class RuntimeSettingsRequest(BaseModel):
    port: Optional[int] = Field(None, ge=1, le=65535)
    device_name: Optional[str] = None
    device_address: Optional[str] = None
    api_token: Optional[str] = None
    reconnect_seconds: Optional[float] = Field(None, ge=1.0)
    connect_timeout_seconds: Optional[int] = Field(None, ge=5)
    event_buffer_size: Optional[int] = Field(None, ge=50)
    log_level: Optional[str] = None
    include_outbound_events: Optional[bool] = None
    want_ack: Optional[bool] = None
    default_hop_limit: Optional[int] = None
    no_nodes: Optional[bool] = None
    shutdown_timeout_seconds: Optional[float] = Field(None, ge=0.1)


class FirmwareDownloadRequest(BaseModel):
    asset_url: str
    asset_name: str
    tag_name: str = ""


class FirmwareOtaUpdateRequest(BaseModel):
    file_path: str
    tcp_host: str
    timeout_seconds: int = Field(600, ge=60, le=1800)


def _configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, str(level or "INFO").upper(), logging.INFO))


_settings = load_settings()
_configure_logging(_settings.log_level)
_service = MeshtasticBridgeService(_settings)
_webui_dir = Path(__file__).resolve().parent / "webui"


def _auth_dependency(
    authorization: Optional[str] = Header(None),
    x_tater_token: Optional[str] = Header(None),
) -> None:
    configured = _service.settings.api_token.strip()
    if not configured:
        return

    supplied = str(x_tater_token or "").strip()
    auth_header = str(authorization or "").strip()
    if auth_header.lower().startswith("bearer "):
        supplied = auth_header[7:].strip() or supplied

    if supplied != configured:
        raise HTTPException(status_code=401, detail="Invalid or missing API token.")


def _admin_call(func: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        return func(*args, **kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _local_host_addresses() -> set[str]:
    addresses = {"127.0.0.1", "::1", "localhost"}
    for name in {socket.gethostname(), socket.getfqdn()}:
        try:
            for info in socket.getaddrinfo(name, None):
                addresses.add(str(info[4][0]))
        except OSError:
            continue
    return addresses


def _is_local_request(request: Request) -> bool:
    host = str(request.client.host if request.client else "").strip()
    if not host:
        return False
    if host.startswith("::ffff:"):
        host = host.removeprefix("::ffff:")
    try:
        if ipaddress.ip_address(host).is_loopback:
            return True
    except ValueError:
        pass
    return host in _local_host_addresses()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _service.start()
    try:
        yield
    finally:
        _service.stop()


app = FastAPI(
    title="Tater Meshtastic Bridge",
    version=__version__,
    lifespan=lifespan,
)

app.mount("/ui/static", StaticFiles(directory=str(_webui_dir)), name="bridge-webui-static")


@app.get("/")
def root(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {
        "ok": True,
        "name": "Tater Meshtastic Bridge",
        "version": __version__,
        "status": _service.status_snapshot(),
        "webui": "/ui",
    }


@app.get("/ui", include_in_schema=False)
def webui_index() -> FileResponse:
    return FileResponse(_webui_dir / "index.html")


@app.get("/health")
def health(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _service.health_snapshot()


@app.get("/status")
def status(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _service.status_snapshot()


@app.get("/events")
def events(
    since_id: int = Query(0, ge=0),
    since: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    items = _service.list_events(since_id=since_id, since_ts=since, limit=limit)
    return {
        "ok": True,
        "events": items,
        "count": len(items),
        "latest_event_id": _service.status_snapshot()["latest_event_id"],
        "connected": _service.status_snapshot()["connected"],
    }


@app.get("/messages")
def messages(
    since_id: int = Query(0, ge=0),
    since: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    items = _service.list_messages(since_id=since_id, since_ts=since, limit=limit)
    return {
        "ok": True,
        "messages": items,
        "count": len(items),
        "latest_event_id": _service.status_snapshot()["latest_event_id"],
        "connected": _service.status_snapshot()["connected"],
    }


@app.post("/send")
def send_message(payload: SendMessageRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.send_text, text=payload.text, channel=payload.channel, destination=payload.destination)


@app.get("/nodes")
def nodes(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    items = _service.list_nodes()
    return {
        "ok": True,
        "nodes": items,
        "count": len(items),
    }


@app.get("/nodes/{node_id}/history")
def node_history(
    node_id: str,
    limit: int = Query(100, ge=1, le=500),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    items = _service.get_node_history(node_id, limit=limit)
    return {
        "ok": True,
        "node_id": node_id,
        "history": items,
        "count": len(items),
    }


@app.get("/channels")
def channels(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    items = _service.list_channels(refresh=refresh)
    return {
        "ok": True,
        "channels": items,
        "urls": _service.channel_urls(),
        "count": len(items),
    }


@app.get("/channels/share")
def channel_share(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.channel_share_snapshot, refresh=refresh)


@app.get("/device")
def device(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {
        "ok": True,
        "device": _service.device_snapshot(refresh=refresh),
    }


@app.get("/stats")
def stats(window_hours: int = Query(24, ge=1, le=168), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {
        "ok": True,
        "stats": _service.stats_summary(window_hours=window_hours),
    }


@app.get("/audit")
def audit(limit: int = Query(100, ge=1, le=500), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    items = _service.list_audit_log(limit=limit)
    return {
        "ok": True,
        "audit": items,
        "count": len(items),
    }


@app.get("/config")
def config_snapshot(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {
        "ok": True,
        "config": _service.config_snapshot(refresh=refresh),
    }


@app.get("/settings")
def runtime_settings(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _service.runtime_settings_snapshot()


@app.get("/firmware/status")
def firmware_status(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _service.firmware_status(refresh=refresh)


@app.get("/firmware/releases")
def firmware_releases(
    include_prerelease: bool = Query(False),
    limit: int = Query(8, ge=1, le=30),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    return _admin_call(_service.firmware_releases, include_prerelease=include_prerelease, limit=limit)


@app.get("/firmware/files")
def firmware_files(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _service.list_firmware_files()


@app.post("/firmware/download")
def firmware_download(payload: FirmwareDownloadRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(
        _service.download_firmware_asset,
        asset_url=payload.asset_url,
        asset_name=payload.asset_name,
        tag_name=payload.tag_name,
    )


@app.post("/firmware/ota-update")
def firmware_ota_update(payload: FirmwareOtaUpdateRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(
        _service.ota_update_firmware,
        file_path=payload.file_path,
        tcp_host=payload.tcp_host,
        timeout_seconds=payload.timeout_seconds,
    )


@app.post("/settings")
def update_runtime_settings(payload: RuntimeSettingsRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.update_runtime_settings, values=payload.model_dump(exclude_unset=True))


@app.post("/auth/clear-token")
def clear_api_token(request: Request) -> Dict[str, Any]:
    if not _is_local_request(request):
        raise HTTPException(
            status_code=403,
            detail="Bridge API token recovery is only allowed from the bridge host. Open the UI as http://localhost:8433/ui on the machine running the bridge.",
        )
    result = _admin_call(_service.update_runtime_settings, values={"api_token": ""})
    result["token_cleared"] = True
    return result


@app.delete("/settings")
def clear_runtime_settings(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.clear_runtime_settings)


@app.delete("/data")
def clear_all_data(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.clear_all_data)


@app.post("/ble/scan")
def ble_scan() -> Dict[str, Any]:
    return _admin_call(_service.scan_ble_devices)


@app.post("/device/owner")
def device_owner(payload: OwnerUpdateRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(
        _service.update_owner,
        long_name=payload.long_name,
        short_name=payload.short_name,
        is_licensed=payload.is_licensed,
        is_unmessagable=payload.is_unmessagable,
    )


@app.post("/device/fixed-position")
def fixed_position(payload: FixedPositionRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(
        _service.set_fixed_position,
        latitude=payload.latitude,
        longitude=payload.longitude,
        altitude=payload.altitude,
    )


@app.post("/device/channel-url")
def set_channel_url(payload: ChannelUrlRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.set_channel_url, url=payload.url, add_only=payload.add_only)


@app.post("/device/canned-message")
def canned_message(payload: TextBlobRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.set_canned_message, text=payload.text)


@app.post("/device/ringtone")
def ringtone(payload: TextBlobRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.set_ringtone, text=payload.text)


@app.post("/device/action/{action}")
def device_action(action: str, payload: DeviceActionRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.perform_device_action, action=action, seconds=payload.seconds)


@app.post("/config/{scope}/{section}")
def update_config(scope: str, section: str, payload: ConfigSectionRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.update_config_section, scope=scope, section=section, values=payload.values)


@app.post("/channels/{index}")
def update_channel(index: int, payload: ChannelUpdateRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.update_channel, index=index, channel_data=payload.channel)


@app.delete("/channels/{index}")
def delete_channel(index: int, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return _admin_call(_service.delete_channel, index=index)


@app.get("/ui/api/bootstrap", include_in_schema=False)
def ui_bootstrap(window_hours: int = Query(24, ge=1, le=168), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {
        "ok": True,
        "version": __version__,
        "status": _service.status_snapshot(),
        "stats": _service.stats_summary(window_hours=window_hours),
        "device": _service.device_snapshot(refresh=False),
        "settings": _service.runtime_settings_snapshot(),
        "config": _service.config_snapshot(refresh=False),
        "channels": _service.list_channels(refresh=False),
        "nodes": _service.list_nodes(),
        "messages": _service.list_messages(limit=80),
        "audit": _service.list_audit_log(limit=30),
    }


@app.get("/ui/api/messages", include_in_schema=False)
def ui_messages(
    limit: int = Query(100, ge=1, le=1000),
    since_id: int = Query(0, ge=0),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    items = _service.list_messages(limit=limit, since_id=since_id)
    return {"ok": True, "messages": items, "count": len(items)}


@app.get("/ui/api/events", include_in_schema=False)
def ui_events(
    limit: int = Query(100, ge=1, le=1000),
    since_id: int = Query(0, ge=0),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    items = _service.list_events(limit=limit, since_id=since_id)
    return {"ok": True, "events": items, "count": len(items)}


@app.get("/ui/api/nodes", include_in_schema=False)
def ui_nodes(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    items = _service.list_nodes()
    return {"ok": True, "nodes": items, "count": len(items)}


@app.get("/ui/api/nodes/{node_id}/history", include_in_schema=False)
def ui_node_history(
    node_id: str,
    limit: int = Query(100, ge=1, le=500),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    items = _service.get_node_history(node_id, limit=limit)
    return {"ok": True, "history": items, "count": len(items), "node_id": node_id}


@app.get("/ui/api/channels", include_in_schema=False)
def ui_channels(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {"ok": True, "channels": _service.list_channels(refresh=refresh), "urls": _service.channel_urls()}


@app.get("/ui/api/channels/share", include_in_schema=False)
def ui_channel_share(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return channel_share(refresh, _)


@app.get("/ui/api/device", include_in_schema=False)
def ui_device(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {"ok": True, "device": _service.device_snapshot(refresh=refresh)}


@app.get("/ui/api/config", include_in_schema=False)
def ui_config(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {"ok": True, "config": _service.config_snapshot(refresh=refresh)}


@app.get("/ui/api/settings", include_in_schema=False)
def ui_runtime_settings(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return runtime_settings(_)


@app.get("/ui/api/firmware/status", include_in_schema=False)
def ui_firmware_status(refresh: bool = Query(False), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return firmware_status(refresh, _)


@app.get("/ui/api/firmware/releases", include_in_schema=False)
def ui_firmware_releases(
    include_prerelease: bool = Query(False),
    limit: int = Query(8, ge=1, le=30),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    return firmware_releases(include_prerelease, limit, _)


@app.get("/ui/api/firmware/files", include_in_schema=False)
def ui_firmware_files(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return firmware_files(_)


@app.post("/ui/api/firmware/download", include_in_schema=False)
def ui_firmware_download(payload: FirmwareDownloadRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return firmware_download(payload, _)


@app.post("/ui/api/firmware/ota-update", include_in_schema=False)
def ui_firmware_ota_update(payload: FirmwareOtaUpdateRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return firmware_ota_update(payload, _)


@app.post("/ui/api/ble/scan", include_in_schema=False)
def ui_ble_scan() -> Dict[str, Any]:
    return ble_scan()


@app.get("/ui/api/stats", include_in_schema=False)
def ui_stats(window_hours: int = Query(24, ge=1, le=168), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {"ok": True, "stats": _service.stats_summary(window_hours=window_hours)}


@app.get("/ui/api/audit", include_in_schema=False)
def ui_audit(limit: int = Query(100, ge=1, le=500), _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {"ok": True, "audit": _service.list_audit_log(limit=limit)}


@app.post("/ui/api/device/owner", include_in_schema=False)
def ui_device_owner(payload: OwnerUpdateRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return device_owner(payload, _)


@app.post("/ui/api/send", include_in_schema=False)
def ui_send_message(payload: SendMessageRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return send_message(payload, _)


@app.post("/ui/api/device/fixed-position", include_in_schema=False)
def ui_fixed_position(payload: FixedPositionRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return fixed_position(payload, _)


@app.post("/ui/api/device/channel-url", include_in_schema=False)
def ui_set_channel_url(payload: ChannelUrlRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return set_channel_url(payload, _)


@app.post("/ui/api/device/canned-message", include_in_schema=False)
def ui_canned_message(payload: TextBlobRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return canned_message(payload, _)


@app.post("/ui/api/device/ringtone", include_in_schema=False)
def ui_ringtone(payload: TextBlobRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return ringtone(payload, _)


@app.post("/ui/api/device/action/{action}", include_in_schema=False)
def ui_device_action(action: str, payload: DeviceActionRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return device_action(action, payload, _)


@app.post("/ui/api/config/{scope}/{section}", include_in_schema=False)
def ui_update_config(scope: str, section: str, payload: ConfigSectionRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return update_config(scope, section, payload, _)


@app.post("/ui/api/settings", include_in_schema=False)
def ui_update_runtime_settings(payload: RuntimeSettingsRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return update_runtime_settings(payload, _)


@app.post("/ui/api/auth/clear-token", include_in_schema=False)
def ui_clear_api_token(request: Request) -> Dict[str, Any]:
    return clear_api_token(request)


@app.delete("/ui/api/settings", include_in_schema=False)
def ui_clear_runtime_settings(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return clear_runtime_settings(_)


@app.delete("/ui/api/data", include_in_schema=False)
def ui_clear_all_data(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return clear_all_data(_)


@app.post("/ui/api/channels/{index}", include_in_schema=False)
def ui_update_channel(index: int, payload: ChannelUpdateRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return update_channel(index, payload, _)


@app.delete("/ui/api/channels/{index}", include_in_schema=False)
def ui_delete_channel(index: int, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return delete_channel(index, _)


def main() -> None:
    uvicorn.run(
        "tater_meshtastic_bridge.main:app",
        host=_settings.host,
        port=_settings.port,
        log_level=str(_settings.log_level or "INFO").lower(),
        access_log=False,
        reload=False,
    )


if __name__ == "__main__":
    main()
