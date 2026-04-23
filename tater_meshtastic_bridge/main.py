from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field

from . import __version__
from .config import load_settings
from .service import MeshtasticBridgeService


class SendMessageRequest(BaseModel):
    text: str = Field(..., description="Plain text to send into the mesh.")
    channel: int = Field(0, ge=0, description="Meshtastic channel index.")
    destination: str = Field("broadcast", description="broadcast, ^local, or a direct node id such as !abcd1234.")


def _configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, str(level or "INFO").upper(), logging.INFO))


_settings = load_settings()
_configure_logging(_settings.log_level)
_service = MeshtasticBridgeService(_settings)


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


@app.get("/")
def root(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {
        "ok": True,
        "name": "Tater Meshtastic Bridge",
        "version": __version__,
        "status": _service.status_snapshot(),
    }


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
    limit: int = Query(100, ge=1, le=500),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    items = _service.list_events(since_id=since_id, since_ts=since, limit=limit)
    return {
        "ok": True,
        "events": items,
        "count": len(items),
        "latest_event_id": _service.events.latest_event_id(),
        "connected": _service.status_snapshot()["connected"],
    }


@app.get("/messages")
def messages(
    since_id: int = Query(0, ge=0),
    since: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    _: None = Depends(_auth_dependency),
) -> Dict[str, Any]:
    items = _service.list_messages(since_id=since_id, since_ts=since, limit=limit)
    return {
        "ok": True,
        "messages": items,
        "count": len(items),
        "latest_event_id": _service.events.latest_event_id(),
        "connected": _service.status_snapshot()["connected"],
    }


@app.post("/send")
def send_message(payload: SendMessageRequest, _: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    try:
        return _service.send_text(text=payload.text, channel=payload.channel, destination=payload.destination)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/nodes")
def nodes(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    items = _service.list_nodes()
    return {
        "ok": True,
        "nodes": items,
        "count": len(items),
    }


@app.get("/channels")
def channels(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    items = _service.list_channels()
    return {
        "ok": True,
        "channels": items,
        "count": len(items),
    }


@app.get("/device")
def device(_: None = Depends(_auth_dependency)) -> Dict[str, Any]:
    return {
        "ok": True,
        "device": _service.device_snapshot(),
    }


def main() -> None:
    uvicorn.run(
        "tater_meshtastic_bridge.main:app",
        host=_settings.host,
        port=_settings.port,
        log_level=str(_settings.log_level or "INFO").lower(),
        reload=False,
    )


if __name__ == "__main__":
    main()
