<p align="center">
  <img 
    src="https://github.com/user-attachments/assets/55e2f607-1e44-4937-8a72-3352520eb272" 
    alt="screenshot"
    width="300"
  />
</p>
<h3 align="center">
  <a href="https://taterassistant.com">taterassistant.com</a>
</h3>

# Tater Meshtastic Bridge

`Tater Meshtastic Bridge` is a small host-side service that keeps Meshtastic BLE transport out of the main Tater app.

## What it does

- Connects to one Meshtastic node over Bluetooth LE
- Reconnects automatically after disconnects
- Normalizes incoming mesh packets into simple JSON events
- Accepts outbound messages from Tater over HTTP
- Keeps durable history in SQLite for messages, nodes, sightings, snapshots, and audit logs
- Exposes a Tater-style admin Web UI at `GET /ui`
- Surfaces node, channel, config, device, stats, and audit endpoints for the bridge UI

## Why run it separately

- Bluetooth is easier to manage on the host than inside Docker
- Reconnect logic stays isolated from portal logic
- Tater only has to talk to a small local API
- The transport can later be swapped for USB or TCP with less churn

## API

- `GET /health`
- `GET /status`
- `GET /messages`
- `GET /events`
- `POST /send`
- `GET /nodes`
- `GET /channels`
- `GET /channels/share`
- `GET /device`
- `GET /stats`
- `GET /audit`
- `GET /config`
- `GET /settings`
- `POST /settings`
- `DELETE /settings`
- `GET /firmware/status`
- `GET /firmware/releases`
- `GET /firmware/files`
- `POST /firmware/download`
- `POST /firmware/ota-update`
- `DELETE /data`
- `POST /ble/scan` no API token required; intended for first-time setup
- `GET /ui`
- `GET /ui/api/bootstrap`
- `POST /device/owner`
- `POST /device/fixed-position`
- `POST /device/channel-url`
- `POST /device/canned-message`
- `POST /device/ringtone`
- `POST /device/action/{action}`
- `POST /config/{scope}/{section}`
- `POST /channels/{index}`
- `DELETE /channels/{index}`

`/messages` and `/events` support `since_id`, `since`, and `limit` query params.

## Web UI

Open the bridge console at:

```text
http://127.0.0.1:8433/ui
```

The bridge binds to `0.0.0.0`, so you can also open it from another machine on your LAN:

```text
http://<bridge-host-ip>:8433/ui
```

The Web UI is meant for bridge administration and radio inspection. It currently includes:

- Dashboard with live connection state, stats, URLs, and recent chat
- Chat log for inbound and outbound mesh messages seen by the bridge, plus a manual broadcast bar for short radio messages
- Node browser with current nodes, past nodes, direct-message composer, and per-node sighting history
- Channel browser with URL export, Meshtastic-compatible QR sharing, plus per-channel JSON editing
- Firmware tab with device detection, GitHub release lookup, firmware download cache, DFU/OTA prep actions, and guarded ESP32 WiFi/TCP OTA update support
- Device + config pages for local config sections, module config sections, owner names, LoRa radio settings, canned messages, ringtone, fixed position, and GPS/position source settings
- Settings page for BLE scanning, bridge runtime settings, API token, reconnect timing, ACK behavior, logging, next-start port, and data reset actions
- Audit log of bridge-side write actions

The Chat tab includes a manual broadcast box for short operator messages. Direct messages live on the Nodes tab after selecting a node. Automated replies are still intended to come from Tater or the HTTP API.

## Quick start

1. Create a venv.
2. Install the project.
3. Copy `.env.example` to `.env` only if you need startup overrides, such as a non-default port.
4. Run the bridge on the host machine that has Bluetooth access.
5. Open `/ui`, go to Settings, and set your Meshtastic BLE name or address.

Example:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
# Optional: cp .env.example .env
tater-meshtastic-bridge
```

If you change `pyproject.toml` or add package assets later, refresh the editable install:

```bash
pip install -e .
```

## Finding the BLE device

The easiest path is the bridge Web UI:

1. Open `http://<bridge-host>:8433/ui`
2. Go to Settings
3. Click Scan for Devices
4. Click Use This Device on the radio you want
5. Click Save Bridge Settings
6. Restart or reconnect the bridge so the new BLE target is used

The scan button uses the same Meshtastic BLE discovery as this terminal command:

```bash
cd /Users/ahphooey/Scripts/Tater_Meshtastic_Bridge
source .venv/bin/activate
meshtastic --ble-scan
```

If you scan from the terminal instead, use the scan result in the bridge Web UI Settings tab as either:

```text
BLE Device Name = exact-name-from-scan
```

or, preferably:

```text
BLE Device Address = exact-address-from-scan
```

Using the BLE address is usually more reliable than matching by name.

If the scan finds nothing:

- Make sure Bluetooth is enabled on the host
- Make sure the Meshtastic node is advertising BLE
- Keep the node close to the host
- Close other apps that may already be connected to the node
- Approve any macOS Bluetooth permission or pairing prompt if it appears

## Notes

- The service uses the official Meshtastic Python package and its pubsub event model.
- BLE discovery is matched by Meshtastic device name or device address.
- Auth is optional. BLE scanning stays available for first-time setup without a token. If the API token is set in the Web UI, protected endpoints should send `Authorization: Bearer <token>` or `X-Tater-Token: <token>`.
- If the bridge has an API token set, the Web UI shell still loads without auth, but the browser needs that token saved in the UI Settings tab before it can call the protected bridge APIs.
- The bridge host is fixed to `0.0.0.0`.
- Runtime settings live in the Web UI and are stored in SQLite.
- Firmware downloads are cached beside the bridge database. ESP32 OTA updates use the Meshtastic CLI over WiFi/TCP and require firmware/device support; USB/Web Serial or drag-and-drop flashing is still recommended for full erase, recovery, nRF52, RP2040, and USB-only workflows.
- `MESHTASTIC_BRIDGE_PORT` is only needed as a hard startup override when the default `8433` port is already in use. Leave it unset for normal UI-managed port changes.
- `MESHTASTIC_DATABASE_PATH` remains a startup-only override for where the bridge keeps its SQLite history, snapshots, and UI-managed settings. Leaving it blank uses a platform-appropriate app-data path.
- The bridge now records text, node updates, radio log lines, and common packet types like `position`, `user`, and `data` when the Meshtastic client publishes them.
- `MESHTASTIC_SHUTDOWN_TIMEOUT_SECONDS` can be changed from the Web UI and controls how long the bridge waits for BLE cleanup before continuing shutdown if the Bluetooth stack is hung.
