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
- `GET /device`
- `GET /stats`
- `GET /audit`
- `GET /config`
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

The Web UI is meant for bridge administration and radio inspection. It currently includes:

- Dashboard with live connection state, stats, URLs, and recent chat
- Read-only chat log for inbound and outbound mesh messages seen by the bridge
- Node browser with current nodes, past nodes, and per-node sighting history
- Channel browser with URL export plus per-channel JSON editing
- Device + config pages for local config sections, module config sections, owner names, canned messages, ringtone, and fixed position
- Audit log of bridge-side write actions

The UI does not provide a send-message box. Outbound sends are still intended to come from Tater or the HTTP API.

## Quick start

1. Create a venv.
2. Install the project.
3. Copy `.env.example` to `.env` and set your node name or address.
4. Run the bridge on the host machine that has Bluetooth access.

Example:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
tater-meshtastic-bridge
```

If you change `pyproject.toml` or add package assets later, refresh the editable install:

```bash
pip install -e .
```

## Finding the BLE device

If you do not know the exact Meshtastic BLE name or address yet, scan first from the bridge venv:

```bash
cd /Users/ahphooey/Scripts/Tater_Meshtastic_Bridge
source .venv/bin/activate
meshtastic --ble-scan
```

Use the scan result in `.env` as either:

```env
MESHTASTIC_DEVICE_NAME=exact-name-from-scan
```

or, preferably:

```env
MESHTASTIC_DEVICE_ADDRESS=exact-address-from-scan
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
- Auth is optional. If `MESHTASTIC_API_TOKEN` is set, send `Authorization: Bearer <token>` or `X-Tater-Token: <token>`.
- If the bridge has an API token set, the Web UI shell still loads without auth, but the browser needs that token saved in the UI Settings tab before it can call the protected bridge APIs.
- `MESHTASTIC_DATABASE_PATH` lets you override where the bridge keeps its SQLite history and snapshots. Leaving it blank uses a platform-appropriate app-data path.
- The bridge now records text, node updates, radio log lines, and common packet types like `position`, `user`, and `data` when the Meshtastic client publishes them.
- `MESHTASTIC_SHUTDOWN_TIMEOUT_SECONDS` controls how long the bridge waits for BLE cleanup before continuing shutdown if the Bluetooth stack is hung.
