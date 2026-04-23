# Tater Meshtastic Bridge

`Tater Meshtastic Bridge` is a small host-side service that keeps Meshtastic BLE transport out of the main Tater app.

## What it does

- Connects to one Meshtastic node over Bluetooth LE
- Reconnects automatically after disconnects
- Normalizes incoming text packets into simple JSON events
- Accepts outbound messages from Tater over HTTP
- Exposes health, status, node, channel, and device endpoints

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

`/messages` and `/events` support `since_id`, `since`, and `limit` query params.

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
- v1 focuses on text messages and connection state. Telemetry and richer packet types can be layered in later.
