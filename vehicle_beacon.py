#!/usr/bin/env python3
"""Vehicle Beacon: read LubeLogger snapshots and broadcast them over NATS.

Webhooks wake the broadcaster; they are not the data source. A periodic full
sweep repairs missed webhook deliveries. This service never writes to LubeLogger.
"""

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from aiohttp import ClientSession, ClientTimeout, web

LOG = logging.getLogger("vehicle-beacon")

# LubeLogger record categories explicitly included in the first version.
# Paths deliberately exclude notes, equipment, reminders, upgrades, repairs,
# and plans. Preserve API responses as-is; interpretation belongs to receivers.
RECORD_ENDPOINTS = {
    "odometer": "/api/vehicle/odometerrecords",
    "fuel": "/api/vehicle/gasrecords",
    "service": "/api/vehicle/servicerecords",
    "supply": "/api/vehicle/supplyrecords",
    "tax": "/api/vehicle/taxrecords",
}

# Unwanted events are ignored; periodic sweeps still reconcile kept categories.
IGNORED_EVENT_PREFIXES = (
    "note", "equipment", "reminder", "upgrade", "repair", "plan",
)

# Any of these imply a vehicle-level change; refresh the entire garage.
VEHICLE_EVENT_PREFIXES = ("vehicle",)


@dataclass(frozen=True)
class Config:
    lubelogger_url: str
    lubelogger_api_key: str
    nats_url: str
    webhook_secret: str
    listen_host: str = "127.0.0.1"
    listen_port: int = 8090
    sweep_seconds: int = 900

    @classmethod
    def from_env(cls) -> "Config":
        """Read all settings from environment variables and fail early."""
        required = (
            "LUBELOGGER_URL", "LUBELOGGER_API_KEY", "WEBHOOK_SECRET",
        )
        missing = [key for key in required if not os.environ.get(key)]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

        secret = os.environ["WEBHOOK_SECRET"]
        if len(secret) < 16 or "/" in secret:
            raise ValueError("WEBHOOK_SECRET must be at least 16 characters without '/'")
        sweep_seconds = int(os.getenv("SWEEP_SECONDS", "900"))
        if sweep_seconds < 10:
            raise ValueError("SWEEP_SECONDS must be >= 10")
        return cls(
            lubelogger_url=os.environ["LUBELOGGER_URL"].rstrip("/"),
            lubelogger_api_key=os.environ["LUBELOGGER_API_KEY"],
            nats_url=os.getenv("NATS_URL", "nats://127.0.0.1:4222"),
            webhook_secret=secret,
            listen_host=os.getenv("LISTEN_HOST", "127.0.0.1"),
            listen_port=int(os.getenv("LISTEN_PORT", "8090")),
            sweep_seconds=sweep_seconds,
        )


def current_time() -> str:
    """Produce a UTC time for each observed snapshot."""
    return datetime.now(timezone.utc).isoformat()


def vehicle_id_of(item: Any) -> int | None:
    """Read the numeric LubeLogger vehicle ID without relying on key casing."""
    if not isinstance(item, dict):
        return None
    raw = item.get("id", item.get("Id", item.get("vehicleId", item.get("VehicleId"))))
    try:
        value = int(raw)
        return value if value > 0 else None
    except (ValueError, TypeError):
        return None


def garage_vehicle_ids(data: Any) -> list[int]:
    """Extract IDs, failing rather than silently broadcasting an empty garage."""
    if isinstance(data, list):
        vehicles = data
    elif isinstance(data, dict):
        vehicles = next(
            (data[key] for key in ("vehicles", "Vehicles", "data", "Data")
             if isinstance(data.get(key), list)),
            None,
        )
    else:
        vehicles = None
    if vehicles is None:
        raise ValueError("Unsupported /api/vehicles response shape (expected list)")

    ids = [vehicle_id_of(item) for item in vehicles]
    if any(value is None for value in ids):
        raise ValueError("Vehicle list includes a vehicle without a numeric ID")
    return sorted(set(ids))


def affected_vehicle(payload: dict[str, Any]) -> int | None:
    """Parse the webhook's vehicleId; None requests a full-garage refresh."""
    return vehicle_id_of({"vehicleId": payload.get("vehicleId")})


def classify_webhook(payload: dict[str, Any]) -> tuple[bool, int | None]:
    """Return (should_refresh, vehicle_id), routing unknown events safely."""
    event_type = str(payload.get("type", "")).lower().replace("_", "")
    if event_type.startswith(IGNORED_EVENT_PREFIXES):
        return False, None
    if not event_type or event_type.startswith(VEHICLE_EVENT_PREFIXES):
        return True, None
    return True, affected_vehicle(payload)


class LubeLoggerClient:
    """Read-only API client; the only place that knows LubeLogger endpoints."""

    def __init__(self, session: ClientSession, base_url: str):
        self.session = session
        self.base_url = base_url

    async def get(self, path: str, *, vehicle_id: int | None = None) -> Any:
        params = {"vehicleId": vehicle_id} if vehicle_id is not None else None
        async with self.session.get(self.base_url + path, params=params) as response:
            response.raise_for_status()
            return await response.json(content_type=None)


class Beacon:
    """A stateless transmitter. NATS subscribers own all persistent data."""

    def __init__(self, api: LubeLoggerClient, nats_client: Any):
        self.api = api
        self.nats = nats_client
        self.wake = asyncio.Event()
        self.pending_all = False
        self.pending_ids: set[int] = set()

    def schedule(self, vehicle_id: int | None = None) -> None:
        """Coalesce bursts of webhooks while a previous sweep is running."""
        if vehicle_id is None:
            self.pending_all = True
            self.pending_ids.clear()
        elif not self.pending_all:
            self.pending_ids.add(vehicle_id)
        self.wake.set()

    async def publish(self, subject: str, category: str, data: Any, *,
                      vehicle_id: int | None = None) -> None:
        """Broadcast a complete category snapshot with stable metadata."""
        payload = {
            "schema": 1,
            "source": "lubelogger",
            "observed_at": current_time(),
            "vehicle_id": vehicle_id,
            "category": category,
            "data": data,
        }
        await self.nats.publish(
            subject,
            json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        )
        LOG.info("Published %s", subject)

    async def refresh_garage(self) -> list[int]:
        """Emit the available vehicle list and discover vehicle IDs."""
        garage = await self.api.get("/api/vehicles")
        ids = garage_vehicle_ids(garage)
        await self.publish("vehicle.garage.snapshot", "garage", garage)
        return ids

    async def refresh_vehicle(self, vehicle_id: int) -> None:
        """Emit all included categories for one vehicle, independently."""
        endpoints = {"info": "/api/vehicle/info", **RECORD_ENDPOINTS}
        for category, path in endpoints.items():
            try:
                data = await self.api.get(path, vehicle_id=vehicle_id)
                await self.publish(
                    f"vehicle.{vehicle_id}.{category}.snapshot",
                    category, data, vehicle_id=vehicle_id,
                )
            except Exception:
                LOG.exception("Failed to refresh vehicle=%s category=%s", vehicle_id, category)

    async def refresh_all(self) -> None:
        """A complete sweep repairs missed webhook events and deletions."""
        ids = await self.refresh_garage()
        for vehicle_id in ids:
            await self.refresh_vehicle(vehicle_id)

    async def worker(self) -> None:
        """Serialize refreshes so a burst of webhooks cannot overload the API."""
        while True:
            await self.wake.wait()
            self.wake.clear()
            full = self.pending_all
            ids = sorted(self.pending_ids)
            self.pending_all = False
            self.pending_ids.clear()
            try:
                if full:
                    await self.refresh_all()
                else:
                    for vehicle_id in ids:
                        await self.refresh_vehicle(vehicle_id)
            except Exception:
                # A failed full sweep is retried on the next periodic sweep.
                LOG.exception("Vehicle refresh failed")

    async def periodic(self, seconds: int) -> None:
        """Rebroadcast everything periodically, even without webhook activity."""
        while True:
            await asyncio.sleep(seconds)
            self.schedule()


async def handle_webhook(request: web.Request) -> web.Response:
    """Accept the hook, then let a worker retrieve authoritative API data."""
    try:
        payload = await request.json()
    except (ValueError, UnicodeDecodeError):
        raise web.HTTPBadRequest(text="Expected a JSON object")
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="Expected a JSON object")

    beacon: Beacon = request.app["beacon"]
    refresh, vehicle_id = classify_webhook(payload)
    if refresh:
        beacon.schedule(vehicle_id)
    return web.Response(status=202, text="accepted")


async def handle_refresh_message(message: Any, beacon: Beacon) -> None:
    """NATS 'vehicle.refresh': empty payload means refresh the whole garage."""
    if not message.data:
        beacon.schedule()
        return
    try:
        payload = json.loads(message.data)
        if not isinstance(payload, dict):
            raise ValueError("Expected a JSON object")
        if "vehicleId" not in payload:
            beacon.schedule()
            return
        vehicle_id = affected_vehicle(payload)
        if vehicle_id is None:
            raise ValueError("vehicleId must be a positive integer")
        beacon.schedule(vehicle_id)
    except (ValueError, UnicodeDecodeError):
        LOG.warning("Ignoring malformed vehicle.refresh request")


async def run(config: Config) -> None:
    """Own network clients, HTTP listener, and the two background loops."""
    from nats import connect  # imported only at runtime to keep pure helpers testable

    headers = {
        "x-api-key": config.lubelogger_api_key,
        "culture-invariant": "",
    }
    async with ClientSession(headers=headers, timeout=ClientTimeout(total=20)) as session:
        nats = await connect(
            servers=[config.nats_url],
            allow_reconnect=True,
            max_reconnect_attempts=-1,
            reconnect_time_wait=2,
        )
        api = LubeLoggerClient(session, config.lubelogger_url)
        beacon = Beacon(api, nats)
        app = web.Application(client_max_size=64 * 1024)
        app["beacon"] = beacon
        app.router.add_post(f"/webhook/{config.webhook_secret}", handle_webhook)
        # Only the receiver's secret URL is configured in LubeLogger.
        runner = web.AppRunner(app, access_log=None)
        tasks: list[asyncio.Task] = []
        try:
            await runner.setup()
            site = web.TCPSite(runner, config.listen_host, config.listen_port)
            await site.start()

            async def on_refresh(message):
                await handle_refresh_message(message, beacon)

            await nats.subscribe("vehicle.refresh", cb=on_refresh)

            tasks = [
                asyncio.create_task(beacon.worker(), name="refresh-worker"),
                asyncio.create_task(beacon.periodic(config.sweep_seconds), name="periodic-sweep"),
            ]
            beacon.schedule()  # Initial full snapshot after listeners are ready.
            LOG.info("Beacon started on %s:%s", config.listen_host, config.listen_port)
            await asyncio.Event().wait()
        finally:
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await runner.cleanup()
            await nats.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(run(Config.from_env()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
