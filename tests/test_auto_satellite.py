"""Small offline tests: no running LubeLogger or NATS instance required."""
import json
import unittest
from unittest.mock import AsyncMock

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from vehicle_beacon import (
    Beacon, LubeLoggerClient, classify_webhook, garage_vehicle_ids, handle_webhook,
    handle_refresh_message,
)


class FakeApi:
    def __init__(self):
        self.calls = []

    async def get(self, path, *, vehicle_id=None):
        self.calls.append((path, vehicle_id))
        return [{"id": 2}] if path == "/api/vehicles" else [{"id": 1}]


class BeaconTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_sweep(self):
        api = FakeApi()
        nats = AsyncMock()
        beacon = Beacon(api, nats)
        await beacon.refresh_all()
        subjects = [call.args[0] for call in nats.publish.call_args_list]
        self.assertEqual(subjects, [
            "vehicle.garage.snapshot", "vehicle.2.info.snapshot",
            "vehicle.2.odometer.snapshot", "vehicle.2.fuel.snapshot",
            "vehicle.2.service.snapshot", "vehicle.2.supply.snapshot",
            "vehicle.2.tax.snapshot",
        ])
        payload = json.loads(nats.publish.call_args_list[3].args[1])
        self.assertEqual(payload["vehicle_id"], 2)
        self.assertEqual(payload["category"], "fuel")

    async def test_webhook_coalesces(self):
        beacon = Beacon(FakeApi(), AsyncMock())
        beacon.schedule(2)
        beacon.schedule(2)
        beacon.schedule(3)
        self.assertEqual(beacon.pending_ids, {2, 3})
        beacon.schedule()
        self.assertTrue(beacon.pending_all)
        self.assertFalse(beacon.pending_ids)

    async def test_http_webhook_authenticates_and_schedules(self):
        beacon = Beacon(FakeApi(), AsyncMock())
        app = web.Application()
        app["beacon"] = beacon
        app.router.add_post("/webhook/secret", handle_webhook)
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                blocked = await client.post("/webhook/wrong", json={"type": "servicerecord.add"})
                self.assertEqual(blocked.status, 404)
                ok = await client.post("/webhook/secret", json={"type": "servicerecord.add", "vehicleId": 2})
                self.assertEqual(ok.status, 202)
                self.assertEqual(beacon.pending_ids, {2})
                bad = await client.post("/webhook/secret", json=[1, 2])
                self.assertEqual(bad.status, 400)

    async def test_api_uses_viewer_key_and_vehicle_id(self):
        async def reply(request):
            self.assertEqual(request.headers.get("x-api-key"), "viewer")
            self.assertIn("culture-invariant", request.headers)
            self.assertEqual(request.query.get("vehicleId"), "2")
            return web.json_response([{"id": 42}])
        app = web.Application()
        app.router.add_get("/api/vehicle/gasrecords", reply)
        async with TestServer(app) as server:
            async with TestClient(server, headers={"x-api-key": "viewer", "culture-invariant": ""}) as client:
                api = LubeLoggerClient(client.session, str(server.make_url("/")).rstrip("/"))
                result = await api.get("/api/vehicle/gasrecords", vehicle_id=2)
                self.assertEqual(result, [{"id": 42}])

    async def test_manual_refresh(self):
        beacon = Beacon(FakeApi(), AsyncMock())
        class Message:
            data = b'{"vehicleId": 5}'
        await handle_refresh_message(Message(), beacon)
        self.assertEqual(beacon.pending_ids, {5})

    def test_webhook_routing(self):
        self.assertEqual(classify_webhook({"type":"servicerecord.add", "vehicleId":"2"}), (True, 2))
        self.assertEqual(classify_webhook({"type":"vehicle.delete", "vehicleId":"2"}), (True, None))
        self.assertEqual(classify_webhook({"type":"noterecord.add", "vehicleId":"2"}), (False, None))
        self.assertEqual(classify_webhook({"type":"something.future", "vehicleId":"bad"}), (True, None))

    def test_garage_ids(self):
        self.assertEqual(garage_vehicle_ids([{"id": 3}, {"Id": "2"}]), [2, 3])
        with self.assertRaises(ValueError):
            garage_vehicle_ids([{"make": "unknown"}])


if __name__ == "__main__":
    unittest.main()
