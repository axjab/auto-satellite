# Vehicle Beacon

Read-only **LubeLogger → NATS** sidecar. Inspired by satellite telemetry: one
transmitter publishes observations; independent subscribers decide what to do.
No database, no business logic, no local record persistence.

## Install

```bash
uv sync
cp .env.example .env
# Edit .env: supply the LubeLogger URL, VIEWER API key, and random webhook secret.
set -a; . ./.env; set +a
uv run vehicle-beacon
```

Run the offline tests with `python -m unittest discover -s tests -v`.

Works with an accessible LubeLogger instance and NATS server. For Docker,
`127.0.0.1` refers to the container itself; use network hostnames instead.

Configure the **LubeLogger Server Settings → WebHook URL** to:

```text
http://<beacon-host>:8090/webhook/<WEBHOOK_SECRET>
```

The listener defaults to `127.0.0.1`: configure `LISTEN_HOST` to a private,
reachable interface when LubeLogger runs on another host/container. Do **not**
expose the webhook on the public Internet. LubeLogger does not document a
signature header for webhook authenticity; the random URL secret and network
access controls are important.

## NATS telemetry

```text
vehicle.garage.snapshot
vehicle.<id>.info.snapshot
vehicle.<id>.odometer.snapshot
vehicle.<id>.fuel.snapshot
vehicle.<id>.service.snapshot
vehicle.<id>.supply.snapshot
vehicle.<id>.tax.snapshot
```

Example payload (`data` is **the unchanged LubeLogger GET JSON response**):

```json
{
  "schema": 1,
  "source": "lubelogger",
  "observed_at": "2026-11-06T06:00:00+00:00",
  "vehicle_id": 2,
  "category": "service",
  "data": [
    {"id": 17, "date": "2026-01-03", "odometer": 15000, "description": "Oil change"}
  ]
}
```

### Refresh

- Startup: publish a complete snapshot of the garage and every vehicle/category.
- Webhook: refresh the affected vehicle; vehicle add/edit/delete refreshes garage.
- Every `SWEEP_SECONDS`: refresh everything, including deletion recovery.
- Manual: `nats pub vehicle.refresh '{}'` to refresh all, or
  `nats pub vehicle.refresh '{"vehicleId":2}'` for one vehicle.

The full category history is transmitted each time, not individual deltas.
That lets new receivers obtain state without needing historical NATS events.
Consumers should treat snapshots as **replace-the-current-category**, not append.
LubeLogger record IDs remain scoped to their vehicle/category.

Supported categories: vehicles, vehicle info, odometer, fuel, service, supply,
tax. **Excluded by design:** notes, equipment, reminders, upgrades, repairs, plans.

## Limits

- Regular NATS is at-most-once; receivers can miss intermediate events. Periodic
  snapshots restore current state but not an audit history of transient changes.
- A failed category fetch does not emit an empty snapshot; remaining categories
  continue. The next sweep retries it.
- Webhooks are coalesced in memory; no disk queue in this sidecar. A restart
  triggers a new full sweep.
- Snapshot messages must fit the broker's maximum payload size. Large record
  histories might later require paging or sharding.
- LubeLogger response shapes can vary by version. `GET /api/vehicles` must
  return a JSON array (or an object with a `vehicles`/`data` array) containing
  numeric vehicle IDs; otherwise the service logs an error instead of guessing.

Sources: https://docs.lubelogger.com/Advanced/Webhook/ and
https://docs.lubelogger.com/Advanced/API/ .
