# WandererKills

A standalone Elixir service that streams and caches EVE Online killmails from
zKillboard, enriches them with data from ESI, and fans them out over REST,
WebSocket, SSE, and PubSub.

Point it at the systems or characters you care about and it handles the parts
that are tedious to get right: staying inside zKillboard's and ESI's rate
limits, retrying and circuit-breaking around upstream failures, resolving IDs to
names and ship types, and keeping a warm cache so your clients aren't re-fetching
the same killmail.

## Quick start

Requires Elixir 1.19.5 / OTP 28 (see `.tool-versions`; a Nix flake is provided).

```bash
mix deps.get
mix phx.server          # or: make start, for iex + the server
```

The service listens on port 4004. Confirm it's up and pull some kills:

```bash
curl http://localhost:4004/health

# Kills in Jita (system 30000142) from the last 24 hours
curl "http://localhost:4004/api/v1/kills/system/30000142?since_hours=24&limit=50"
```

Each killmail comes back enriched — character, corporation, and ship names are
resolved, alongside zKillboard's own valuation metadata:

```json
{
  "data": {
    "kills": [
      {
        "killmail_id": 123456789,
        "kill_time": "2024-01-15T14:30:00Z",
        "system_id": 30000142,
        "victim": {
          "character_name": "Victim Name",
          "corporation_name": "Victim Corp",
          "ship_name": "Raven",
          "damage_taken": 2847
        },
        "attackers": [{ "ship_name": "Rattlesnake", "final_blow": true }],
        "zkb": { "total_value": 152000000.0, "points": 15, "solo": true }
      }
    ],
    "cached": false
  }
}
```

Or run it in a container:

```bash
docker compose up --build
```

## Choosing an integration

The service exposes the same data five ways. Pick based on how you consume it:

| Approach | Use it when |
|---|---|
| **REST** | You poll, backfill, or batch-fetch. Simplest to start with. |
| **WebSocket** | You want push updates with dynamic subscriptions — dashboards, live tools. Phoenix Channels, so any Phoenix client works. |
| **SSE** | You want push updates without a channel client. Plain HTTP, native `EventSource`, proxy-friendly. |
| **PubSub** | Your Elixir app runs in the same cluster and you want the lowest possible latency. |
| **Client library** | You're on Elixir and want a typed interface over the REST API. |

Full request/response details, subscription semantics, and client examples in
Python, JavaScript, and Elixir live in the
[API & Integration Guide](docs/API_AND_INTEGRATION_GUIDE.md). Elixir consumers
should also read the [Elixir Client Guide](docs/ELIXIR_CLIENT_GUIDE.md).
Runnable clients are in [`examples/`](examples/).

## Endpoints

**Killmails**

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/kills/system/:system_id` | Kills in a system (`since_hours`, `limit`) |
| `POST` | `/api/v1/kills/systems` | Bulk fetch across multiple systems |
| `GET` | `/api/v1/kills/cached/:system_id` | Cached kills only, no upstream fetch |
| `GET` | `/api/v1/kills/count/:system_id` | Kill count for a system |
| `GET` | `/api/v1/killmail/:killmail_id` | A single killmail |

**Streaming**

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/kills/stream` | SSE stream |
| `GET` | `/api/v1/kills/stream/enhanced` | SSE with historical preload (`character_ids`, `system_ids`, `preload_days`) |
| — | `/socket` → `killmails:lobby` | WebSocket (Phoenix Channels), no auth |

**Subscriptions**

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/subscriptions` | Create a webhook subscription |
| `GET` | `/api/v1/subscriptions` | List subscriptions |
| `GET` | `/api/v1/subscriptions/stats` | Subscription statistics |
| `DELETE` | `/api/v1/subscriptions/:subscriber_id` | Remove a subscription |

**Operations**

`GET /ping` · `GET /health` · `GET /status` · `GET /metrics` ·
`GET /api/openapi` · `GET /websocket` (connection info)

The service root (`GET /`) serves an HTML status dashboard with live health and
cache metrics.

## Configuration

Set at runtime via environment variables (see `config/runtime.exs`):

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `4004` | HTTP listen port |
| `BIND_IP` | `0.0.0.0` | Listen address |
| `HOST` / `SCHEME` / `URL_PORT` | `localhost` / env-dependent | Externally advertised URL |
| `SMART_RATE_LIMITING` | `true` | Adaptive upstream rate limiting |
| `REQUEST_COALESCING` | `true` | Collapse duplicate in-flight requests |
| `HISTORICAL_STREAMING_ENABLED` | `false` | Backfill historical killmails on boot |
| `HISTORICAL_START_DATE` | `20240101` | Backfill start date (`YYYYMMDD`) |

Rate limit, timeout, and backoff defaults encode observed zKillboard and ESI
behavior — change them only with a reason to.

## How it works

```
zKillboard (RedisQ) ─┐
                     ├─→ Validation → ESI enrichment → Cache ─→ REST / WS / SSE / PubSub
Historical fetch ────┘
```

- **`Ingest.R2Z2`** consumes zKillboard's RedisQ stream and drives the
  processing pipeline.
- **`Ingest.SmartRateLimiter`** governs upstream request rates, with circuit
  breaking and request coalescing around zKillboard and ESI.
- **`Ingest.HistoricalFetcher`** backfills past killmails when enabled.
- **`Core.Cache`** stores everything across four namespaces — `killmails`,
  `systems`, `esi_data`, and `temp_data` — each with its own TTL.
- **`Subs`** maintains subscriptions and their system/character indexes, and
  broadcasts matches to WebSocket, SSE, and PubSub consumers.

The `Core`, `Domain`, and `Ingest` layers declare compile-time boundaries
enforced by the `:boundary` compiler, so cross-layer calls have to go through
each layer's declared public API.

## Development

```bash
mix test              # full suite
mix test.core         # library tests, no web endpoint
mix test.headless     # offline-safe run (excludes :web)
mix test.perf         # performance suite
mix check             # format --check-formatted + credo + dialyzer
mix format
```

`mix check` is the gate to run before opening a PR.

Subscriber payload shapes are a public contract documented in the guides under
`docs/`. Additive fields are fine; renames and removals are breaking and need
those docs updated in the same change.

Debugging recipes live in [DEBUG_COMMANDS.md](DEBUG_COMMANDS.md).
