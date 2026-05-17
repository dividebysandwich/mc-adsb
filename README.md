# mc-adsb

ADS-B visualization for an RC club. A small Rust service polls
[adsb.lol](https://adsb.lol) for traffic around a fixed point and rebroadcasts
it to a browser over a WebSocket, together with a configurable cylindrical
restricted area and per-aircraft trajectory-intrusion alerts.

## Run

```
cargo run --release
```

Useful flags (also available as env vars, see `--help`):

| Flag | Default | Meaning |
| --- | --- | --- |
| `--lat` / `--lon` | 48.2082 / 16.3738 | Center of the search and the restricted area |
| `--radius` | 100 | adsb.lol query radius (nm) |
| `--poll-interval` | 3 | Seconds between adsb.lol polls |
| `--bind` | 0.0.0.0:3008 | HTTP/WebSocket listen address |
| `--restricted-radius-m` | 500 | Restricted-area radius in meters |
| `--restricted-alt-ft` | 400 | Restricted-area ceiling in feet (aircraft above are ignored) |
| `--predict-horizon-s` | 60 | How far ahead to project each aircraft's straight-line path |

Open `http://<host>:3008/adsb/` in a browser.

## WebSocket payload

The server pushes one JSON message per poll on `/adsb/ws`. The newest snapshot
is also sent immediately on connect. Every message has the same shape:

```jsonc
{
  "center":   { "lat": 48.2082, "lon": 16.3738 },
  "radius_nm": 100,                       // adsb.lol query radius (nm)
  "restricted": {
    "lat": 48.2082,                       // restricted-area center
    "lon": 16.3738,
    "radius_m": 500.0,                    // restricted-area radius (meters)
    "altitude_ft": 400.0                  // ceiling — aircraft above are not checked
  },
  "now":   1715680000,                    // server timestamp from adsb.lol (epoch s, optional)
  "total": 42,                            // total aircraft adsb.lol saw (optional)
  "aircraft": [ /* see below */ ]
}
```

### Aircraft entries

Each entry in `aircraft[]` is the **raw adsb.lol record** (the `ac[]` objects
from the [adsb.lol v2 API](https://api.adsb.lol/)) passed through unmodified.
The fields the frontend uses are:

| Field | Type | Notes |
| --- | --- | --- |
| `hex` | string | ICAO 24-bit address — used as the stable id |
| `flight` | string | Callsign (may be padded with spaces) |
| `r` | string | Registration |
| `t` | string | ICAO type designator |
| `lat`, `lon` | number | Position (degrees) |
| `alt_baro` | number \| `"ground"` | Barometric altitude (ft) |
| `alt_geom` | number | Geometric altitude (ft), used as a fallback |
| `gs` | number | Ground speed (knots) |
| `track` | number | True track (degrees, 0=N, clockwise) |
| `baro_rate` | number | Vertical speed (ft/min) |
| `squawk`, `category` | string | Standard ADS-B fields |

Records with missing `lat`/`lon` are still passed through; the frontend skips
drawing them.

### Trail history (`mc_history`)

mc-adsb tracks each aircraft's recent positions on the server and attaches them
as `mc_history` on every aircraft entry. The list contains up to the last 10
**previous** positions, oldest first, as `[lat, lon]` pairs. The aircraft's
current `lat`/`lon` is *not* duplicated in `mc_history`. Aircraft that go
unseen for more than 30 seconds are dropped from the trail map, so reconnecting
clients receive whatever trail the server has accumulated so far rather than
starting from empty.

```jsonc
"mc_history": [
  [48.2104, 16.3712],   // oldest
  [48.2099, 16.3720],
  [48.2090, 16.3729]    // most recent previous fix
]
```

### Alert annotation (`mc_alert`)

mc-adsb adds **one extra field**, `mc_alert`, to every aircraft that is
predicted to enter the restricted cylinder within `--predict-horizon-s`
seconds **and** whose altitude is at or below `--restricted-alt-ft`. The field
is omitted entirely on aircraft that are not alerting.

```jsonc
"mc_alert": {
  "min_distance_m": 320.4,   // minimum predicted distance from the center (m)
                             // along the straight-line projection
  "eta_s": 18.5,             // seconds until closest approach (0 if already inside)
  "inside": false            // true if the aircraft is currently inside the cylinder
}
```

The prediction is a straight-line extrapolation of the current `lat`/`lon`
along `track` at `gs`. Aircraft without a valid `gs`/`track` are not alerted
unless they are already inside the cylinder.

## HTTP snapshot

```
curl http://<host>:3008/adsb/snapshot
```

Returns the most recent snapshot — the same JSON payload that is pushed over
the WebSocket — for clients that cannot use WebSockets. Polls should be paced
to `--poll-interval`; faster polling will only return the same cached snapshot.
Returns `503 Service Unavailable` until the first upstream poll has completed.

## Simulating an intrusion

```
curl http://<host>:3008/adsb/simulate
```

Spawns (or restarts) a synthetic aircraft `SIM001` at 200 ft and ~97 kt. For
the first 3 seconds it flies due east on a perpendicular course north of the
restricted area (no intrusion predicted), then rolls into a coordinated 6 °/s
right-hand turn (~30° bank, rate-2). After rolling out heading due south it
flies straight through the cylinder. The aircraft disappears 10 seconds after
exiting; hitting the endpoint again resets it to the start.

## Frontend behavior

- The adsb.lol query radius is drawn as a faint blue circle.
- The restricted area is drawn as a red filled circle.
- Aircraft labels flash white-on-red at 2 Hz when `mc_alert` is present.
