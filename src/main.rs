use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about = "ADS-B websocket relay backed by adsb.lol")]
struct Args {
    /// Latitude for the centre of the search area
    #[arg(long, env = "ADSB_LAT", default_value_t = 48.2082)]
    lat: f64,

    /// Longitude for the centre of the search area
    #[arg(long, env = "ADSB_LON", default_value_t = 16.3738)]
    lon: f64,

    /// Search radius in nautical miles (adsb.lol caps this at 250)
    #[arg(long, env = "ADSB_RADIUS", default_value_t = 100)]
    radius: u32,

    /// Polling interval in seconds. adsb.lol does not stream, so we poll.
    #[arg(long, env = "ADSB_POLL_INTERVAL", default_value_t = 3)]
    poll_interval: u64,

    /// Address to bind the HTTP/WebSocket server to (host:port)
    #[arg(long, env = "ADSB_BIND", default_value = "0.0.0.0:3008")]
    bind: SocketAddr,

    /// Radius of the restricted area in meters, centered on lat/lon.
    #[arg(long, env = "ADSB_RESTRICTED_RADIUS_M", default_value_t = 500.0)]
    restricted_radius_m: f64,

    /// Upper altitude bound (feet) for the restricted area. Aircraft above this are not alerted.
    #[arg(long, env = "ADSB_RESTRICTED_ALT_FT", default_value_t = 400.0)]
    restricted_alt_ft: f64,

    /// How far ahead (seconds) to project each aircraft's trajectory when checking intrusion.
    #[arg(long, env = "ADSB_PREDICT_HORIZON_S", default_value_t = 60.0)]
    predict_horizon_s: f64,
}

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<String>,
    latest: Arc<tokio::sync::RwLock<Option<String>>>,
    args: Args,
    sim: Arc<tokio::sync::Mutex<Option<Instant>>>,
    history: Arc<tokio::sync::RwLock<HashMap<String, Trail>>>,
}

struct Trail {
    /// Previously-reported positions, oldest first. The current position is
    /// tracked separately in `last_pos` and is *not* included here.
    /// Tuple is (lat, lon, alt_ft) — altitude in feet, 0 for ground.
    positions: VecDeque<(f64, f64, f64)>,
    last_pos: Option<(f64, f64, f64)>,
    last_seen: Instant,
}

const HISTORY_LEN: usize = 10;
const HISTORY_STALE_S: u64 = 30;

/// Simulated aircraft parameters. The aircraft starts north of the
/// restricted-area center on an east-bound (perpendicular) course. After
/// `SIM_PERPENDICULAR_S` seconds it begins a coordinated right-hand turn at
/// `SIM_TURN_RATE_DEG_S` until rolled out heading due south, then flies
/// straight through the cylinder. The start position is offset west so that
/// the post-turn track passes through the center. After exit it lingers for
/// `SIM_LINGER_S` before disappearing.
const SIM_START_OFFSET_M: f64 = 2500.0;
const SIM_SPEED_MS: f64 = 50.0; // ≈ 97 kt
const SIM_ALT_FT: f64 = 200.0;
const SIM_LINGER_S: f64 = 10.0;
const SIM_PERPENDICULAR_S: f64 = 3.0;
const SIM_TURN_RATE_DEG_S: f64 = 6.0; // Rate-2 turn, ~30° bank for a light aircraft

#[derive(Debug, Deserialize)]
struct AdsbResponse {
    #[serde(default)]
    ac: Vec<serde_json::Value>,
    #[serde(default)]
    now: Option<u64>,
    #[serde(default)]
    total: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Snapshot {
    center: Center,
    radius_nm: u32,
    restricted: RestrictedArea,
    now: Option<u64>,
    total: Option<u64>,
    aircraft: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct Center {
    lat: f64,
    lon: f64,
}

#[derive(Debug, Serialize, Clone)]
struct RestrictedArea {
    lat: f64,
    lon: f64,
    radius_m: f64,
    altitude_ft: f64,
}

#[derive(Debug, Serialize)]
struct AlertInfo {
    /// Closest predicted distance (meters) from the restricted-area center within the horizon.
    min_distance_m: f64,
    /// Seconds until that closest approach (0 if already inside).
    eta_s: f64,
    /// Whether the aircraft is currently inside the restricted cylinder.
    inside: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,hyper=warn")),
        )
        .init();

    let args = Args::parse();
    tracing::info!(
        "starting mc-adsb: lat={}, lon={}, radius={}nm, poll={}s, bind={}, restricted=(r={}m, alt<={}ft, horizon={}s)",
        args.lat,
        args.lon,
        args.radius,
        args.poll_interval,
        args.bind,
        args.restricted_radius_m,
        args.restricted_alt_ft,
        args.predict_horizon_s,
    );

    let (tx, _rx) = broadcast::channel::<String>(64);
    let state = AppState {
        tx: tx.clone(),
        latest: Arc::new(tokio::sync::RwLock::new(None)),
        args: args.clone(),
        sim: Arc::new(tokio::sync::Mutex::new(None)),
        history: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
    };

    let poller_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = poll_loop(poller_state).await {
            tracing::error!("poll loop terminated: {e:#}");
        }
    });

    let app = Router::new()
        .route("/adsb", get(index))
        .route("/adsb/", get(index))
        .route("/adsb/at_asp.json", get(airspaces_json))
        .route("/adsb/at_apt.json", get(airports_json))
        .route("/adsb/ws", get(ws_handler))
        .route("/adsb/snapshot", get(snapshot))
        .route("/adsb/simulate", get(simulate))
        .route("/adsb/healthz", get(|| async { "ok" }))
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    tracing::info!("listening on http://{}/adsb/", args.bind);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn poll_loop(state: AppState) -> anyhow::Result<()> {
    // Per-request timeout of 1s: adsb.lol is sometimes slow, and the API caps
    // us at 1 req/sec anyway, so a stuck request must not be allowed to stall
    // or pile up behind subsequent attempts.
    let client = reqwest::Client::builder()
        .user_agent("mc-adsb/0.1")
        .timeout(Duration::from_secs(1))
        .build()?;

    let interval = Duration::from_secs(state.args.poll_interval.max(1));
    let url = format!(
        "https://api.adsb.lol/v2/lat/{}/lon/{}/dist/{}",
        state.args.lat, state.args.lon, state.args.radius
    );

    let restricted = RestrictedArea {
        lat: state.args.lat,
        lon: state.args.lon,
        radius_m: state.args.restricted_radius_m,
        altitude_ft: state.args.restricted_alt_ft,
    };

    let backoff_max = Duration::from_secs(10);
    let mut backoff = Duration::ZERO;

    loop {
        tokio::time::sleep(interval + backoff).await;
        let (ac_upstream, now, total) = match fetch(&client, &url).await {
            Ok(payload) => {
                backoff = Duration::ZERO;
                (payload.ac, payload.now, payload.total)
            }
            Err(e) => {
                backoff = if backoff.is_zero() {
                    Duration::from_secs(1)
                } else {
                    (backoff * 2).min(backoff_max)
                };
                tracing::warn!("fetch failed (next retry in {}s): {e:#}", (interval + backoff).as_secs());
                (Vec::new(), None, None)
            }
        };

        let mut ac = ac_upstream;
        let sim_active = if let Some(sim_ac) = simulated_aircraft(&state, &restricted).await {
            ac.push(sim_ac);
            true
        } else {
            false
        };

        // Skip the broadcast when both upstream is dead and sim is idle —
        // there is nothing new to say, and the existing `latest` snapshot
        // stays available for new WebSocket clients.
        if ac.is_empty() && !sim_active && now.is_none() {
            continue;
        }

        update_history(&state.history, &mut ac).await;
        let aircraft = annotate_aircraft(ac, &restricted, state.args.predict_horizon_s);
        let snapshot = Snapshot {
            center: Center {
                lat: state.args.lat,
                lon: state.args.lon,
            },
            radius_nm: state.args.radius,
            restricted: restricted.clone(),
            now,
            total,
            aircraft,
        };
        let count = snapshot.aircraft.len();
        let json = serde_json::to_string(&snapshot)?;
        {
            let mut latest = state.latest.write().await;
            *latest = Some(json.clone());
        }
        let _ = state.tx.send(json);
        tracing::debug!("broadcast {} aircraft", count);
    }
}

async fn fetch(client: &reqwest::Client, url: &str) -> anyhow::Result<AdsbResponse> {
    let resp = client
        .get(url)
        .header("Accept", "application/json")
        .send()
        .await?
        .error_for_status()?;
    let parsed = resp.json::<AdsbResponse>().await?;
    Ok(parsed)
}

async fn index() -> impl IntoResponse {
    Html(include_str!("../web/index.html"))
}

async fn snapshot(State(state): State<AppState>) -> impl IntoResponse {
    match state.latest.read().await.clone() {
        Some(json) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            json,
        )
            .into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "no snapshot available yet",
        )
            .into_response(),
    }
}

async fn airspaces_json() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        include_str!("../web/at_asp.json"),
    )
}

async fn airports_json() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        include_str!("../web/at_apt.json"),
    )
}

async fn simulate(State(state): State<AppState>) -> impl IntoResponse {
    let mut guard = state.sim.lock().await;
    *guard = Some(Instant::now());
    // Drop any stale trail from a prior run so the restarted aircraft does
    // not appear to be connected to its previous track.
    state.history.write().await.remove("SIM001");
    tracing::info!("simulated aircraft SIM001 (re)started");
    "started"
}

/// Updates the per-aircraft trail map in place and injects an `mc_history`
/// array on each aircraft. The injected list holds previous positions only,
/// oldest first — the current `lat`/`lon` is not duplicated. Entries for
/// aircraft not seen this tick are pruned after `HISTORY_STALE_S`.
async fn update_history(
    history: &Arc<tokio::sync::RwLock<HashMap<String, Trail>>>,
    aircraft: &mut [serde_json::Value],
) {
    let now = Instant::now();
    let mut hist = history.write().await;

    for ac in aircraft.iter_mut() {
        let id = ac
            .get("hex")
            .and_then(|v| v.as_str())
            .or_else(|| ac.get("r").and_then(|v| v.as_str()))
            .or_else(|| ac.get("flight").and_then(|v| v.as_str()))
            .map(|s| s.trim().to_string());
        let id = match id.filter(|s| !s.is_empty()) {
            Some(id) => id,
            None => continue,
        };
        let lat = ac.get("lat").and_then(|v| v.as_f64());
        let lon = ac.get("lon").and_then(|v| v.as_f64());
        let (lat, lon) = match (lat, lon) {
            (Some(la), Some(lo)) => (la, lo),
            _ => continue,
        };

        let trail = hist.entry(id).or_insert_with(|| Trail {
            positions: VecDeque::new(),
            last_pos: None,
            last_seen: now,
        });
        trail.last_seen = now;
        let alt_ft = match ac.get("alt_baro") {
            Some(serde_json::Value::String(s)) if s == "ground" => 0.0,
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => ac
                .get("alt_geom")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
        };
        let new_pos = (lat, lon, alt_ft);
        if let Some(prev) = trail.last_pos {
            if (prev.0, prev.1) != (new_pos.0, new_pos.1) {
                trail.positions.push_back(prev);
                while trail.positions.len() > HISTORY_LEN {
                    trail.positions.pop_front();
                }
            }
        }
        trail.last_pos = Some(new_pos);

        let trail_arr: Vec<serde_json::Value> = trail
            .positions
            .iter()
            .map(|&(la, lo, al)| serde_json::json!([la, lo, al]))
            .collect();
        if let Some(obj) = ac.as_object_mut() {
            obj.insert("mc_history".to_string(), serde_json::Value::Array(trail_arr));
        }
    }

    hist.retain(|_, t| now.duration_since(t.last_seen).as_secs() < HISTORY_STALE_S);
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = state.tx.subscribe();

    if let Some(snapshot) = state.latest.read().await.clone() {
        if sender.send(Message::Text(snapshot)).await.is_err() {
            return;
        }
    }

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let mut recv_task = tokio::spawn(async move {
        while let Some(msg) = receiver.next().await {
            match msg {
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
}

/// Returns a synthesized SIM001 aircraft record if a simulation is currently
/// active. The aircraft starts north of the restricted area, flies due south
/// through it, then lingers `SIM_LINGER_S` after exiting before disappearing.
/// Clears the sim state once the lifetime is exceeded.
async fn simulated_aircraft(
    state: &AppState,
    r: &RestrictedArea,
) -> Option<serde_json::Value> {
    let mut guard = state.sim.lock().await;
    let started = (*guard)?;
    let t = started.elapsed().as_secs_f64();

    // Turn geometry: a 90° right-hand turn at SIM_TURN_RATE_DEG_S sweeps an
    // arc of radius v/ω and takes 90°/ω seconds.
    let turn_rate_rad = SIM_TURN_RATE_DEG_S.to_radians();
    let turn_radius = SIM_SPEED_MS / turn_rate_rad;
    let turn_duration = std::f64::consts::FRAC_PI_2 / turn_rate_rad;

    // Local equirectangular frame: +x east, +y north. To make the post-turn
    // southbound leg pass through (0, 0), back the start position west so
    // that east-leg drift plus arc displacement zeroes out at rollout.
    let perp_dx = SIM_PERPENDICULAR_S * SIM_SPEED_MS;
    let x_start = -(perp_dx + turn_radius);
    let y_start = SIM_START_OFFSET_M;
    let x_perp_end = x_start + perp_dx; // = -turn_radius
    let arc_cx = x_perp_end;
    let arc_cy = y_start - turn_radius;
    let rollout_y = y_start - turn_radius;

    let exit_t = SIM_PERPENDICULAR_S
        + turn_duration
        + (rollout_y + r.radius_m) / SIM_SPEED_MS;
    let lifetime = exit_t + SIM_LINGER_S;
    if t > lifetime {
        *guard = None;
        return None;
    }

    let (x_m, y_m, track_deg) = if t < SIM_PERPENDICULAR_S {
        // Phase 1: straight east on a perpendicular course.
        (x_start + SIM_SPEED_MS * t, y_start, 90.0)
    } else if t < SIM_PERPENDICULAR_S + turn_duration {
        // Phase 2: coordinated right turn from 090° to 180°.
        let s = t - SIM_PERPENDICULAR_S;
        let alpha = turn_rate_rad * s;
        let x = arc_cx + turn_radius * alpha.sin();
        let y = arc_cy + turn_radius * alpha.cos();
        let track = 90.0 + SIM_TURN_RATE_DEG_S * s;
        (x, y, track)
    } else {
        // Phase 3: straight south through the cylinder.
        let s = t - SIM_PERPENDICULAR_S - turn_duration;
        (0.0, rollout_y - SIM_SPEED_MS * s, 180.0)
    };
    let m_per_deg_lat = 111_320.0_f64;
    let m_per_deg_lon = 111_320.0_f64 * r.lat.to_radians().cos();
    let lat = r.lat + y_m / m_per_deg_lat;
    let lon = r.lon + x_m / m_per_deg_lon;

    Some(serde_json::json!({
        "hex": "SIM001",
        "flight": "SIM001  ",
        "r": "SIM001",
        "t": "SIMA",
        "lat": lat,
        "lon": lon,
        "alt_baro": SIM_ALT_FT,
        "gs": SIM_SPEED_MS / 0.514_444,
        "track": track_deg,
        "category": "A1",
    }))
}

/// Adds an `mc_alert` field to each aircraft that is predicted to enter the
/// restricted cylinder within the prediction horizon.
fn annotate_aircraft(
    mut aircraft: Vec<serde_json::Value>,
    restricted: &RestrictedArea,
    horizon_s: f64,
) -> Vec<serde_json::Value> {
    for ac in aircraft.iter_mut() {
        if let Some(alert) = predict_intrusion(ac, restricted, horizon_s) {
            if let Some(obj) = ac.as_object_mut() {
                obj.insert(
                    "mc_alert".to_string(),
                    serde_json::to_value(alert).unwrap_or(serde_json::Value::Null),
                );
            }
        }
    }
    aircraft
}

/// Returns Some(AlertInfo) if the aircraft is predicted to enter the restricted
/// cylinder (radius_m, at or below altitude_ft) within `horizon_s` seconds.
fn predict_intrusion(
    ac: &serde_json::Value,
    r: &RestrictedArea,
    horizon_s: f64,
) -> Option<AlertInfo> {
    let lat = ac.get("lat")?.as_f64()?;
    let lon = ac.get("lon")?.as_f64()?;

    // Altitude gate: ignore aircraft known to be above the restricted ceiling.
    // alt_baro may be "ground", a number, or missing. alt_geom is a fallback.
    let alt_ft = match ac.get("alt_baro") {
        Some(serde_json::Value::String(s)) if s == "ground" => 0.0,
        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(f64::INFINITY),
        _ => match ac.get("alt_geom").and_then(|v| v.as_f64()) {
            Some(v) => v,
            None => f64::INFINITY,
        },
    };
    if alt_ft > r.altitude_ft {
        return None;
    }

    // Local equirectangular projection centered on the restricted-area center.
    // The restricted area is small (hundreds of meters), so this is accurate enough.
    let lat_rad = r.lat.to_radians();
    let m_per_deg_lat = 111_320.0_f64;
    let m_per_deg_lon = 111_320.0_f64 * lat_rad.cos();

    let x0 = (lon - r.lon) * m_per_deg_lon;
    let y0 = (lat - r.lat) * m_per_deg_lat;
    let dist0 = (x0 * x0 + y0 * y0).sqrt();

    // Already inside the cylinder.
    if dist0 <= r.radius_m {
        return Some(AlertInfo {
            min_distance_m: dist0,
            eta_s: 0.0,
            inside: true,
        });
    }

    // Need a velocity to project forward.
    let gs_kt = ac.get("gs").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let track_deg = ac.get("track").and_then(|v| v.as_f64());
    if gs_kt <= 0.0 || track_deg.is_none() {
        return None;
    }
    let gs_ms = gs_kt * 0.514_444;
    let track_rad = track_deg.unwrap().to_radians();
    let vx = gs_ms * track_rad.sin();
    let vy = gs_ms * track_rad.cos();

    // Closest approach of the ray (p + t*v) to the origin, t >= 0.
    let v2 = vx * vx + vy * vy;
    let t_closest = -(x0 * vx + y0 * vy) / v2;
    let t_eval = t_closest.clamp(0.0, horizon_s);
    let cx = x0 + vx * t_eval;
    let cy = y0 + vy * t_eval;
    let min_distance = (cx * cx + cy * cy).sqrt();

    if min_distance <= r.radius_m {
        Some(AlertInfo {
            min_distance_m: min_distance,
            eta_s: t_eval,
            inside: false,
        })
    } else {
        None
    }
}
