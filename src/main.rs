use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
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
}

/// Simulated aircraft parameters. The aircraft starts `SIM_START_OFFSET_M`
/// north of the restricted-area center. For the first `SIM_PERPENDICULAR_S`
/// seconds it flies east (perpendicular to the line to the center, so the
/// trajectory predictor sees no intrusion), then turns due south and
/// continues until it has passed through the cylinder. After exit it lingers
/// for `SIM_LINGER_S` before disappearing.
const SIM_START_OFFSET_M: f64 = 2500.0;
const SIM_SPEED_MS: f64 = 50.0; // ≈ 97 kt
const SIM_ALT_FT: f64 = 200.0;
const SIM_LINGER_S: f64 = 10.0;
const SIM_PERPENDICULAR_S: f64 = 3.0;

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
        .route("/adsb/ws", get(ws_handler))
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
    let client = reqwest::Client::builder()
        .user_agent("mc-adsb/0.1")
        .timeout(Duration::from_secs(10))
        .build()?;

    let interval = Duration::from_secs(state.args.poll_interval.max(1));
    let url = format!(
        "https://api.adsb.lol/v2/lat/{}/lon/{}/dist/{}",
        state.args.lat, state.args.lon, state.args.radius
    );

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let restricted = RestrictedArea {
        lat: state.args.lat,
        lon: state.args.lon,
        radius_m: state.args.restricted_radius_m,
        altitude_ft: state.args.restricted_alt_ft,
    };

    loop {
        ticker.tick().await;
        match fetch(&client, &url).await {
            Ok(payload) => {
                let mut ac = payload.ac;
                if let Some(sim_ac) = simulated_aircraft(&state, &restricted).await {
                    ac.push(sim_ac);
                }
                let aircraft = annotate_aircraft(
                    ac,
                    &restricted,
                    state.args.predict_horizon_s,
                );
                let snapshot = Snapshot {
                    center: Center {
                        lat: state.args.lat,
                        lon: state.args.lon,
                    },
                    radius_nm: state.args.radius,
                    restricted: restricted.clone(),
                    now: payload.now,
                    total: payload.total,
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
            Err(e) => {
                tracing::warn!("fetch failed: {e:#}");
            }
        }
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

async fn simulate(State(state): State<AppState>) -> impl IntoResponse {
    let mut guard = state.sim.lock().await;
    *guard = Some(Instant::now());
    tracing::info!("simulated aircraft SIM001 (re)started");
    "started"
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

    // x_off is the constant east offset that accumulates during the
    // perpendicular leg; the southbound leg then flies a line offset by x_off
    // from the center, so the aircraft must still travel inside the cylinder.
    let x_off = SIM_PERPENDICULAR_S * SIM_SPEED_MS;
    // Half-chord through the cylinder along that offset line.
    let half_chord = (r.radius_m * r.radius_m - x_off * x_off).max(0.0).sqrt();
    let exit_t = SIM_PERPENDICULAR_S + (SIM_START_OFFSET_M + half_chord) / SIM_SPEED_MS;
    let lifetime = exit_t + SIM_LINGER_S;
    if t > lifetime {
        *guard = None;
        return None;
    }

    // Local equirectangular frame centered on the restricted area. +x is east,
    // +y is north. Phase 1: fly east. Phase 2: turn south, x stays at x_off.
    let (x_m, y_m, track_deg) = if t < SIM_PERPENDICULAR_S {
        (SIM_SPEED_MS * t, SIM_START_OFFSET_M, 90.0)
    } else {
        let dt = t - SIM_PERPENDICULAR_S;
        (x_off, SIM_START_OFFSET_M - SIM_SPEED_MS * dt, 180.0)
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
