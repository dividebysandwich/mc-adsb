use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

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

    /// Address to bind the HTTP/WebSocket server to
    #[arg(long, env = "ADSB_BIND", default_value = "0.0.0.0:8080")]
    bind: SocketAddr,
}

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<String>,
    latest: Arc<tokio::sync::RwLock<Option<String>>>,
    args: Args,
}

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
struct Snapshot<'a> {
    center: Center,
    radius_nm: u32,
    now: Option<u64>,
    total: Option<u64>,
    aircraft: &'a [serde_json::Value],
}

#[derive(Debug, Serialize)]
struct Center {
    lat: f64,
    lon: f64,
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
        "starting mc-adsb: lat={}, lon={}, radius={}nm, poll={}s, bind={}",
        args.lat,
        args.lon,
        args.radius,
        args.poll_interval,
        args.bind
    );

    let (tx, _rx) = broadcast::channel::<String>(64);
    let state = AppState {
        tx: tx.clone(),
        latest: Arc::new(tokio::sync::RwLock::new(None)),
        args: args.clone(),
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

    loop {
        ticker.tick().await;
        match fetch(&client, &url).await {
            Ok(payload) => {
                let snapshot = Snapshot {
                    center: Center {
                        lat: state.args.lat,
                        lon: state.args.lon,
                    },
                    radius_nm: state.args.radius,
                    now: payload.now,
                    total: payload.total,
                    aircraft: &payload.ac,
                };
                let json = serde_json::to_string(&snapshot)?;
                {
                    let mut latest = state.latest.write().await;
                    *latest = Some(json.clone());
                }
                let _ = state.tx.send(json);
                tracing::debug!("broadcast {} aircraft", payload.ac.len());
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
