//! Tauri GUI entry point.
//!
//! Exposes `start_server` / `stop_server` / `get_stats` commands to the
//! frontend and forwards all tracing logs to the webview via the `log-line`
//! event.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tauri::{Emitter, Manager, State};
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;

use crate::models::{self, DialoutMode, ServerConfig};
use crate::server::Server;

/// Configuration payload sent from the frontend.
#[derive(Debug, Clone, Deserialize)]
pub struct GuiConfig {
    pub mode: String,
    pub port: u16,
    pub tls: bool,
    pub orignal: bool,
    #[serde(rename = "formatJson")]
    pub format_json: bool,
    #[serde(rename = "performanceMode")]
    pub performance_mode: bool,
    #[serde(rename = "logOutput", default)]
    pub log_output: bool,
    #[serde(rename = "onlyLogFile", default)]
    pub only_log_file: bool,
    #[serde(rename = "logfileSize", default = "default_logfile_size")]
    pub logfile_size: usize,
    #[serde(rename = "logfileNum", default = "default_logfile_num")]
    pub logfile_num: usize,
}

fn default_logfile_size() -> usize { 100 }
fn default_logfile_num() -> usize { 50 }

impl GuiConfig {
    fn dialout_mode(&self) -> Result<DialoutMode, String> {
        match self.mode.as_str() {
            "normal" => Ok(DialoutMode::Normal),
            "gpb" => Ok(DialoutMode::Gpb),
            "gnmi" => Ok(DialoutMode::Gnmi),
            "udp" => Ok(DialoutMode::Udp),
            other => Err(format!("Unknown mode: '{}'", other)),
        }
    }

    fn to_server_config(&self, mode: DialoutMode) -> ServerConfig {
        ServerConfig {
            mode,
            port: self.port,
            tls: self.tls,
            orignal: self.orignal,
            format_json: self.format_json,
            including_default: true,
            performance_mode: self.performance_mode,
            debug: "error".into(),
            trace: None,
            log_output: self.log_output,
            logfile_num: self.logfile_num,
            logfile_size: self.logfile_size,
            only_log_file: self.only_log_file,
        }
    }
}

/// Response from `get_stats`.
#[derive(Debug, Clone, Serialize)]
pub struct StatsResponse {
    pub is_running: bool,
    pub message_count: u64,
    pub client_count: u64,
    pub uptime_secs: u64,
}

/// Holds the background server thread plus the stop sender.
pub struct AppState {
    server_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    stop_tx: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
    start_time: Mutex<Option<Instant>>,
    /// Shared log file writer — `None` when file logging is disabled.
    file_writer: SharedFileWriter,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            server_thread: Mutex::new(None),
            stop_tx: Mutex::new(None),
            start_time: Mutex::new(None),
            file_writer: Arc::new(Mutex::new(None)),
        }
    }
}

// ── tracing → frontend bridge ─────────────────────────────────────────

/// Shared file writer for optional log-to-file.
/// When `Some`, every log event is also appended to the file.
pub type SharedFileWriter = Arc<Mutex<Option<std::fs::File>>>;

struct TauriLogLayer {
    app: tauri::AppHandle,
    file_writer: SharedFileWriter,
}

impl<S> Layer<S> for TauriLogLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = LogVisitor::default();
        event.record(&mut visitor);

        let level = event.metadata().level();
        let msg = if visitor.message.is_empty() {
            format!("{}", level)
        } else {
            format!("{}: {}", level, visitor.message)
        };

        // Emit to frontend
        let _ = self.app.emit("log-line", &msg);

        // Optionally write to file
        if let Ok(mut fw) = self.file_writer.lock() {
            if let Some(ref mut file) = *fw {
                let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
                let _ = writeln!(file, "{} {}", timestamp, msg);
            }
        }
    }
}

#[derive(Default)]
struct LogVisitor {
    message: String,
}

impl Visit for LogVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        }
    }
}

fn install_global_logger(app: &tauri::AppHandle, file_writer: SharedFileWriter) {
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(TauriLogLayer { app: app.clone(), file_writer });

    let _ = tracing::subscriber::set_global_default(subscriber);
}

// ── Tauri commands ─────────────────────────────────────────────────────

#[tauri::command]
async fn start_server(
    config: GuiConfig,
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<(), String> {
    {
        let mut thread_lock = state.server_thread.lock().unwrap();
        if let Some(handle) = thread_lock.as_ref() {
            if !handle.is_finished() {
                return Err("Server is already running".into());
            }
            let _ = thread_lock.take();
        }
        *state.stop_tx.lock().unwrap() = None;
    }

    // Reset global counters
    models::reset_global_stats();
    *state.start_time.lock().unwrap() = Some(Instant::now());

    // Setup log file if requested
    {
        let mut fw = state.file_writer.lock().unwrap();
        *fw = None; // clear previous
    }

    if config.log_output {
        match open_log_file(&config) {
            Ok(file) => {
                *state.file_writer.lock().unwrap() = Some(file);
            }
            Err(e) => {
                let _ = app.emit("log-line", format!("WARNING: Failed to open log file: {}", e));
            }
        }
    }

    let mode = config.dialout_mode()?;
    let server_config = config.to_server_config(mode);

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

    let mut server = Server::new(server_config);
    let app_for_thread = app.clone();

    let handle = std::thread::spawn(move || {
        let result = server.start_with_stop(stop_rx);

        match result {
            Ok(()) => {
                let _ = app_for_thread.emit("log-line", "Server exited cleanly".to_string());
            }
            Err(e) => {
                let msg = format!("Server error: {}", e);
                let _ = app_for_thread.emit("log-line", msg);
            }
        }
    });

    *state.server_thread.lock().unwrap() = Some(handle);
    *state.stop_tx.lock().unwrap() = Some(stop_tx);

    Ok(())
}

#[tauri::command]
async fn stop_server(state: State<'_, AppState>) -> Result<(), String> {
    let handle = {
        if let Some(tx) = state.stop_tx.lock().unwrap().take() {
            let _ = tx.send(true);
        }
        state.server_thread.lock().unwrap().take()
    };

    if let Some(handle) = handle {
        let _ = tokio::task::spawn_blocking(move || handle.join()).await;
    }

    *state.start_time.lock().unwrap() = None;

    // Close log file
    *state.file_writer.lock().unwrap() = None;

    Ok(())
}

/// Return live statistics to the frontend (polled via `setInterval`).
#[tauri::command]
async fn get_stats(state: State<'_, AppState>) -> Result<StatsResponse, String> {
    let is_running = {
        let lock = state.server_thread.lock().unwrap();
        lock.as_ref().map(|h| !h.is_finished()).unwrap_or(false)
    };

    let uptime_secs = {
        let lock = state.start_time.lock().unwrap();
        lock.map(|t| t.elapsed().as_secs()).unwrap_or(0)
    };

    Ok(StatsResponse {
        is_running,
        message_count: models::get_global_msg_count(),
        client_count: models::get_global_client_count(),
        uptime_secs,
    })
}

// ── entry point ────────────────────────────────────────────────────────

/// Open (or create) a log file under `./logs/` with mode+port+timestamp name.
fn open_log_file(config: &GuiConfig) -> std::io::Result<std::fs::File> {
    let log_dir = std::env::current_dir()?.join("logs");
    if !log_dir.exists() {
        std::fs::create_dir_all(&log_dir)?;
    }

    let mode_name = match config.mode.as_str() {
        "normal" => "grpc_2_layer",
        "gpb" => "grpc_3_layer",
        "gnmi" => "grpc_gnmi",
        "udp" => "udp_2_layer",
        _ => "server",
    };

    let timestamp = chrono::Local::now().format("%Y%m%d%H%M%S");
    let filename = format!("{}_server_{}_{}.log", mode_name, config.port, timestamp);
    let filepath = log_dir.join(&filename);

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&filepath)?;

    // Emit a notice to frontend
    // (file_writer not yet set at this point, so just emit)
    tracing::info!("Log file: {}", filepath.display());

    Ok(file)
}

pub fn run() {
    tauri::Builder::default()
        .manage(AppState::default())
        .setup(|app| {
            let state: tauri::State<AppState> = app.state();
            install_global_logger(app.handle(), state.file_writer.clone());
            let _ = app.emit("log-line", "gRPC Dialout Server GUI ready".to_string());
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            start_server,
            stop_server,
            get_stats
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
