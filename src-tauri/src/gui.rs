//! Tauri GUI entry point.
//!
//! Exposes `start_server` / `stop_server` / `get_stats` commands to the
//! frontend and forwards all tracing logs to the webview via the `log-line`
//! event.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::sync::mpsc as std_mpsc;
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
    /// 选中的模式列表（单模式 ["normal"] 或全模式 ["normal","gpb","gnmi","udp"]）
    pub modes: Vec<String>,
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
    fn dialout_modes(&self) -> Result<Vec<DialoutMode>, String> {
        if self.modes.is_empty() {
            return Ok(vec![DialoutMode::Normal]);
        }
        self.modes
            .iter()
            .map(|s| {
                DialoutMode::from_str_lossy(s)
                    .ok_or_else(|| format!("Unknown mode: '{}'", s))
            })
            .collect()
    }

    fn to_server_config(&self, modes: Vec<DialoutMode>) -> ServerConfig {
        ServerConfig {
            modes,
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
    log_tx: std_mpsc::Sender<String>,
    file_writer: SharedFileWriter,
}

impl Default for AppState {
    fn default() -> Self {
        let (log_tx, _log_rx) = std_mpsc::channel::<String>();
        drop(_log_rx);
        Self {
            server_thread: Mutex::new(None),
            stop_tx: Mutex::new(None),
            start_time: Mutex::new(None),
            log_tx,
            file_writer: Arc::new(Mutex::new(None)),
        }
    }
}

// ── tracing → frontend bridge ─────────────────────────────────────────

/// Shared file writer for optional log-to-file.
/// When `Some`, every log event is also appended to the file.
pub type SharedFileWriter = Arc<Mutex<Option<std::fs::File>>>;

struct TauriLogLayer {
    log_tx: std_mpsc::Sender<String>,
    file_writer: SharedFileWriter,
}

const MAX_CHANNEL_BACKLOG: usize = 2000;

/// Global atomic counter shared between TauriLogLayer (producer) and
/// the Tauri async poll task (consumer).
static LOG_BACKLOG_COUNTER: std::sync::LazyLock<Arc<std::sync::atomic::AtomicUsize>> =
    std::sync::LazyLock::new(|| Arc::new(std::sync::atomic::AtomicUsize::new(0)));

/// Global log config — updated by start_server, read by async poll task.
/// Using globals avoids lifetime issues with Tauri State (which is &T only).
static LOG_ONLY_FILE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static LOG_MAX_BYTES: std::sync::Mutex<u64> = std::sync::Mutex::new(5 * 1024 * 1024);
static LOG_MAX_NUM: std::sync::Mutex<usize> = std::sync::Mutex::new(50);
static LOG_DIR: std::sync::Mutex<std::path::PathBuf> = std::sync::Mutex::new(std::path::PathBuf::from("logs"));
static LOG_PREFIX: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());

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

        // Drop this log if channel backlog exceeds threshold.
        // The consumer (Tauri async poll) decrements log_count after draining.
        if LOG_BACKLOG_COUNTER.load(std::sync::atomic::Ordering::Relaxed) >= MAX_CHANNEL_BACKLOG {
            // Still write to file even if we skip the channel
            if let Ok(mut fw) = self.file_writer.lock() {
                if let Some(ref mut file) = *fw {
                    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
                    let _ = writeln!(file, "{} {}", timestamp, msg);
                }
            }
            return;
        }

        // Send log to Tauri main thread via channel
        if self.log_tx.send(msg.clone()).is_ok() {
            LOG_BACKLOG_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

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
        // For the "message" field, use Display-like formatting (not Debug)
        // to avoid extra quotes and escape characters on long strings.
        if field.name() == "message" {
            // Use the Debug formatter but strip surrounding quotes if it's a string
            let debug_str = format!("{:?}", value);
            // If it looks like a quoted string ("..."), strip the quotes
            if debug_str.starts_with('"') && debug_str.ends_with('"') {
                self.message = debug_str[1..debug_str.len()-1].to_string();
            } else {
                self.message = debug_str;
            }
        }
    }
}

fn install_global_logger(log_tx: std_mpsc::Sender<String>, file_writer: SharedFileWriter) {
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(TauriLogLayer { log_tx, file_writer });

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
        // Set global log config (read by async poll task on every iteration)
        LOG_ONLY_FILE.store(config.only_log_file, std::sync::atomic::Ordering::Relaxed);
        *LOG_MAX_BYTES.lock().unwrap() = (config.logfile_size as u64) * 1024 * 1024;
        *LOG_MAX_NUM.lock().unwrap() = config.logfile_num;

        match open_log_file(&config) {
            Ok((file, dir, prefix)) => {
                *state.file_writer.lock().unwrap() = Some(file);
                *LOG_DIR.lock().unwrap() = dir;
                *LOG_PREFIX.lock().unwrap() = prefix;
            }
            Err(e) => {
                let _ = app.emit("log-line", format!("WARNING: Failed to open log file: {}", e));
            }
        }
    }

    let modes = config.dialout_modes()?;
    let server_config = config.to_server_config(modes);

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
/// Returns (File, log_dir, prefix) for rotation tracking.
fn open_log_file(config: &GuiConfig) -> std::io::Result<(std::fs::File, std::path::PathBuf, String)> {
    let log_dir = std::env::current_dir()?.join("logs");
    if !log_dir.exists() {
        std::fs::create_dir_all(&log_dir)?;
    }

    let mode_name = if config.modes.len() > 1 {
        "multi_mode".to_string()
    } else {
        match config.modes.first().map(|s| s.as_str()) {
            Some("normal") => "grpc_2_layer".into(),
            Some("gpb") => "grpc_3_layer".into(),
            Some("gnmi") => "grpc_gnmi".into(),
            Some("udp") => "udp_2_layer".into(),
            _ => "server".into(),
        }
    };

    let prefix = format!("{}_server_{}", mode_name, config.port);
    let timestamp = chrono::Local::now().format("%Y%m%d%H%M%S");
    let filename = format!("{}_{}.log", prefix, timestamp);
    let filepath = log_dir.join(&filename);

    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&filepath)?;

    tracing::info!("Log file: {}", filepath.display());

    Ok((file, log_dir, prefix))
}

/// Delete oldest log files matching prefix if count exceeds max_num.
fn cleanup_old_logs(log_dir: &std::path::Path, prefix: &str, max_num: usize) {
    let mut files: Vec<(std::path::PathBuf, std::time::SystemTime)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(log_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(prefix) && name.ends_with(".log") {
                if let Ok(meta) = entry.metadata() {
                    if let Ok(modified) = meta.modified() {
                        files.push((entry.path(), modified));
                    }
                }
            }
        }
    }
    // Sort by modified time, oldest first
    files.sort_by_key(|(_, t)| *t);
    // Delete oldest until we have max_num - 1 (leaving room for the new file)
    while files.len() >= max_num {
        let (path, _) = files.remove(0);
        let _ = std::fs::remove_file(&path);
    }
}

pub fn run() {
    let (log_tx, log_rx) = std_mpsc::channel::<String>();

    // Install global tracing subscriber BEFORE Tauri app starts
    // so that all threads (including server thread) inherit it.
    let dummy_fw: SharedFileWriter = Arc::new(Mutex::new(None));
    install_global_logger(log_tx.clone(), dummy_fw);

    // Wrap log_rx so the Tauri async task can poll it
    let log_rx = Arc::new(Mutex::new(log_rx));

    tauri::Builder::default()
        .manage(AppState {
            log_tx: log_tx.clone(),
            ..Default::default()
        })
        .setup(move |app| {
            let app_handle = app.handle().clone();
            let log_rx_clone = log_rx.clone();
            let file_writer: SharedFileWriter = app.state::<AppState>().file_writer.clone();
            let log_count_atomic = LOG_BACKLOG_COUNTER.clone();

            // Poll the channel every 100ms and batch-emit to frontend + write file
            tauri::async_runtime::spawn(async move {
                let mut current_file_size: u64 = 0u64;

                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    let mut messages = Vec::new();
                    {
                        let rx = log_rx_clone.lock().unwrap();
                        while let Ok(msg) = rx.try_recv() {
                            messages.push(msg);
                        }
                    }
                    if messages.is_empty() { continue; }

                    let n = messages.len();
                    log_count_atomic.fetch_sub(n, std::sync::atomic::Ordering::Relaxed);

                    // Read latest config from globals (updated by start_server)
                    let only_log = LOG_ONLY_FILE.load(std::sync::atomic::Ordering::Relaxed);
                    let max_bytes = *LOG_MAX_BYTES.lock().unwrap();
                    let max_num = *LOG_MAX_NUM.lock().unwrap();
                    let log_dir = LOG_DIR.lock().unwrap().clone();
                    let log_prefix = LOG_PREFIX.lock().unwrap().clone();

                    // Reset size counter when file_writer changes (new session)
                    let fw_empty = file_writer.lock().map_or(true, |fw| fw.is_none());
                    if fw_empty { current_file_size = 0; }

                    // Emit to frontend unless only_log_file is set
                    if !only_log {
                        let batch = messages.join("\n");
                        let _ = app_handle.emit("log-line", &batch);
                    }

                    // Write to file if enabled, with size-based rotation
                    if let Ok(mut fw) = file_writer.lock() {
                        if let Some(ref mut file) = *fw {
                            for msg in &messages {
                                let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
                                let line = format!("{} {}\n", timestamp, msg);
                                let line_bytes = line.len() as u64;

                                if current_file_size + line_bytes > max_bytes {
                                    let _ = file.flush();
                                    // Delete old files beyond max_num
                                    cleanup_old_logs(&log_dir, &log_prefix, max_num);
                                    // Create new file
                                    let ts = chrono::Local::now().format("%Y%m%d%H%M%S");
                                    let new_name = format!("{}_{}.log", log_prefix, ts);
                                    let new_path = log_dir.join(&new_name);
                                    match std::fs::OpenOptions::new()
                                        .create(true)
                                        .append(true)
                                        .open(&new_path)
                                    {
                                        Ok(new_file) => {
                                            *fw = Some(new_file);
                                            current_file_size = 0;
                                        }
                                        Err(_) => break,
                                    }
                                }
                                if let Some(ref mut f) = *fw {
                                    let _ = f.write_all(line.as_bytes());
                                    current_file_size += line_bytes;
                                }
                            }
                        }
                    }
                }
            });

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
