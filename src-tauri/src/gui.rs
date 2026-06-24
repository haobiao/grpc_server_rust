//! Tauri GUI entry point.
//!
//! Exposes `start_server` / `stop_server` commands to the frontend and forwards
//! all tracing logs to the webview via the `log-line` event.
//!
//! The server runs on a dedicated OS thread that owns its own tokio runtime;
//! stopping is signalled through a `tokio::sync::watch` channel, after which
//! dropping the runtime tears down every spawned task (gRPC serve futures /
//! UDP listeners).

use std::sync::Mutex;

use serde::Deserialize;
use tauri::{Emitter, State};
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;

use crate::models::{DialoutMode, ServerConfig};
use crate::server::Server;

/// Configuration payload sent from the frontend.
#[derive(Debug, Clone, Deserialize)]
pub struct GuiConfig {
    /// "normal" | "gpb" | "gnmi" | "udp"
    pub mode: String,
    pub port: u16,
    pub tls: bool,
    pub orignal: bool,
    #[serde(rename = "formatJson")]
    pub format_json: bool,
    #[serde(rename = "performanceMode")]
    pub performance_mode: bool,
}

impl GuiConfig {
    /// Parse the mode string into a `DialoutMode`.
    fn dialout_mode(&self) -> Result<DialoutMode, String> {
        match self.mode.as_str() {
            "normal" => Ok(DialoutMode::Normal),
            "gpb" => Ok(DialoutMode::Gpb),
            "gnmi" => Ok(DialoutMode::Gnmi),
            "udp" => Ok(DialoutMode::Udp),
            other => Err(format!("Unknown mode: '{}'", other)),
        }
    }

    /// Build a `ServerConfig` (CLI defaults fill in the GUI-irrelevant fields).
    fn to_server_config(&self, mode: DialoutMode) -> ServerConfig {
        ServerConfig {
            mode,
            port: self.port,
            tls: self.tls,
            orignal: self.orignal,
            format_json: self.format_json,
            // Always include default fields in the GUI — mirrors CLI default.
            including_default: true,
            performance_mode: self.performance_mode,
            debug: "error".into(),
            trace: None,
            log_output: false,
            logfile_num: 50,
            logfile_size: 50,
            only_log_file: false,
        }
    }
}

/// Holds the background server thread plus the stop sender.
///
/// Both are `Option` so they can be taken (and the thread joined) by
/// `stop_server` / the next `start_server`.
#[derive(Default)]
pub struct AppState {
    server_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    stop_tx: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
}

// ── tracing → frontend bridge ─────────────────────────────────────────

/// `tracing` layer that mirrors every event to the webview as a `log-line` event.
struct TauriLogLayer {
    app: tauri::AppHandle,
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

        let msg = if visitor.message.is_empty() {
            format!("{}", event.level())
        } else {
            format!("{}: {}", event.level(), visitor.message)
        };

        let _ = self.app.emit("log-line", msg);
    }
}

/// tracing field visitor that collects only the conventional `message` field.
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

/// Install the global tracing subscriber so that logs produced by the server
/// thread (and its tokio worker threads) are forwarded to the frontend.
///
/// `set_global_default` permanently installs the subscriber for the whole
/// process — there is no guard to keep alive. It can only succeed once; a
/// second call returns `Err`, which we safely ignore.
fn install_global_logger(app: &tauri::AppHandle) {
    use tracing_subscriber::layer::SubscriberExt;
    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(TauriLogLayer { app: app.clone() });

    let _ = tracing::subscriber::set_global_default(subscriber);
}

// ── Tauri commands ─────────────────────────────────────────────────────

/// Start the dial-out server on a background OS thread.
#[tauri::command]
async fn start_server(
    config: GuiConfig,
    state: State<'_, AppState>,
    app: tauri::AppHandle,
) -> Result<(), String> {
    // If a previous run finished on its own, reap it before starting again.
    {
        let mut thread_lock = state.server_thread.lock().unwrap();
        if let Some(handle) = thread_lock.as_ref() {
            if !handle.is_finished() {
                return Err("Server is already running".into());
            }
            // Thread is done — take & discard the handle to free the slot.
            let _ = thread_lock.take();
        }
        // Drop the stale stop sender (it has already been consumed).
        *state.stop_tx.lock().unwrap() = None;
    }

    let mode = config.dialout_mode()?;
    let server_config = config.to_server_config(mode);

    // watch channel used to ask the server thread to stop.
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

/// Stop the currently running server, if any.
#[tauri::command]
async fn stop_server(state: State<'_, AppState>) -> Result<(), String> {
    // Signal the server thread to stop, then take the handle out of the
    // mutex *before* awaiting its join (so we don't hold the lock across the
    // await — that would deadlock any start_server call happening concurrently).
    let handle = {
        if let Some(tx) = state.stop_tx.lock().unwrap().take() {
            let _ = tx.send(true);
        }
        state.server_thread.lock().unwrap().take()
    };

    if let Some(handle) = handle {
        let _ = tokio::task::spawn_blocking(move || handle.join()).await;
    }

    Ok(())
}

// ── entry point ────────────────────────────────────────────────────────

/// Build and run the Tauri application.
pub fn run() {
    tauri::Builder::default()
        .manage(AppState::default())
        .setup(|app| {
            // Install the global tracing subscriber so all logs (including
            // those from the server thread and its tokio workers) are
            // forwarded to the frontend via the `log-line` event.
            install_global_logger(app.handle());

            let _ = app.emit("log-line", "gRPC Dialout Server GUI ready".to_string());
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![start_server, stop_server])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
