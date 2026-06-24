//! Logging setup for the gRPC Dialout server.
//!
//! Replaces Python's CustomRotatingFileHandler and TextHandler with
//! tracing-subscriber + tracing-appender for rotating file output.

use std::path::{Path, PathBuf};

use crate::error::{AppError, Result};
use crate::models::DialoutMode;
use chrono::Local;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Log directory name (relative to binary path).
const LOGS_DIR: &str = "logs";
const BACKUP_DIR: &str = "backup";

/// Initialize file-based logging with rotation.
///
/// Creates the log directory if needed, cleans up old backup logs,
/// and attaches a rolling file appender to the tracing subscriber.
pub fn init_file_logging(
    mode: &DialoutMode,
    port: u16,
    max_files: usize,
    _max_size_mb: usize,
) -> Result<()> {
    let base_dir = std::env::current_dir().map_err(|e| AppError::Io(e))?;
    let log_dir = base_dir.join(LOGS_DIR);

    if !log_dir.exists() {
        std::fs::create_dir_all(&log_dir)?;
    }

    // Move old logs to backup
    rotate_old_logs(&log_dir)?;

    // Build a mode-specific filename prefix
    let mode_name = mode.as_str()
        .replace(' ', "_")
        .replace('-', "_")
        .to_lowercase();
    let prefix = format!("{}_server_{}", mode_name, port);

    // Use a timestamped file name for the current log
    let timestamp = Local::now().format("_%Y%m%d%H%M%S");
    let _filename = format!("{}.log", prefix);

    // Use custom rotation by size via tracing-appender
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::NEVER) // We manage rotation ourselves
        .filename_prefix(&prefix)
        .filename_suffix(&timestamp.to_string())
        .max_log_files(max_files)
        .build(&log_dir)
        .map_err(|e| AppError::Config(format!("{}", e)))?;

    // Build a non-blocking writer
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_target(true)
        .with_file(true)
        .with_line_number(true);

    // Combine with the existing console subscriber
    let filter = EnvFilter::new("debug");
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(std::io::stdout))
        .with(file_layer)
        .try_init()
        .map_err(|e| AppError::Config(format!("Failed to initialize logging: {}", e)))?;

    tracing::info!("Log file initialized: {}", log_dir.display());
    Ok(())
}

/// Rotate old log files: move current logs to backup, delete backup-of-backup.
fn rotate_old_logs(log_dir: &Path) -> Result<()> {
    let backup_dir = log_dir.join(BACKUP_DIR);

    if !backup_dir.exists() {
        std::fs::create_dir_all(&backup_dir)?;
    }

    // Delete all files in backup directory (backup-of-backup)
    if let Ok(entries) = std::fs::read_dir(&backup_dir) {
        for entry in entries.flatten() {
            if entry.path().is_file() {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    // Move current log files to backup
    if let Ok(entries) = std::fs::read_dir(log_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                let dest = backup_dir.join(
                    path.file_name()
                        .ok_or_else(|| AppError::Config("Invalid file name in log dir".into()))?,
                );
                let _ = std::fs::rename(&path, &dest);
            }
        }
    }

    Ok(())
}

/// Returns the path to the current working directory.
pub fn current_dir() -> Result<PathBuf> {
    std::env::current_dir().map_err(Into::into)
}
