//! CLI argument parsing via clap.
//!
//! Replaces Python's argparse-based `ArgsAnalyze` + `ArgsPreProc`.

use crate::models::{DialoutMode, ServerConfig};
use clap::Parser;

/// gRPC Dialout Telemetry Collector Server
#[derive(Parser, Debug)]
#[command(name = "grpc-server-rust", version = "3.0.7")]
#[command(about = "Production-grade gRPC Dialout Telemetry Collector")]
pub struct CliArgs {
    // ── Dialout mode (mutually exclusive) ──────────────────────────
    /// gRPC 2-layer dial-out
    #[arg(short = 'n', long = "normal", group = "mode")]
    pub normal: bool,

    /// gRPC 3-layer dial-out (GPB)
    #[arg(short = 'b', long = "gpb", group = "mode")]
    pub gpb: bool,

    /// gRPC gNMI dial-out
    #[arg(short = 'm', long = "gnmi", group = "mode")]
    pub gnmi: bool,

    /// UDP 2-layer dial-out
    #[arg(short = 'u', long = "udp", group = "mode")]
    pub udp: bool,

    // ── General options ─────────────────────────────────────────────
    /// Listen port (default: 50051)
    #[arg(short = 'p', long = "port", default_value_t = 50051)]
    pub port: u16,

    /// Enable TLS mode (not supported for UDP)
    #[arg(short = 't', long = "tls")]
    pub tls: bool,

    /// Generate proto descriptor files and exit
    #[arg(short = 'g', long = "generate")]
    pub generate: bool,

    /// Enable log file output
    #[arg(short = 'l', long = "log")]
    pub log: bool,

    /// Output original data without parsing
    #[arg(short = 'o', long = "orignal")]
    pub orignal: bool,

    /// Format JSON data with indentation
    #[arg(short = 'f', long = "format")]
    pub format: bool,

    /// Include default value fields
    #[arg(short = 'i', long = "including_default")]
    pub including_default: bool,

    /// Performance mode (only count messages)
    #[arg(short = 's', long = "performance_mode")]
    pub performance_mode: bool,

    /// gRPC verbosity log level: debug, info, error
    #[arg(short = 'd', long = "debug", default_value = "error")]
    pub debug: String,

    /// gRPC GRPC_TRACE type (comma-separated)
    #[arg(short = 'c', long = "trace")]
    pub trace: Option<String>,

    // ── Log file configuration ──────────────────────────────────────
    /// Log file max count (default: 50)
    #[arg(long = "logfile-num", default_value_t = 50)]
    pub logfile_num: usize,

    /// Log file max size in MB (default: 50)
    #[arg(long = "logfile-size", default_value_t = 50)]
    pub logfile_size: usize,
}

impl CliArgs {
    /// Determine the dialout mode from CLI flags.
    /// Maps Python's `ArgsPreProc` logic.
    pub fn dialout_mode(&self) -> DialoutMode {
        if self.gpb {
            DialoutMode::Gpb
        } else if self.gnmi {
            DialoutMode::Gnmi
        } else if self.udp {
            DialoutMode::Udp
        } else {
            DialoutMode::Normal
        }
    }

    /// Convert CLI args into a `ServerConfig`.
    pub fn into_server_config(self, mode: DialoutMode) -> ServerConfig {
        ServerConfig {
            mode,
            port: self.port,
            tls: self.tls,
            orignal: self.orignal,
            format_json: self.format,
            including_default: self.including_default,
            performance_mode: self.performance_mode,
            debug: self.debug,
            trace: self.trace,
            log_output: self.log,
            logfile_num: self.logfile_num,
            logfile_size: self.logfile_size,
            only_log_file: false,
        }
    }
}
