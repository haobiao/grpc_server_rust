//! gRPC Dialout Telemetry Collector - Rust Rewrite
//!
//! Production-grade Rust implementation with zero-copy strategies
//! to address high memory usage from the original Python version.

// In GUI mode, hide the Windows console window on release builds.
#![cfg_attr(all(feature = "gui", not(debug_assertions)), windows_subsystem = "windows")]

#![allow(dead_code, unused_imports)]

mod chunk;
mod config;
mod dialout;
mod error;
mod logging_setup;
mod models;
mod proto_dynamic;
mod server;
mod udp_header;

#[cfg(feature = "gui")]
mod gui;

use clap::Parser;
use config::CliArgs;
use server::Server;
use tracing_subscriber::EnvFilter;

fn main() {
    #[cfg(feature = "gui")]
    {
        gui::run();
        return;
    }

    #[cfg(not(feature = "gui"))]
    {
        run_cli();
    }
}

/// CLI entry point (default build, no `gui` feature).
#[cfg(not(feature = "gui"))]
fn run_cli() {
    // 初始化日志：支持 -d debug / -d info 控制日志级别
    let log_level = std::env::args()
        .position(|a| a == "-d" || a == "--debug")
        .and_then(|pos| std::env::args().nth(pos + 1))
        .unwrap_or_else(|| "info".to_string());

    let filter = match log_level.as_str() {
        "debug" => EnvFilter::new("debug"),
        _ => EnvFilter::new("info"),
    };

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();

    // 解析 CLI 参数
    let args = CliArgs::parse();

    // 处理特殊命令
    if args.generate {
        if let Err(e) = proto_dynamic::generate_descriptor_files() {
            eprintln!("ERROR: Failed to generate descriptor files: {}", e);
            std::process::exit(1);
        }
        tracing::info!("Generated .desc proto descriptor files successfully");
        return;
    }

    let dialout_mode = args.dialout_mode();

    // 文件日志（如果请求）
    if args.log {
        if let Err(e) = logging_setup::init_file_logging(
            &dialout_mode,
            args.port,
            args.logfile_num,
            args.logfile_size,
        ) {
            eprintln!("ERROR: Failed to init file logging: {}", e);
        }
    }

    // 创建并启动服务器
    let mut srv = Server::new(args.into_server_config(dialout_mode));
    if let Err(e) = srv.start() {
        eprintln!("ERROR: Server failed to start: {}", e);
        tracing::error!("Server failed to start: {}", e);
        std::process::exit(1);
    }
}
