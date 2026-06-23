//! gRPC Dialout Telemetry Collector - Rust Rewrite
//!
//! Production-grade Rust implementation with zero-copy strategies
//! to address high memory usage from the original Python version.

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

use clap::Parser;
use config::CliArgs;
use error::AppError;
use server::Server;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = CliArgs::parse();

    // Initialize console logging
    let filter = match args.debug.as_str() {
        "debug" => EnvFilter::new("debug"),
        "info" => EnvFilter::new("info"),
        _ => EnvFilter::new("info"),
    };

    // Only initialize console subscriber if not already initialized
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();

    // Handle special commands
    if args.generate {
        proto_dynamic::generate_descriptor_files()?;
        tracing::info!("Generated .desc proto descriptor files successfully");
        return Ok(());
    }

    let dialout_mode = args.dialout_mode();

    // Setup file logging if requested
    if args.log {
        logging_setup::init_file_logging(
            &dialout_mode,
            args.port,
            args.logfile_num,
            args.logfile_size,
        )?;
    }

    // Create and start the server
    let mut srv = Server::new(args.into_server_config(dialout_mode));
    srv.start()?;

    Ok(())
}
