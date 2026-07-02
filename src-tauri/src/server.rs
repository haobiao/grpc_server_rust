//! Unified server management.
//!
//! Creates and starts the appropriate dialout services based on the configured modes.
//! Supports multi-mode coexistence: gRPC services share a single TCP port (HTTP/2 multiplexing),
//! while UDP runs independently on the same port number (separate protocol namespace).
//! Handles TLS configuration, port binding, and graceful shutdown.

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::{Certificate, Identity, Server as TonicServer, ServerTlsConfig};

use crate::dialout::gnmi::{GnmiConfig, GnmiDialoutService};
use crate::dialout::gnmi::proto::gnmi::sonic::g_nmi_dial_out_server::GNmiDialOutServer;
use crate::dialout::gpb_v3::{DialoutV3Service, V3Config};
use crate::dialout::gpb_v3::proto::grpc_dialout_v3::g_rpc_dialout_v3_server::GRpcDialoutV3Server;
use crate::dialout::normal::{DialoutConfig, DialoutService};
use crate::dialout::normal::proto::grpc_dialout::grpc_dialout_server::GrpcDialoutServer;
use crate::dialout::udp::{UdpConfig, UdpDialoutServer};
use crate::error::{AppError, Result};
use crate::models::{DialoutMode, ServerConfig};
use crate::proto_dynamic::ProtoDynamicRegistry;

/// TLS file names.
const ROOT_CERT: &str = "rootCA.crt";
const PRIVATE_KEY: &str = "client.key";
const CERT_CHAIN: &str = "client.crt";
const TLS_DIR: &str = "tls";
const AUTOGEN_DIR: &str = "autogen";
const PROTO_DIR: &str = "proto";

/// Maximum gRPC message size (10 MB).
const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Main server that manages all dialout modes.
pub struct Server {
    config: ServerConfig,
}

impl Server {
    /// Create a new Server with the given configuration.
    pub fn new(config: ServerConfig) -> Self {
        Self { config }
    }

    /// Start the server based on the configured modes (CLI entry point).
    pub fn start(&mut self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .map_err(|e| AppError::Config(format!("Failed to create tokio runtime: {}", e)))?;

        rt.block_on(async { self.start_inner().await })
    }

    /// Start the server with an external stop signal (GUI entry point).
    ///
    /// When `stop_rx` receives a value, the runtime is torn down, aborting all
    /// running tasks (gRPC serve futures / UDP listener tasks).
    pub fn start_with_stop(
        &mut self,
        mut stop_rx: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .map_err(|e| AppError::Config(format!("Failed to create tokio runtime: {}", e)))?;

        rt.block_on(async {
            tokio::select! {
                result = self.start_inner() => result,
                _ = stop_rx.changed() => {
                    tracing::info!("Server stopped by user");
                    Ok(())
                }
            }
        })
    }

    /// Dispatch to multi-mode coexistence start logic.
    async fn start_inner(&self) -> Result<()> {
        self.start_multi().await
    }

    /// Start all configured modes concurrently.
    ///
    /// - gRPC modes (Normal/Gpb/Gnmi) share a single TCP port via HTTP/2 multiplexing.
    /// - UDP mode runs independently on the same port number (separate protocol namespace).
    /// - IPv4 and IPv6 servers run concurrently via `tokio::try_join!`.
    async fn start_multi(&self) -> Result<()> {
        let modes: Vec<DialoutMode> = if self.config.modes.is_empty() {
            vec![DialoutMode::Normal]
        } else {
            self.config.modes.clone()
        };

        let mode_names: Vec<&str> = modes.iter().map(|m| m.as_str()).collect();
        tracing::info!("Server modes [{}] on port {}", mode_names.join(", "), self.config.port);

        // ── Pre-check: verify port availability before starting ──
        let port = self.config.port;
        if let Err(e) = check_port_available(port) {
            tracing::error!("{}", e);
            return Err(AppError::Config(e));
        }

        let addr_v4: SocketAddr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        let addr_v6: SocketAddr = format!("[::]:{}", self.config.port)
            .parse()
            .unwrap_or(addr_v4);

        // ── Determine which sub-services to start ──────────────────────
        let grpc_modes = self.config.grpc_modes();
        let has_udp = self.config.has_udp();

        if grpc_modes.is_empty() && !has_udp {
            return Err(AppError::Config("No dialout modes configured".into()));
        }

        // ── Build shared config objects ────────────────────────────────
        let dialout_config = DialoutConfig {
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };
        let gnmi_config = GnmiConfig {
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };
        let v3_config = V3Config {
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };

        // ── Load proto registry if GPB mode is active ──────────────────
        let registry = if grpc_modes.contains(&DialoutMode::Gpb) {
            let mut reg = ProtoDynamicRegistry::new();
            let current_dir = std::env::current_dir()?;
            let proto_dir = current_dir.join(PROTO_DIR);
            let autogen_dir = current_dir.join(AUTOGEN_DIR);

            if !autogen_dir.exists() {
                if !proto_dir.exists() {
                    return Err(AppError::Config(format!(
                        "Proto directory '{}' does not exist",
                        proto_dir.display()
                    )));
                }
                std::fs::create_dir_all(&autogen_dir)?;
            }
            reg.load_all_v3(&proto_dir, &autogen_dir)?;
            Some(Arc::new(reg))
        } else {
            None
        };

        // ── Build gRPC router with chainable add_service ──────────────
        // We need two routers (IPv4 + IPv6), each with the same set of services.

        let mut futures: Vec<tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>> = Vec::new();

        for (addr, label) in [(addr_v4, "IPv4"), (addr_v6, "IPv6")] {
            if grpc_modes.is_empty() {
                continue;
            }

            let mut builder = TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32));

            if self.config.tls {
                let tls_config = self.load_tls_config()?;
                let tls = ServerTlsConfig::new()
                    .identity(tls_config.identity.clone())
                    .client_ca_root(tls_config.ca.clone())
                    .client_auth_optional(true);
                builder = builder.tls_config(tls)?;
                tracing::info!("gRPC {} server listen on: {} with TLS", label, addr);
            } else {
                tracing::info!("gRPC {} server listen on: {}", label, addr);
            }

            // Chain services — each add_service returns a new router type.
            // We handle up to 3 gRPC modes via nested add_service calls.
            if grpc_modes.contains(&DialoutMode::Normal)
                && grpc_modes.contains(&DialoutMode::Gpb)
                && grpc_modes.contains(&DialoutMode::Gnmi)
            {
                // All three gRPC modes
                let registry = registry.clone().unwrap_or_default();
                let r = builder
                    .add_service(GrpcDialoutServer::new(DialoutService::new(dialout_config.clone())))
                    .add_service(GRpcDialoutV3Server::new(DialoutV3Service::new(registry.clone(), v3_config.clone())))
                    .add_service(GNmiDialOutServer::new(GnmiDialoutService::new(gnmi_config.clone())))
                    .serve(addr);
                futures.push(tokio::spawn(r));
            } else if grpc_modes.contains(&DialoutMode::Normal)
                && grpc_modes.contains(&DialoutMode::Gpb)
            {
                let registry = registry.clone().unwrap_or_default();
                let r = builder
                    .add_service(GrpcDialoutServer::new(DialoutService::new(dialout_config.clone())))
                    .add_service(GRpcDialoutV3Server::new(DialoutV3Service::new(registry.clone(), v3_config.clone())))
                    .serve(addr);
                futures.push(tokio::spawn(r));
            } else if grpc_modes.contains(&DialoutMode::Normal)
                && grpc_modes.contains(&DialoutMode::Gnmi)
            {
                let r = builder
                    .add_service(GrpcDialoutServer::new(DialoutService::new(dialout_config.clone())))
                    .add_service(GNmiDialOutServer::new(GnmiDialoutService::new(gnmi_config.clone())))
                    .serve(addr);
                futures.push(tokio::spawn(r));
            } else if grpc_modes.contains(&DialoutMode::Gpb)
                && grpc_modes.contains(&DialoutMode::Gnmi)
            {
                let registry = registry.clone().unwrap_or_default();
                let r = builder
                    .add_service(GRpcDialoutV3Server::new(DialoutV3Service::new(registry.clone(), v3_config.clone())))
                    .add_service(GNmiDialOutServer::new(GnmiDialoutService::new(gnmi_config.clone())))
                    .serve(addr);
                futures.push(tokio::spawn(r));
            } else if grpc_modes.contains(&DialoutMode::Normal) {
                let r = builder
                    .add_service(GrpcDialoutServer::new(DialoutService::new(dialout_config.clone())))
                    .serve(addr);
                futures.push(tokio::spawn(r));
            } else if grpc_modes.contains(&DialoutMode::Gpb) {
                let registry = registry.clone().unwrap_or_default();
                let r = builder
                    .add_service(GRpcDialoutV3Server::new(DialoutV3Service::new(registry.clone(), v3_config.clone())))
                    .serve(addr);
                futures.push(tokio::spawn(r));
            } else if grpc_modes.contains(&DialoutMode::Gnmi) {
                let r = builder
                    .add_service(GNmiDialOutServer::new(GnmiDialoutService::new(gnmi_config.clone())))
                    .serve(addr);
                futures.push(tokio::spawn(r));
            }
        }

        // ── Start UDP if configured ────────────────────────────────────
        let udp_handle = if has_udp {
            let udp_config = UdpConfig {
                port: self.config.port,
                orignal: self.config.orignal,
                format_json: self.config.format_json,
                including_default: self.config.including_default,
                performance_mode: self.config.performance_mode,
            };
            Some(tokio::spawn(async move {
                let mut server = UdpDialoutServer::new(udp_config);
                server.start().await
            }))
        } else {
            None
        };

        // ── Wait for all futures ───────────────────────────────────────
        // Collect results; first error aborts.
        for f in &mut futures {
            match f.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(AppError::from(e)),
                Err(e) => return Err(AppError::Config(format!("gRPC task panicked: {}", e))),
            }
        }

        if let Some(handle) = udp_handle {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(AppError::Config(format!("UDP task panicked: {}", e))),
            }
        }

        Ok(())
    }

    /// Load TLS configuration from the tls/ directory.
    fn load_tls_config(&self) -> Result<TlsConfig> {
        // Try multiple candidate directories for TLS files
        let candidates: Vec<std::path::PathBuf> = vec![
            std::env::current_exe().ok()
                .and_then(|p| p.parent().map(|d| d.join(TLS_DIR)))
                .unwrap_or_else(|| std::path::PathBuf::from(TLS_DIR)),
            std::env::current_dir().map(|d| d.join(TLS_DIR)).unwrap_or_default(),
            std::path::PathBuf::from(TLS_DIR),
        ];

        let tls_dir = candidates.iter()
            .find(|d| d.join(CERT_CHAIN).exists())
            .cloned()
            .ok_or_else(|| AppError::Tls(format!(
                "TLS files not found. Searched: {:?}. Need: {}, {}, {}",
                candidates, CERT_CHAIN, PRIVATE_KEY, ROOT_CERT
            )))?;

        let cert_path = tls_dir.join(CERT_CHAIN);
        let key_path = tls_dir.join(PRIVATE_KEY);
        let ca_path = tls_dir.join(ROOT_CERT);

        if !cert_path.exists() || !key_path.exists() || !ca_path.exists() {
            return Err(AppError::Tls(format!(
                "TLS files do not exist in '{}'. Need: {}, {}, {}",
                tls_dir.display(),
                CERT_CHAIN,
                PRIVATE_KEY,
                ROOT_CERT
            )));
        }

        let cert = std::fs::read_to_string(&cert_path)?;
        let key = std::fs::read_to_string(&key_path)?;
        let ca = std::fs::read_to_string(&ca_path)?;

        tracing::info!(
            "TLS configured with cert: {}, key: {}, ca: {}",
            cert_path.display(),
            key_path.display(),
            ca_path.display()
        );

        Ok(TlsConfig {
            identity: Identity::from_pem(cert, key),
            ca: Certificate::from_pem(ca),
        })
    }
}

/// TLS configuration container.
#[derive(Clone)]
struct TlsConfig {
    identity: Identity,
    ca: Certificate,
}

/// Check if a TCP port is available on IPv4.
/// Returns Ok(()) if available, Err with message if occupied.
fn check_port_available(port: u16) -> std::result::Result<(), String> {
    // Try binding IPv4
    let addr_v4: SocketAddr = SocketAddr::from(([0, 0, 0, 0], port));
    match std::net::TcpListener::bind(addr_v4) {
        Ok(_) => {}
        Err(_) => {
            return Err(format!(
                "端口 {} 已被占用，请更换其他端口后重试 (Port {} is already in use)",
                port, port
            ));
        }
    }

    // Try binding IPv6 (may fail if IPv6 not available, which is OK)
    let addr_v6: SocketAddr = format!("[::]:{}", port)
        .parse()
        .unwrap_or(addr_v4);
    match std::net::TcpListener::bind(addr_v6) {
        Ok(_) => Ok(()),
        Err(_) => {
            // IPv6 bind failed — check if it's because IPv6 is unavailable vs port occupied
            // Try binding a random IPv6 port to see if IPv6 works at all
            let test_addr: SocketAddr = "[::]:0".parse().unwrap_or(addr_v4);
            if std::net::TcpListener::bind(test_addr).is_err() {
                // IPv6 not available on this system — that's OK
                return Ok(());
            }
            // IPv6 works but our port is occupied
            Err(format!(
                "端口 {} 已被占用 (IPv6)，请更换其他端口后重试 (Port {} is already in use on IPv6)",
                port, port
            ))
        }
    }
}
