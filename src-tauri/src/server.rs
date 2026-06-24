//! Unified server management.
//!
//! Creates and starts the appropriate dialout service based on the configured mode.
//! Handles TLS configuration, port binding, and graceful shutdown via Ctrl+C.

use std::net::SocketAddr;

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

    /// Start the server based on the configured mode.
    pub fn start(&mut self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .map_err(|e| AppError::Config(format!("Failed to create tokio runtime: {}", e)))?;

        rt.block_on(async {
            match self.config.mode {
                DialoutMode::Normal => self.start_normal().await,
                DialoutMode::Gpb => self.start_gpb_v3().await,
                DialoutMode::Gnmi => self.start_gnmi().await,
                DialoutMode::Udp => self.start_udp().await,
            }
        })
    }

    /// Start gRPC 2-layer dial-out (Normal mode).
    async fn start_normal(&self) -> Result<()> {
        // 显式创建 SocketAddr，确保 IPv4 双栈监听
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.port)
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], self.config.port)));

        tracing::info!("Server {} listen on: {}", self.config.mode, addr);

        let config = DialoutConfig {
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };

        let service = GrpcDialoutServer::new(DialoutService::new(config));

        if self.config.tls {
            tracing::info!("Server {} listen on: {} with TLS", self.config.mode, addr);
            let tls_config = self.load_tls_config()?;
            let tls = ServerTlsConfig::new()
                .identity(tls_config.identity)
                .client_ca_root(tls_config.ca);
            TonicServer::builder()
                .tls_config(tls)?
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service)
                .serve(addr)
                .await
                .map_err(Into::into)
        } else {
            TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service)
                .serve(addr)
                .await
                .map_err(Into::into)
        }
    }

    /// Start gRPC 3-layer dial-out (GPB mode).
    async fn start_gpb_v3(&self) -> Result<()> {
        // 显式创建 SocketAddr，确保 IPv4 双栈监听
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.port)
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], self.config.port)));

        tracing::info!("Server {} listen on: {}", self.config.mode, addr);

        // Load dynamic proto registry
        let mut registry = ProtoDynamicRegistry::new();
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

        registry.load_all_v3(&proto_dir, &autogen_dir)?;

        let config = V3Config {
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };

        let service = GRpcDialoutV3Server::new(DialoutV3Service::new(registry, config));

        if self.config.tls {
            let tls_config = self.load_tls_config()?;
            let tls = ServerTlsConfig::new()
                .identity(tls_config.identity)
                .client_ca_root(tls_config.ca);
            TonicServer::builder()
                .tls_config(tls)?
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service)
                .serve(addr)
                .await
                .map_err(Into::into)
        } else {
            TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service)
                .serve(addr)
                .await
                .map_err(Into::into)
        }
    }

    /// Start gNMI dial-out.
    async fn start_gnmi(&self) -> Result<()> {
        // 显式创建 SocketAddr，确保 IPv4 双栈监听
        let addr: SocketAddr = format!("0.0.0.0:{}", self.config.port)
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], self.config.port)));

        tracing::info!("Server {} listen on: {}", self.config.mode, addr);

        let config = GnmiConfig {
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };

        let service = GNmiDialOutServer::new(GnmiDialoutService::new(config));

        if self.config.tls {
            let tls_config = self.load_tls_config()?;
            let tls = ServerTlsConfig::new()
                .identity(tls_config.identity)
                .client_ca_root(tls_config.ca);
            TonicServer::builder()
                .tls_config(tls)?
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service)
                .serve(addr)
                .await
                .map_err(Into::into)
        } else {
            TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service)
                .serve(addr)
                .await
                .map_err(Into::into)
        }
    }

    /// Start UDP dial-out.
    async fn start_udp(&self) -> Result<()> {
        let config = UdpConfig {
            port: self.config.port,
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };

        let mut server = UdpDialoutServer::new(config);
        server.start().await
    }

    /// Load TLS configuration from the tls/ directory.
    fn load_tls_config(&self) -> Result<TlsConfig> {
        let current_dir = std::env::current_dir()?;
        let tls_dir = current_dir.join(TLS_DIR);

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
struct TlsConfig {
    identity: Identity,
    ca: Certificate,
}
