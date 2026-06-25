//! Unified server management.
//!
//! Creates and starts the appropriate dialout service based on the configured mode.
//! Handles TLS configuration, port binding, and graceful shutdown via Ctrl+C.

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

    /// Start the server based on the configured mode (CLI entry point).
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

    /// Dispatch to the appropriate dialout service based on the configured mode.
    async fn start_inner(&self) -> Result<()> {
        match self.config.mode {
            DialoutMode::Normal => self.start_normal().await,
            DialoutMode::Gpb => self.start_gpb_v3().await,
            DialoutMode::Gnmi => self.start_gnmi().await,
            DialoutMode::Udp => self.start_udp().await,
        }
    }

    /// Start gRPC 2-layer dial-out (Normal mode).
    async fn start_normal(&self) -> Result<()> {
        let addr_v4: SocketAddr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        let addr_v6: SocketAddr = format!("[::]:{}", self.config.port)
            .parse()
            .unwrap_or(addr_v4);

        tracing::info!("Server {} listen on: {} (IPv4) and {} (IPv6)",
            self.config.mode, addr_v4, addr_v6);

        let config = DialoutConfig {
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };

        if self.config.tls {
            let tls_config = self.load_tls_config()?;

            let tls_v4 = ServerTlsConfig::new()
                .identity(tls_config.identity.clone())
                .client_ca_root(tls_config.ca.clone());
            let tls_v6 = ServerTlsConfig::new()
                .identity(tls_config.identity)
                .client_ca_root(tls_config.ca);

            let service_v4 = GrpcDialoutServer::new(DialoutService::new(config.clone()));
            let service_v6 = GrpcDialoutServer::new(DialoutService::new(config));

            let server_v4 = TonicServer::builder()
                .tls_config(tls_v4)?
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v4)
                .serve(addr_v4);

            let server_v6 = TonicServer::builder()
                .tls_config(tls_v6)?
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v6)
                .serve(addr_v6);

            tokio::try_join!(server_v4, server_v6).map(|_| ()).map_err(Into::into)
        } else {
            let service_v4 = GrpcDialoutServer::new(DialoutService::new(config.clone()));
            let service_v6 = GrpcDialoutServer::new(DialoutService::new(config));

            let server_v4 = TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v4)
                .serve(addr_v4);

            let server_v6 = TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v6)
                .serve(addr_v6);

            tokio::try_join!(server_v4, server_v6).map(|_| ()).map_err(Into::into)
        }
    }

    /// Start gRPC 3-layer dial-out (GPB mode).
    async fn start_gpb_v3(&self) -> Result<()> {
        let addr_v4: SocketAddr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        let addr_v6: SocketAddr = format!("[::]:{}", self.config.port)
            .parse()
            .unwrap_or(addr_v4);

        tracing::info!("Server {} listen on: {} (IPv4) and {} (IPv6)",
            self.config.mode, addr_v4, addr_v6);

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

        let registry = Arc::new(registry);
        let config = V3Config {
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };

        if self.config.tls {
            let tls_config = self.load_tls_config()?;

            let tls_v4 = ServerTlsConfig::new()
                .identity(tls_config.identity.clone())
                .client_ca_root(tls_config.ca.clone());
            let tls_v6 = ServerTlsConfig::new()
                .identity(tls_config.identity)
                .client_ca_root(tls_config.ca);

            let service_v4 = GRpcDialoutV3Server::new(DialoutV3Service::new(registry.clone(), config.clone()));
            let service_v6 = GRpcDialoutV3Server::new(DialoutV3Service::new(registry.clone(), config));

            let server_v4 = TonicServer::builder()
                .tls_config(tls_v4)?
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v4)
                .serve(addr_v4);

            let server_v6 = TonicServer::builder()
                .tls_config(tls_v6)?
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v6)
                .serve(addr_v6);

            tokio::try_join!(server_v4, server_v6).map(|_| ()).map_err(Into::into)
        } else {
            let service_v4 = GRpcDialoutV3Server::new(DialoutV3Service::new(registry.clone(), config.clone()));
            let service_v6 = GRpcDialoutV3Server::new(DialoutV3Service::new(registry.clone(), config));

            let server_v4 = TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v4)
                .serve(addr_v4);

            let server_v6 = TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v6)
                .serve(addr_v6);

            tokio::try_join!(server_v4, server_v6).map(|_| ()).map_err(Into::into)
        }
    }

    /// Start gNMI dial-out.
    async fn start_gnmi(&self) -> Result<()> {
        let addr_v4: SocketAddr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        let addr_v6: SocketAddr = format!("[::]:{}", self.config.port)
            .parse()
            .unwrap_or(addr_v4);

        tracing::info!("Server {} listen on: {} (IPv4) and {} (IPv6)",
            self.config.mode, addr_v4, addr_v6);

        let config = GnmiConfig {
            orignal: self.config.orignal,
            format_json: self.config.format_json,
            including_default: self.config.including_default,
            performance_mode: self.config.performance_mode,
        };

        if self.config.tls {
            let tls_config = self.load_tls_config()?;

            let tls_v4 = ServerTlsConfig::new()
                .identity(tls_config.identity.clone())
                .client_ca_root(tls_config.ca.clone());
            let tls_v6 = ServerTlsConfig::new()
                .identity(tls_config.identity)
                .client_ca_root(tls_config.ca);

            let service_v4 = GNmiDialOutServer::new(GnmiDialoutService::new(config.clone()));
            let service_v6 = GNmiDialOutServer::new(GnmiDialoutService::new(config));

            let server_v4 = TonicServer::builder()
                .tls_config(tls_v4)?
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v4)
                .serve(addr_v4);

            let server_v6 = TonicServer::builder()
                .tls_config(tls_v6)?
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v6)
                .serve(addr_v6);

            tokio::try_join!(server_v4, server_v6).map(|_| ()).map_err(Into::into)
        } else {
            let service_v4 = GNmiDialOutServer::new(GnmiDialoutService::new(config.clone()));
            let service_v6 = GNmiDialOutServer::new(GnmiDialoutService::new(config));

            let server_v4 = TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v4)
                .serve(addr_v4);

            let server_v6 = TonicServer::builder()
                .max_frame_size(Some(MAX_MESSAGE_SIZE as u32))
                .add_service(service_v6)
                .serve(addr_v6);

            tokio::try_join!(server_v4, server_v6).map(|_| ()).map_err(Into::into)
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
