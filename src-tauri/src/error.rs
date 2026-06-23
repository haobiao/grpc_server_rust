//! Custom error types for the gRPC Dialout server.
//!
//! All functions return `Result<T, AppError>` to avoid panics.

use thiserror::Error;

/// Unified application error type.
#[derive(Error, Debug)]
pub enum AppError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protobuf parse error: {0}")]
    ProtoParse(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("gRPC status: {0}")]
    GrpcStatus(#[from] tonic::Status),

    #[error("gRPC transport error: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),

    #[error("UDP header parse error: {0}")]
    UdpParse(String),

    #[error("Proto reflection / dynamic decode error: {0}")]
    ProtoReflect(String),

    #[error("TLS configuration error: {0}")]
    Tls(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Chunk assembly error: {0}")]
    Chunk(String),

    #[error("Protoc invocation failed: {0}")]
    Protoc(String),

    #[error("Channel closed unexpectedly")]
    ChannelClosed,

    #[error("Message too large: {size} bytes (max {max} bytes)")]
    MessageTooLarge { size: usize, max: usize },

    #[error("Unsupported operation: {0}")]
    Unsupported(String),
}

/// Convenience type alias for results in this crate.
pub type Result<T> = std::result::Result<T, AppError>;
