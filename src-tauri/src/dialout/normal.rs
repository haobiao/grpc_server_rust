//! gRPC 2-layer dial-out service implementation.
//!
//! Implements the `GRPCDialout` service from `grpc_dialout.proto`.
//! Handles client-side streaming of `DialoutMsg` messages.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use crate::chunk::ChunkRecords;
use crate::error::Result;

/// Include the compiled protobuf definitions.
pub mod proto {
    pub mod grpc_dialout {
        tonic::include_proto!("grpc_dialout");
    }
}

use proto::grpc_dialout::{
    grpc_dialout_server::GrpcDialout, ChunkInfo, DialoutMsg,
    DialoutResponse, DeviceInfo,
};

/// gRPC 2-layer dialout service.
///
/// Each connected client gets its own `ChunkRecords` for fragment assembly.
pub struct DialoutService {
    /// Global message counter (atomic, no lock needed).
    count: Arc<AtomicU64>,
    /// Output options (shared across all connections).
    config: Arc<DialoutConfig>,
}

/// Configuration for the dialout service, shared via Arc.
#[derive(Debug, Clone)]
pub struct DialoutConfig {
    /// Output original data without parsing
    pub orignal: bool,
    /// Format JSON output with indentation
    pub format_json: bool,
    /// Include default value fields in output
    pub including_default: bool,
    /// Performance mode (only count)
    pub performance_mode: bool,
}

impl DialoutService {
    /// Create a new DialoutService with the given configuration.
    pub fn new(config: DialoutConfig) -> Self {
        Self {
            count: Arc::new(AtomicU64::new(0)),
            config: Arc::new(config),
        }
    }
}

#[tonic::async_trait]
impl GrpcDialout for DialoutService {
    /// Handle a client stream of DialoutMsg messages.
    async fn dialout(
        &self,
        request: Request<Streaming<DialoutMsg>>,
    ) -> std::result::Result<Response<DialoutResponse>, Status> {
        let peer = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        tracing::info!("A client connected. (2-layer) source addr: {}", peer);
        crate::models::incr_client_count();

        let mut stream = request.into_inner();
        let mut client_count: u64 = 0;
        let mut chunk_records = ChunkRecords::new();

        while let Some(msg) = stream.message().await.map_err(|e| {
            tracing::error!("Stream error from {}: {}", peer, e);
            e
        })? {
            client_count += 1;
            crate::models::incr_msg_count();

            // Performance mode: only log every 1000 messages
            if self.config.performance_mode {
                if client_count % 1000 == 0 {
                    tracing::info!(
                        "gRPC 2-layer server count: {} source addr: {}",
                        client_count,
                        peer
                    );
                }
                continue;
            }

            // Original mode: output raw message
            if self.config.orignal {
                tracing::info!(
                    "original msg:\n{:?}\n==================gRPC 2-layer server count: {} source addr: {}==================",
                    msg,
                    client_count,
                    peer
                );
                self.count.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            // Process chunk assembly
            let sensor_path = msg.sensor_path.clone();
            let json_data = Bytes::from(msg.json_data.clone());

            let device_msg_bytes: Bytes = msg.device_msg.encode_to_vec().into();

            let (_dev_msg, merged_json) = match chunk_records.add(
                device_msg_bytes,
                &sensor_path,
                json_data,
                msg.chunk_msg.as_ref().map(|c| c.total_size),
                msg.chunk_msg.as_ref().map(|c| c.total_fragments),
                msg.chunk_msg.as_ref().map(|c| c.node_id),
            ) {
                Some(pair) => pair,
                None => {
                    self.count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let msg_len = merged_json.len();
            if msg_len > 4 * 1024 * 1024 {
                tracing::info!("msg size: {}", msg_len);
            }

            // Parse JSON
            let json_str = std::str::from_utf8(&merged_json).unwrap_or("<invalid utf8>");
            let formatted = match serde_json::from_str::<serde_json::Value>(json_str) {
                Ok(val) => {
                    if self.config.format_json {
                        serde_json::to_string_pretty(&val).unwrap_or_else(|_| json_str.to_string())
                    } else {
                        serde_json::to_string(&val).unwrap_or_else(|_| json_str.to_string())
                    }
                }
                Err(e) => {
                    let content = format!(
                        "DialoutMsg:\nsensorPath: \"{}\"\nOrignalData:\n{}\njson format error with: \"{}\"\n\
                         ==================gRPC 2-layer server count: {} source addr: {}==================",
                        sensor_path, json_str, e, client_count, peer
                    );
                    tracing::error!("{}", content);
                    self.count.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let content = format!(
                "DialoutMsg:\nsensorPath: \"{}\"\nJsonData:\n{}\n\
                 ==================gRPC 2-layer server count: {} source addr: {}==================",
                sensor_path, formatted, client_count, peer
            );
            tracing::info!("{}", content);
            self.count.fetch_add(1, Ordering::Relaxed);
        }

        tracing::info!("A client disconnected. (2-layer) source addr: {}", peer);
        Ok(Response::new(DialoutResponse {
            response: String::new(),
        }))
    }
}
