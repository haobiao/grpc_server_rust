//! gNMI dial-out service implementation.
//!
//! Implements the `gNMIDialOut` service from `dial_out.proto` (package `gnmi.sonic`).
//! Handles bidirectional streaming of gNMI SubscribeResponse messages.

use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};

use crate::error::Result;

/// Include the compiled protobuf definitions.
///
/// prost-build generates one .rs file per package name (dots→underscores):
///   gnmi_ext    (package gnmi_ext) → gnmi_ext.rs
///   gnmi        (package gnmi)     → gnmi.rs
///   gnmi_sonic  (package gnmi.sonic) → gnmi_sonic.rs  ← from dial_out.proto
pub mod proto {
    pub mod gnmi_ext {
        tonic::include_proto!("gnmi_ext");
    }
    pub mod gnmi {
        tonic::include_proto!("gnmi");
    }
    pub mod gnmi_sonic {
        tonic::include_proto!("gnmi.sonic");
    }
}

// Import types from generated gnmi_sonic.rs (package gnmi.sonic)
use proto::gnmi_sonic::gnmi::sonic::g_nmi_dial_out_server::GNmiDialOutServer;
use proto::gnmi_sonic::gnmi::sonic::PublishResponse;
// SubscribeResponse comes from gnmi.proto (package gnmi)
use proto::gnmi::SubscribeResponse;
// Oneof enum types for accessing nested fields
use proto::gnmi::subscribe_response::Response as SubscribeResponse_Response;
use proto::gnmi::typed_value::Value as TypedValue_Value;

/// gNMI dial-out service.
pub struct GnmiDialoutService {
    config: Arc<GnmiConfig>,
}

/// Configuration for the gNMI service.
#[derive(Debug, Clone)]
pub struct GnmiConfig {
    pub orignal: bool,
    pub format_json: bool,
    pub including_default: bool,
    pub performance_mode: bool,
}

impl GnmiDialoutService {
    pub fn new(config: GnmiConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

#[tonic::async_trait]
impl proto::gnmi_sonic::gnmi::sonic::g_nmi_dial_out_server::GNmiDialOut for GnmiDialoutService {
    async fn publish(
        &self,
        request: Request<Streaming<SubscribeResponse>>,
    ) -> std::result::Result<Response<Streaming<PublishResponse>>, Status> {
        let peer = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        tracing::info!(
            "A client connected. (gNMI) source addr: {}",
            peer
        );

        let mut stream = request.into_inner();
        let mut count: u64 = 0;

        // Response channel
        let (tx, rx) =
            tokio::sync::mpsc::channel::<std::result::Result<PublishResponse, Status>>(64);

        while let Some(r) = stream.message().await.map_err(|e| {
            tracing::error!("gNMI stream error from {}: {}", peer, e);
            e
        })? {
            count += 1;

            // Performance mode
            if self.config.performance_mode {
                if count % 1000 == 0 {
                    tracing::info!(
                        "gRPC gNMI server count: {} source addr: {}",
                        count,
                        peer
                    );
                }
                // Send empty acknowledgement
                let _ = tx.send(Ok(PublishResponse::default())).await;
                continue;
            }

            if self.config.orignal {
                tracing::info!(
                    "original response:\n{:?}\n=======================gRPC gNMI server count: {} source addr: {}==================",
                    r,
                    count,
                    peer
                );
                let _ = tx.send(Ok(PublishResponse::default())).await;
                continue;
            }

            // Parse notification
            Self::print_gnmi_response(&r, count, &peer, &self.config);
            let _ = tx.send(Ok(PublishResponse::default())).await;
        }

        tracing::info!("A client disconnected. (gNMI) source addr: {}", peer);

        // Final response
        let _ = tx.send(Ok(PublishResponse::default())).await;
        let response_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(response_stream)))
    }
}

impl GnmiDialoutService {
    /// Parse and format a gNMI SubscribeResponse.
    fn print_gnmi_response(
        r: &SubscribeResponse,
        count: u64,
        peer: &str,
        config: &GnmiConfig,
    ) {
        // SubscribeResponse uses oneof `response`, field 1 is `update` (Notification)
        let notification = match &r.response {
            Some(SubscribeResponse_Response::Update(n)) => n,
            _ => {
                tracing::error!(
                    "gNMI response does not contain a Notification: {:?}",
                    r
                );
                return;
            }
        };

        let timestamp = notification.timestamp;
        let prefix_elems: Vec<&str> = notification
            .prefix
            .as_ref()
            .map(|p| p.elem.iter().map(|e| e.name.as_str()).collect())
            .unwrap_or_default();

        let updates = &notification.update;
        if updates.is_empty() {
            tracing::error!(
                "Notification does not contain any updates: {:?}",
                r
            );
            return;
        }

        // Build xpath from prefix + first update path (iterator chain)
        let first_update = &updates[0];
        let update_path_elems: Vec<&str> = first_update
            .path
            .as_ref()
            .map(|p| p.elem.iter().map(|e| e.name.as_str()).collect())
            .unwrap_or_default();

        let xpath: String = prefix_elems
            .iter()
            .chain(update_path_elems.iter())
            .copied()
            .collect::<Vec<_>>()
            .join("/");

        // Process all updates using iterator chain (zero-copy)
        let row_contents: String = updates
            .iter()
            .enumerate()
            .filter_map(|(row_num, update)| {
                let val = update.val.as_ref()?;

                // Get json_val bytes from the TypedValue oneof
                let json_bytes = match &val.value {
                    Some(TypedValue_Value::JsonVal(bytes)) => bytes,
                    Some(TypedValue_Value::JsonIetfVal(bytes)) => bytes,
                    _ => return Some(format!(
                        "row {}:\npath: {:?}\nno json_val field\n",
                        row_num + 1,
                        update.path
                    )),
                };

                // Try to parse as UTF-8 and JSON
                let json_str = match std::str::from_utf8(json_bytes) {
                    Ok(s) => s,
                    Err(_) => return Some(format!(
                        "row {}:\npath: {:?}\njson_val is not valid UTF-8\n",
                        row_num + 1,
                        update.path
                    )),
                };

                if json_str == "null" {
                    Some(format!(
                        "row {}:\npath: {:?}\njson_val: \"null\"\n",
                        row_num + 1,
                        update.path
                    ))
                } else {
                    match serde_json::from_str::<serde_json::Value>(json_str) {
                        Ok(val) => {
                            let formatted = if config.format_json {
                                serde_json::to_string_pretty(&val)
                                    .unwrap_or_else(|_| json_str.to_string())
                            } else {
                                serde_json::to_string(&val)
                                    .unwrap_or_else(|_| json_str.to_string())
                            };
                            Some(format!(
                                "row {}:\npath: {:?}\njson_val:\n{}\n",
                                row_num + 1,
                                update.path,
                                formatted
                            ))
                        }
                        Err(_) => Some(format!(
                            "row {}:\npath: {:?}\noriginal json_val:\n{}\njson format error\n",
                            row_num + 1,
                            update.path,
                            json_str
                        )),
                    }
                }
            })
            .collect();

        tracing::info!(
            "response:\ntimestamp: \"{}\"\nxpath: \"{}\"\n{}\n\
             =======================gRPC gNMI server count: {} source addr: {}==================",
            timestamp,
            xpath,
            row_contents,
            count,
            peer
        );
    }
}
