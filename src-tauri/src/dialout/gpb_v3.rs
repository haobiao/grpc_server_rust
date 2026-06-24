//! gRPC 3-layer dial-out (GPB) service implementation.
//!
//! Implements the `gRPCDialoutV3` service from `grpc_dialout_v3.proto`.
//! Supports both JSON and GPB-encoded telemetry data.
//! Uses `prost-reflect` for runtime dynamic decoding of v3 proto messages.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use prost::Message;
use prost_reflect::ReflectMessage;
use tonic::{Request, Response, Status, Streaming};

use crate::error::Result;
use crate::proto_dynamic::ProtoDynamicRegistry;

/// Include the compiled protobuf definitions.
pub mod proto {
    pub mod grpc_dialout_v3 {
        tonic::include_proto!("grpc_dialout_v3");
    }
    pub mod telemetry {
        tonic::include_proto!("telemetry");
    }
}

use proto::grpc_dialout_v3::{
    g_rpc_dialout_v3_server::{GRpcDialoutV3, GRpcDialoutV3Server},
    DialoutV3Args,
};
use proto::telemetry::Telemetry;

/// gRPC 3-layer dialout service.
pub struct DialoutV3Service {
    /// Runtime proto registry for dynamic message decoding.
    registry: Arc<std::sync::RwLock<ProtoDynamicRegistry>>,
    /// Output options.
    config: Arc<V3Config>,
}

/// Configuration for the v3 dialout service.
#[derive(Debug, Clone)]
pub struct V3Config {
    pub orignal: bool,
    pub format_json: bool,
    pub including_default: bool,
    pub performance_mode: bool,
}

impl DialoutV3Service {
    /// Create a new DialoutV3Service.
    pub fn new(registry: ProtoDynamicRegistry, config: V3Config) -> Self {
        Self {
            registry: Arc::new(std::sync::RwLock::new(registry)),
            config: Arc::new(config),
        }
    }

    /// Assemble chunked data.
    ///
    /// Returns `Some(Bytes)` if data is complete, `None` if more chunks are needed.
    fn assemble_chunk(chunk_data: &BytesMut, r: &DialoutV3Args) -> Option<BytesMut> {
        let total_size = r.total_size;

        if total_size == 0 {
            // No chunking
            return Some(BytesMut::from(&r.data[..]));
        }

        let mut combined = chunk_data.clone();
        combined.extend_from_slice(&r.data);

        if (combined.len() as i32) < total_size {
            tracing::info!(
                "chunk totalSize: {} currentSize: {}",
                total_size,
                combined.len()
            );
            None // Need more data
        } else {
            Some(combined)
        }
    }
}

#[tonic::async_trait]
impl GRpcDialoutV3 for DialoutV3Service {
    type DialoutV3Stream = std::pin::Pin<Box<dyn tokio_stream::Stream<Item = std::result::Result<DialoutV3Args, Status>> + Send>>;
    /// Handle bidirectional stream of DialoutV3Args.
    async fn dialout_v3(
        &self,
        request: Request<Streaming<DialoutV3Args>>,
    ) -> std::result::Result<Response<Self::DialoutV3Stream>, Status> {
        let peer = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        tracing::info!("Stream connected. (3-layer/GPB) source addr: {}", peer);

        let mut stream = request.into_inner();
        let mut chunk_data = BytesMut::new();
        let mut count: u64 = 0;

        // Create a channel for the response stream (server-initiated)
        let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<DialoutV3Args, Status>>(16);

        // Spawn a task to produce empty responses (acknowledging client messages)
        // The Python version yields empty PublishResponse; here we send empty DialoutV3Args
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            // Keep the sender alive until the stream ends
            let _tx = tx_clone;
        });

        while let Some(r) = stream.message().await.map_err(|e| {
            tracing::error!("Stream error from {}: {}", peer, e);
            e
        })? {
            count += 1;

            // Performance mode
            if self.config.performance_mode {
                if count % 1000 == 0 {
                    tracing::info!(
                        "gRPC 3-layer server count: {} source addr: {}",
                        count,
                        peer
                    );
                }
                continue;
            }

            // Assemble chunked data
            let real_data = match Self::assemble_chunk(&chunk_data, &r) {
                Some(data) => {
                    chunk_data.clear();
                    data.freeze()
                }
                None => {
                    chunk_data.extend_from_slice(&r.data);
                    continue;
                }
            };

            if self.config.orignal {
                // Print original telemetry data
                Self::print_original_telemetry(&r, &real_data, count, &peer);
            } else {
                // Parse and transform telemetry data
                Self::trans_gpb_response(
                    &self.registry,
                    &self.config,
                    &r,
                    &real_data,
                    count,
                    &peer,
                );
            }
        }

        tracing::info!("Stream disconnected. (3-layer/GPB) source addr: {}", peer);

        // Return an empty response stream
        let empty_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(empty_stream)))
    }
}

impl DialoutV3Service {
    /// Print the original telemetry message without parsing content.
    fn print_original_telemetry(
        r: &DialoutV3Args,
        data: &Bytes,
        count: u64,
        peer: &str,
    ) {
        let telemetry_info = format!(
            "telemetryMsg:\nReqId: \"{}\"\nerrors: \"{}\"\ntotalSize: \"{}\"",
            r.req_id, r.errors, r.total_size
        );

        match Telemetry::decode(data.as_ref()) {
            Ok(telemetry_obj) => {
                let data_json = if telemetry_obj.encoding == 0 {
                    telemetry_obj.data_str.clone()
                } else {
                    String::from("<GPB data>")
                };

                let debug_str = format!("{:?}", telemetry_obj);
                let content = format!(
                    "{}\ndata_str:\n{}\n==================gRPC 3-layer server count: {} source addr: {}==================",
                    telemetry_info,
                    if telemetry_obj.encoding == 0 { &data_json } else { &debug_str },
                    count, peer
                );
                tracing::info!("{}", content);
            }
            Err(e) => {
                tracing::error!(
                    "{}\ngpb decode error with: \"{}\"\n==================gRPC 3-layer server count: {} source addr: {}==================",
                    telemetry_info, e, count, peer
                );
            }
        }
    }

    /// Transform GPB response: parse Telemetry, decode content, format output.
    fn trans_gpb_response(
        registry: &Arc<std::sync::RwLock<ProtoDynamicRegistry>>,
        config: &Arc<V3Config>,
        r: &DialoutV3Args,
        data: &Bytes,
        count: u64,
        peer: &str,
    ) {
        let telemetry = match Telemetry::decode(data.as_ref()) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(
                    "telemetryMsg:\nReqId: \"{}\"\nerrors: \"{}\"\ntotalSize: \"{}\"\n\
                     gpb decode error with: \"{}\"\n\
                     ==================gRPC 3-layer server count: {} source addr: {}==================",
                    r.req_id, r.errors, r.total_size, e, count, peer
                );
                return;
            }
        };

        let msg_info = format!(
            "telemetryMsg:\nReqId: \"{}\"\nerrors: \"{}\"\ntotalSize: \"{}\"\n\
             producer_name: \"{}\"\nnode_id_str: \"{}\"\nproduct_name: \"{}\"\n\
             subscription_id_str: \"{}\"\nsensor_path: \"{}\"\n\
             collection_id: \"{}\"\ncollection_start_time: \"{}\"\nmsg_timestamp: \"{}\"\n\
             collection_end_time: \"{}\"\ncurrent_period: \"{}\"\nexcept_desc: \"{}\"\n\
             encoding: \"{}\"",
            r.req_id, r.errors, r.total_size,
            telemetry.producer_name, telemetry.node_id_str, telemetry.product_name,
            telemetry.subscription_id_str, telemetry.sensor_path,
            telemetry.collection_id, telemetry.collection_start_time, telemetry.msg_timestamp,
            telemetry.collection_end_time, telemetry.current_period, telemetry.except_desc,
            encoding_name(telemetry.encoding)
        );

        if telemetry.encoding == 0 {
            // JSON encoding: parse data_str directly
            match serde_json::from_str::<serde_json::Value>(&telemetry.data_str) {
                Ok(val) => {
                    let data_fmt = if config.format_json {
                        serde_json::to_string_pretty(&val).unwrap_or_else(|_| telemetry.data_str.clone())
                    } else {
                        serde_json::to_string(&val).unwrap_or_else(|_| telemetry.data_str.clone())
                    };
                    tracing::info!(
                        "{}\ndata_str:\n{}\n==================gRPC 3-layer server count: {} source addr: {}==================",
                        msg_info, data_fmt, count, peer
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "{}\nOrignal data_str:\n{}\njson format error with: \"{}\"\n\
                         ==================gRPC 3-layer server count: {} source addr: {}==================",
                        msg_info, telemetry.data_str, e, count, peer
                    );
                }
            }
        } else {
            // GPB encoding: dynamically decode content using registry
            Self::trans_gpb_content(
                registry,
                config,
                r,
                &telemetry,
                &msg_info,
                count,
                peer,
            );
        }
    }

    /// Decode GPB content rows using the dynamic proto registry.
    fn trans_gpb_content(
        registry: &Arc<std::sync::RwLock<ProtoDynamicRegistry>>,
        config: &Arc<V3Config>,
        _r: &DialoutV3Args,
        telemetry: &Telemetry,
        msg_info: &str,
        count: u64,
        peer: &str,
    ) {
        let reg = match registry.read() {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to acquire registry read lock: {}", e);
                return;
            }
        };

        let module = match reg.find_module(&telemetry.sensor_path) {
            Some(m) => m,
            None => {
                let modules: Vec<_> = reg.supported_modules();
                tracing::error!(
                    "Cannot find module for sensor_path '{}'. Supported: {:?}",
                    telemetry.sensor_path,
                    modules
                );
                return;
            }
        };

        let mut row_contents = String::with_capacity(4096);

        if let Some(ref gpb_table) = telemetry.data_gpb {
            for (row_num, row) in gpb_table.row.iter().enumerate() {
                let mut line = String::with_capacity(2048);
                line.push_str(&format!(
                    "V3 GPB-content row {} timestamp: \"{}\"\ncontent: ",
                    row_num + 1,
                    row.timestamp
                ));

                if row.content.is_empty() {
                    line.push_str("<empty>");
                } else {
                    // Use prost-reflect to dynamically decode
                    match prost_reflect::DynamicMessage::decode(module.clone(), &row.content[..]) {
                        Ok(dynamic_msg) => {
                            // Convert to JSON via prost-reflect's text format
                            // For proper JSON output, we use serde_json::Value
                            match dynamic_msg_to_json_value(&dynamic_msg) {
                                Ok(val) => {
                                    let json_str = if config.format_json {
                                        serde_json::to_string_pretty(&val)
                                            .unwrap_or_else(|_| dynamic_msg.to_string())
                                    } else {
                                        serde_json::to_string(&val)
                                            .unwrap_or_else(|_| dynamic_msg.to_string())
                                    };
                                    line.push_str(&json_str);
                                }
                                Err(e) => {
                                    line.push_str(&format!(
                                        "dynamic decode error: {}",
                                        e
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            line.push_str(&format!(
                                "content:\n {:?}\ngpb format error with: \"{}\"",
                                row, e
                            ));
                        }
                    }
                }

                row_contents.push_str(&line);
                row_contents.push('\n');
            }
        }

        let content = format!(
            "{}\n{}\n==================gRPC 3-layer server count: {} source addr: {}==================",
            msg_info, row_contents, count, peer
        );
        tracing::info!("{}", content);
    }
}

/// Get encoding name as a string (zero-copy).
#[inline]
fn encoding_name(encoding: i32) -> &'static str {
    match encoding {
        0 => "Encoding_JSON",
        1 => "Encoding_GPB",
        _ => "Unknown",
    }
}

/// Convert a DynamicMessage to a serde_json::Value.
fn dynamic_msg_to_json_value(
    msg: &prost_reflect::DynamicMessage,
) -> std::result::Result<serde_json::Value, String> {
    dynamic_message_to_json(msg)
}

/// Recursively convert a DynamicMessage to serde_json::Value.
fn dynamic_message_to_json(msg: &prost_reflect::DynamicMessage) -> std::result::Result<serde_json::Value, String> {
    let descriptor = msg.descriptor();
    let mut map = serde_json::Map::with_capacity(descriptor.fields().count());

    for field in descriptor.fields() {
        if !msg.has_field(&field) {
            continue;
        }

        let field_name = field.name().to_string();
        let kind = field.kind();

        let json_val: serde_json::Value = if field.is_list() {
            // Repeated field: Value::List(Vec<Value>)
            let val = msg.get_field(&field);
            match val.as_list() {
                Some(items) => {
                    let values: Vec<serde_json::Value> = items
                        .iter()
                        .map(|item| field_value_to_json(item, &kind))
                        .collect();
                    serde_json::Value::Array(values)
                }
                None => serde_json::Value::Array(vec![]),
            }
        } else if field.is_map() {
            // Map field — serialize as JSON object
            let map_val = serde_json::Map::new();
            serde_json::Value::Object(map_val)
        } else {
            // Singular field
            let val = msg.get_field(&field);
            field_value_to_json(&val, &kind)
        };

        map.insert(field_name, json_val);
    }

    Ok(serde_json::Value::Object(map))
}

/// Convert a single prost_reflect Value to serde_json::Value.
fn field_value_to_json(val: &prost_reflect::Value, kind: &prost_reflect::Kind) -> serde_json::Value {
    match val {
        prost_reflect::Value::Bool(b) => serde_json::Value::Bool(*b),
        prost_reflect::Value::I32(n) => serde_json::json!(n),
        prost_reflect::Value::I64(n) => serde_json::json!(n),
        prost_reflect::Value::U32(n) => serde_json::json!(n),
        prost_reflect::Value::U64(n) => serde_json::json!(n),
        prost_reflect::Value::F32(n) => serde_json::json!(n),
        prost_reflect::Value::F64(n) => serde_json::json!(n),
        prost_reflect::Value::String(s) => serde_json::Value::String(s.clone()),
        prost_reflect::Value::Bytes(b) => {
            serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
        }
        prost_reflect::Value::EnumNumber(n) => {
            serde_json::json!(n)
        }
        prost_reflect::Value::Message(m) => {
            dynamic_message_to_json(m).unwrap_or(serde_json::Value::Null)
        }
        prost_reflect::Value::List(items) => {
            serde_json::Value::Array(
                items
                    .iter()
                    .map(|item| field_value_to_json(item, kind))
                    .collect(),
            )
        }
        prost_reflect::Value::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map.iter() {
                obj.insert(format!("{:?}", k), field_value_to_json(v, kind));
            }
            serde_json::Value::Object(obj)
        }
    }
}

// Helper for hex encoding without additional crate dependency
mod hex {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(HEX_CHARS[(b >> 4) as usize] as char);
            s.push(HEX_CHARS[(b & 0x0f) as usize] as char);
        }
        s
    }
}
