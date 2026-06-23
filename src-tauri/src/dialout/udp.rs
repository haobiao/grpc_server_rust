//! UDP 2-layer dial-out server implementation.
//!
//! Listens on both IPv4 and IPv6 UDP sockets concurrently.
//! Parses UDP headers, handles fragmentation, and decodes gRPC DialoutMsg payloads.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::net::UdpSocket;
use tokio::sync::watch;

use crate::chunk::ChunkRecords;
use crate::dialout::normal::proto::grpc_dialout::DialoutMsg;
use crate::error::{AppError, Result};
use crate::models::{DialoutMode, UDPHeader};
use crate::udp_header;

/// Maximum UDP datagram size.
const UDP_MAX_SIZE: usize = 65535;
/// Maximum gRPC message size (10 MB).
const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// UDP dial-out server configuration.
#[derive(Debug, Clone)]
pub struct UdpConfig {
    pub port: u16,
    pub orignal: bool,
    pub format_json: bool,
    pub including_default: bool,
    pub performance_mode: bool,
}

/// UDP dial-out server.
pub struct UdpDialoutServer {
    config: UdpConfig,
    stop_signal: Arc<watch::Sender<bool>>,
    stop_receiver: watch::Receiver<bool>,
}

impl UdpDialoutServer {
    /// Create a new UDP dial-out server.
    pub fn new(config: UdpConfig) -> Self {
        let (stop_tx, stop_rx) = watch::channel(false);
        Self {
            config,
            stop_signal: Arc::new(stop_tx),
            stop_receiver: stop_rx,
        }
    }

    /// Start the UDP server (listens on both IPv4 and IPv6).
    pub async fn start(&mut self) -> Result<()> {
        let addr_v4: SocketAddr = format!("0.0.0.0:{}", self.config.port).parse()?;
        let addr_v6: SocketAddr = format!("[::]:{}", self.config.port).parse()?;

        let socket_v4 = UdpSocket::bind(addr_v4).await?;
        let socket_v6 = UdpSocket::bind(addr_v6).await?;

        tracing::info!(
            "Server UDP 2-layer dial-out listen on: {} (IPv4) and {} (IPv6)",
            addr_v4,
            addr_v6
        );

        let config_clone = self.config.clone();
        let mut stop_rx_v4 = self.stop_signal.subscribe();
        let mut stop_rx_v6 = self.stop_signal.subscribe();

        // Spawn IPv4 listener task
        let handle_v4 = tokio::spawn(async move {
            run_udp_listener(socket_v4, "ipv4", config_clone, &mut stop_rx_v4).await;
        });

        // Spawn IPv6 listener task
        let config_clone = self.config.clone();
        let handle_v6 = tokio::spawn(async move {
            run_udp_listener(socket_v6, "ipv6", config_clone, &mut stop_rx_v6).await;
        });

        // Wait for stop signal
        let mut stop_rx = self.stop_receiver.clone();
        stop_rx.changed().await.map_err(|_| AppError::ChannelClosed)?;

        // Wait for tasks to complete
        let _ = tokio::join!(handle_v4, handle_v6);

        tracing::info!("Server UDP 2-layer dial-out exit");
        Ok(())
    }

    /// Signal the server to stop.
    pub fn stop(&self) {
        let _ = self.stop_signal.send(true);
    }
}

/// Run a UDP listener on a single socket.
async fn run_udp_listener(
    socket: UdpSocket,
    ip_type: &str,
    config: UdpConfig,
    stop_rx: &mut watch::Receiver<bool>,
) {
    let mut buf = BytesMut::with_capacity(UDP_MAX_SIZE);
    let mut count: u64 = 0;
    let mut chunk_data: BytesMut::new();

    loop {
        tokio::select! {
            // Check stop signal
            _ = stop_rx.changed() => {
                break;
            }
            // Receive UDP datagram
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((len, addr)) => {
                        count += 1;

                        if config.performance_mode {
                            if count % 1000 == 0 {
                                tracing::info!(
                                    "gRPC UDP server count: {} ({})",
                                    count, ip_type
                                );
                            }
                            continue;
                        }

                        let data = &buf[..len];
                        process_udp_packet(data, ip_type, count, addr, &config, &mut chunk_data);
                    }
                    Err(e) => {
                        // Socket timeout is expected
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            tracing::error!("UDP recv error ({}): {}", ip_type, e);
                        }
                    }
                }
            }
        }
    }
}

/// Process a single UDP datagram.
fn process_udp_packet(
    data: &[u8],
    ip_type: &str,
    count: u64,
    addr: SocketAddr,
    config: &UdpConfig,
    chunk_data: &mut BytesMut,
) {
    let real_data = match udp_header::parse_udp_header(data) {
        Ok(header) => {
            let header_bytes = udp_header::header_bytes(data, header.header_length);
            tracing::info!(
                "UDP Head:\nOriginal:0x{}\nVersion:{} ET:{} HeaderLen:{} MsgLen:{} DomainID:{} MsgID:{}",
                hex_encode(header_bytes),
                header.version,
                header.et,
                header.header_length,
                header.message_length,
                header.message_generator_id,
                header.message_id,
            );

            let payload = udp_header::extract_payload(data, header.header_length);

            match &header.option {
                Some(opt) => {
                    // Fragmented message: accumulate
                    chunk_data.extend_from_slice(payload);
                    if !opt.last {
                        return; // Wait for more fragments
                    }
                    // Last fragment received
                    let merged = chunk_data.split().freeze();
                    Some(merged)
                }
                None => {
                    // Non-fragmented
                    Some(Bytes::copy_from_slice(payload))
                }
            }
        }
        Err(e) => {
            // Header parse failed
            tracing::error!("Failed to parse UDP header: {}", e);
            if data.len() <= 12 {
                return;
            }
            // Fall back: use data after 12 bytes
            tracing::info!("Defaulting to first 12 bytes as UDP header");
            Some(Bytes::copy_from_slice(&data[12..]))
        }
    };

    let payload = match real_data {
        Some(d) => d,
        None => return,
    };

    if config.orignal {
        print_original_json(&payload, ip_type, count, &addr);
    } else {
        deserialize_and_print(&payload, ip_type, count, &addr, config);
    }
}

/// Print original serialized data (decode as DialoutMsg and display).
fn print_original_json(data: &Bytes, ip_type: &str, count: u64, addr: &SocketAddr) {
    match DialoutMsg::decode(data.as_ref()) {
        Ok(msg) => {
            // Try to format as JSON
            match serde_json::to_string_pretty(&format!("{:?}", msg)) {
                Ok(formatted) => {
                    tracing::info!("original msg:\n{}", formatted);
                }
                Err(_) => {
                    tracing::info!("original msg:\n{:?}", msg);
                }
            }
        }
        Err(e) => {
            tracing::error!("Message deserialization failed: \"{}\"", e);
        }
    }
    tracing::info!(
        "=============={} gRPC UDP 2-layer server count: {}, source addr: {}==============",
        ip_type, count, addr.ip()
    );
}

/// Deserialize and print the DialoutMsg JSON data.
fn deserialize_and_print(
    data: &Bytes,
    ip_type: &str,
    count: u64,
    addr: &SocketAddr,
    config: &UdpConfig,
) {
    match DialoutMsg::decode(data.as_ref()) {
        Ok(msg) => {
            let sensor_path = msg.sensor_path.clone();
            let json_data = &msg.json_data.unwrap_or_default();

            match serde_json::from_str::<serde_json::Value>(json_data) {
                Ok(val) => {
                    let data_fmt = if config.format_json {
                        serde_json::to_string_pretty(&val)
                            .unwrap_or_else(|_| json_data.clone())
                    } else {
                        serde_json::to_string(&val)
                            .unwrap_or_else(|_| json_data.clone())
                    };
                    let content = format!(
                        "DialoutMsg:\nsensorPath: \"{}\"\nJsonData:\n{}\n",
                        sensor_path, data_fmt
                    );
                    tracing::info!("{}", content);
                }
                Err(e) => {
                    tracing::error!(
                        "DialoutMsg:\nsensorPath: \"{}\"\nOrignalData:\n{}\njson format error with: \"{}\"",
                        sensor_path, json_data, e
                    );
                }
            }
        }
        Err(e) => {
            tracing::error!("Message deserialization failed: \"{}\"", e);
        }
    }

    tracing::info!(
        "=============={} gRPC UDP 2-layer server count: {}, source addr: {}==============\n",
        ip_type, count, addr.ip()
    );
}

/// Simple hex encoding (no external dependency).
fn hex_encode(data: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut s = String::with_capacity(data.len() * 2);
    for &b in data {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}
