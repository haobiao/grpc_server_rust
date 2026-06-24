//! Data structures mapping Python dataclasses to Rust structs.
//!
//! All structs derive Debug, Clone, PartialEq as required.
//! Zero-copy: string fields use `Cow<'_, str>` or `bytes::Bytes` where possible.

use std::borrow::Cow;
use std::sync::atomic::{AtomicU64, Ordering};

// ── 全局统计计数器（供 GUI get_stats 读取）──────────────────────────
/// 全局消息总数计数器（所有 dialout 模式共享）
static GLOBAL_MSG_COUNT: AtomicU64 = AtomicU64::new(0);
/// 全局已连接客户端总数
static GLOBAL_CLIENT_COUNT: AtomicU64 = AtomicU64::new(0);

/// 增加消息计数（dialout 模块每收到一条消息调用一次）
#[inline]
pub fn incr_msg_count() {
    GLOBAL_MSG_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// 增加客户端连接计数（每次新连接调用一次）
#[inline]
pub fn incr_client_count() {
    GLOBAL_CLIENT_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// 读取当前消息总数
#[inline]
pub fn get_global_msg_count() -> u64 {
    GLOBAL_MSG_COUNT.load(Ordering::Relaxed)
}

/// 读取当前客户端连接总数
#[inline]
pub fn get_global_client_count() -> u64 {
    GLOBAL_CLIENT_COUNT.load(Ordering::Relaxed)
}

/// 重置所有全局计数器（start_server 时调用）
#[inline]
pub fn reset_global_stats() {
    GLOBAL_MSG_COUNT.store(0, Ordering::Relaxed);
    GLOBAL_CLIENT_COUNT.store(0, Ordering::Relaxed);
}

/// gRPC dialout mode enumeration.
/// Maps Python's integer constants: NORMAL=0, GPB=1, GNMI=2, UDP=3
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DialoutMode {
    /// gRPC 2-layer dial-out
    Normal,
    /// gRPC 3-layer dial-out (GPB)
    Gpb,
    /// gRPC gNMI dial-out
    Gnmi,
    /// UDP 2-layer dial-out
    Udp,
}

impl DialoutMode {
    /// Returns a human-readable mode name (zero-copy).
    pub fn as_str(&self) -> &'static str {
        match self {
            DialoutMode::Normal => "gRPC 2-layer dial-out",
            DialoutMode::Gpb => "gRPC 3-layer dial-out",
            DialoutMode::Gnmi => "gRPC gNMI dialout",
            DialoutMode::Udp => "UDP 2-layer dialout",
        }
    }
}

impl std::fmt::Display for DialoutMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Server configuration (maps Python `Server.__init__` fields).
#[derive(Debug, Clone, PartialEq)]
pub struct ServerConfig {
    pub mode: DialoutMode,
    pub port: u16,
    pub tls: bool,
    /// Output original data without parsing
    pub orignal: bool,
    /// Format JSON output with indentation
    pub format_json: bool,
    /// Include default value fields in output
    pub including_default: bool,
    /// Performance mode: only count messages
    pub performance_mode: bool,
    /// gRPC debug verbosity level
    pub debug: String,
    /// gRPC trace types
    pub trace: Option<String>,
    /// Output to log file
    pub log_output: bool,
    /// Log file max count
    pub logfile_num: usize,
    /// Log file max size in MB
    pub logfile_size: usize,
    /// Only output to log file (suppress UI)
    pub only_log_file: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            mode: DialoutMode::Normal,
            port: 50051,
            tls: false,
            orignal: false,
            format_json: true,
            including_default: true,
            performance_mode: false,
            debug: "error".into(),
            trace: None,
            log_output: false,
            logfile_num: 50,
            logfile_size: 50,
            only_log_file: false,
        }
    }
}

/// UDP header option (fragmentation info).
#[derive(Debug, Clone, PartialEq)]
pub struct UdpHeaderOption {
    pub option_type: u8,
    pub length: u8,
    pub segment_number: u16,
    pub last: bool,
}

/// Parsed UDP header (standard format).
#[derive(Debug, Clone, PartialEq)]
pub struct UDPHeader {
    pub version: u16,
    pub header_length: u16,
    pub et: u16,
    pub message_length: u16,
    pub message_generator_id: u32,
    pub message_id: u32,
    pub option: Option<UdpHeaderOption>,
}

/// Parsed UDP header (mobile/standard format with S bit).
#[derive(Debug, Clone, PartialEq)]
pub struct UDPHeaderMobile {
    pub version: u16,
    pub standard: u16,
    pub header_length: u16,
    pub et: u16,
    pub message_length: u16,
    pub message_domain_id: u32,
    pub message_id: u32,
    pub option: Option<UdpHeaderOption>,
}

/// Pre-allocated message buffer for building formatted output.
/// Avoids repeated String allocations in hot loops.
#[derive(Debug, Clone)]
pub struct MessageBuffer {
    inner: String,
}

impl MessageBuffer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: String::with_capacity(capacity),
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn push_str(&mut self, s: &str) {
        self.inner.push_str(s);
    }

    pub fn push_cow(&mut self, s: Cow<'_, str>) {
        self.inner.push_str(&s);
    }

    pub fn into_string(self) -> String {
        self.inner
    }

    pub fn as_str(&self) -> &str {
        &self.inner
    }
}
