//! Data structures mapping Python dataclasses to Rust structs.
//!
//! All structs derive Debug, Clone, PartialEq as required.
//! Zero-copy: string fields use `Cow<'_, str>` or `bytes::Bytes` where possible.

use std::borrow::Cow;

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
