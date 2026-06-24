//! CLI argument parsing via clap.
//!
//! Replaces Python's argparse-based `ArgsAnalyze` + `ArgsPreProc`.

use crate::models::{DialoutMode, ServerConfig};
use clap::Parser;

/// gRPC Dialout Telemetry Collector Server
///
/// 支持的 dial-out 模式：
///   -n / --normal   gRPC 2 层拨号 (JSON)
///   -b / --gpb      gRPC 3 层拨号 (GPB/Telemetry)
///   -m / --gnmi     gRPC gNMI 拨号
///   -u / --udp      UDP 2 层拨号
///
/// 不指定模式时默认使用 -n (gRPC 2 层)
#[derive(Parser, Debug)]
#[command(
    name = "grpc-server-rust",
    version = "3.0.7",
    about = "生产级 gRPC Dialout 遥测采集器",
    long_about = "gRPC Dialout Telemetry Collector — 从 Python 版重写为 Rust\n\n\
                  支持四种 dial-out 模式，可通过以下选项选择：\n  \
                  -n, --normal    gRPC 2 层拨号 (JSON 格式，默认)\n  \
                  -b, --gpb       gRPC 3 层拨号 (GPB/Telemetry)\n  \
                  -m, --gnmi      gRPC gNMI 拨号\n  \
                  -u, --udp       UDP 2 层拨号"
)]
pub struct CliArgs {
    // ── 拨号模式（互斥，不指定则默认 normal）──────────────────────
    /// gRPC 2 层拨号 (JSON 格式) — 默认模式
    #[arg(short = 'n', long = "normal", group = "mode")]
    pub normal: bool,

    /// gRPC 3 层拨号 (GPB/Telemetry)
    #[arg(short = 'b', long = "gpb", group = "mode")]
    pub gpb: bool,

    /// gRPC gNMI 拨号
    #[arg(short = 'm', long = "gnmi", group = "mode")]
    pub gnmi: bool,

    /// UDP 2 层拨号
    #[arg(short = 'u', long = "udp", group = "mode")]
    pub udp: bool,

    // ── 通用选项 ────────────────────────────────────────────────────
    /// 监听端口 (默认: 50051)
    #[arg(short = 'p', long = "port", default_value_t = 50051)]
    pub port: u16,

    /// 启用 TLS 模式 (UDP 模式不支持)
    #[arg(short = 't', long = "tls")]
    pub tls: bool,

    /// 生成 proto 描述文件后退出
    #[arg(short = 'g', long = "generate")]
    pub generate: bool,

    /// 启用日志文件输出
    #[arg(short = 'l', long = "log")]
    pub log: bool,

    /// 输出原始数据（不解析）
    #[arg(short = 'o', long = "orignal")]
    pub orignal: bool,

    /// 格式化 JSON 数据（缩进）
    #[arg(short = 'f', long = "format")]
    pub format: bool,

    /// 输出包含默认值字段
    #[arg(short = 'i', long = "including_default")]
    pub including_default: bool,

    /// 性能模式（仅计数，不解析消息）
    #[arg(short = 's', long = "performance_mode")]
    pub performance_mode: bool,

    /// 日志级别: debug, info, error
    #[arg(short = 'd', long = "debug", default_value = "error")]
    pub debug: String,

    /// gRPC GRPC_TRACE 类型（逗号分隔）
    #[arg(short = 'c', long = "trace")]
    pub trace: Option<String>,

    // ── 日志文件配置 ────────────────────────────────────────────────
    /// 日志文件最大数量 (默认: 50)
    #[arg(long = "logfile-num", default_value_t = 50)]
    pub logfile_num: usize,

    /// 日志文件最大大小，单位 MB (默认: 50)
    #[arg(long = "logfile-size", default_value_t = 50)]
    pub logfile_size: usize,
}

impl CliArgs {
    /// Determine the dialout mode from CLI flags.
    /// Maps Python's `ArgsPreProc` logic.
    pub fn dialout_mode(&self) -> DialoutMode {
        if self.gpb {
            DialoutMode::Gpb
        } else if self.gnmi {
            DialoutMode::Gnmi
        } else if self.udp {
            DialoutMode::Udp
        } else {
            DialoutMode::Normal
        }
    }

    /// Convert CLI args into a `ServerConfig`.
    pub fn into_server_config(self, mode: DialoutMode) -> ServerConfig {
        ServerConfig {
            mode,
            port: self.port,
            tls: self.tls,
            orignal: self.orignal,
            format_json: self.format,
            including_default: self.including_default,
            performance_mode: self.performance_mode,
            debug: self.debug,
            trace: self.trace,
            log_output: self.log,
            logfile_num: self.logfile_num,
            logfile_size: self.logfile_size,
            only_log_file: false,
        }
    }
}
