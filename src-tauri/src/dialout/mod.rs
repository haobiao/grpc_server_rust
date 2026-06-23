//! gRPC dialout module — all four dialout modes.
//!
//! - `normal`: gRPC 2-layer dial-out (stream DialoutMsg)
//! - `gpb_v3`: gRPC 3-layer dial-out (GPB/Telemetry)
//! - `gnmi`: gNMI dial-out (Publish)
//! - `udp`: UDP 2-layer dial-out

pub mod normal;
pub mod gpb_v3;
pub mod gnmi;
pub mod udp;
