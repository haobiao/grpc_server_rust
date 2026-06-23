use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../proto");

    // Core service protos that must be compiled at build time
    let core_protos = [
        "grpc_dialout.proto",
        "grpc_dialout_v3.proto",
        "dial_out.proto",
        "telemetry.proto",
        "gnmi.proto",
        "gnmi_ext.proto",
    ];

    // google well-known protos needed by gnmi.proto
    let google_protos = [
        "google/protobuf/any.proto",
        "google/protobuf/descriptor.proto",
    ];

    let mut proto_files: Vec<PathBuf> = core_protos
        .iter()
        .map(|f| proto_root.join(f))
        .collect();

    let mut google_proto_files: Vec<PathBuf> = google_protos
        .iter()
        .map(|f| proto_root.join(f))
        .collect();

    proto_files.append(&mut google_proto_files);

    let protos_include: Vec<PathBuf> = vec![proto_root.clone()];

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(&proto_files, &protos_include)?;

    // Tauri mobile build hook (no-op when Tauri is not enabled)
    #[cfg(feature = "gui")]
    tauri_build::build();

    // Rerun build script when protos change
    println!("cargo:rerun-if-changed=../proto");

    Ok(())
}
