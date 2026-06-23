use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../proto");

    // ── All core protos compiled together ──
    // prost-build merges protos sharing a package tree into one file.
    // gnmi.proto (package gnmi) + dial_out.proto (package gnmi.sonic) → gnmi.rs
    // gnmi_ext.proto (package gnmi_ext) → gnmi_ext.rs
    // grpc_dialout.proto (package grpc_dialout) → grpc_dialout.rs
    // grpc_dialout_v3.proto (package grpc_dialout_v3) → grpc_dialout_v3.rs
    // telemetry.proto (package telemetry) → telemetry.rs
    let core_protos = [
        "grpc_dialout.proto",
        "grpc_dialout_v3.proto",
        "telemetry.proto",
        "gnmi_ext.proto",
        "gnmi.proto",
        "dial_out.proto",
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

    let google_proto_files: Vec<PathBuf> = google_protos
        .iter()
        .map(|f| proto_root.join(f))
        .collect();

    proto_files.extend(google_proto_files);

    let protos_include: Vec<PathBuf> = vec![proto_root.clone()];

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&proto_files, &protos_include)?;

    // Tauri mobile build hook (no-op when Tauri is not enabled)
    #[cfg(feature = "gui")]
    tauri_build::build();

    // Rerun build script when protos change
    println!("cargo:rerun-if-changed=../proto");

    Ok(())
}
