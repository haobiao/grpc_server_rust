use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../proto");

    // Core service protos that must be compiled at build time.
    // Each proto's package determines the generated module name.
    // dial_out.proto imports gnmi.proto (package "gnmi") → prost generates
    //   dial_out.rs containing gnmi module tree.
    // BUT gnmi_ext.proto also has package "gnmi_ext" → separate file.
    // The key issue: when multiple .proto files are compiled together,
    // prost-build groups them by package into ONE .rs file per package.
    // dial_out.proto (package gnmi.sonic) + gnmi.proto (package gnmi) →
    //   prost may merge into gnmi.rs (no dial_out.rs generated!).
    let core_protos = [
        "grpc_dialout.proto",      // package grpc_dialout      → grpc_dialout.rs
        "grpc_dialout_v3.proto",   // package grpc_dialout_v3   → grpc_dialout_v3.rs
        "telemetry.proto",          // package telemetry         → telemetry.rs
        "gnmi_ext.proto",           // package gnmi_ext          → gnmi_ext.rs
        "gnmi.proto",               // package gnmi              → gnmi.rs
        "dial_out.proto",           // package gnmi.sonic        → dial_out.rs (separate package)
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

    // Use compile_protos (the renamed, non-deprecated method)
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&proto_files, &protos_include)?;

    // Debug: list what files were generated in OUT_DIR
    if let Ok(out_dir) = std::env::var("OUT_DIR") {
        let out_path = PathBuf::from(&out_dir);
        if let Ok(entries) = std::fs::read_dir(&out_path) {
            eprintln!("=== Generated proto files in {} ===", out_dir);
            for entry in entries.flatten() {
                let name = entry.file_name();
                eprintln!("  {}", name.to_string_lossy());
            }
        }
    }

    // Tauri mobile build hook (no-op when Tauri is not enabled)
    #[cfg(feature = "gui")]
    tauri_build::build();

    // Rerun build script when protos change
    println!("cargo:rerun-if-changed=../proto");

    Ok(())
}
