use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../proto");

    // Core service protos that must be compiled at build time.
    // Each proto's package determines the generated module name.
    // dial_out.proto (package gnmi.sonic) + gnmi.proto (package gnmi) →
    //   prost-build merges them into ONE file named after the top-level package.
    //   So dial_out.rs does NOT exist; it's merged into gnmi.rs.
    // Fix: split compilation into separate groups so each package gets its own .rs file.
    let gnmi_protos = [
        "gnmi_ext.proto",           // package gnmi_ext → gnmi_ext.rs
        "gnmi.proto",               // package gnmi
        "dial_out.proto",           // package gnmi.sonic → merged into gnmi.rs
    ];

    let other_protos = [
        "grpc_dialout.proto",       // package grpc_dialout → grpc_dialout.rs
        "grpc_dialout_v3.proto",    // package grpc_dialout_v3 → grpc_dialout_v3.rs
        "telemetry.proto",          // package telemetry → telemetry.rs
    ];

    // google well-known protos needed by gnmi.proto
    let google_protos = [
        "google/protobuf/any.proto",
        "google/protobuf/descriptor.proto",
    ];

    let protos_include: Vec<PathBuf> = vec![proto_root.clone()];

    // ── Group 1: gnmi + dial_out (merged, module name = "gnmi") ──
    let mut gnmi_files: Vec<PathBuf> = gnmi_protos
        .iter()
        .map(|f| proto_root.join(f))
        .collect();
    let google_proto_files: Vec<PathBuf> = google_protos
        .iter()
        .map(|f| proto_root.join(f))
        .collect();
    gnmi_files.extend(google_proto_files.clone());

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&gnmi_files, &protos_include)?;

    // ── Group 2: other service protos ──
    let other_files: Vec<PathBuf> = other_protos
        .iter()
        .map(|f| proto_root.join(f))
        .collect();

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&other_files, &protos_include)?;

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
