use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../proto");

    // ── Group 1: gnmi family ──
    // gnmi.proto (package gnmi) and dial_out.proto (package gnmi.sonic)
    // share the "gnmi" package tree. prost-build merges them into gnmi.rs.
    // gnmi_ext.proto (package gnmi_ext) → separate gnmi_ext.rs.
    //
    // KEY INSIGHT: When protos with nested packages (gnmi, gnmi.sonic) are
    // compiled together, prost-build generates ONE file named after the
    // root package (gnmi.rs). dial_out.rs is NOT generated separately.
    let gnmi_group = [
        "gnmi_ext.proto",
        "gnmi.proto",
        "dial_out.proto",
    ];

    // ── Group 2: independent protos ──
    let other_group = [
        "grpc_dialout.proto",
        "grpc_dialout_v3.proto",
        "telemetry.proto",
    ];

    // google well-known protos needed by gnmi.proto
    let google_protos = [
        "google/protobuf/any.proto",
        "google/protobuf/descriptor.proto",
    ];

    let protos_include: Vec<PathBuf> = vec![proto_root.clone()];

    // ── Compile gnmi group ──
    let mut gnmi_files: Vec<PathBuf> = gnmi_group
        .iter()
        .map(|f| proto_root.join(f))
        .collect();
    let google_proto_files: Vec<PathBuf> = google_protos
        .iter()
        .map(|f| proto_root.join(f))
        .collect();
    gnmi_files.extend(google_proto_files);

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&gnmi_files, &protos_include)?;

    // ── Compile other group (independent packages) ──
    let other_files: Vec<PathBuf> = other_group
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
