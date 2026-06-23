use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../proto");

    // All protos compiled together. prost-build generates one .rs file per
    // PACKAGE name (dots → underscores). So:
    //   gnmi_ext     (package gnmi_ext)     → gnmi_ext.rs
    //   gnmi         (package gnmi)         → gnmi.rs
    //   gnmi_sonic   (package gnmi.sonic)   → gnmi_sonic.rs  ← dial_out.proto!
    //   grpc_dialout (package grpc_dialout) → grpc_dialout.rs
    //   etc.
    let core_protos = [
        "grpc_dialout.proto",
        "grpc_dialout_v3.proto",
        "telemetry.proto",
        "gnmi_ext.proto",
        "gnmi.proto",
        "dial_out.proto",
    ];

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

    // Debug: list generated files using cargo:warning (shows in CI logs)
    if let Ok(out_dir) = std::env::var("OUT_DIR") {
        let out_path = PathBuf::from(&out_dir);
        if let Ok(entries) = std::fs::read_dir(&out_path) {
            println!("cargo:warning=== Generated proto files in {} ===", out_dir);
            for entry in entries.flatten() {
                let name = entry.file_name();
                println!("cargo:warning=  {}", name.to_string_lossy());
            }
        }
    }

    // Tauri mobile build hook (no-op when Tauri is not enabled)
    #[cfg(feature = "gui")]
    tauri_build::build();

    println!("cargo:rerun-if-changed=../proto");

    Ok(())
}
