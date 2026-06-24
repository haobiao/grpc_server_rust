use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("../proto");

    let core_protos = [
        "grpc_dialout.proto",
        "grpc_dialout_v3.proto",
        "telemetry.proto",
        "gnmi_ext.proto",
        "gnmi.proto",
        "dial_out.proto",
    ];

    let proto_files: Vec<PathBuf> = core_protos
        .iter()
        .map(|f| proto_root.join(f))
        .collect();

    // Use protox (pure-Rust protoc) to compile .proto files into a
    // FileDescriptorSet — no external protoc binary required at build time.
    let proto_files_str: Vec<String> = proto_files
        .iter()
        .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    let mut compiler = protox::Compiler::new([&proto_root])
        .map_err(|e| format!("protox compiler init failed: {}", e))?;
    compiler.include_imports(true);
    compiler.open_files(&proto_files_str)
        .map_err(|e| format!("protox compile failed: {}", e))?;
    let file_descriptor_set = compiler.file_descriptor_set();

    // Use tonic-build to generate Rust server code from the FileDescriptorSet.
    // This replaces the old tonic_build::compile_protos() which needed external protoc.
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_fds(file_descriptor_set)
        .map_err(|e| format!("tonic_build compile_fds failed: {}", e))?;

    // Tauri mobile build hook (no-op when Tauri is not enabled)
    #[cfg(feature = "gui")]
    {
        tauri_build::build();
    }

    println!("cargo:rerun-if-changed=../proto");

    Ok(())
}
