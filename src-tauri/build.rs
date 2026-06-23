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

    #[cfg(feature = "gui")]
    {
        tauri_build::build();
    }

    println!("cargo:rerun-if-changed=../proto");

    Ok(())
}
