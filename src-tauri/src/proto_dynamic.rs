//! Runtime dynamic proto loading for gRPC 3-layer (GPB) mode.
//!
//! At startup, this module scans the `proto/` directory for `*_v3.proto` files,
//! invokes `protoc` to generate a combined `.desc` FileDescriptorSet,
//! and loads it via `prost-reflect` for runtime message decoding.
//!
//! This replaces Python's `importlib.import_module` + `protobuf.message_factory`
//! dynamic loading approach.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::{AppError, Result};
use prost::Message;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FileDescriptor, MessageDescriptor,
};

/// File extension for compiled descriptor sets.
const DESC_EXT: &str = ".desc";

/// Proto files to skip when loading v3 modules.
const SKIP_PROTOS: &[&str] = &["grpc_dialout_v3.proto"];

/// Combined descriptor file name for all v3 protos.
const V3_DESC_FILENAME: &str = "v3_modules.desc";

/// Runtime proto module registry.
///
/// Holds a descriptor pool and a map from message name (without package)
/// to its `MessageDescriptor` for fast lookup by sensor_path.
pub struct ProtoDynamicRegistry {
    /// The descriptor pool containing all loaded proto definitions.
    pool: DescriptorPool,
    /// Map from top-level message name (no package) to MessageDescriptor.
    /// e.g. "Device" → MessageDescriptor for device_v3.Device
    message_by_name: HashMap<String, MessageDescriptor>,
    /// Loaded v3 proto file names for reference.
    v3_files: Vec<String>,
}

impl ProtoDynamicRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            pool: DescriptorPool::global(),
            message_by_name: HashMap::new(),
            v3_files: Vec::new(),
        }
    }

    /// Load all v3 proto modules from the given proto directory.
    ///
    /// Scans for `*_v3.proto` files, generates a combined `.desc` file,
    /// and indexes all message types.
    pub fn load_all_v3(&mut self, proto_dir: &Path, autogen_dir: &Path) -> Result<()> {
        // Ensure autogen dir exists
        if !autogen_dir.exists() {
            std::fs::create_dir_all(autogen_dir)?;
        }

        // Find all v3 proto files (excluding skipped ones)
        let v3_proto_files: Vec<PathBuf> = std::fs::read_dir(proto_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension().map(|ext| ext == "proto").unwrap_or(false)
                    && p.file_name()
                        .map(|n| n.to_string_lossy().ends_with("_v3.proto"))
                        .unwrap_or(false)
            })
            .filter(|p| {
                p.file_name()
                    .map(|n| !SKIP_PROTOS.contains(&n.to_string_lossy().as_ref()))
                    .unwrap_or(true)
            })
            .collect();

        if v3_proto_files.is_empty() {
            tracing::warn!("No v3 proto files found in {}", proto_dir.display());
            return Ok(());
        }

        let desc_path = autogen_dir.join(V3_DESC_FILENAME);

        // Build protoc command to generate descriptor set
        let mut cmd = Command::new("protoc");
        cmd.arg(format!("--proto_path={}", proto_dir.display()));
        cmd.arg(format!(
            "--descriptor_set_out={}",
            desc_path.display()
        ));
        cmd.arg("--include_imports");

        for proto in &v3_proto_files {
            cmd.arg(proto);
        }

        let output = cmd
            .output()
            .map_err(|e| AppError::Protoc(format!("Failed to execute protoc: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(AppError::Protoc(format!(
                "protoc failed: {}",
                stderr
            )));
        }

        // Load the descriptor set
        let desc_bytes = std::fs::read(&desc_path)?;
        let descriptor_set = prost_types::FileDescriptorSet::decode(&desc_bytes[..])
            .map_err(|e| AppError::ProtoParse(format!("Failed to parse descriptor set: {}", e)))?;

        // Build a new pool and add all files
        let mut pool = DescriptorPool::new();

        for file_proto in &descriptor_set.file {
            let file_bytes = file_proto
                .encode_to_vec()
                .map_err(|e| AppError::ProtoParse(format!("Failed to encode file descriptor: {}", e)))?;

            pool.add_file_descriptor_bytes(&file_bytes)
                .map_err(|e| AppError::ProtoReflect(format!("Failed to add file descriptor to pool: {}", e)))?;

            let file_name = file_proto.name().to_owned();
            self.v3_files.push(file_name.clone());
        }

        // Index messages by short name (without package prefix)
        for file_proto in &descriptor_set.file {
            let package = file_proto.package();
            for msg in &file_proto.message_type {
                let full_name = msg.name();
                let short_name = full_name
                    .rsplit('.')
                    .next()
                    .unwrap_or(full_name)
                    .to_owned();

                if let Ok(desc) = pool.get_message_by_name(full_name) {
                    self.message_by_name.insert(short_name, desc);
                }
            }
        }

        self.pool = pool;
        tracing::info!(
            "Loaded {} v3 proto files, {} message types indexed",
            self.v3_files.len(),
            self.message_by_name.len()
        );

        Ok(())
    }

    /// Find a message descriptor by sensor_path.
    ///
    /// Extracts the first segment of the path, replaces '-' with '_',
    /// and looks up in the message registry.
    pub fn find_module(&self, sensor_path: &str) -> Option<&MessageDescriptor> {
        let module_name = sensor_path
            .split('/')
            .next()
            .unwrap_or(sensor_path)
            .replace('-', "_");

        if let Some(desc) = self.message_by_name.get(&module_name) {
            return Some(desc);
        }

        // Special case: InSuitOam uses second path segment
        if module_name == "InSuitOam" {
            if let Some(second) = sensor_path.split('/').nth(1) {
                return self.message_by_name.get(second);
            }
        }

        None
    }

    /// Get a list of supported module names for logging.
    pub fn supported_modules(&self) -> Vec<&str> {
        self.message_by_name.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for ProtoDynamicRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate descriptor files from all proto files.
/// This is the Rust equivalent of Python's `proto2py.py::parse()`.
pub fn generate_descriptor_files() -> Result<()> {
    let current_dir = std::env::current_dir()?;
    let proto_dir = current_dir.join("proto");
    let autogen_dir = current_dir.join("autogen");

    if !proto_dir.exists() {
        return Err(AppError::Config(format!(
            "Proto directory '{}' does not exist",
            proto_dir.display()
        )));
    }

    if !autogen_dir.exists() {
        std::fs::create_dir_all(&autogen_dir)?;
    }

    // Find all .proto files (excluding any.proto and backups)
    let proto_files: Vec<PathBuf> = std::fs::read_dir(&proto_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().map(|ext| ext == "proto").unwrap_or(false)
        })
        .filter(|p| {
            p.file_name()
                .map(|n| {
                    let name = n.to_string_lossy();
                    !name.starts_with("any.proto") && !name.contains("_bak_")
                })
                .unwrap_or(true)
        })
        .collect();

    if proto_files.is_empty() {
        return Err(AppError::Config("No proto files found to compile".into()));
    }

    // Generate compiled Rust output for each proto
    for proto_path in &proto_files {
        let file_name = proto_path
            .file_name()
            .ok_or_else(|| AppError::Config("Invalid proto file name".into()))?
            .to_string_lossy()
            .to_string();

        let mut cmd = Command::new("protoc");
        cmd.arg(format!("--proto_path={}", proto_dir.display()));
        cmd.arg("--include_imports");
        cmd.arg(format!(
            "--descriptor_set_out={}/{}{}",
            autogen_dir.display(),
            file_name.replace(".proto", ""),
            DESC_EXT
        ));
        cmd.arg(proto_path);

        let output = cmd
            .output()
            .map_err(|e| AppError::Protoc(format!("Failed to execute protoc for {}: {}", file_name, e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            tracing::error!("protoc failed for {}: {}", file_name, stderr);
        } else {
            tracing::info!("Generated descriptor for {}", file_name);
        }
    }

    // Generate v3 combined descriptor
    let mut registry = ProtoDynamicRegistry::new();
    registry.load_all_v3(&proto_dir, &autogen_dir)?;

    Ok(())
}
