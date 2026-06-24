//! Runtime dynamic proto loading for gRPC 3-layer (GPB) mode.
//!
//! Uses `protox` (a pure-Rust protobuf compiler) to compile `.proto` files
//! at runtime — no external `protoc` binary required.
//!
//! This replaces Python's `importlib.import_module` + `protobuf.message_factory`
//! dynamic loading approach.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use prost::Message;
use prost_reflect::{DescriptorPool, MessageDescriptor};

use crate::error::{AppError, Result};

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
    /// Uses `protox::compile` (pure-Rust protoc) to compile `*_v3.proto`
    /// files into a `FileDescriptorSet`, then loads it into a `DescriptorPool`.
    pub fn load_all_v3(&mut self, proto_dir: &Path, autogen_dir: &Path) -> Result<()> {
        // Ensure autogen dir exists
        if !autogen_dir.exists() {
            std::fs::create_dir_all(autogen_dir)?;
        }

        // Find all v3 proto files (excluding grpc_dialout_v3.proto)
        let v3_proto_files: Vec<PathBuf> = std::fs::read_dir(proto_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension().map(|ext| ext == "proto").unwrap_or(false)
                    && p.file_name()
                        .map(|n| n.to_string_lossy().ends_with("_v3.proto"))
                        .unwrap_or(false)
                    && p.file_name()
                        .map(|n| n.to_string_lossy() != "grpc_dialout_v3.proto")
                        .unwrap_or(true)
            })
            .collect();

        if v3_proto_files.is_empty() {
            tracing::warn!("No v3 proto files found in {}", proto_dir.display());
            return Ok(());
        }

        // Convert to string paths for protox
        let proto_dir_str = proto_dir.to_string_lossy().to_string();
        let v3_proto_strs: Vec<String> = v3_proto_files
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        tracing::info!(
            "Compiling {} v3 proto files with protox (pure-Rust protoc)...",
            v3_proto_strs.len()
        );

        // Use protox to compile — returns a FileDescriptorSet
        let file_descriptor_set = protox::compile(&v3_proto_strs, &[proto_dir_str])
            .map_err(|e| AppError::Protoc(format!("protox compile failed: {}", e)))?;

        // Optionally write the descriptor set for debugging/reuse
        let desc_path = autogen_dir.join(V3_DESC_FILENAME);
        let desc_bytes = file_descriptor_set.encode_to_vec();
        let _ = std::fs::write(&desc_path, &desc_bytes);

        // Build a new pool and add all files
        let mut pool = DescriptorPool::new();

        for file_proto in &file_descriptor_set.file {
            pool.add_file_descriptor_proto(file_proto.clone())
                .map_err(|e| {
                    AppError::ProtoReflect(format!("Failed to add file descriptor to pool: {}", e))
                })?;

            let file_name = file_proto.name().to_owned();
            self.v3_files.push(file_name);
        }

        // Index messages by short name (without package prefix)
        for file_proto in &file_descriptor_set.file {
            let package = file_proto.package();
            for msg in &file_proto.message_type {
                let relative_name = msg.name();
                let full_name = if package.is_empty() {
                    relative_name.to_owned()
                } else {
                    format!("{}.{}", package, relative_name)
                };
                let short_name = relative_name
                    .rsplit('.')
                    .next()
                    .unwrap_or(relative_name)
                    .to_owned();

                if let Some(desc) = pool.get_message_by_name(&full_name) {
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
///
/// Uses `protox` (pure-Rust protoc) instead of external protoc binary.
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

    let proto_dir_str = proto_dir.to_string_lossy().to_string();

    // Compile each proto file individually and write descriptor
    for proto_path in &proto_files {
        let file_name = proto_path
            .file_name()
            .ok_or_else(|| AppError::Config("Invalid proto file name".into()))?
            .to_string_lossy()
            .to_string();

        let proto_path_str = proto_path.to_string_lossy().to_string();

        match protox::compile([&proto_path_str], [&proto_dir_str]) {
            Ok(file_descriptor_set) => {
                let desc_bytes = file_descriptor_set.encode_to_vec();
                let desc_name = file_name.replace(".proto", ".desc");
                let desc_out = autogen_dir.join(&desc_name);
                std::fs::write(&desc_out, &desc_bytes)?;
                tracing::info!("Generated descriptor for {}", file_name);
            }
            Err(e) => {
                tracing::error!("protox failed for {}: {}", file_name, e);
            }
        }
    }

    // Generate v3 combined descriptor
    let mut registry = ProtoDynamicRegistry::new();
    registry.load_all_v3(&proto_dir, &autogen_dir)?;

    Ok(())
}
