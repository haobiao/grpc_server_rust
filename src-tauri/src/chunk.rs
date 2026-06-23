//! Chunk record assembly for fragmented gRPC messages.
//!
//! Replaces Python's `ChunkRecords` class.
//! Uses `bytes::Bytes` for zero-copy storage of JSON fragments.

use bytes::Bytes;
use std::collections::HashMap;

use crate::error::{AppError, Result};
use crate::models::MessageBuffer;

/// A single chunked record being assembled.
#[derive(Debug, Clone)]
pub struct ChunkRecord {
    /// Device message bytes (from DialoutMsg.deviceMsg).
    pub device_msg: Bytes,
    /// Sensor path this record belongs to.
    pub sensor_path: String,
    /// Total expected size of the assembled data.
    pub total_size: i64,
    /// Total number of fragments expected.
    pub total_fragments: u64,
    /// Node ID of the last received fragment.
    pub last_node_id: u64,
    /// Accumulated JSON data fragments (zero-copy Bytes).
    pub json_datas: Vec<Bytes>,
}

impl ChunkRecord {
    /// Create a new ChunkRecord from an initial message.
    pub fn new(
        device_msg: Bytes,
        sensor_path: String,
        total_size: i64,
        total_fragments: u64,
        node_id: u64,
        json_data: Bytes,
    ) -> Self {
        Self {
            device_msg,
            sensor_path,
            total_size,
            total_fragments,
            last_node_id: node_id,
            json_datas: vec![json_data],
        }
    }

    /// Check if the incoming message matches this record (sequential node_id).
    pub fn matches(&self, node_id: u64) -> bool {
        self.last_node_id + 1 == node_id
    }

    /// Check if all fragments have been received.
    pub fn is_complete(&self) -> bool {
        self.total_fragments as usize == self.json_datas.len()
    }

    /// Add a fragment to this record.
    pub fn add(&mut self, node_id: u64, json_data: Bytes) {
        self.last_node_id = node_id;
        self.json_datas.push(json_data);
    }

    /// Merge all JSON data fragments into a single Bytes (zero-copy concat).
    pub fn merge_json(&self) -> Bytes {
        let total_len: usize = self.json_datas.iter().map(|b| b.len()).sum();
        let mut merged = bytes::BytesMut::with_capacity(total_len);
        for fragment in &self.json_datas {
            merged.extend_from_slice(fragment);
        }
        merged.freeze()
    }
}

/// Manages chunked record assembly across sensor paths.
///
/// Each sensor_path can have multiple in-flight chunk records.
/// This replaces Python's `ChunkRecords` with a thread-safe HashMap.
pub struct ChunkRecords {
    /// key = sensor_path, value = list of in-flight chunk records
    records: HashMap<String, Vec<ChunkRecord>>,
}

impl ChunkRecords {
    /// Create a new empty ChunkRecords manager.
    pub fn new() -> Self {
        Self {
            records: HashMap::new(),
        }
    }

    /// Process an incoming message.
    ///
    /// Returns `Some(merged_json_data)` if the message completed a chunk record,
    /// or `None` if the message was added to an existing record / new record was started.
    pub fn add(
        &mut self,
        device_msg: Bytes,
        sensor_path: &str,
        json_data: Bytes,
        total_size: Option<i64>,
        total_fragments: Option<u64>,
        node_id: Option<u64>,
    ) -> Option<(Bytes, Bytes)> {
        // No chunk info → pass through directly
        if total_size.is_none() && total_fragments.is_none() {
            return Some((device_msg, json_data));
        }

        let ts = total_size.unwrap_or(0);
        let tf = total_fragments.unwrap_or(0);
        let nid = node_id.unwrap_or(0);

        let record_list = self.records.entry(sensor_path.to_owned()).or_default();

        // Try to find a matching record
        let matched_index = record_list.iter().position(|r| r.matches(nid));

        if let Some(idx) = matched_index {
            // Add fragment to existing record
            record_list[idx].add(nid, json_data);
            if record_list[idx].is_complete() {
                let record = record_list.remove(idx);
                let merged_json = record.merge_json();
                return Some((record.device_msg, merged_json));
            }
        } else {
            // Start a new record
            let record = ChunkRecord::new(
                device_msg,
                sensor_path.to_owned(),
                ts,
                tf,
                nid,
                json_data,
            );
            record_list.push(record);
        }

        None
    }
}

impl Default for ChunkRecords {
    fn default() -> Self {
        Self::new()
    }
}
