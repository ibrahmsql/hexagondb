//! Stream operations.
//!
//! Redis-compatible stream (Kafka-like) operations.

use crate::db::core::DB;
use crate::db::ops::generic::GenericOps;
use crate::db::types::{DataType, Entry, StreamData};
use std::collections::HashMap;
use std::sync::atomic::Ordering;

/// Stream operations trait
pub trait StreamOps {
    /// Add entry to stream (XADD)
    fn xadd(&mut self, key: String, id: Option<String>, fields: Vec<(String, String)>) -> Result<String, String>;
    
    /// Get stream length (XLEN)
    fn xlen(&mut self, key: String) -> usize;
    
    /// Get range of entries (XRANGE)
    fn xrange(&mut self, key: String, start: String, end: String, count: Option<usize>) -> Vec<(String, Vec<(String, String)>)>;
    
    /// Get reverse range (XREVRANGE)
    fn xrevrange(&mut self, key: String, end: String, start: String, count: Option<usize>) -> Vec<(String, Vec<(String, String)>)>;
    
    /// Read from streams (XREAD) - simplified version
    fn xread(&mut self, keys: Vec<String>, ids: Vec<String>, count: Option<usize>) -> Vec<(String, Vec<(String, Vec<(String, String)>)>)>;
    
    /// Trim stream (XTRIM)
    fn xtrim(&mut self, key: String, maxlen: usize, approximate: bool) -> usize;
    
    /// Delete entries (XDEL)
    fn xdel(&mut self, key: String, ids: Vec<String>) -> usize;
    
    /// Get stream info (XINFO STREAM)
    fn xinfo_stream(&mut self, key: String) -> Option<StreamInfo>;
}

/// Stream information
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub length: usize,
    pub first_entry: Option<String>,
    pub last_entry: Option<String>,
    pub last_generated_id: String,
}

impl StreamOps for DB {
    fn xadd(&mut self, key: String, id: Option<String>, fields: Vec<(String, String)>) -> Result<String, String> {
        self.check_expiration(&key);

        let fields_map: HashMap<String, String> = fields.into_iter().collect();

        let entry = self.items.entry(key).or_insert_with(|| Entry {
            value: DataType::Stream(StreamData::new()),
            expires_at: None,
        });

        match &mut entry.value {
            DataType::Stream(stream) => {
                let entry_id = stream.add(id, fields_map);
                self.changes_since_save.fetch_add(1, Ordering::Relaxed);
                Ok(entry_id)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    fn xlen(&mut self, key: String) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Stream(stream) = &entry.value {
                return stream.entries.len();
            }
        }
        0
    }

    fn xrange(&mut self, key: String, start: String, end: String, count: Option<usize>) -> Vec<(String, Vec<(String, String)>)> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Stream(stream) = &entry.value {
                let start_id = if start == "-" { "" } else { &start };
                let end_id = if end == "+" { "\u{FFFF}" } else { &end };

                let mut results: Vec<_> = stream.entries
                    .iter()
                    .filter(|e| e.id.as_str() >= start_id && e.id.as_str() <= end_id)
                    .map(|e| {
                        let fields: Vec<(String, String)> = e.fields.iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        (e.id.clone(), fields)
                    })
                    .collect();

                if let Some(n) = count {
                    results.truncate(n);
                }

                return results;
            }
        }
        vec![]
    }

    fn xrevrange(&mut self, key: String, end: String, start: String, count: Option<usize>) -> Vec<(String, Vec<(String, String)>)> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Stream(stream) = &entry.value {
                let start_id = if start == "-" { "" } else { &start };
                let end_id = if end == "+" { "\u{FFFF}" } else { &end };

                let mut results: Vec<_> = stream.entries
                    .iter()
                    .rev()
                    .filter(|e| e.id.as_str() >= start_id && e.id.as_str() <= end_id)
                    .map(|e| {
                        let fields: Vec<(String, String)> = e.fields.iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        (e.id.clone(), fields)
                    })
                    .collect();

                if let Some(n) = count {
                    results.truncate(n);
                }

                return results;
            }
        }
        vec![]
    }

    fn xread(&mut self, keys: Vec<String>, ids: Vec<String>, count: Option<usize>) -> Vec<(String, Vec<(String, Vec<(String, String)>)>)> {
        let mut results = Vec::new();

        for (key, last_id) in keys.iter().zip(ids.iter()) {
            if !self.check_expiration(key) {
                continue;
            }

            if let Some(entry) = self.items.get(key) {
                if let DataType::Stream(stream) = &entry.value {
                    let start_id = if last_id == "0" || last_id == "0-0" {
                        ""
                    } else {
                        last_id.as_str()
                    };

                    let mut entries: Vec<_> = stream.entries
                        .iter()
                        .filter(|e| e.id.as_str() > start_id)
                        .map(|e| {
                            let fields: Vec<(String, String)> = e.fields.iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();
                            (e.id.clone(), fields)
                        })
                        .collect();

                    if let Some(n) = count {
                        entries.truncate(n);
                    }

                    if !entries.is_empty() {
                        results.push((key.clone(), entries));
                    }
                }
            }
        }

        results
    }

    fn xtrim(&mut self, key: String, maxlen: usize, _approximate: bool) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        if let Some(entry) = self.items.get_mut(&key) {
            if let DataType::Stream(stream) = &mut entry.value {
                let current_len = stream.entries.len();
                if current_len > maxlen {
                    let to_remove = current_len - maxlen;
                    stream.entries.drain(0..to_remove);
                    self.changes_since_save.fetch_add(1, Ordering::Relaxed);
                    return to_remove;
                }
            }
        }
        0
    }

    fn xdel(&mut self, key: String, ids: Vec<String>) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        if let Some(entry) = self.items.get_mut(&key) {
            if let DataType::Stream(stream) = &mut entry.value {
                let original_len = stream.entries.len();
                stream.entries.retain(|e| !ids.contains(&e.id));
                let deleted = original_len - stream.entries.len();
                if deleted > 0 {
                    self.changes_since_save.fetch_add(1, Ordering::Relaxed);
                }
                return deleted;
            }
        }
        0
    }

    fn xinfo_stream(&mut self, key: String) -> Option<StreamInfo> {
        if !self.check_expiration(&key) {
            return None;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Stream(stream) = &entry.value {
                return Some(StreamInfo {
                    length: stream.entries.len(),
                    first_entry: stream.entries.first().map(|e| e.id.clone()),
                    last_entry: stream.entries.last().map(|e| e.id.clone()),
                    last_generated_id: format!("{}", stream.last_id),
                });
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xadd_xlen() {
        let mut db = DB::new();
        
        let id1 = db.xadd("mystream".to_string(), None, vec![
            ("field1".to_string(), "value1".to_string()),
        ]).unwrap();
        
        let id2 = db.xadd("mystream".to_string(), None, vec![
            ("field2".to_string(), "value2".to_string()),
        ]).unwrap();
        
        assert_eq!(db.xlen("mystream".to_string()), 2);
        assert!(id2 > id1);
    }

    #[test]
    fn test_xrange() {
        let mut db = DB::new();
        
        db.xadd("mystream".to_string(), Some("1-0".to_string()), vec![
            ("a".to_string(), "1".to_string()),
        ]).unwrap();
        
        db.xadd("mystream".to_string(), Some("2-0".to_string()), vec![
            ("b".to_string(), "2".to_string()),
        ]).unwrap();
        
        let range = db.xrange("mystream".to_string(), "-".to_string(), "+".to_string(), None);
        assert_eq!(range.len(), 2);
    }
}
