//! Slow log for command monitoring.
//!
//! Tracks slow commands for performance analysis.

use parking_lot::RwLock;
use std::collections::VecDeque;
use std::time::Instant;

/// Slow log entry
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    /// Unique ID
    pub id: u64,
    /// Timestamp when command started
    pub timestamp: u64,
    /// Duration in microseconds
    pub duration_us: u64,
    /// Command and arguments
    pub command: Vec<String>,
    /// Client address
    pub client_addr: String,
    /// Client name (if set)
    pub client_name: Option<String>,
}

/// Slow log manager
pub struct SlowLog {
    /// Log entries (newest first)
    entries: RwLock<VecDeque<SlowLogEntry>>,
    /// Maximum entries to keep
    max_len: RwLock<usize>,
    /// Threshold in microseconds (commands slower than this are logged)
    threshold_us: RwLock<u64>,
    /// Next log ID
    next_id: RwLock<u64>,
}

impl SlowLog {
    /// Create a new slow log with default settings
    pub fn new() -> Self {
        SlowLog {
            entries: RwLock::new(VecDeque::with_capacity(128)),
            max_len: RwLock::new(128),
            threshold_us: RwLock::new(10000), // 10ms default
            next_id: RwLock::new(0),
        }
    }

    /// Set maximum log length
    pub fn set_max_len(&self, max_len: usize) {
        *self.max_len.write() = max_len;
        // Trim if needed
        let mut entries = self.entries.write();
        while entries.len() > max_len {
            entries.pop_back();
        }
    }

    /// Set threshold in microseconds
    pub fn set_threshold(&self, threshold_us: u64) {
        *self.threshold_us.write() = threshold_us;
    }

    /// Get threshold
    pub fn get_threshold(&self) -> u64 {
        *self.threshold_us.read()
    }

    /// Log a command if it exceeds the threshold
    pub fn log_if_slow(
        &self,
        start_time: Instant,
        command: Vec<String>,
        client_addr: String,
        client_name: Option<String>,
    ) {
        let duration = start_time.elapsed();
        let duration_us = duration.as_micros() as u64;

        if duration_us < *self.threshold_us.read() {
            return;
        }

        let id = {
            let mut next_id = self.next_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };

        let entry = SlowLogEntry {
            id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            duration_us,
            command,
            client_addr,
            client_name,
        };

        {
            let mut entries = self.entries.write();
            let max_len = *self.max_len.read();
            
            entries.push_front(entry);
            
            while entries.len() > max_len {
                entries.pop_back();
            }
        }
    }

    /// Get slow log entries
    pub fn get(&self, count: Option<usize>) -> Vec<SlowLogEntry> {
        let entries = self.entries.read();
        let count = count.unwrap_or(10).min(entries.len());
        entries.iter().take(count).cloned().collect()
    }

    /// Get log length
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Reset slow log
    pub fn reset(&self) {
        self.entries.write().clear();
        *self.next_id.write() = 0;
    }
}

impl Default for SlowLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory usage information
#[derive(Debug, Clone)]
pub struct MemoryInfo {
    /// Used memory in bytes
    pub used_memory: usize,
    /// Used memory human readable
    pub used_memory_human: String,
    /// Used memory RSS
    pub used_memory_rss: usize,
    /// Peak memory
    pub used_memory_peak: usize,
    /// Fragmentation ratio
    pub fragmentation_ratio: f64,
}

/// Get memory usage of a key (approximate)
pub fn key_memory_usage(data_type: &str, size: usize) -> usize {
    // Approximate memory calculation
    // Base overhead + data size
    let base = 48; // Object header + pointer
    
    match data_type {
        "string" => base + size,
        "list" => base + size * 24, // Vec overhead per element
        "set" => base + size * 40,  // HashSet overhead per element
        "hash" => base + size * 80, // HashMap overhead per entry
        "zset" => base + size * 64, // BTreeSet + HashMap overhead
        "bitmap" => base + size,
        "stream" => base + size * 100,
        "geo" => base + size * 50,
        "hyperloglog" => base + 16384, // Fixed HLL size
        _ => base + size,
    }
}

/// Format bytes as human readable
pub fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.2}G", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}M", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2}K", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_slowlog() {
        let slowlog = SlowLog::new();
        slowlog.set_threshold(0); // Log everything
        
        // Log a command
        let start = Instant::now();
        std::thread::sleep(Duration::from_micros(100));
        slowlog.log_if_slow(
            start,
            vec!["SET".to_string(), "key".to_string(), "value".to_string()],
            "127.0.0.1:12345".to_string(),
            None,
        );

        assert_eq!(slowlog.len(), 1);
        
        let entries = slowlog.get(Some(10));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].command[0], "SET");
    }

    #[test]
    fn test_slowlog_threshold() {
        let slowlog = SlowLog::new();
        slowlog.set_threshold(1_000_000); // 1 second threshold
        
        // This should not be logged
        let start = Instant::now();
        slowlog.log_if_slow(
            start,
            vec!["GET".to_string(), "key".to_string()],
            "127.0.0.1:12345".to_string(),
            None,
        );

        assert_eq!(slowlog.len(), 0);
    }
}
