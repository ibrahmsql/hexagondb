//! Server information and statistics.
//!
//! Provides runtime information about the HexagonDB server.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Server information and statistics
pub struct ServerInfo {
    /// Server start time
    start_time: Instant,
    /// Total commands processed
    total_commands: AtomicU64,
    /// Total connections received
    total_connections: AtomicU64,
    /// Current connected clients
    connected_clients: AtomicUsize,
    /// Total bytes received
    bytes_received: AtomicU64,
    /// Total bytes sent
    bytes_sent: AtomicU64,
    /// Rejected connections (over limit)
    rejected_connections: AtomicU64,
    /// Expired keys counter
    expired_keys: AtomicU64,
}

impl ServerInfo {
    /// Create a new ServerInfo instance
    pub fn new() -> Self {
        ServerInfo {
            start_time: Instant::now(),
            total_commands: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            connected_clients: AtomicUsize::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            expired_keys: AtomicU64::new(0),
        }
    }

    /// Increment total commands counter
    pub fn increment_commands(&self) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment total connections counter
    pub fn increment_connections(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment connected clients counter
    pub fn client_connected(&self) {
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement connected clients counter
    pub fn client_disconnected(&self) {
        self.connected_clients.fetch_sub(1, Ordering::Relaxed);
    }

    /// Add bytes received
    pub fn add_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add bytes sent
    pub fn add_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Increment rejected connections counter
    pub fn increment_rejected(&self) {
        self.rejected_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment expired keys counter
    pub fn increment_expired_keys(&self) {
        self.expired_keys.fetch_add(1, Ordering::Relaxed);
    }

    /// Get uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Generate INFO command response
    pub fn generate_info(&self, db_size: usize) -> String {
        let uptime = self.uptime_seconds();
        let total_cmds = self.total_commands.load(Ordering::Relaxed);
        let total_conns = self.total_connections.load(Ordering::Relaxed);
        let connected = self.connected_clients.load(Ordering::Relaxed);
        let bytes_in = self.bytes_received.load(Ordering::Relaxed);
        let bytes_out = self.bytes_sent.load(Ordering::Relaxed);
        let rejected = self.rejected_connections.load(Ordering::Relaxed);
        let expired = self.expired_keys.load(Ordering::Relaxed);
        let (used_memory, used_memory_human) = get_memory_usage();

        format!(
            r#"# Server
hexagondb_version:0.1.0
os:{}
arch:{}
process_id:{}
uptime_in_seconds:{}
uptime_in_days:{}

# Clients
connected_clients:{}
total_connections_received:{}
rejected_connections:{}

# Stats
total_commands_processed:{}
total_net_input_bytes:{}
total_net_output_bytes:{}
expired_keys:{}

# Memory
used_memory:{}
used_memory_human:{}

# Keyspace
db0:keys={}
"#,
            std::env::consts::OS,
            std::env::consts::ARCH,
            std::process::id(),
            uptime,
            uptime / 86400,
            connected,
            total_conns,
            rejected,
            total_cmds,
            bytes_in,
            bytes_out,
            expired,
            used_memory,
            used_memory_human,
            db_size
        )
    }
}

impl Default for ServerInfo {
    fn default() -> Self {
        Self::new()
    }
}

/// Get memory usage from the operating system.
/// Returns (bytes, human_readable_string)
fn get_memory_usage() -> (usize, String) {
    #[cfg(target_os = "macos")]
    {
        get_memory_usage_macos()
    }
    #[cfg(target_os = "linux")]
    {
        get_memory_usage_linux()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        (0, "0B".to_string())
    }
}

#[cfg(target_os = "macos")]
fn get_memory_usage_macos() -> (usize, String) {
    use std::process::Command;
    
    // Use ps command to get RSS (Resident Set Size)
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();
    
    match output {
        Ok(output) => {
            if let Ok(rss_str) = String::from_utf8(output.stdout) {
                if let Ok(rss_kb) = rss_str.trim().parse::<usize>() {
                    let bytes = rss_kb * 1024;
                    return (bytes, format_bytes(bytes));
                }
            }
            (0, "0B".to_string())
        }
        Err(_) => (0, "0B".to_string()),
    }
}

#[cfg(target_os = "linux")]
fn get_memory_usage_linux() -> (usize, String) {
    use std::fs;
    
    // Read from /proc/self/statm
    if let Ok(content) = fs::read_to_string("/proc/self/statm") {
        let parts: Vec<&str> = content.split_whitespace().collect();
        if parts.len() >= 2 {
            // Second field is RSS in pages
            if let Ok(pages) = parts[1].parse::<usize>() {
                // Page size is typically 4KB on Linux
                let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
                let bytes = pages * page_size;
                return (bytes, format_bytes(bytes));
            }
        }
    }
    
    // Fallback: try /proc/self/status
    if let Ok(content) = fs::read_to_string("/proc/self/status") {
        for line in content.lines() {
            if line.starts_with("VmRSS:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<usize>() {
                        let bytes = kb * 1024;
                        return (bytes, format_bytes(bytes));
                    }
                }
            }
        }
    }
    
    (0, "0B".to_string())
}

/// Format bytes into human-readable string
fn format_bytes(bytes: usize) -> String {
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
