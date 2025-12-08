//! Replication module.
//!
//! Provides master-slave replication support.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::info;

/// Replication role
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationRole {
    Master,
    Slave,
}

/// Replication state
#[derive(Debug, Clone)]
pub struct ReplicationState {
    /// Current role
    pub role: ReplicationRole,
    /// Master host (if slave)
    pub master_host: Option<String>,
    /// Master port (if slave)
    pub master_port: Option<u16>,
    /// Replication offset
    pub repl_offset: u64,
    /// Master replication ID
    pub master_replid: String,
    /// Number of connected slaves
    pub connected_slaves: usize,
}

/// Slave information
#[derive(Debug, Clone)]
pub struct SlaveInfo {
    pub id: String,
    pub addr: SocketAddr,
    pub offset: u64,
    pub lag: u64,
    pub state: SlaveState,
}

/// Slave connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlaveState {
    Connecting,
    Sync,
    Connected,
    Disconnected,
}

/// Replication manager
pub struct ReplicationManager {
    /// Current role
    role: RwLock<ReplicationRole>,
    /// Master info (when slave)
    master_host: RwLock<Option<String>>,
    master_port: RwLock<Option<u16>>,
    /// Replication offset
    repl_offset: AtomicU64,
    /// Master replication ID
    master_replid: RwLock<String>,
    /// Connected slaves (when master)
    slaves: RwLock<HashMap<String, SlaveInfo>>,
    /// Replication backlog for partial sync
    backlog: RwLock<ReplicationBacklog>,
    /// Whether replication is active (reserved for future use)
    #[allow(dead_code)]
    active: AtomicBool,
    /// Command broadcast channel for slaves
    command_tx: broadcast::Sender<ReplicationCommand>,
}

/// Command to replicate
#[derive(Debug, Clone)]
pub struct ReplicationCommand {
    pub offset: u64,
    pub command: Vec<String>,
}

/// Replication backlog for partial resync
#[derive(Debug, Clone)]
struct ReplicationBacklog {
    buffer: Vec<ReplicationCommand>,
    first_offset: u64,
    max_size: usize,
}

impl Default for ReplicationBacklog {
    fn default() -> Self {
        ReplicationBacklog {
            buffer: Vec::new(),
            first_offset: 0,
            max_size: 1_000_000, // 1MB default
        }
    }
}

impl ReplicationManager {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(10000);
        
        ReplicationManager {
            role: RwLock::new(ReplicationRole::Master),
            master_host: RwLock::new(None),
            master_port: RwLock::new(None),
            repl_offset: AtomicU64::new(0),
            master_replid: RwLock::new(generate_replid()),
            slaves: RwLock::new(HashMap::new()),
            backlog: RwLock::new(ReplicationBacklog::default()),
            active: AtomicBool::new(false),
            command_tx: tx,
        }
    }

    /// Get current role
    pub fn role(&self) -> ReplicationRole {
        *self.role.read()
    }

    /// Get replication state
    pub fn state(&self) -> ReplicationState {
        ReplicationState {
            role: *self.role.read(),
            master_host: self.master_host.read().clone(),
            master_port: *self.master_port.read(),
            repl_offset: self.repl_offset.load(Ordering::SeqCst),
            master_replid: self.master_replid.read().clone(),
            connected_slaves: self.slaves.read().len(),
        }
    }

    /// Set as slave of master (SLAVEOF/REPLICAOF)
    pub fn slaveof(&self, host: String, port: u16) {
        *self.role.write() = ReplicationRole::Slave;
        *self.master_host.write() = Some(host.clone());
        *self.master_port.write() = Some(port);
        
        info!("Configured as slave of {}:{}", host, port);
    }

    /// Clear slave status (SLAVEOF NO ONE)
    pub fn slaveof_no_one(&self) {
        *self.role.write() = ReplicationRole::Master;
        *self.master_host.write() = None;
        *self.master_port.write() = None;
        *self.master_replid.write() = generate_replid();
        
        info!("Slave mode disabled, now master");
    }

    /// Register a slave connection
    pub fn register_slave(&self, id: String, addr: SocketAddr) {
        let slave = SlaveInfo {
            id: id.clone(),
            addr,
            offset: 0,
            lag: 0,
            state: SlaveState::Connecting,
        };
        self.slaves.write().insert(id.clone(), slave);
        info!("Slave {} registered from {}", id, addr);
    }

    /// Update slave offset
    pub fn update_slave_offset(&self, id: &str, offset: u64) {
        if let Some(slave) = self.slaves.write().get_mut(id) {
            slave.offset = offset;
            slave.lag = self.repl_offset.load(Ordering::SeqCst).saturating_sub(offset);
            slave.state = SlaveState::Connected;
        }
    }

    /// Remove a slave
    pub fn remove_slave(&self, id: &str) {
        self.slaves.write().remove(id);
        info!("Slave {} removed", id);
    }

    /// Get list of slaves
    pub fn list_slaves(&self) -> Vec<SlaveInfo> {
        self.slaves.read().values().cloned().collect()
    }

    /// Add command to replication stream (called by master on writes)
    pub fn replicate_command(&self, command: Vec<String>) {
        if *self.role.read() != ReplicationRole::Master {
            return;
        }

        let offset = self.repl_offset.fetch_add(1, Ordering::SeqCst);
        
        let cmd = ReplicationCommand {
            offset,
            command: command.clone(),
        };

        // Add to backlog
        {
            let mut backlog = self.backlog.write();
            backlog.buffer.push(cmd.clone());
            
            // Trim backlog if needed
            while backlog.buffer.len() > backlog.max_size {
                backlog.buffer.remove(0);
                backlog.first_offset += 1;
            }
        }

        // Broadcast to slaves
        let _ = self.command_tx.send(cmd);
    }

    /// Subscribe to replication commands (for slave connections)
    pub fn subscribe(&self) -> broadcast::Receiver<ReplicationCommand> {
        self.command_tx.subscribe()
    }

    /// Get commands from backlog for partial sync
    pub fn get_backlog_from(&self, offset: u64) -> Option<Vec<ReplicationCommand>> {
        let backlog = self.backlog.read();
        
        if offset < backlog.first_offset {
            // Full sync required
            return None;
        }

        let start_idx = (offset - backlog.first_offset) as usize;
        if start_idx >= backlog.buffer.len() {
            return Some(vec![]);
        }

        Some(backlog.buffer[start_idx..].to_vec())
    }

    /// Get current offset
    pub fn offset(&self) -> u64 {
        self.repl_offset.load(Ordering::SeqCst)
    }

    /// Get master replication ID
    pub fn replid(&self) -> String {
        self.master_replid.read().clone()
    }
}

impl Default for ReplicationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a random replication ID
fn generate_replid() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: Vec<u8> = (0..20).map(|_| rng.gen()).collect();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Replication INFO section
pub fn info_replication(manager: &ReplicationManager) -> String {
    let state = manager.state();
    
    let mut info = format!(
        "# Replication\nrole:{}\nmaster_replid:{}\nmaster_repl_offset:{}\n",
        match state.role {
            ReplicationRole::Master => "master",
            ReplicationRole::Slave => "slave",
        },
        state.master_replid,
        state.repl_offset,
    );

    if state.role == ReplicationRole::Master {
        info.push_str(&format!("connected_slaves:{}\n", state.connected_slaves));
        
        for (i, slave) in manager.list_slaves().iter().enumerate() {
            info.push_str(&format!(
                "slave{}:ip={},port={},state={:?},offset={},lag={}\n",
                i,
                slave.addr.ip(),
                slave.addr.port(),
                slave.state,
                slave.offset,
                slave.lag,
            ));
        }
    } else {
        if let Some(host) = state.master_host {
            info.push_str(&format!("master_host:{}\n", host));
        }
        if let Some(port) = state.master_port {
            info.push_str(&format!("master_port:{}\n", port));
        }
    }

    info
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_replication_manager() {
        let manager = ReplicationManager::new();
        assert_eq!(manager.role(), ReplicationRole::Master);
        
        // Configure as slave
        manager.slaveof("127.0.0.1".to_string(), 6379);
        assert_eq!(manager.role(), ReplicationRole::Slave);
        
        // Back to master
        manager.slaveof_no_one();
        assert_eq!(manager.role(), ReplicationRole::Master);
    }

    #[test]
    fn test_slave_registration() {
        let manager = ReplicationManager::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), 6379);
        
        manager.register_slave("slave1".to_string(), addr);
        assert_eq!(manager.list_slaves().len(), 1);
        
        manager.remove_slave("slave1");
        assert_eq!(manager.list_slaves().len(), 0);
    }
}
