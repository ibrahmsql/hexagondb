//! Client management module.
//!
//! Tracks connected clients and their state.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Client information
#[derive(Debug, Clone)]
pub struct ClientInfo {
    /// Unique client ID
    pub id: u64,
    /// Client address
    pub addr: SocketAddr,
    /// Client name (set via CLIENT SETNAME)
    pub name: Option<String>,
    /// Connection time
    pub connected_at: Instant,
    /// Last command time
    pub last_cmd_time: Instant,
    /// Current database index
    pub db: usize,
    /// Whether client is authenticated
    pub authenticated: bool,
    /// Current transaction state
    pub in_transaction: bool,
    /// Number of subscriptions
    pub subscriptions: usize,
    /// Pending output buffer size
    pub output_buffer_size: usize,
    /// Current command being executed
    pub current_cmd: Option<String>,
    /// Flags
    pub flags: ClientFlags,
}

/// Client flags
#[derive(Debug, Clone, Default)]
pub struct ClientFlags {
    /// Is a slave connection
    pub slave: bool,
    /// Is a master connection
    pub master: bool,
    /// Is in monitor mode
    pub monitor: bool,
    /// Is blocked on a command
    pub blocked: bool,
    /// Connection is closing
    pub close_asap: bool,
    /// Authenticated
    pub authenticated: bool,
}

impl ClientInfo {
    pub fn new(id: u64, addr: SocketAddr) -> Self {
        let now = Instant::now();
        ClientInfo {
            id,
            addr,
            name: None,
            connected_at: now,
            last_cmd_time: now,
            db: 0,
            authenticated: false,
            in_transaction: false,
            subscriptions: 0,
            output_buffer_size: 0,
            current_cmd: None,
            flags: ClientFlags::default(),
        }
    }

    /// Get age in seconds
    pub fn age_seconds(&self) -> u64 {
        self.connected_at.elapsed().as_secs()
    }

    /// Get idle time in seconds
    pub fn idle_seconds(&self) -> u64 {
        self.last_cmd_time.elapsed().as_secs()
    }

    /// Update last command time
    pub fn touch(&mut self) {
        self.last_cmd_time = Instant::now();
    }

    /// Generate CLIENT LIST format string
    pub fn to_client_list_string(&self) -> String {
        format!(
            "id={} addr={} name={} age={} idle={} db={} sub={} cmd={}\n",
            self.id,
            self.addr,
            self.name.as_deref().unwrap_or(""),
            self.age_seconds(),
            self.idle_seconds(),
            self.db,
            self.subscriptions,
            self.current_cmd.as_deref().unwrap_or("NULL"),
        )
    }
}

/// Client manager
pub struct ClientManager {
    /// Active clients (id -> ClientInfo)
    clients: RwLock<HashMap<u64, ClientInfo>>,
    /// Next client ID
    next_id: AtomicU64,
    /// Address to ID mapping for quick lookup
    addr_to_id: RwLock<HashMap<SocketAddr, u64>>,
}

impl ClientManager {
    pub fn new() -> Self {
        ClientManager {
            clients: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            addr_to_id: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new client
    pub fn register(&self, addr: SocketAddr) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let client = ClientInfo::new(id, addr);
        
        self.clients.write().insert(id, client);
        self.addr_to_id.write().insert(addr, id);
        
        id
    }

    /// Unregister a client
    pub fn unregister(&self, id: u64) {
        if let Some(client) = self.clients.write().remove(&id) {
            self.addr_to_id.write().remove(&client.addr);
        }
    }

    /// Get client by ID
    pub fn get(&self, id: u64) -> Option<ClientInfo> {
        self.clients.read().get(&id).cloned()
    }

    /// Get client by address
    pub fn get_by_addr(&self, addr: &SocketAddr) -> Option<ClientInfo> {
        let id = self.addr_to_id.read().get(addr).copied()?;
        self.get(id)
    }

    /// Update client
    pub fn update<F>(&self, id: u64, f: F) 
    where
        F: FnOnce(&mut ClientInfo),
    {
        if let Some(client) = self.clients.write().get_mut(&id) {
            f(client);
        }
    }

    /// Set client name
    pub fn set_name(&self, id: u64, name: Option<String>) {
        self.update(id, |c| c.name = name);
    }

    /// Touch client (update last command time)
    pub fn touch(&self, id: u64) {
        self.update(id, |c| c.touch());
    }

    /// List all clients
    pub fn list(&self) -> Vec<ClientInfo> {
        self.clients.read().values().cloned().collect()
    }

    /// Get client count
    pub fn count(&self) -> usize {
        self.clients.read().len()
    }

    /// Kill client by ID
    pub fn kill_by_id(&self, id: u64) -> bool {
        self.clients.write().remove(&id).is_some()
    }

    /// Kill client by address
    pub fn kill_by_addr(&self, addr: &SocketAddr) -> bool {
        if let Some(id) = self.addr_to_id.write().remove(addr) {
            return self.clients.write().remove(&id).is_some();
        }
        false
    }

    /// Kill clients by filter
    pub fn kill_by_filter<F>(&self, filter: F) -> usize
    where
        F: Fn(&ClientInfo) -> bool,
    {
        let mut clients = self.clients.write();
        let mut addr_to_id = self.addr_to_id.write();
        
        let to_remove: Vec<u64> = clients
            .iter()
            .filter(|(_, c)| filter(c))
            .map(|(id, _)| *id)
            .collect();
        
        let count = to_remove.len();
        for id in to_remove {
            if let Some(client) = clients.remove(&id) {
                addr_to_id.remove(&client.addr);
            }
        }
        
        count
    }

    /// Generate CLIENT LIST output
    pub fn client_list(&self) -> String {
        self.clients
            .read()
            .values()
            .map(|c| c.to_client_list_string())
            .collect()
    }
}

impl Default for ClientManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_client_manager() {
        let manager = ClientManager::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345);
        
        let id = manager.register(addr);
        assert!(manager.get(id).is_some());
        assert_eq!(manager.count(), 1);
        
        manager.set_name(id, Some("test-client".to_string()));
        let client = manager.get(id).unwrap();
        assert_eq!(client.name, Some("test-client".to_string()));
        
        manager.unregister(id);
        assert!(manager.get(id).is_none());
        assert_eq!(manager.count(), 0);
    }
}
