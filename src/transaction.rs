//! Transaction support for HexagonDB.
//!
//! Implements MULTI/EXEC/DISCARD/WATCH commands for atomic operations.

use std::collections::HashMap;
use parking_lot::RwLock;

/// Transaction state for a client
#[derive(Debug, Clone, Default)]
pub struct Transaction {
    /// Whether MULTI has been called
    pub in_multi: bool,
    /// Queued commands
    pub queue: Vec<QueuedCommand>,
    /// Watched keys and their values at watch time
    pub watched_keys: HashMap<String, Option<u64>>,
    /// Whether any watched key was modified
    pub watch_dirty: bool,
}

/// A queued command in a transaction
#[derive(Debug, Clone)]
pub struct QueuedCommand {
    pub command: String,
    pub args: Vec<String>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            in_multi: false,
            queue: Vec::new(),
            watched_keys: HashMap::new(),
            watch_dirty: false,
        }
    }

    /// Start a transaction (MULTI)
    pub fn multi(&mut self) -> Result<(), &'static str> {
        if self.in_multi {
            return Err("ERR MULTI calls can not be nested");
        }
        self.in_multi = true;
        self.queue.clear();
        Ok(())
    }

    /// Queue a command
    pub fn queue(&mut self, command: String, args: Vec<String>) -> Result<(), &'static str> {
        if !self.in_multi {
            return Err("ERR EXEC without MULTI");
        }
        self.queue.push(QueuedCommand { command, args });
        Ok(())
    }

    /// Get queued commands for execution (EXEC)
    pub fn exec(&mut self) -> Result<Vec<QueuedCommand>, &'static str> {
        if !self.in_multi {
            return Err("ERR EXEC without MULTI");
        }

        // Check if any watched key was modified
        if self.watch_dirty {
            let _ = self.discard();
            return Err("EXECABORT Transaction discarded because of previous errors.");
        }

        let commands = std::mem::take(&mut self.queue);
        self.in_multi = false;
        self.watched_keys.clear();
        self.watch_dirty = false;

        Ok(commands)
    }

    /// Discard the transaction (DISCARD)
    pub fn discard(&mut self) -> Result<(), &'static str> {
        if !self.in_multi {
            return Err("ERR DISCARD without MULTI");
        }
        self.in_multi = false;
        self.queue.clear();
        self.watched_keys.clear();
        self.watch_dirty = false;
        Ok(())
    }

    /// Watch keys (WATCH)
    pub fn watch(&mut self, keys: Vec<String>, key_versions: HashMap<String, Option<u64>>) -> Result<(), &'static str> {
        if self.in_multi {
            return Err("ERR WATCH inside MULTI is not allowed");
        }
        for key in keys {
            let version = key_versions.get(&key).copied().flatten();
            self.watched_keys.insert(key, version);
        }
        Ok(())
    }

    /// Unwatch all keys (UNWATCH)
    pub fn unwatch(&mut self) {
        self.watched_keys.clear();
        self.watch_dirty = false;
    }

    /// Check if a key modification should mark this transaction dirty
    pub fn check_key_modified(&mut self, key: &str, new_version: Option<u64>) {
        if let Some(old_version) = self.watched_keys.get(key) {
            if *old_version != new_version {
                self.watch_dirty = true;
            }
        }
    }

    /// Check if currently in a transaction
    pub fn is_in_multi(&self) -> bool {
        self.in_multi
    }

    /// Get number of queued commands
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }
}

/// Transaction manager for all clients
pub struct TransactionManager {
    /// Client ID -> Transaction state
    transactions: RwLock<HashMap<String, Transaction>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            transactions: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create transaction for a client
    pub fn get_or_create(&self, client_id: &str) -> Transaction {
        let transactions = self.transactions.read();
        transactions.get(client_id).cloned().unwrap_or_default()
    }

    /// Update transaction for a client
    pub fn update(&self, client_id: &str, transaction: Transaction) {
        self.transactions.write().insert(client_id.to_string(), transaction);
    }

    /// Remove transaction state for a client
    pub fn remove(&self, client_id: &str) {
        self.transactions.write().remove(client_id);
    }

    /// Notify all transactions that a key was modified
    pub fn notify_key_modified(&self, key: &str, new_version: Option<u64>) {
        let mut transactions = self.transactions.write();
        for (_, tx) in transactions.iter_mut() {
            tx.check_key_modified(key, new_version);
        }
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_exec() {
        let mut tx = Transaction::new();
        
        // Start transaction
        tx.multi().unwrap();
        assert!(tx.is_in_multi());
        
        // Queue commands
        tx.queue("SET".to_string(), vec!["key1".to_string(), "value1".to_string()]).unwrap();
        tx.queue("SET".to_string(), vec!["key2".to_string(), "value2".to_string()]).unwrap();
        assert_eq!(tx.queue_len(), 2);
        
        // Execute
        let commands = tx.exec().unwrap();
        assert_eq!(commands.len(), 2);
        assert!(!tx.is_in_multi());
    }

    #[test]
    fn test_discard() {
        let mut tx = Transaction::new();
        
        tx.multi().unwrap();
        tx.queue("SET".to_string(), vec!["key".to_string(), "value".to_string()]).unwrap();
        tx.discard().unwrap();
        
        assert!(!tx.is_in_multi());
        assert_eq!(tx.queue_len(), 0);
    }

    #[test]
    fn test_watch() {
        let mut tx = Transaction::new();
        
        let mut versions = HashMap::new();
        versions.insert("key1".to_string(), Some(1u64));
        
        tx.watch(vec!["key1".to_string()], versions).unwrap();
        
        // Key modified with different version
        tx.check_key_modified("key1", Some(2));
        assert!(tx.watch_dirty);
        
        // EXEC should fail
        tx.multi().unwrap();
        tx.queue("SET".to_string(), vec!["key1".to_string(), "value".to_string()]).unwrap();
        assert!(tx.exec().is_err());
    }
}
