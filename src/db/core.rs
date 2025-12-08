//! Core database structure.
//!
//! The heart of HexagonDB - an in-memory HashMap storing all data.

use crate::db::types::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The core database structure.
/// All data is stored in memory in this HashMap.
pub struct DB {
    /// Main data store
    pub items: HashMap<String, Entry>,
    /// Changes since last save (for persistence triggers)
    pub(crate) changes_since_save: Arc<AtomicUsize>,
}

impl DB {
    /// Create a new empty database
    pub fn new() -> Self {
        DB {
            items: HashMap::new(),
            changes_since_save: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a database with initial capacity
    pub fn with_capacity(capacity: usize) -> Self {
        DB {
            items: HashMap::with_capacity(capacity),
            changes_since_save: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Increment the changes counter
    pub fn increment_changes(&self) {
        self.changes_since_save.fetch_add(1, Ordering::Relaxed);
    }

    /// Reset the changes counter (after save)
    pub fn reset_changes(&self) {
        self.changes_since_save.store(0, Ordering::Relaxed);
    }

    /// Get the number of changes since last save
    pub fn get_changes(&self) -> usize {
        self.changes_since_save.load(Ordering::Relaxed)
    }

    /// Get a clone of the changes counter (for background tasks)
    pub fn get_changes_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.changes_since_save)
    }
}

impl Default for DB {
    fn default() -> Self {
        Self::new()
    }
}
