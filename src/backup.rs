//! Backup scheduling module.
//!
//! Provides automated backup scheduling for RDB and AOF.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{info, error};

use crate::db::DB;

/// Backup configuration
#[derive(Debug, Clone)]
pub struct BackupConfig {
    /// Enable automatic RDB saves
    pub rdb_enabled: bool,
    /// RDB save interval in seconds
    pub rdb_interval_secs: u64,
    /// Minimum changes before RDB save
    pub rdb_min_changes: usize,
    /// RDB file path
    pub rdb_path: PathBuf,
    /// Enable AOF
    pub aof_enabled: bool,
    /// AOF file path
    pub aof_path: PathBuf,
    /// Enable backup rotation
    pub rotation_enabled: bool,
    /// Number of backups to keep
    pub rotation_count: usize,
}

impl Default for BackupConfig {
    fn default() -> Self {
        BackupConfig {
            rdb_enabled: true,
            rdb_interval_secs: 900, // 15 minutes
            rdb_min_changes: 100,
            rdb_path: PathBuf::from("hexagon.rdb"),
            aof_enabled: true,
            aof_path: PathBuf::from("hexagon.aof"),
            rotation_enabled: true,
            rotation_count: 5,
        }
    }
}

/// Backup scheduler
pub struct BackupScheduler {
    config: Arc<RwLock<BackupConfig>>,
    db: Arc<RwLock<DB>>,
    last_save_changes: Arc<std::sync::atomic::AtomicUsize>,
}

impl BackupScheduler {
    pub fn new(config: BackupConfig, db: Arc<RwLock<DB>>) -> Self {
        BackupScheduler {
            config: Arc::new(RwLock::new(config)),
            db,
            last_save_changes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Start the backup scheduler as a background task
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&self) {
        let config = self.config.read().await;
        let mut tick = interval(Duration::from_secs(config.rdb_interval_secs));
        drop(config);

        loop {
            tick.tick().await;
            
            let config = self.config.read().await;
            if !config.rdb_enabled {
                continue;
            }

            // Check if enough changes have occurred
            let db = self.db.read().await;
            let current_changes = db.changes_since_save.load(std::sync::atomic::Ordering::Relaxed);
            drop(db);

            let last_changes = self.last_save_changes.load(std::sync::atomic::Ordering::Relaxed);
            let changes_since_last = current_changes.saturating_sub(last_changes);

            if changes_since_last < config.rdb_min_changes {
                continue;
            }

            info!("Backup scheduler: {} changes detected, saving RDB", changes_since_last);

            // Rotate if enabled
            if config.rotation_enabled {
                self.rotate_backups(&config.rdb_path, config.rotation_count).await;
            }

            // Perform save
            match crate::persistence::snapshot::save(&config.rdb_path, &self.db).await {
                Ok(_) => {
                    info!("RDB saved successfully to {:?}", config.rdb_path);
                    self.last_save_changes.store(current_changes, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => {
                    error!("Failed to save RDB: {}", e);
                }
            }
        }
    }

    async fn rotate_backups(&self, base_path: &PathBuf, count: usize) {
        // Rotate old backups: .4 -> .5, .3 -> .4, etc.
        for i in (1..count).rev() {
            let old_path = format!("{}.{}", base_path.display(), i);
            let new_path = format!("{}.{}", base_path.display(), i + 1);
            let _ = tokio::fs::rename(&old_path, &new_path).await;
        }

        // Current -> .1
        if base_path.exists() {
            let backup_path = format!("{}.1", base_path.display());
            let _ = tokio::fs::rename(base_path, &backup_path).await;
        }
    }

    /// Trigger immediate backup
    pub async fn save_now(&self) -> std::io::Result<()> {
        let config = self.config.read().await;
        
        if config.rotation_enabled {
            self.rotate_backups(&config.rdb_path, config.rotation_count).await;
        }

        crate::persistence::snapshot::save(&config.rdb_path, &self.db).await?;
        
        let db = self.db.read().await;
        let current_changes = db.changes_since_save.load(std::sync::atomic::Ordering::Relaxed);
        self.last_save_changes.store(current_changes, std::sync::atomic::Ordering::Relaxed);
        
        info!("Manual RDB save completed");
        Ok(())
    }

    /// Update configuration
    pub async fn update_config(&self, config: BackupConfig) {
        *self.config.write().await = config;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_config_default() {
        let config = BackupConfig::default();
        assert!(config.rdb_enabled);
        assert_eq!(config.rdb_interval_secs, 900);
    }
}
