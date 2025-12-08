//! Configuration management for HexagonDB.
//!
//! Supports TOML configuration files and hot-reload via SIGHUP.

use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Main configuration structure
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub memory: MemoryConfig,
    #[serde(default)]
    pub security: SecurityConfig,
}

/// Server configuration
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    #[serde(default)]
    pub tcp_keepalive: bool,
}

/// Persistence configuration
#[derive(Debug, Clone, Deserialize)]
pub struct PersistenceConfig {
    #[serde(default = "default_aof_enabled")]
    pub aof_enabled: bool,
    #[serde(default = "default_aof_fsync")]
    pub aof_fsync: String, // "always", "everysec", "no"
    #[serde(default)]
    pub aof_path: Option<String>,
    #[serde(default = "default_rdb_enabled")]
    pub rdb_enabled: bool,
    #[serde(default = "default_rdb_save_interval")]
    pub rdb_save_interval: u64,
    #[serde(default = "default_rdb_min_changes")]
    pub rdb_min_changes: u64,
    #[serde(default)]
    pub rdb_compression: bool,
}

/// Logging configuration
#[derive(Debug, Clone, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub json_format: bool,
}

/// Memory configuration
#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    #[serde(default)]
    pub maxmemory: Option<usize>, // bytes
    #[serde(default = "default_eviction_policy")]
    pub eviction_policy: String, // "noeviction", "allkeys-lru", "volatile-lru", etc.
}

/// Security configuration
#[derive(Debug, Clone, Deserialize)]
pub struct SecurityConfig {
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub tls_enabled: bool,
    #[serde(default)]
    pub tls_cert_file: Option<String>,
    #[serde(default)]
    pub tls_key_file: Option<String>,
}

// Default value functions
fn default_bind_address() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    2112
}

fn default_max_connections() -> usize {
    10000
}

fn default_timeout() -> u64 {
    0 // No timeout
}

fn default_aof_enabled() -> bool {
    true
}

fn default_aof_fsync() -> String {
    "everysec".to_string()
}

fn default_rdb_enabled() -> bool {
    true
}

fn default_rdb_save_interval() -> u64 {
    300 // 5 minutes
}

fn default_rdb_min_changes() -> u64 {
    1
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_eviction_policy() -> String {
    "noeviction".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig::default(),
            persistence: PersistenceConfig::default(),
            logging: LoggingConfig::default(),
            memory: MemoryConfig::default(),
            security: SecurityConfig::default(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind_address: default_bind_address(),
            port: default_port(),
            max_connections: default_max_connections(),
            timeout_seconds: default_timeout(),
            tcp_keepalive: false,
        }
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        PersistenceConfig {
            aof_enabled: default_aof_enabled(),
            aof_fsync: default_aof_fsync(),
            aof_path: None,
            rdb_enabled: default_rdb_enabled(),
            rdb_save_interval: default_rdb_save_interval(),
            rdb_min_changes: default_rdb_min_changes(),
            rdb_compression: false,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        LoggingConfig {
            level: default_log_level(),
            file: None,
            json_format: false,
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        MemoryConfig {
            maxmemory: None,
            eviction_policy: default_eviction_policy(),
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        SecurityConfig {
            password: None,
            tls_enabled: false,
            tls_cert_file: None,
            tls_key_file: None,
        }
    }
}

impl Config {
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path.as_ref())
            .map_err(|e| ConfigError::IoError(e.to_string()))?;
        
        toml::from_str(&contents)
            .map_err(|e| ConfigError::ParseError(e.to_string()))
    }

    /// Get the server address as a string
    pub fn server_address(&self) -> String {
        format!("{}:{}", self.server.bind_address, self.server.port)
    }
}

/// Configuration error types
#[derive(Debug, Clone)]
pub enum ConfigError {
    IoError(String),
    ParseError(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::IoError(msg) => write!(f, "IO error: {}", msg),
            ConfigError::ParseError(msg) => write!(f, "Parse error: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {}
