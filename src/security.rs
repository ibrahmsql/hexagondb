//! Security module for HexagonDB.
//!
//! Provides authentication, authorization, and access control.

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use parking_lot::RwLock;
use tracing::{info, warn};
use std::hash::{Hash, Hasher};
use siphasher::sip::SipHasher;

/// Hash a password using SipHash (fast, suitable for non-persistent auth)
/// For persistent storage, consider using bcrypt/argon2 crate
pub fn hash_password(password: &str) -> String {
    let mut hasher = SipHasher::new();
    password.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Verify a password against a stored hash
pub fn verify_password(password: &str, stored_hash: &str) -> bool {
    // If stored_hash looks like a hex hash, compare hashes
    if stored_hash.len() == 16 && stored_hash.chars().all(|c| c.is_ascii_hexdigit()) {
        hash_password(password) == stored_hash
    } else {
        // Legacy: plain text comparison for backward compatibility
        password == stored_hash
    }
}

/// Authentication and authorization manager
pub struct Security {
    /// Password for default user (legacy AUTH)
    default_password: RwLock<Option<String>>,
    /// ACL users (username -> User)
    users: RwLock<HashMap<String, User>>,
    /// IP whitelist (if empty, all IPs allowed)
    ip_whitelist: RwLock<HashSet<IpAddr>>,
    /// IP blacklist
    ip_blacklist: RwLock<HashSet<IpAddr>>,
    /// Command rate limiter
    rate_limiter: RwLock<HashMap<String, RateLimitState>>,
    /// Commands that are always allowed without auth
    pub no_auth_commands: HashSet<String>,
}

/// ACL User 
#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    pub password_hash: Option<String>,
    pub enabled: bool,
    /// Allowed commands (empty = all allowed)
    pub allowed_commands: HashSet<String>,
    /// Denied commands
    pub denied_commands: HashSet<String>,
    /// Allowed key patterns (empty = all keys)
    pub allowed_keys: Vec<String>,
    /// Allowed channels for pub/sub
    pub allowed_channels: Vec<String>,
}

impl Default for User {
    fn default() -> Self {
        User {
            name: String::new(),
            password_hash: None,
            enabled: true,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
            allowed_keys: vec![],
            allowed_channels: vec![],
        }
    }
}

/// Rate limit state per connection
#[derive(Debug, Clone)]
struct RateLimitState {
    tokens: f64,
    last_update: std::time::Instant,
    max_tokens: f64,
    refill_rate: f64, // tokens per second
}

impl Security {
    /// Create a new Security manager
    pub fn new() -> Self {
        let mut no_auth = HashSet::new();
        // Commands allowed without authentication
        no_auth.insert("AUTH".to_string());
        no_auth.insert("PING".to_string());
        no_auth.insert("QUIT".to_string());
        no_auth.insert("HELLO".to_string());

        Security {
            default_password: RwLock::new(None),
            users: RwLock::new(HashMap::new()),
            ip_whitelist: RwLock::new(HashSet::new()),
            ip_blacklist: RwLock::new(HashSet::new()),
            rate_limiter: RwLock::new(HashMap::new()),
            no_auth_commands: no_auth,
        }
    }

    /// Set the default password (for legacy AUTH command)
    pub fn set_password(&self, password: Option<String>) {
        *self.default_password.write() = password;
    }

    /// Check if authentication is required
    pub fn is_auth_required(&self) -> bool {
        self.default_password.read().is_some() || !self.users.read().is_empty()
    }

    /// Authenticate with password (legacy AUTH)
    pub fn auth(&self, password: &str) -> bool {
        if let Some(ref stored) = *self.default_password.read() {
            if stored == password {
                info!("Authentication successful (default user)");
                return true;
            }
        }
        warn!("Authentication failed");
        false
    }

    /// Authenticate with username and password (AUTH username password)
    pub fn auth_user(&self, username: &str, password: &str) -> Option<User> {
        let users = self.users.read();
        if let Some(user) = users.get(username) {
            if !user.enabled {
                warn!("User {} is disabled", username);
                return None;
            }
            
            if let Some(ref stored_hash) = user.password_hash {
                // Use SHA256 for password verification
                if verify_password(password, stored_hash) {
                    info!("User {} authenticated successfully", username);
                    return Some(user.clone());
                }
            }
        }
        warn!("Authentication failed for user {}", username);
        None
    }

    /// Add or update a user
    pub fn acl_setuser(&self, name: String, rules: Vec<AclRule>) -> Result<(), String> {
        let mut users = self.users.write();
        let user = users.entry(name.clone()).or_insert_with(|| User {
            name: name.clone(),
            ..Default::default()
        });

        for rule in rules {
            match rule {
                AclRule::On => user.enabled = true,
                AclRule::Off => user.enabled = false,
                AclRule::Password(p) => user.password_hash = Some(p),
                AclRule::NoPass => user.password_hash = None,
                AclRule::AllCommands => {
                    user.allowed_commands.clear();
                    user.denied_commands.clear();
                }
                AclRule::NoCommands => {
                    user.denied_commands.insert("*".to_string());
                }
                AclRule::AllowCommand(cmd) => {
                    user.allowed_commands.insert(cmd.to_uppercase());
                    user.denied_commands.remove(&cmd.to_uppercase());
                }
                AclRule::DenyCommand(cmd) => {
                    user.denied_commands.insert(cmd.to_uppercase());
                    user.allowed_commands.remove(&cmd.to_uppercase());
                }
                AclRule::AllKeys => {
                    user.allowed_keys = vec!["*".to_string()];
                }
                AclRule::KeyPattern(pattern) => {
                    user.allowed_keys.push(pattern);
                }
                AclRule::AllChannels => {
                    user.allowed_channels = vec!["*".to_string()];
                }
                AclRule::ChannelPattern(pattern) => {
                    user.allowed_channels.push(pattern);
                }
                AclRule::Reset => {
                    *user = User {
                        name: name.clone(),
                        ..Default::default()
                    };
                }
            }
        }

        info!("ACL user {} updated", name);
        Ok(())
    }

    /// Delete a user
    pub fn acl_deluser(&self, names: Vec<String>) -> usize {
        let mut users = self.users.write();
        let mut count = 0;
        for name in names {
            if name != "default" && users.remove(&name).is_some() {
                count += 1;
            }
        }
        count
    }

    /// List all users
    pub fn acl_list(&self) -> Vec<String> {
        self.users.read().keys().cloned().collect()
    }

    /// Get user details
    pub fn acl_getuser(&self, name: &str) -> Option<User> {
        self.users.read().get(name).cloned()
    }

    /// Check if user can execute command
    pub fn can_execute(&self, user: Option<&User>, command: &str, keys: &[String]) -> bool {
        let cmd_upper = command.to_uppercase();

        // No-auth commands always allowed
        if self.no_auth_commands.contains(&cmd_upper) {
            return true;
        }

        // If no auth required, allow all
        if !self.is_auth_required() {
            return true;
        }

        let user = match user {
            Some(u) => u,
            None => return false, // Auth required but no user
        };

        if !user.enabled {
            return false;
        }

        // Check command permissions
        if user.denied_commands.contains("*") || user.denied_commands.contains(&cmd_upper) {
            return false;
        }

        if !user.allowed_commands.is_empty() && !user.allowed_commands.contains(&cmd_upper) {
            // Check if * is in allowed
            if !user.allowed_commands.contains("*") {
                return false;
            }
        }

        // Check key permissions
        if !user.allowed_keys.is_empty() && !user.allowed_keys.iter().any(|p| p == "*") {
            for key in keys {
                if !user.allowed_keys.iter().any(|pattern| key_matches(key, pattern)) {
                    return false;
                }
            }
        }

        true
    }

    /// Check if IP is allowed
    pub fn is_ip_allowed(&self, ip: IpAddr) -> bool {
        // Check blacklist first
        if self.ip_blacklist.read().contains(&ip) {
            return false;
        }

        // If whitelist is empty, all IPs allowed
        let whitelist = self.ip_whitelist.read();
        if whitelist.is_empty() {
            return true;
        }

        whitelist.contains(&ip)
    }

    /// Add IP to whitelist
    pub fn add_whitelist(&self, ip: IpAddr) {
        self.ip_whitelist.write().insert(ip);
    }

    /// Add IP to blacklist
    pub fn add_blacklist(&self, ip: IpAddr) {
        self.ip_blacklist.write().insert(ip);
    }

    /// Remove IP from whitelist
    pub fn remove_whitelist(&self, ip: IpAddr) {
        self.ip_whitelist.write().remove(&ip);
    }

    /// Remove IP from blacklist
    pub fn remove_blacklist(&self, ip: IpAddr) {
        self.ip_blacklist.write().remove(&ip);
    }

    /// Check rate limit (returns true if allowed)
    pub fn check_rate_limit(&self, client_id: &str, max_commands_per_second: f64) -> bool {
        let mut limiter = self.rate_limiter.write();
        let now = std::time::Instant::now();

        let state = limiter.entry(client_id.to_string()).or_insert_with(|| {
            RateLimitState {
                tokens: max_commands_per_second,
                last_update: now,
                max_tokens: max_commands_per_second,
                refill_rate: max_commands_per_second,
            }
        });

        // Refill tokens
        let elapsed = now.duration_since(state.last_update).as_secs_f64();
        state.tokens = (state.tokens + elapsed * state.refill_rate).min(state.max_tokens);
        state.last_update = now;

        // Try to consume a token
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Clear rate limit state for a client
    pub fn clear_rate_limit(&self, client_id: &str) {
        self.rate_limiter.write().remove(client_id);
    }
}

impl Default for Security {
    fn default() -> Self {
        Self::new()
    }
}

/// ACL rule for user configuration
#[derive(Debug, Clone)]
pub enum AclRule {
    On,
    Off,
    Password(String),
    NoPass,
    AllCommands,
    NoCommands,
    AllowCommand(String),
    DenyCommand(String),
    AllKeys,
    KeyPattern(String),
    AllChannels,
    ChannelPattern(String),
    Reset,
}

/// Parse ACL rule from string
pub fn parse_acl_rule(s: &str) -> Option<AclRule> {
    let s = s.trim();
    
    if s == "on" {
        return Some(AclRule::On);
    }
    if s == "off" {
        return Some(AclRule::Off);
    }
    if s == "nopass" {
        return Some(AclRule::NoPass);
    }
    if s == "allcommands" || s == "+@all" {
        return Some(AclRule::AllCommands);
    }
    if s == "nocommands" || s == "-@all" {
        return Some(AclRule::NoCommands);
    }
    if s == "allkeys" || s == "~*" {
        return Some(AclRule::AllKeys);
    }
    if s == "allchannels" || s == "&*" {
        return Some(AclRule::AllChannels);
    }
    if s == "reset" || s == "resetkeys" || s == "resetchannels" {
        return Some(AclRule::Reset);
    }
    if s.starts_with('>') {
        return Some(AclRule::Password(s[1..].to_string()));
    }
    if s.starts_with('+') {
        return Some(AclRule::AllowCommand(s[1..].to_string()));
    }
    if s.starts_with('-') {
        return Some(AclRule::DenyCommand(s[1..].to_string()));
    }
    if s.starts_with('~') {
        return Some(AclRule::KeyPattern(s[1..].to_string()));
    }
    if s.starts_with('&') {
        return Some(AclRule::ChannelPattern(s[1..].to_string()));
    }
    
    None
}

/// Check if key matches pattern
fn key_matches(key: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    
    // Simple glob matching
    let mut pattern_chars = pattern.chars().peekable();
    let mut key_chars = key.chars().peekable();

    while pattern_chars.peek().is_some() || key_chars.peek().is_some() {
        match pattern_chars.peek() {
            Some('*') => {
                pattern_chars.next();
                if pattern_chars.peek().is_none() {
                    return true;
                }
                while key_chars.peek().is_some() {
                    let remaining_pattern: String = pattern_chars.clone().collect();
                    let remaining_key: String = key_chars.clone().collect();
                    if key_matches(&remaining_key, &remaining_pattern) {
                        return true;
                    }
                    key_chars.next();
                }
                return false;
            }
            Some(pc) => {
                if Some(*pc) != key_chars.next() {
                    return false;
                }
                pattern_chars.next();
            }
            None => {
                return key_chars.peek().is_none();
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth() {
        let security = Security::new();
        
        // No auth required initially
        assert!(!security.is_auth_required());
        
        // Set password
        security.set_password(Some("secret123".to_string()));
        assert!(security.is_auth_required());
        
        // Test auth
        assert!(security.auth("secret123"));
        assert!(!security.auth("wrong"));
    }

    #[test]
    fn test_acl_user() {
        let security = Security::new();
        
        // Create user
        security.acl_setuser("testuser".to_string(), vec![
            AclRule::On,
            AclRule::Password("mypass".to_string()),
            AclRule::AllCommands,
            AclRule::AllKeys,
        ]).unwrap();

        // Auth
        let user = security.auth_user("testuser", "mypass");
        assert!(user.is_some());

        // Wrong password
        assert!(security.auth_user("testuser", "wrong").is_none());
    }

    #[test]
    fn test_key_pattern() {
        assert!(key_matches("user:123", "user:*"));
        assert!(key_matches("user:123:name", "user:*"));
        assert!(!key_matches("admin:123", "user:*"));
        assert!(key_matches("anything", "*"));
    }

    #[test]
    fn test_rate_limit() {
        let security = Security::new();
        
        // Allow 2 commands per second
        assert!(security.check_rate_limit("client1", 2.0));
        assert!(security.check_rate_limit("client1", 2.0));
        assert!(!security.check_rate_limit("client1", 2.0)); // Should be limited
    }
}
