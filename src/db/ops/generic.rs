//! Generic key operations.
//!
//! Operations that work on any key regardless of data type.

use crate::db::core::DB;
use crate::db::types::{DataType, Entry};
use rand::seq::IteratorRandom;
use std::time::{Duration, Instant};

/// Generic operations trait
pub trait GenericOps {
    /// Check and handle key expiration. Returns false if key was expired.
    fn check_expiration(&mut self, key: &str) -> bool;
    
    /// Check if a key exists
    fn exists(&self, key: &str) -> bool;
    
    /// Delete a key
    fn del(&mut self, key: &str) -> bool;
    
    /// Get the type of a key
    fn type_of(&self, key: &str) -> Option<String>;
    
    /// Set expiration on a key
    fn expire(&mut self, key: &str, seconds: u64) -> bool;
    
    /// Set expiration at a specific timestamp
    fn expireat(&mut self, key: &str, timestamp: u64) -> bool;
    
    /// Get TTL in seconds
    fn ttl(&mut self, key: &str) -> i64;
    
    /// Get TTL in milliseconds
    fn pttl(&mut self, key: &str) -> i64;
    
    /// Remove expiration from a key
    fn persist(&mut self, key: &str) -> bool;
    
    /// Find all keys matching a pattern
    fn keys(&self, pattern: &str) -> Vec<String>;
    
    /// Scan keys with cursor
    fn scan(&self, cursor: u64, pattern: Option<&str>, count: Option<usize>) -> (u64, Vec<String>);
    
    /// Rename a key
    fn rename(&mut self, key: &str, newkey: &str) -> Result<(), String>;
    
    /// Rename key if newkey doesn't exist
    fn renamenx(&mut self, key: &str, newkey: &str) -> bool;
    
    /// Get database size
    fn dbsize(&self) -> usize;
    
    /// Flush all keys
    fn flushdb(&mut self);
    
    /// Get a random key
    fn randomkey(&self) -> Option<String>;
    
    /// Copy a key to another
    fn copy(&mut self, src: &str, dst: &str, replace: bool) -> bool;
    
    /// Delete keys asynchronously (UNLINK).
    /// Note: In Redis, UNLINK performs deletion in a background thread.
    /// In HexagonDB single-threaded mode, this behaves identically to DEL.
    /// For true async deletion, use the async runtime variant.
    fn unlink(&mut self, keys: Vec<&str>) -> usize;
    
    /// Touch keys (update access time)
    fn touch(&mut self, keys: Vec<&str>) -> usize;
}

impl GenericOps for DB {
    fn check_expiration(&mut self, key: &str) -> bool {
        if let Some(entry) = self.items.get(key) {
            if let Some(expires_at) = entry.expires_at {
                if Instant::now() >= expires_at {
                    self.items.remove(key);
                    return false;
                }
            }
        }
        true
    }

    fn exists(&self, key: &str) -> bool {
        if let Some(entry) = self.items.get(key) {
            if let Some(expires_at) = entry.expires_at {
                return Instant::now() < expires_at;
            }
            return true;
        }
        false
    }

    fn del(&mut self, key: &str) -> bool {
        if self.items.remove(key).is_some() {
            self.increment_changes();
            true
        } else {
            false
        }
    }

    fn type_of(&self, key: &str) -> Option<String> {
        self.items.get(key).map(|entry| {
            match &entry.value {
                DataType::String(_) => "string".to_string(),
                DataType::List(_) => "list".to_string(),
                DataType::Hash(_) => "hash".to_string(),
                DataType::Set(_) => "set".to_string(),
                DataType::ZSet(_) => "zset".to_string(),
                DataType::Stream(_) => "stream".to_string(),
                DataType::Bitmap(_) => "string".to_string(), // Bitmap is stored as string in Redis
                DataType::Geo(_) => "zset".to_string(), // Geo uses zset internally
                DataType::HyperLogLog(_) => "string".to_string(),
            }
        })
    }

    fn expire(&mut self, key: &str, seconds: u64) -> bool {
        if let Some(entry) = self.items.get_mut(key) {
            entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds));
            self.increment_changes();
            true
        } else {
            false
        }
    }

    fn expireat(&mut self, key: &str, timestamp: u64) -> bool {
        // Calculate duration from now to timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        if timestamp > now {
            let secs = timestamp - now;
            self.expire(key, secs)
        } else {
            // Timestamp in the past - delete the key
            self.del(key);
            true
        }
    }

    fn ttl(&mut self, key: &str) -> i64 {
        if !self.check_expiration(key) {
            return -2; // Key doesn't exist
        }

        if let Some(entry) = self.items.get(key) {
            if let Some(expires_at) = entry.expires_at {
                let now = Instant::now();
                if expires_at > now {
                    return (expires_at - now).as_secs() as i64;
                }
            }
            return -1; // No expiration
        }
        -2 // Key doesn't exist
    }

    fn pttl(&mut self, key: &str) -> i64 {
        if !self.check_expiration(key) {
            return -2;
        }

        if let Some(entry) = self.items.get(key) {
            if let Some(expires_at) = entry.expires_at {
                let now = Instant::now();
                if expires_at > now {
                    return (expires_at - now).as_millis() as i64;
                }
            }
            return -1;
        }
        -2
    }

    fn persist(&mut self, key: &str) -> bool {
        if let Some(entry) = self.items.get_mut(key) {
            if entry.expires_at.is_some() {
                entry.expires_at = None;
                self.increment_changes();
                return true;
            }
        }
        false
    }

    fn keys(&self, pattern: &str) -> Vec<String> {
        if pattern == "*" {
            return self.items.keys().cloned().collect();
        }

        let _regex_pattern = pattern
            .replace("*", ".*")
            .replace("?", ".")
            .replace("[", "\\[")
            .replace("]", "\\]");

        self.items
            .keys()
            .filter(|key| {
                if pattern.contains('*') || pattern.contains('?') {
                    glob_match(pattern, key)
                } else {
                    key.as_str() == pattern
                }
            })
            .cloned()
            .collect()
    }

    fn scan(&self, cursor: u64, pattern: Option<&str>, count: Option<usize>) -> (u64, Vec<String>) {
        let count = count.unwrap_or(10);
        let keys: Vec<String> = self.items.keys().cloned().collect();
        let total = keys.len();
        
        if total == 0 {
            return (0, vec![]);
        }

        let start = cursor as usize;
        if start >= total {
            return (0, vec![]);
        }

        let mut result = Vec::new();
        let mut end = start;

        for (i, key) in keys.iter().enumerate().skip(start) {
            if result.len() >= count {
                break;
            }

            let matches = pattern
                .map(|p| glob_match(p, key))
                .unwrap_or(true);

            if matches {
                result.push(key.clone());
            }
            end = i + 1;
        }

        let next_cursor = if end >= total { 0 } else { end as u64 };
        (next_cursor, result)
    }

    fn rename(&mut self, key: &str, newkey: &str) -> Result<(), String> {
        if let Some(entry) = self.items.remove(key) {
            self.items.insert(newkey.to_string(), entry);
            self.increment_changes();
            Ok(())
        } else {
            Err("ERR no such key".to_string())
        }
    }

    fn renamenx(&mut self, key: &str, newkey: &str) -> bool {
        if self.items.contains_key(newkey) {
            return false;
        }
        self.rename(key, newkey).is_ok()
    }

    fn dbsize(&self) -> usize {
        self.items.len()
    }

    fn flushdb(&mut self) {
        self.items.clear();
        self.increment_changes();
    }

    fn randomkey(&self) -> Option<String> {
        let mut rng = rand::thread_rng();
        self.items.keys().choose(&mut rng).cloned()
    }

    fn copy(&mut self, src: &str, dst: &str, replace: bool) -> bool {
        if !replace && self.items.contains_key(dst) {
            return false;
        }

        if let Some(entry) = self.items.get(src) {
            let new_entry = Entry {
                value: entry.value.clone(),
                expires_at: entry.expires_at,
            };
            self.items.insert(dst.to_string(), new_entry);
            self.increment_changes();
            true
        } else {
            false
        }
    }

    fn unlink(&mut self, keys: Vec<&str>) -> usize {
        let mut count = 0;
        for key in keys {
            if self.items.remove(key).is_some() {
                count += 1;
            }
        }
        if count > 0 {
            self.increment_changes();
        }
        count
    }

    fn touch(&mut self, keys: Vec<&str>) -> usize {
        keys.iter().filter(|k| self.items.contains_key(&k.to_string())).count()
    }
}

/// Simple glob pattern matching
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut pattern_chars = pattern.chars().peekable();
    let mut text_chars = text.chars().peekable();

    while pattern_chars.peek().is_some() || text_chars.peek().is_some() {
        match pattern_chars.peek() {
            Some('*') => {
                pattern_chars.next();
                if pattern_chars.peek().is_none() {
                    return true;
                }
                while text_chars.peek().is_some() {
                    let remaining_pattern: String = pattern_chars.clone().collect();
                    let remaining_text: String = text_chars.clone().collect();
                    if glob_match(&remaining_pattern, &remaining_text) {
                        return true;
                    }
                    text_chars.next();
                }
                return false;
            }
            Some('?') => {
                pattern_chars.next();
                if text_chars.next().is_none() {
                    return false;
                }
            }
            Some(pc) => {
                if Some(*pc) != text_chars.next() {
                    return false;
                }
                pattern_chars.next();
            }
            None => {
                return text_chars.peek().is_none();
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("hello*", "hello world"));
        assert!(glob_match("*world", "hello world"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("foo", "bar"));
    }
}
