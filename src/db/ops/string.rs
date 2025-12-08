//! String operations.
//!
//! Basic key-value operations for string data type.

use crate::db::core::DB;
use crate::db::ops::generic::GenericOps;
use crate::db::types::{DataType, Entry};
use std::sync::atomic::Ordering;

/// String operations trait
pub trait StringOps {
    /// Get the value of a key
    fn get(&mut self, key: String) -> Result<Option<String>, String>;
    
    /// Set the value of a key
    fn set(&mut self, key: String, value: String);
    
    /// Set key with expiration in seconds
    fn setex(&mut self, key: String, seconds: u64, value: String);
    
    /// Set key with expiration in milliseconds
    fn psetex(&mut self, key: String, milliseconds: u64, value: String);
    
    /// Set key only if it doesn't exist
    fn setnx(&mut self, key: String, value: String) -> bool;
    
    /// Get old value and set new value
    fn getset(&mut self, key: String, value: String) -> Result<Option<String>, String>;
    
    /// Get multiple values
    fn mget(&mut self, keys: Vec<String>) -> Vec<Option<String>>;
    
    /// Set multiple values
    fn mset(&mut self, pairs: Vec<(String, String)>);
    
    /// Set multiple only if none exist
    fn msetnx(&mut self, pairs: Vec<(String, String)>) -> bool;
    
    /// Append to a string
    fn append(&mut self, key: String, value: String) -> usize;
    
    /// Get string length
    fn strlen(&mut self, key: String) -> usize;
    
    /// Get substring
    fn getrange(&mut self, key: String, start: i64, end: i64) -> String;
    
    /// Set substring
    fn setrange(&mut self, key: String, offset: usize, value: String) -> usize;
    
    /// Increment integer value
    fn incr(&mut self, key: String) -> Result<i64, String>;
    
    /// Decrement integer value
    fn decr(&mut self, key: String) -> Result<i64, String>;
    
    /// Increment by amount
    fn incrby(&mut self, key: String, delta: i64) -> Result<i64, String>;
    
    /// Decrement by amount
    fn decrby(&mut self, key: String, delta: i64) -> Result<i64, String>;
    
    /// Increment by float
    fn incrbyfloat(&mut self, key: String, delta: f64) -> Result<f64, String>;
}

impl StringOps for DB {
    fn get(&mut self, key: String) -> Result<Option<String>, String> {
        if !self.check_expiration(&key) {
            return Ok(None);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::String(s) => Ok(Some(s.clone())),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(None)
        }
    }

    fn set(&mut self, key: String, value: String) {
        self.items.insert(
            key,
            Entry {
                value: DataType::String(value),
                expires_at: None,
            },
        );
        self.changes_since_save.fetch_add(1, Ordering::Relaxed);
    }

    fn setex(&mut self, key: String, seconds: u64, value: String) {
        let expires_at = Some(std::time::Instant::now() + std::time::Duration::from_secs(seconds));
        self.items.insert(
            key,
            Entry {
                value: DataType::String(value),
                expires_at,
            },
        );
        self.changes_since_save.fetch_add(1, Ordering::Relaxed);
    }

    fn psetex(&mut self, key: String, milliseconds: u64, value: String) {
        let expires_at = Some(std::time::Instant::now() + std::time::Duration::from_millis(milliseconds));
        self.items.insert(
            key,
            Entry {
                value: DataType::String(value),
                expires_at,
            },
        );
        self.changes_since_save.fetch_add(1, Ordering::Relaxed);
    }

    fn setnx(&mut self, key: String, value: String) -> bool {
        if self.items.contains_key(&key) {
            false
        } else {
            self.set(key, value);
            true
        }
    }

    fn getset(&mut self, key: String, value: String) -> Result<Option<String>, String> {
        let old = self.get(key.clone())?;
        self.set(key, value);
        Ok(old)
    }

    fn mget(&mut self, keys: Vec<String>) -> Vec<Option<String>> {
        keys.into_iter()
            .map(|key| self.get(key).ok().flatten())
            .collect()
    }

    fn mset(&mut self, pairs: Vec<(String, String)>) {
        for (key, value) in pairs {
            self.set(key, value);
        }
    }

    fn msetnx(&mut self, pairs: Vec<(String, String)>) -> bool {
        // Check if any key exists
        for (key, _) in &pairs {
            if self.items.contains_key(key) {
                return false;
            }
        }
        // Set all
        self.mset(pairs);
        true
    }

    fn append(&mut self, key: String, value: String) -> usize {
        let result = if let Some(entry) = self.items.get_mut(&key) {
            if let DataType::String(ref mut s) = entry.value {
                s.push_str(&value);
                Some(s.len())
            } else {
                None
            }
        } else {
            None
        };
        
        if let Some(len) = result {
            self.changes_since_save.fetch_add(1, Ordering::Relaxed);
            return len;
        }
        
        // Key doesn't exist, create it
        let len = value.len();
        self.set(key, value);
        len
    }

    fn strlen(&mut self, key: String) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::String(s) = &entry.value {
                return s.len();
            }
        }
        0
    }

    fn getrange(&mut self, key: String, start: i64, end: i64) -> String {
        if !self.check_expiration(&key) {
            return String::new();
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::String(s) = &entry.value {
                let len = s.len() as i64;
                let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                let end = if end < 0 { (len + end).max(0) } else { end.min(len - 1) } as usize;

                if start > end {
                    return String::new();
                }

                return s.chars().skip(start).take(end - start + 1).collect();
            }
        }
        String::new()
    }

    fn setrange(&mut self, key: String, offset: usize, value: String) -> usize {
        let result = if let Some(entry) = self.items.get_mut(&key) {
            if let DataType::String(ref mut s) = entry.value {
                // Pad with null bytes if needed
                while s.len() < offset {
                    s.push('\0');
                }
                
                // Extend with null bytes if new value goes beyond current length
                let new_len = offset + value.len();
                while s.len() < new_len {
                    s.push('\0');
                }

                // Convert to bytes for replacement
                let mut bytes: Vec<u8> = s.as_bytes().to_vec();
                for (i, b) in value.bytes().enumerate() {
                    if offset + i < bytes.len() {
                        bytes[offset + i] = b;
                    }
                }
                *s = String::from_utf8_lossy(&bytes).to_string();
                Some(s.len())
            } else {
                None
            }
        } else {
            None
        };
        
        if let Some(len) = result {
            self.changes_since_save.fetch_add(1, Ordering::Relaxed);
            return len;
        }

        // Key doesn't exist, create padded string
        let mut new_value = String::new();
        for _ in 0..offset {
            new_value.push('\0');
        }
        new_value.push_str(&value);
        let len = new_value.len();
        self.set(key, new_value);
        len
    }

    fn incr(&mut self, key: String) -> Result<i64, String> {
        self.incrby(key, 1)
    }

    fn decr(&mut self, key: String) -> Result<i64, String> {
        self.incrby(key, -1)
    }

    fn incrby(&mut self, key: String, delta: i64) -> Result<i64, String> {
        let _ = self.check_expiration(&key);

        let current_val = if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::String(s) => s.clone(),
                _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            "0".to_string()
        };

        match current_val.parse::<i64>() {
            Ok(num) => {
                let new_val = num.checked_add(delta)
                    .ok_or_else(|| "ERR increment or decrement would overflow".to_string())?;
                
                let expires_at = self.items.get(&key).and_then(|e| e.expires_at);
                self.items.insert(
                    key,
                    Entry {
                        value: DataType::String(new_val.to_string()),
                        expires_at,
                    },
                );
                self.changes_since_save.fetch_add(1, Ordering::Relaxed);
                Ok(new_val)
            }
            Err(_) => Err("ERR value is not an integer or out of range".to_string()),
        }
    }

    fn decrby(&mut self, key: String, delta: i64) -> Result<i64, String> {
        self.incrby(key, -delta)
    }

    fn incrbyfloat(&mut self, key: String, delta: f64) -> Result<f64, String> {
        let _ = self.check_expiration(&key);

        let current_val = if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::String(s) => s.clone(),
                _ => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            "0".to_string()
        };

        match current_val.parse::<f64>() {
            Ok(num) => {
                let new_val = num + delta;
                if new_val.is_nan() || new_val.is_infinite() {
                    return Err("ERR increment would produce NaN or Infinity".to_string());
                }

                let expires_at = self.items.get(&key).and_then(|e| e.expires_at);
                self.items.insert(
                    key,
                    Entry {
                        value: DataType::String(format!("{}", new_val)),
                        expires_at,
                    },
                );
                self.changes_since_save.fetch_add(1, Ordering::Relaxed);
                Ok(new_val)
            }
            Err(_) => Err("ERR value is not a valid float".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_ops() {
        let mut db = DB::new();
        
        db.set("foo".to_string(), "bar".to_string());
        assert_eq!(db.get("foo".to_string()).unwrap(), Some("bar".to_string()));
        
        assert!(db.setnx("foo".to_string(), "baz".to_string()) == false);
        assert!(db.setnx("new".to_string(), "value".to_string()) == true);
    }

    #[test]
    fn test_incr_decr() {
        let mut db = DB::new();
        
        assert_eq!(db.incr("counter".to_string()).unwrap(), 1);
        assert_eq!(db.incr("counter".to_string()).unwrap(), 2);
        assert_eq!(db.decr("counter".to_string()).unwrap(), 1);
        assert_eq!(db.incrby("counter".to_string(), 10).unwrap(), 11);
    }

    #[test]
    fn test_append() {
        let mut db = DB::new();
        
        assert_eq!(db.append("key".to_string(), "Hello".to_string()), 5);
        assert_eq!(db.append("key".to_string(), " World".to_string()), 11);
        assert_eq!(db.get("key".to_string()).unwrap(), Some("Hello World".to_string()));
    }
}
