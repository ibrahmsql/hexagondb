//! Hash operations.
//!
//! Operations for the hash data type (field->value mapping).

use crate::db::core::DB;
use crate::db::ops::generic::GenericOps;
use crate::db::types::{DataType, Entry};
use std::collections::HashMap;

/// Hash operations trait
pub trait HashOps {
    /// Set hash field value
    fn hset(&mut self, key: String, field: String, value: String) -> Result<usize, String>;
    
    /// Set multiple hash fields
    fn hmset(&mut self, key: String, pairs: Vec<(String, String)>) -> Result<(), String>;
    
    /// Set field only if it doesn't exist
    fn hsetnx(&mut self, key: String, field: String, value: String) -> bool;
    
    /// Get hash field value
    fn hget(&mut self, key: String, field: String) -> Result<Option<String>, String>;
    
    /// Get multiple hash field values
    fn hmget(&mut self, key: String, fields: Vec<String>) -> Result<Vec<Option<String>>, String>;
    
    /// Get all fields and values
    fn hgetall(&mut self, key: String) -> Result<Vec<String>, String>;
    
    /// Delete hash field
    fn hdel(&mut self, key: String, field: String) -> Result<usize, String>;
    
    /// Delete multiple hash fields
    fn hdel_multi(&mut self, key: String, fields: Vec<String>) -> Result<usize, String>;
    
    /// Check if field exists
    fn hexists(&mut self, key: String, field: String) -> bool;
    
    /// Get number of fields
    fn hlen(&mut self, key: String) -> usize;
    
    /// Get all field names
    fn hkeys(&mut self, key: String) -> Vec<String>;
    
    /// Get all values
    fn hvals(&mut self, key: String) -> Vec<String>;
    
    /// Increment field by integer
    fn hincrby(&mut self, key: String, field: String, delta: i64) -> Result<i64, String>;
    
    /// Increment field by float
    fn hincrbyfloat(&mut self, key: String, field: String, delta: f64) -> Result<f64, String>;
    
    /// Get field string length
    fn hstrlen(&mut self, key: String, field: String) -> usize;
    
    /// Scan hash fields
    fn hscan(&self, key: &str, cursor: u64, pattern: Option<&str>, count: Option<usize>) -> (u64, Vec<(String, String)>);
}

impl HashOps for DB {
    fn hset(&mut self, key: String, field: String, value: String) -> Result<usize, String> {
        self.check_expiration(&key);

        let entry = self.items.entry(key).or_insert_with(|| Entry {
            value: DataType::Hash(HashMap::new()),
            expires_at: None,
        });

        match &mut entry.value {
            DataType::Hash(hash) => {
                let is_new = !hash.contains_key(&field);
                hash.insert(field, value);
                self.increment_changes();
                Ok(if is_new { 1 } else { 0 })
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    fn hmset(&mut self, key: String, pairs: Vec<(String, String)>) -> Result<(), String> {
        for (field, value) in pairs {
            self.hset(key.clone(), field, value)?;
        }
        Ok(())
    }

    fn hsetnx(&mut self, key: String, field: String, value: String) -> bool {
        self.check_expiration(&key);

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Hash(hash) = &entry.value {
                if hash.contains_key(&field) {
                    return false;
                }
            }
        }
        
        self.hset(key, field, value).is_ok()
    }

    fn hget(&mut self, key: String, field: String) -> Result<Option<String>, String> {
        if !self.check_expiration(&key) {
            return Ok(None);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::Hash(hash) => Ok(hash.get(&field).cloned()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(None)
        }
    }

    fn hmget(&mut self, key: String, fields: Vec<String>) -> Result<Vec<Option<String>>, String> {
        if !self.check_expiration(&key) {
            return Ok(vec![None; fields.len()]);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::Hash(hash) => {
                    Ok(fields.iter().map(|f| hash.get(f).cloned()).collect())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(vec![None; fields.len()])
        }
    }

    fn hgetall(&mut self, key: String) -> Result<Vec<String>, String> {
        if !self.check_expiration(&key) {
            return Ok(vec![]);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::Hash(hash) => {
                    let mut result = Vec::with_capacity(hash.len() * 2);
                    for (field, value) in hash {
                        result.push(field.clone());
                        result.push(value.clone());
                    }
                    Ok(result)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(vec![])
        }
    }

    fn hdel(&mut self, key: String, field: String) -> Result<usize, String> {
        if !self.check_expiration(&key) {
            return Ok(0);
        }

        if let Some(entry) = self.items.get_mut(&key) {
            match &mut entry.value {
                DataType::Hash(hash) => {
                    if hash.remove(&field).is_some() {
                        self.increment_changes();
                        Ok(1)
                    } else {
                        Ok(0)
                    }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(0)
        }
    }

    fn hdel_multi(&mut self, key: String, fields: Vec<String>) -> Result<usize, String> {
        let mut count = 0;
        for field in fields {
            count += self.hdel(key.clone(), field)?;
        }
        Ok(count)
    }

    fn hexists(&mut self, key: String, field: String) -> bool {
        if !self.check_expiration(&key) {
            return false;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Hash(hash) = &entry.value {
                return hash.contains_key(&field);
            }
        }
        false
    }

    fn hlen(&mut self, key: String) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Hash(hash) = &entry.value {
                return hash.len();
            }
        }
        0
    }

    fn hkeys(&mut self, key: String) -> Vec<String> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Hash(hash) = &entry.value {
                return hash.keys().cloned().collect();
            }
        }
        vec![]
    }

    fn hvals(&mut self, key: String) -> Vec<String> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Hash(hash) = &entry.value {
                return hash.values().cloned().collect();
            }
        }
        vec![]
    }

    fn hincrby(&mut self, key: String, field: String, delta: i64) -> Result<i64, String> {
        self.check_expiration(&key);

        let current = self.hget(key.clone(), field.clone())?.unwrap_or_else(|| "0".to_string());
        
        match current.parse::<i64>() {
            Ok(num) => {
                let new_val = num.checked_add(delta)
                    .ok_or_else(|| "ERR increment would overflow".to_string())?;
                self.hset(key, field, new_val.to_string())?;
                Ok(new_val)
            }
            Err(_) => Err("ERR hash value is not an integer".to_string()),
        }
    }

    fn hincrbyfloat(&mut self, key: String, field: String, delta: f64) -> Result<f64, String> {
        self.check_expiration(&key);

        let current = self.hget(key.clone(), field.clone())?.unwrap_or_else(|| "0".to_string());
        
        match current.parse::<f64>() {
            Ok(num) => {
                let new_val = num + delta;
                if new_val.is_nan() || new_val.is_infinite() {
                    return Err("ERR increment would produce NaN or Infinity".to_string());
                }
                self.hset(key, field, format!("{}", new_val))?;
                Ok(new_val)
            }
            Err(_) => Err("ERR hash value is not a float".to_string()),
        }
    }

    fn hstrlen(&mut self, key: String, field: String) -> usize {
        self.hget(key, field).ok().flatten().map(|s| s.len()).unwrap_or(0)
    }

    fn hscan(&self, key: &str, cursor: u64, pattern: Option<&str>, count: Option<usize>) -> (u64, Vec<(String, String)>) {
        let count = count.unwrap_or(10);

        if let Some(entry) = self.items.get(key) {
            if let DataType::Hash(hash) = &entry.value {
                let pairs: Vec<(&String, &String)> = hash.iter().collect();
                let total = pairs.len();

                if total == 0 {
                    return (0, vec![]);
                }

                let start = cursor as usize;
                if start >= total {
                    return (0, vec![]);
                }

                let mut result = Vec::new();
                let mut end = start;

                for (i, (field, value)) in pairs.iter().enumerate().skip(start) {
                    if result.len() >= count {
                        break;
                    }

                    let matches = pattern
                        .map(|p| field.contains(p) || p == "*")
                        .unwrap_or(true);

                    if matches {
                        result.push(((*field).clone(), (*value).clone()));
                    }
                    end = i + 1;
                }

                let next_cursor = if end >= total { 0 } else { end as u64 };
                return (next_cursor, result);
            }
        }
        (0, vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_ops() {
        let mut db = DB::new();
        
        assert_eq!(db.hset("myhash".to_string(), "field1".to_string(), "value1".to_string()).unwrap(), 1);
        assert_eq!(db.hset("myhash".to_string(), "field1".to_string(), "value2".to_string()).unwrap(), 0);
        assert_eq!(db.hget("myhash".to_string(), "field1".to_string()).unwrap(), Some("value2".to_string()));
        assert_eq!(db.hlen("myhash".to_string()), 1);
    }

    #[test]
    fn test_hincrby() {
        let mut db = DB::new();
        
        assert_eq!(db.hincrby("myhash".to_string(), "counter".to_string(), 1).unwrap(), 1);
        assert_eq!(db.hincrby("myhash".to_string(), "counter".to_string(), 5).unwrap(), 6);
        assert_eq!(db.hincrby("myhash".to_string(), "counter".to_string(), -3).unwrap(), 3);
    }
}
