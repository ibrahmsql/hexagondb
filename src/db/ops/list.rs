//! List operations.
//!
//! Operations for the list data type (linked list of strings).

use crate::db::core::DB;
use crate::db::ops::generic::GenericOps;
use crate::db::types::{DataType, Entry};

/// List operations trait
pub trait ListOps {
    /// Push values to the left of a list
    fn lpush(&mut self, key: String, values: Vec<String>) -> Result<usize, String>;
    
    /// Push values to the right of a list
    fn rpush(&mut self, key: String, values: Vec<String>) -> Result<usize, String>;
    
    /// Push to left only if list exists
    fn lpushx(&mut self, key: String, values: Vec<String>) -> usize;
    
    /// Push to right only if list exists
    fn rpushx(&mut self, key: String, values: Vec<String>) -> usize;
    
    /// Pop from left
    fn lpop(&mut self, key: String) -> Result<Option<String>, String>;
    
    /// Pop from right
    fn rpop(&mut self, key: String) -> Result<Option<String>, String>;
    
    /// Pop multiple from left
    fn lpop_count(&mut self, key: String, count: usize) -> Result<Vec<String>, String>;
    
    /// Pop multiple from right
    fn rpop_count(&mut self, key: String, count: usize) -> Result<Vec<String>, String>;
    
    /// Get list length
    fn llen(&mut self, key: String) -> Result<usize, String>;
    
    /// Get range of elements
    fn lrange(&mut self, key: String, start: i64, stop: i64) -> Result<Vec<String>, String>;
    
    /// Get element at index
    fn lindex(&mut self, key: String, index: i64) -> Result<Option<String>, String>;
    
    /// Set element at index
    fn lset(&mut self, key: String, index: i64, value: String) -> Result<(), String>;
    
    /// Insert before or after pivot
    fn linsert(&mut self, key: String, before: bool, pivot: String, value: String) -> Result<i64, String>;
    
    /// Remove count occurrences of element
    fn lrem(&mut self, key: String, count: i64, element: String) -> usize;
    
    /// Trim list to specified range
    fn ltrim(&mut self, key: String, start: i64, stop: i64);
    
    /// Find position of element
    fn lpos(&mut self, key: String, element: String) -> Option<usize>;
    
    /// Move element from one list to another
    fn lmove(&mut self, src: String, dst: String, src_left: bool, dst_left: bool) -> Option<String>;
    
    /// Pop from src, push to dst (RPOPLPUSH)
    fn rpoplpush(&mut self, src: String, dst: String) -> Option<String>;
}

impl ListOps for DB {
    fn lpush(&mut self, key: String, values: Vec<String>) -> Result<usize, String> {
        self.check_expiration(&key);

        // Check existing entry type first
        if let Some(entry) = self.items.get(&key) {
            if !matches!(&entry.value, DataType::List(_)) {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        }

        let entry = self.items.entry(key).or_insert_with(|| Entry {
            value: DataType::List(Vec::new()),
            expires_at: None,
        });

        if let DataType::List(list) = &mut entry.value {
            for value in values.into_iter().rev() {
                list.insert(0, value);
            }
            let len = list.len();
            // Increment after we're done with borrowing list
            let _ = list;
            self.changes_since_save.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(len)
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    fn rpush(&mut self, key: String, values: Vec<String>) -> Result<usize, String> {
        self.check_expiration(&key);

        // Check existing entry type first
        if let Some(entry) = self.items.get(&key) {
            if !matches!(&entry.value, DataType::List(_)) {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        }

        let entry = self.items.entry(key).or_insert_with(|| Entry {
            value: DataType::List(Vec::new()),
            expires_at: None,
        });

        if let DataType::List(list) = &mut entry.value {
            list.extend(values);
            let len = list.len();
            let _ = list;
            self.changes_since_save.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(len)
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    fn lpushx(&mut self, key: String, values: Vec<String>) -> usize {
        if !self.items.contains_key(&key) {
            return 0;
        }
        self.lpush(key, values).unwrap_or(0)
    }

    fn rpushx(&mut self, key: String, values: Vec<String>) -> usize {
        if !self.items.contains_key(&key) {
            return 0;
        }
        self.rpush(key, values).unwrap_or(0)
    }

    fn lpop(&mut self, key: String) -> Result<Option<String>, String> {
        if !self.check_expiration(&key) {
            return Ok(None);
        }

        let result = if let Some(entry) = self.items.get_mut(&key) {
            match &mut entry.value {
                DataType::List(list) => {
                    if list.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(list.remove(0)))
                    }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(None)
        };
        
        if result.as_ref().map(|r| r.is_some()).unwrap_or(false) {
            self.changes_since_save.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    fn rpop(&mut self, key: String) -> Result<Option<String>, String> {
        if !self.check_expiration(&key) {
            return Ok(None);
        }

        let result = if let Some(entry) = self.items.get_mut(&key) {
            match &mut entry.value {
                DataType::List(list) => Ok(list.pop()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(None)
        };
        
        if result.as_ref().map(|r| r.is_some()).unwrap_or(false) {
            self.changes_since_save.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    fn lpop_count(&mut self, key: String, count: usize) -> Result<Vec<String>, String> {
        let mut result = Vec::new();
        for _ in 0..count {
            match self.lpop(key.clone())? {
                Some(val) => result.push(val),
                None => break,
            }
        }
        Ok(result)
    }

    fn rpop_count(&mut self, key: String, count: usize) -> Result<Vec<String>, String> {
        let mut result = Vec::new();
        for _ in 0..count {
            match self.rpop(key.clone())? {
                Some(val) => result.push(val),
                None => break,
            }
        }
        Ok(result)
    }

    fn llen(&mut self, key: String) -> Result<usize, String> {
        if !self.check_expiration(&key) {
            return Ok(0);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::List(list) => Ok(list.len()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(0)
        }
    }

    fn lrange(&mut self, key: String, start: i64, stop: i64) -> Result<Vec<String>, String> {
        if !self.check_expiration(&key) {
            return Ok(vec![]);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::List(list) => {
                    let len = list.len() as i64;
                    let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                    let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;

                    if start > stop || start >= list.len() {
                        return Ok(vec![]);
                    }

                    Ok(list[start..=stop.min(list.len() - 1)].to_vec())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(vec![])
        }
    }

    fn lindex(&mut self, key: String, index: i64) -> Result<Option<String>, String> {
        if !self.check_expiration(&key) {
            return Ok(None);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::List(list) => {
                    let len = list.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    
                    if idx < 0 || idx >= len {
                        Ok(None)
                    } else {
                        Ok(Some(list[idx as usize].clone()))
                    }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(None)
        }
    }

    fn lset(&mut self, key: String, index: i64, value: String) -> Result<(), String> {
        if !self.check_expiration(&key) {
            return Err("ERR no such key".to_string());
        }

        if let Some(entry) = self.items.get_mut(&key) {
            match &mut entry.value {
                DataType::List(list) => {
                    let len = list.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    
                    if idx < 0 || idx >= len {
                        Err("ERR index out of range".to_string())
                    } else {
                        list[idx as usize] = value;
                        self.changes_since_save.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Ok(())
                    }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Err("ERR no such key".to_string())
        }
    }

    fn linsert(&mut self, key: String, before: bool, pivot: String, value: String) -> Result<i64, String> {
        if !self.check_expiration(&key) {
            return Ok(-1);
        }

        let result = if let Some(entry) = self.items.get_mut(&key) {
            match &mut entry.value {
                DataType::List(list) => {
                    if let Some(pos) = list.iter().position(|x| x == &pivot) {
                        let insert_pos = if before { pos } else { pos + 1 };
                        list.insert(insert_pos, value);
                        Ok(list.len() as i64)
                    } else {
                        Ok(-1)
                    }
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(0)
        };
        
        if result.as_ref().map(|&r| r > 0).unwrap_or(false) {
            self.changes_since_save.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        result
    }

    fn lrem(&mut self, key: String, count: i64, element: String) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        let removed = if let Some(entry) = self.items.get_mut(&key) {
            if let DataType::List(list) = &mut entry.value {
                let abs_count = count.unsigned_abs() as usize;
                let mut removed = 0;

                if count == 0 {
                    let original_len = list.len();
                    list.retain(|x| x != &element);
                    removed = original_len - list.len();
                } else if count > 0 {
                    let mut i = 0;
                    while i < list.len() && removed < abs_count {
                        if list[i] == element {
                            list.remove(i);
                            removed += 1;
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    let mut i = list.len();
                    while i > 0 && removed < abs_count {
                        i -= 1;
                        if list[i] == element {
                            list.remove(i);
                            removed += 1;
                        }
                    }
                }
                removed
            } else {
                0
            }
        } else {
            0
        };

        if removed > 0 {
            self.changes_since_save.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        removed
    }

    fn ltrim(&mut self, key: String, start: i64, stop: i64) {
        if !self.check_expiration(&key) {
            return;
        }

        if let Some(entry) = self.items.get_mut(&key) {
            if let DataType::List(list) = &mut entry.value {
                let len = list.len() as i64;
                let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;

                if start > stop || start >= list.len() {
                    list.clear();
                } else {
                    *list = list[start..=stop.min(list.len() - 1)].to_vec();
                }
            }
        }
        self.changes_since_save.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn lpos(&mut self, key: String, element: String) -> Option<usize> {
        if !self.check_expiration(&key) {
            return None;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::List(list) = &entry.value {
                return list.iter().position(|x| x == &element);
            }
        }
        None
    }

    fn lmove(&mut self, src: String, dst: String, src_left: bool, dst_left: bool) -> Option<String> {
        let value = if src_left {
            self.lpop(src.clone()).ok().flatten()?
        } else {
            self.rpop(src.clone()).ok().flatten()?
        };

        if dst_left {
            let _ = self.lpush(dst, vec![value.clone()]);
        } else {
            let _ = self.rpush(dst, vec![value.clone()]);
        }

        Some(value)
    }

    fn rpoplpush(&mut self, src: String, dst: String) -> Option<String> {
        self.lmove(src, dst, false, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_ops() {
        let mut db = DB::new();
        
        assert_eq!(db.rpush("mylist".to_string(), vec!["a".to_string(), "b".to_string(), "c".to_string()]).unwrap(), 3);
        assert_eq!(db.lrange("mylist".to_string(), 0, -1).unwrap(), vec!["a", "b", "c"]);
        assert_eq!(db.lpop("mylist".to_string()).unwrap(), Some("a".to_string()));
        assert_eq!(db.rpop("mylist".to_string()).unwrap(), Some("c".to_string()));
        assert_eq!(db.llen("mylist".to_string()).unwrap(), 1);
    }

    #[test]
    fn test_lindex_lset() {
        let mut db = DB::new();
        db.rpush("mylist".to_string(), vec!["a".to_string(), "b".to_string(), "c".to_string()]).unwrap();
        
        assert_eq!(db.lindex("mylist".to_string(), 0).unwrap(), Some("a".to_string()));
        assert_eq!(db.lindex("mylist".to_string(), -1).unwrap(), Some("c".to_string()));
        
        db.lset("mylist".to_string(), 1, "B".to_string()).unwrap();
        assert_eq!(db.lindex("mylist".to_string(), 1).unwrap(), Some("B".to_string()));
    }
}
