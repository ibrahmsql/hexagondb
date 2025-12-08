//! Set operations.
//!
//! Operations for the set data type (unordered unique strings).

use crate::db::core::DB;
use crate::db::ops::generic::GenericOps;
use crate::db::types::{DataType, Entry};
use rand::seq::IteratorRandom;
use std::collections::HashSet;

/// Set operations trait
pub trait SetOps {
    /// Add members to set
    fn sadd(&mut self, key: String, members: Vec<String>) -> Result<usize, String>;
    
    /// Remove a member from set
    fn srem(&mut self, key: String, member: String) -> Result<usize, String>;
    
    /// Remove multiple members from set
    fn srem_multi(&mut self, key: String, members: Vec<String>) -> Result<usize, String>;
    
    /// Get all members
    fn smembers(&mut self, key: String) -> Result<Vec<String>, String>;
    
    /// Check if member exists
    fn sismember(&mut self, key: String, member: String) -> Result<bool, String>;
    
    /// Check multiple members
    fn smismember(&mut self, key: String, members: Vec<String>) -> Result<Vec<bool>, String>;
    
    /// Get set cardinality
    fn scard(&mut self, key: String) -> Result<usize, String>;
    
    /// Get random members
    fn srandmember(&mut self, key: String, count: Option<i64>) -> Vec<String>;
    
    /// Remove and return random members
    fn spop(&mut self, key: String, count: Option<usize>) -> Vec<String>;
    
    /// Move member from one set to another
    fn smove(&mut self, src: String, dst: String, member: String) -> bool;
    
    /// Union of sets
    fn sunion(&mut self, keys: Vec<String>) -> HashSet<String>;
    
    /// Store union result
    fn sunionstore(&mut self, dst: String, keys: Vec<String>) -> usize;
    
    /// Intersection of sets
    fn sinter(&mut self, keys: Vec<String>) -> HashSet<String>;
    
    /// Store intersection result
    fn sinterstore(&mut self, dst: String, keys: Vec<String>) -> usize;
    
    /// Difference of sets
    fn sdiff(&mut self, keys: Vec<String>) -> HashSet<String>;
    
    /// Store difference result
    fn sdiffstore(&mut self, dst: String, keys: Vec<String>) -> usize;
    
    /// Scan set members with cursor
    fn sscan(&mut self, key: String, cursor: u64, pattern: Option<&str>, count: Option<usize>) -> (u64, Vec<String>);
}

impl SetOps for DB {
    fn sadd(&mut self, key: String, members: Vec<String>) -> Result<usize, String> {
        self.check_expiration(&key);

        let entry = self.items.entry(key).or_insert_with(|| Entry {
            value: DataType::Set(HashSet::new()),
            expires_at: None,
        });

        match &mut entry.value {
            DataType::Set(set) => {
                let mut added = 0;
                for member in members {
                    if set.insert(member) {
                        added += 1;
                    }
                }
                if added > 0 {
                    self.increment_changes();
                }
                Ok(added)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    fn srem(&mut self, key: String, member: String) -> Result<usize, String> {
        if !self.check_expiration(&key) {
            return Ok(0);
        }

        if let Some(entry) = self.items.get_mut(&key) {
            match &mut entry.value {
                DataType::Set(set) => {
                    if set.remove(&member) {
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

    fn srem_multi(&mut self, key: String, members: Vec<String>) -> Result<usize, String> {
        let mut count = 0;
        for member in members {
            count += self.srem(key.clone(), member)?;
        }
        Ok(count)
    }

    fn smembers(&mut self, key: String) -> Result<Vec<String>, String> {
        if !self.check_expiration(&key) {
            return Ok(vec![]);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::Set(set) => Ok(set.iter().cloned().collect()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(vec![])
        }
    }

    fn sismember(&mut self, key: String, member: String) -> Result<bool, String> {
        if !self.check_expiration(&key) {
            return Ok(false);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::Set(set) => Ok(set.contains(&member)),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(false)
        }
    }

    fn smismember(&mut self, key: String, members: Vec<String>) -> Result<Vec<bool>, String> {
        if !self.check_expiration(&key) {
            return Ok(vec![false; members.len()]);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::Set(set) => {
                    Ok(members.iter().map(|m| set.contains(m)).collect())
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(vec![false; members.len()])
        }
    }

    fn scard(&mut self, key: String) -> Result<usize, String> {
        if !self.check_expiration(&key) {
            return Ok(0);
        }

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::Set(set) => Ok(set.len()),
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(0)
        }
    }

    fn srandmember(&mut self, key: String, count: Option<i64>) -> Vec<String> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Set(set) = &entry.value {
                let mut rng = rand::thread_rng();
                
                match count {
                    None => {
                        // Return single random element
                        set.iter().choose(&mut rng).cloned().into_iter().collect()
                    }
                    Some(n) if n > 0 => {
                        // Return n distinct elements
                        let n = n as usize;
                        set.iter().choose_multiple(&mut rng, n.min(set.len()))
                            .into_iter().cloned().collect()
                    }
                    Some(n) => {
                        // Return |n| elements with possible repeats
                        let n = n.unsigned_abs() as usize;
                        let mut result = Vec::with_capacity(n);
                        let members: Vec<_> = set.iter().collect();
                        if !members.is_empty() {
                            for _ in 0..n {
                                if let Some(m) = members.iter().choose(&mut rng) {
                                    result.push((*m).clone());
                                }
                            }
                        }
                        result
                    }
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }

    fn spop(&mut self, key: String, count: Option<usize>) -> Vec<String> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get_mut(&key) {
            if let DataType::Set(set) = &mut entry.value {
                let count = count.unwrap_or(1).min(set.len());
                let mut result = Vec::with_capacity(count);
                let mut rng = rand::thread_rng();

                for _ in 0..count {
                    let members: Vec<_> = set.iter().cloned().collect();
                    if let Some(member) = members.iter().choose(&mut rng) {
                        result.push(member.clone());
                        set.remove(member);
                    }
                }

                if !result.is_empty() {
                    self.increment_changes();
                }
                return result;
            }
        }
        vec![]
    }

    fn smove(&mut self, src: String, dst: String, member: String) -> bool {
        if self.srem(src, member.clone()).unwrap_or(0) == 0 {
            return false;
        }
        self.sadd(dst, vec![member]).is_ok()
    }

    fn sunion(&mut self, keys: Vec<String>) -> HashSet<String> {
        let mut result = HashSet::new();
        
        for key in keys {
            if self.check_expiration(&key) {
                if let Some(entry) = self.items.get(&key) {
                    if let DataType::Set(set) = &entry.value {
                        result.extend(set.iter().cloned());
                    }
                }
            }
        }
        
        result
    }

    fn sunionstore(&mut self, dst: String, keys: Vec<String>) -> usize {
        let result = self.sunion(keys);
        let len = result.len();
        
        self.items.insert(dst, Entry {
            value: DataType::Set(result),
            expires_at: None,
        });
        self.increment_changes();
        
        len
    }

    fn sinter(&mut self, keys: Vec<String>) -> HashSet<String> {
        if keys.is_empty() {
            return HashSet::new();
        }

        let mut iter = keys.into_iter();
        let first_key = iter.next().unwrap();
        
        self.check_expiration(&first_key);
        
        let mut result: HashSet<String> = if let Some(entry) = self.items.get(&first_key) {
            if let DataType::Set(set) = &entry.value {
                set.clone()
            } else {
                return HashSet::new();
            }
        } else {
            return HashSet::new();
        };

        for key in iter {
            if self.check_expiration(&key) {
                if let Some(entry) = self.items.get(&key) {
                    if let DataType::Set(set) = &entry.value {
                        result = result.intersection(set).cloned().collect();
                    } else {
                        return HashSet::new();
                    }
                } else {
                    return HashSet::new();
                }
            } else {
                return HashSet::new();
            }
        }
        
        result
    }

    fn sinterstore(&mut self, dst: String, keys: Vec<String>) -> usize {
        let result = self.sinter(keys);
        let len = result.len();
        
        self.items.insert(dst, Entry {
            value: DataType::Set(result),
            expires_at: None,
        });
        self.increment_changes();
        
        len
    }

    fn sdiff(&mut self, keys: Vec<String>) -> HashSet<String> {
        if keys.is_empty() {
            return HashSet::new();
        }

        let mut iter = keys.into_iter();
        let first_key = iter.next().unwrap();
        
        self.check_expiration(&first_key);
        
        let mut result: HashSet<String> = if let Some(entry) = self.items.get(&first_key) {
            if let DataType::Set(set) = &entry.value {
                set.clone()
            } else {
                return HashSet::new();
            }
        } else {
            return HashSet::new();
        };

        for key in iter {
            if self.check_expiration(&key) {
                if let Some(entry) = self.items.get(&key) {
                    if let DataType::Set(set) = &entry.value {
                        result = result.difference(set).cloned().collect();
                    }
                }
            }
        }
        
        result
    }

    fn sdiffstore(&mut self, dst: String, keys: Vec<String>) -> usize {
        let result = self.sdiff(keys);
        let len = result.len();
        
        self.items.insert(dst, Entry {
            value: DataType::Set(result),
            expires_at: None,
        });
        self.increment_changes();
        
        len
    }

    fn sscan(&mut self, key: String, cursor: u64, pattern: Option<&str>, count: Option<usize>) -> (u64, Vec<String>) {
        if !self.check_expiration(&key) {
            return (0, vec![]);
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::Set(set) = &entry.value {
                let count = count.unwrap_or(10);
                let members: Vec<String> = set.iter().cloned().collect();
                let total = members.len();

                if total == 0 {
                    return (0, vec![]);
                }

                let start = cursor as usize;
                if start >= total {
                    return (0, vec![]);
                }

                let mut result = Vec::new();
                let mut end = start;

                for (i, member) in members.iter().enumerate().skip(start) {
                    if result.len() >= count {
                        break;
                    }

                    let matches = pattern
                        .map(|p| glob_match(p, member))
                        .unwrap_or(true);

                    if matches {
                        result.push(member.clone());
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

/// Simple glob pattern matching for SSCAN
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    
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
    fn test_set_ops() {
        let mut db = DB::new();
        
        assert_eq!(db.sadd("myset".to_string(), vec!["a".to_string(), "b".to_string(), "c".to_string()]).unwrap(), 3);
        assert_eq!(db.sadd("myset".to_string(), vec!["a".to_string()]).unwrap(), 0);
        assert_eq!(db.scard("myset".to_string()).unwrap(), 3);
        assert!(db.sismember("myset".to_string(), "a".to_string()).unwrap());
        assert!(!db.sismember("myset".to_string(), "d".to_string()).unwrap());
    }

    #[test]
    fn test_set_operations() {
        let mut db = DB::new();
        
        db.sadd("set1".to_string(), vec!["a".to_string(), "b".to_string(), "c".to_string()]).unwrap();
        db.sadd("set2".to_string(), vec!["b".to_string(), "c".to_string(), "d".to_string()]).unwrap();
        
        let union = db.sunion(vec!["set1".to_string(), "set2".to_string()]);
        assert_eq!(union.len(), 4);
        
        let inter = db.sinter(vec!["set1".to_string(), "set2".to_string()]);
        assert_eq!(inter.len(), 2);
        assert!(inter.contains("b"));
        assert!(inter.contains("c"));
        
        let diff = db.sdiff(vec!["set1".to_string(), "set2".to_string()]);
        assert_eq!(diff.len(), 1);
        assert!(diff.contains("a"));
    }
}
