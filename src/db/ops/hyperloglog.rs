//! HyperLogLog operations.
//!
//! Redis-compatible probabilistic cardinality estimation.

use crate::db::core::DB;
use crate::db::ops::generic::GenericOps;
use crate::db::types::{DataType, Entry, HyperLogLogData};
use std::sync::atomic::Ordering;

/// HyperLogLog operations trait
pub trait HyperLogLogOps {
    /// Add elements to HyperLogLog (PFADD)
    fn pfadd(&mut self, key: String, elements: Vec<String>) -> bool;
    
    /// Count unique elements (PFCOUNT)
    fn pfcount(&mut self, keys: Vec<String>) -> usize;
    
    /// Merge multiple HyperLogLogs (PFMERGE)
    fn pfmerge(&mut self, destkey: String, sourcekeys: Vec<String>) -> bool;
}

impl HyperLogLogOps for DB {
    fn pfadd(&mut self, key: String, elements: Vec<String>) -> bool {
        self.check_expiration(&key);

        let entry = self.items.entry(key).or_insert_with(|| Entry {
            value: DataType::HyperLogLog(HyperLogLogData::new()),
            expires_at: None,
        });

        match &mut entry.value {
            DataType::HyperLogLog(hll) => {
                let mut modified = false;
                for element in elements {
                    if hll.add(&element) {
                        modified = true;
                    }
                }
                if modified {
                    self.changes_since_save.fetch_add(1, Ordering::Relaxed);
                }
                modified
            }
            _ => false,
        }
    }

    fn pfcount(&mut self, keys: Vec<String>) -> usize {
        if keys.is_empty() {
            return 0;
        }

        if keys.len() == 1 {
            // Single key - direct count
            let key = &keys[0];
            if !self.check_expiration(key) {
                return 0;
            }

            if let Some(entry) = self.items.get(key) {
                if let DataType::HyperLogLog(hll) = &entry.value {
                    return hll.count();
                }
            }
            return 0;
        }

        // Multiple keys - merge and count
        let mut merged = HyperLogLogData::new();
        
        for key in &keys {
            if !self.check_expiration(key) {
                continue;
            }

            if let Some(entry) = self.items.get(key) {
                if let DataType::HyperLogLog(hll) = &entry.value {
                    merged.merge(hll);
                }
            }
        }

        merged.count()
    }

    fn pfmerge(&mut self, destkey: String, sourcekeys: Vec<String>) -> bool {
        let mut merged = HyperLogLogData::new();

        // First, collect data from all source keys
        for key in &sourcekeys {
            if !self.check_expiration(key) {
                continue;
            }

            if let Some(entry) = self.items.get(key) {
                if let DataType::HyperLogLog(hll) = &entry.value {
                    merged.merge(hll);
                }
            }
        }

        // Store the merged result
        self.items.insert(destkey, Entry {
            value: DataType::HyperLogLog(merged),
            expires_at: None,
        });
        self.changes_since_save.fetch_add(1, Ordering::Relaxed);

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pfadd_pfcount() {
        let mut db = DB::new();
        
        // Add some elements
        assert!(db.pfadd("hll".to_string(), vec!["a".to_string(), "b".to_string(), "c".to_string()]));
        
        // Count should be 3
        let count = db.pfcount(vec!["hll".to_string()]);
        assert_eq!(count, 3);
        
        // Adding same elements should not modify
        assert!(!db.pfadd("hll".to_string(), vec!["a".to_string(), "b".to_string(), "c".to_string()]));
        
        // Adding new element should modify
        assert!(db.pfadd("hll".to_string(), vec!["d".to_string()]));
        assert_eq!(db.pfcount(vec!["hll".to_string()]), 4);
    }

    #[test]
    fn test_pfmerge() {
        let mut db = DB::new();
        
        db.pfadd("hll1".to_string(), vec!["a".to_string(), "b".to_string()]);
        db.pfadd("hll2".to_string(), vec!["c".to_string(), "d".to_string()]);
        
        db.pfmerge("hll3".to_string(), vec!["hll1".to_string(), "hll2".to_string()]);
        
        assert_eq!(db.pfcount(vec!["hll3".to_string()]), 4);
    }

    #[test]
    fn test_pfcount_multiple_keys() {
        let mut db = DB::new();
        
        db.pfadd("hll1".to_string(), vec!["a".to_string(), "b".to_string()]);
        db.pfadd("hll2".to_string(), vec!["b".to_string(), "c".to_string()]); // 'b' is duplicate
        
        // Union count should be 3 (a, b, c)
        let count = db.pfcount(vec!["hll1".to_string(), "hll2".to_string()]);
        assert_eq!(count, 3);
    }
}
