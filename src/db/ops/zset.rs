//! Sorted Set (ZSet) operations.
//!
//! Operations for the sorted set data type (score-ordered unique strings).

use crate::db::core::DB;
use crate::db::ops::generic::GenericOps;
use crate::db::types::{DataType, Entry, ZSetData};

/// Sorted Set operations trait
pub trait ZSetOps {
    /// Add members with scores
    fn zadd(&mut self, key: String, members: Vec<(f64, String)>) -> Result<usize, String>;
    
    /// Add members with NX option (only if not exists)
    fn zadd_nx(&mut self, key: String, members: Vec<(f64, String)>) -> Result<usize, String>;
    
    /// Add members with XX option (only if exists)
    fn zadd_xx(&mut self, key: String, members: Vec<(f64, String)>) -> Result<usize, String>;
    
    /// Remove members
    fn zrem(&mut self, key: String, members: Vec<String>) -> Result<usize, String>;
    
    /// Get score of member
    fn zscore(&mut self, key: String, member: String) -> Option<f64>;
    
    /// Get rank of member (0-indexed, ascending)
    fn zrank(&mut self, key: String, member: String) -> Option<usize>;
    
    /// Get reverse rank of member (0-indexed, descending)
    fn zrevrank(&mut self, key: String, member: String) -> Option<usize>;
    
    /// Get range by rank (ascending)
    fn zrange(&mut self, key: String, start: i64, stop: i64, withscores: bool) -> Vec<(String, Option<f64>)>;
    
    /// Get range by rank (descending)
    fn zrevrange(&mut self, key: String, start: i64, stop: i64, withscores: bool) -> Vec<(String, Option<f64>)>;
    
    /// Get range by score
    fn zrangebyscore(&mut self, key: String, min: f64, max: f64, withscores: bool, offset: Option<usize>, count: Option<usize>) -> Vec<(String, f64)>;
    
    /// Get reverse range by score
    fn zrevrangebyscore(&mut self, key: String, max: f64, min: f64, withscores: bool, offset: Option<usize>, count: Option<usize>) -> Vec<(String, f64)>;
    
    /// Get cardinality
    fn zcard(&mut self, key: String) -> usize;
    
    /// Count members in score range
    fn zcount(&mut self, key: String, min: f64, max: f64) -> usize;
    
    /// Increment score of member
    fn zincrby(&mut self, key: String, increment: f64, member: String) -> Result<f64, String>;
    
    /// Remove members by rank range
    fn zremrangebyrank(&mut self, key: String, start: i64, stop: i64) -> usize;
    
    /// Remove members by score range
    fn zremrangebyscore(&mut self, key: String, min: f64, max: f64) -> usize;
    
    /// Union of sorted sets with weights
    fn zunionstore(&mut self, dst: String, keys: Vec<String>, weights: Option<Vec<f64>>) -> usize;
    
    /// Intersection of sorted sets with weights
    fn zinterstore(&mut self, dst: String, keys: Vec<String>, weights: Option<Vec<f64>>) -> usize;
    
    /// Get multiple scores
    fn zmscore(&mut self, key: String, members: Vec<String>) -> Vec<Option<f64>>;
    
    /// Pop member with minimum score
    fn zpopmin(&mut self, key: String, count: Option<usize>) -> Vec<(String, f64)>;
    
    /// Pop member with maximum score
    fn zpopmax(&mut self, key: String, count: Option<usize>) -> Vec<(String, f64)>;
}

impl ZSetOps for DB {
    fn zadd(&mut self, key: String, members: Vec<(f64, String)>) -> Result<usize, String> {
        self.check_expiration(&key);

        let entry = self.items.entry(key).or_insert_with(|| Entry {
            value: DataType::ZSet(ZSetData::new()),
            expires_at: None,
        });

        match &mut entry.value {
            DataType::ZSet(zset) => {
                let mut added = 0;
                for (score, member) in members {
                    if zset.insert(member, score) {
                        added += 1;
                    }
                }
                self.increment_changes();
                Ok(added)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    fn zadd_nx(&mut self, key: String, members: Vec<(f64, String)>) -> Result<usize, String> {
        self.check_expiration(&key);

        // Filter out members that already exist
        let existing: std::collections::HashSet<String> = if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                zset.members.keys().cloned().collect()
            } else {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        } else {
            std::collections::HashSet::new()
        };

        let filtered: Vec<_> = members
            .into_iter()
            .filter(|(_, m)| !existing.contains(m))
            .collect();

        self.zadd(key, filtered)
    }

    fn zadd_xx(&mut self, key: String, members: Vec<(f64, String)>) -> Result<usize, String> {
        self.check_expiration(&key);

        // Filter to only include members that already exist
        if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                let existing: std::collections::HashSet<_> = zset.members.keys().cloned().collect();
                let filtered: Vec<_> = members
                    .into_iter()
                    .filter(|(_, m)| existing.contains(m))
                    .collect();
                return self.zadd(key, filtered);
            } else {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        }
        
        Ok(0)
    }

    fn zrem(&mut self, key: String, members: Vec<String>) -> Result<usize, String> {
        if !self.check_expiration(&key) {
            return Ok(0);
        }

        if let Some(entry) = self.items.get_mut(&key) {
            match &mut entry.value {
                DataType::ZSet(zset) => {
                    let mut removed = 0;
                    for member in members {
                        if zset.remove(&member) {
                            removed += 1;
                        }
                    }
                    if removed > 0 {
                        self.increment_changes();
                    }
                    Ok(removed)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(0)
        }
    }

    fn zscore(&mut self, key: String, member: String) -> Option<f64> {
        if !self.check_expiration(&key) {
            return None;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                return zset.score(&member);
            }
        }
        None
    }

    fn zrank(&mut self, key: String, member: String) -> Option<usize> {
        if !self.check_expiration(&key) {
            return None;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                return zset.rank(&member);
            }
        }
        None
    }

    fn zrevrank(&mut self, key: String, member: String) -> Option<usize> {
        if !self.check_expiration(&key) {
            return None;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                return zset.revrank(&member);
            }
        }
        None
    }

    fn zrange(&mut self, key: String, start: i64, stop: i64, withscores: bool) -> Vec<(String, Option<f64>)> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                return zset.range(start, stop)
                    .into_iter()
                    .map(|(m, s)| (m, if withscores { Some(s) } else { None }))
                    .collect();
            }
        }
        vec![]
    }

    fn zrevrange(&mut self, key: String, start: i64, stop: i64, withscores: bool) -> Vec<(String, Option<f64>)> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                let mut result: Vec<_> = zset.range(start, stop)
                    .into_iter()
                    .map(|(m, s)| (m, if withscores { Some(s) } else { None }))
                    .collect();
                result.reverse();
                return result;
            }
        }
        vec![]
    }

    fn zrangebyscore(&mut self, key: String, min: f64, max: f64, _withscores: bool, offset: Option<usize>, count: Option<usize>) -> Vec<(String, f64)> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                let mut result = zset.range_by_score(min, max);
                
                if let Some(off) = offset {
                    if off < result.len() {
                        result = result[off..].to_vec();
                    } else {
                        result.clear();
                    }
                }
                
                if let Some(cnt) = count {
                    result.truncate(cnt);
                }
                
                return result;
            }
        }
        vec![]
    }

    fn zrevrangebyscore(&mut self, key: String, max: f64, min: f64, withscores: bool, offset: Option<usize>, count: Option<usize>) -> Vec<(String, f64)> {
        let mut result = self.zrangebyscore(key, min, max, withscores, offset, count);
        result.reverse();
        result
    }

    fn zcard(&mut self, key: String) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                return zset.len();
            }
        }
        0
    }

    fn zcount(&mut self, key: String, min: f64, max: f64) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        if let Some(entry) = self.items.get(&key) {
            if let DataType::ZSet(zset) = &entry.value {
                return zset.count(min, max);
            }
        }
        0
    }

    fn zincrby(&mut self, key: String, increment: f64, member: String) -> Result<f64, String> {
        self.check_expiration(&key);

        let current_score = self.zscore(key.clone(), member.clone()).unwrap_or(0.0);
        let new_score = current_score + increment;

        if new_score.is_nan() || new_score.is_infinite() {
            return Err("ERR resulting score is not a number (nan) or infinity".to_string());
        }

        self.zadd(key, vec![(new_score, member)])?;
        Ok(new_score)
    }

    fn zremrangebyrank(&mut self, key: String, start: i64, stop: i64) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        let members_to_remove: Vec<String> = self.zrange(key.clone(), start, stop, false)
            .into_iter()
            .map(|(m, _)| m)
            .collect();

        self.zrem(key, members_to_remove).unwrap_or(0)
    }

    fn zremrangebyscore(&mut self, key: String, min: f64, max: f64) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        let members_to_remove: Vec<String> = self.zrangebyscore(key.clone(), min, max, false, None, None)
            .into_iter()
            .map(|(m, _)| m)
            .collect();

        self.zrem(key, members_to_remove).unwrap_or(0)
    }

    fn zunionstore(&mut self, dst: String, keys: Vec<String>, weights: Option<Vec<f64>>) -> usize {
        let weights = weights.unwrap_or_else(|| vec![1.0; keys.len()]);
        let mut result = ZSetData::new();

        for (i, key) in keys.iter().enumerate() {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            
            if self.check_expiration(key) {
                if let Some(entry) = self.items.get(key) {
                    if let DataType::ZSet(zset) = &entry.value {
                        for (member, score) in &zset.members {
                            let weighted_score = score * weight;
                            let current = result.score(member).unwrap_or(0.0);
                            result.insert(member.clone(), current + weighted_score);
                        }
                    }
                }
            }
        }

        let len = result.len();
        self.items.insert(dst, Entry {
            value: DataType::ZSet(result),
            expires_at: None,
        });
        self.increment_changes();
        len
    }

    fn zinterstore(&mut self, dst: String, keys: Vec<String>, weights: Option<Vec<f64>>) -> usize {
        if keys.is_empty() {
            return 0;
        }

        let weights = weights.unwrap_or_else(|| vec![1.0; keys.len()]);
        
        // Get first set
        let first_key = &keys[0];
        let first_weight = weights.get(0).copied().unwrap_or(1.0);
        
        if !self.check_expiration(first_key) {
            return 0;
        }

        let first_members: std::collections::HashMap<String, f64> = if let Some(entry) = self.items.get(first_key) {
            if let DataType::ZSet(zset) = &entry.value {
                zset.members.iter()
                    .map(|(m, s)| (m.clone(), s * first_weight))
                    .collect()
            } else {
                return 0;
            }
        } else {
            return 0;
        };

        let mut result: std::collections::HashMap<String, f64> = first_members;

        // Intersect with remaining sets
        for (i, key) in keys.iter().enumerate().skip(1) {
            let weight = weights.get(i).copied().unwrap_or(1.0);
            
            if !self.check_expiration(key) {
                result.clear();
                break;
            }

            let other_members: std::collections::HashMap<String, f64> = if let Some(entry) = self.items.get(key) {
                if let DataType::ZSet(zset) = &entry.value {
                    zset.members.clone()
                } else {
                    result.clear();
                    break;
                }
            } else {
                result.clear();
                break;
            };

            result.retain(|member, score| {
                if let Some(other_score) = other_members.get(member) {
                    *score += other_score * weight;
                    true
                } else {
                    false
                }
            });
        }

        let mut final_zset = ZSetData::new();
        for (member, score) in result {
            final_zset.insert(member, score);
        }

        let len = final_zset.len();
        self.items.insert(dst, Entry {
            value: DataType::ZSet(final_zset),
            expires_at: None,
        });
        self.increment_changes();
        len
    }

    fn zmscore(&mut self, key: String, members: Vec<String>) -> Vec<Option<f64>> {
        members.into_iter()
            .map(|m| self.zscore(key.clone(), m))
            .collect()
    }

    fn zpopmin(&mut self, key: String, count: Option<usize>) -> Vec<(String, f64)> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        let count = count.unwrap_or(1);
        let mut result = Vec::new();

        if let Some(entry) = self.items.get_mut(&key) {
            if let DataType::ZSet(zset) = &mut entry.value {
                for _ in 0..count {
                    if let Some(entry) = zset.scores.iter().next().cloned() {
                        result.push((entry.member.clone(), entry.score));
                        zset.remove(&entry.member);
                    } else {
                        break;
                    }
                }
                if !result.is_empty() {
                    self.increment_changes();
                }
            }
        }

        result
    }

    fn zpopmax(&mut self, key: String, count: Option<usize>) -> Vec<(String, f64)> {
        if !self.check_expiration(&key) {
            return vec![];
        }

        let count = count.unwrap_or(1);
        let mut result = Vec::new();

        if let Some(entry) = self.items.get_mut(&key) {
            if let DataType::ZSet(zset) = &mut entry.value {
                for _ in 0..count {
                    if let Some(entry) = zset.scores.iter().next_back().cloned() {
                        result.push((entry.member.clone(), entry.score));
                        zset.remove(&entry.member);
                    } else {
                        break;
                    }
                }
                if !result.is_empty() {
                    self.increment_changes();
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zset_basic() {
        let mut db = DB::new();
        
        assert_eq!(db.zadd("myzset".to_string(), vec![(1.0, "one".to_string()), (2.0, "two".to_string()), (3.0, "three".to_string())]).unwrap(), 3);
        assert_eq!(db.zcard("myzset".to_string()), 3);
        assert_eq!(db.zscore("myzset".to_string(), "two".to_string()), Some(2.0));
        assert_eq!(db.zrank("myzset".to_string(), "two".to_string()), Some(1));
    }

    #[test]
    fn test_zrange() {
        let mut db = DB::new();
        db.zadd("myzset".to_string(), vec![(1.0, "a".to_string()), (2.0, "b".to_string()), (3.0, "c".to_string())]).unwrap();
        
        let range = db.zrange("myzset".to_string(), 0, -1, false);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, "a");
        assert_eq!(range[1].0, "b");
        assert_eq!(range[2].0, "c");
    }

    #[test]
    fn test_zincrby() {
        let mut db = DB::new();
        db.zadd("myzset".to_string(), vec![(1.0, "one".to_string())]).unwrap();
        
        assert_eq!(db.zincrby("myzset".to_string(), 2.5, "one".to_string()).unwrap(), 3.5);
        assert_eq!(db.zscore("myzset".to_string(), "one".to_string()), Some(3.5));
    }
}
