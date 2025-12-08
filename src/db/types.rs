//! Data types for HexagonDB.
//!
//! Supports String, List, Hash, Set, Sorted Set, and more.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Instant;

/// All supported data types in HexagonDB
#[derive(Debug, Clone)]
pub enum DataType {
    /// Simple string value
    String(String),
    /// Ordered list of strings
    List(Vec<String>),
    /// Hash map of field -> value
    Hash(HashMap<String, String>),
    /// Unordered set of unique strings
    Set(HashSet<String>),
    /// Sorted set with scores
    ZSet(ZSetData),
    /// Bitmap data
    Bitmap(Vec<u8>),
    /// Stream data (Kafka-like)
    Stream(StreamData),
    /// Geospatial data
    Geo(GeoData),
    /// HyperLogLog data
    HyperLogLog(HyperLogLogData),
}

/// Database entry with value and optional expiration
#[derive(Debug, Clone)]
pub struct Entry {
    pub value: DataType,
    pub expires_at: Option<Instant>,
}

/// Sorted Set data structure
#[derive(Debug, Clone, Default)]
pub struct ZSetData {
    /// Member to score mapping
    pub members: HashMap<String, f64>,
    /// Score to members mapping (for range queries)
    pub scores: BTreeSet<ZSetEntry>,
}

/// Entry in sorted set for ordering
#[derive(Debug, Clone)]
pub struct ZSetEntry {
    pub score: f64,
    pub member: String,
}

impl PartialEq for ZSetEntry {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.member == other.member
    }
}

impl Eq for ZSetEntry {}

impl PartialOrd for ZSetEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ZSetEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.score.partial_cmp(&other.score) {
            Some(std::cmp::Ordering::Equal) | None => self.member.cmp(&other.member),
            Some(ord) => ord,
        }
    }
}

impl ZSetData {
    pub fn new() -> Self {
        ZSetData {
            members: HashMap::new(),
            scores: BTreeSet::new(),
        }
    }

    /// Insert or update a member with score
    pub fn insert(&mut self, member: String, score: f64) -> bool {
        let is_new = if let Some(&old_score) = self.members.get(&member) {
            // Remove old entry from scores
            self.scores.remove(&ZSetEntry {
                score: old_score,
                member: member.clone(),
            });
            false
        } else {
            true
        };

        self.members.insert(member.clone(), score);
        self.scores.insert(ZSetEntry { score, member });
        is_new
    }

    /// Remove a member
    pub fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.members.remove(member) {
            self.scores.remove(&ZSetEntry {
                score,
                member: member.to_string(),
            });
            true
        } else {
            false
        }
    }

    /// Get score of a member
    pub fn score(&self, member: &str) -> Option<f64> {
        self.members.get(member).copied()
    }

    /// Get rank of a member (0-indexed)
    pub fn rank(&self, member: &str) -> Option<usize> {
        let score = self.members.get(member)?;
        let entry = ZSetEntry {
            score: *score,
            member: member.to_string(),
        };
        Some(self.scores.iter().position(|e| e == &entry)?)
    }

    /// Get reverse rank of a member
    pub fn revrank(&self, member: &str) -> Option<usize> {
        let rank = self.rank(member)?;
        Some(self.scores.len() - 1 - rank)
    }

    /// Get members in range by rank
    pub fn range(&self, start: i64, stop: i64) -> Vec<(String, f64)> {
        let len = self.scores.len() as i64;
        let start = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
        let stop = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;

        if start > stop {
            return vec![];
        }

        self.scores
            .iter()
            .skip(start)
            .take(stop - start + 1)
            .map(|e| (e.member.clone(), e.score))
            .collect()
    }

    /// Get members in range by score
    pub fn range_by_score(&self, min: f64, max: f64) -> Vec<(String, f64)> {
        self.scores
            .iter()
            .filter(|e| e.score >= min && e.score <= max)
            .map(|e| (e.member.clone(), e.score))
            .collect()
    }

    /// Count members in score range
    pub fn count(&self, min: f64, max: f64) -> usize {
        self.scores.iter().filter(|e| e.score >= min && e.score <= max).count()
    }

    /// Get length
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

/// Stream data structure
#[derive(Debug, Clone, Default)]
pub struct StreamData {
    pub entries: Vec<StreamEntry>,
    pub groups: HashMap<String, ConsumerGroup>,
    pub last_id: u64,
}

/// Stream entry
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, String>,
    pub timestamp: u64,
}

/// Consumer group for streams
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub name: String,
    pub last_delivered_id: String,
    pub pending: HashMap<String, PendingEntry>,
    pub consumers: HashMap<String, Consumer>,
}

/// Pending entry in consumer group
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub id: String,
    pub consumer: String,
    pub delivery_time: u64,
    pub delivery_count: u32,
}

/// Consumer in a group
#[derive(Debug, Clone)]
pub struct Consumer {
    pub name: String,
    pub pending_count: usize,
}

/// Geospatial data
#[derive(Debug, Clone, Default)]
pub struct GeoData {
    pub locations: HashMap<String, GeoLocation>,
}

/// Geospatial location
#[derive(Debug, Clone, Copy)]
pub struct GeoLocation {
    pub longitude: f64,
    pub latitude: f64,
}

impl GeoData {
    pub fn new() -> Self {
        GeoData {
            locations: HashMap::new(),
        }
    }

    /// Add a location
    pub fn add(&mut self, member: String, lon: f64, lat: f64) -> bool {
        let is_new = !self.locations.contains_key(&member);
        self.locations.insert(member, GeoLocation { longitude: lon, latitude: lat });
        is_new
    }

    /// Get distance between two members in meters
    pub fn distance(&self, member1: &str, member2: &str) -> Option<f64> {
        let loc1 = self.locations.get(member1)?;
        let loc2 = self.locations.get(member2)?;
        Some(haversine_distance(loc1.latitude, loc1.longitude, loc2.latitude, loc2.longitude))
    }
}

/// Calculate distance between two points using Haversine formula
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS: f64 = 6371000.0; // meters

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS * c
}

/// HyperLogLog data for cardinality estimation
#[derive(Debug, Clone)]
pub struct HyperLogLogData {
    pub registers: Vec<u8>,
}

impl Default for HyperLogLogData {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLogData {
    const NUM_REGISTERS: usize = 16384; // 2^14

    pub fn new() -> Self {
        HyperLogLogData {
            registers: vec![0u8; Self::NUM_REGISTERS],
        }
    }

    /// Add an element
    pub fn add(&mut self, element: &str) -> bool {
        use siphasher::sip::SipHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = SipHasher::new();
        element.hash(&mut hasher);
        let hash = hasher.finish();

        let index = (hash & 0x3FFF) as usize; // First 14 bits
        let remaining = hash >> 14;
        let rank = remaining.trailing_zeros() as u8 + 1;

        if self.registers[index] < rank {
            self.registers[index] = rank;
            true
        } else {
            false
        }
    }

    /// Estimate cardinality
    pub fn count(&self) -> usize {
        let m = Self::NUM_REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);

        let sum: f64 = self.registers.iter().map(|&r| 2.0_f64.powi(-(r as i32))).sum();
        let estimate = alpha * m * m / sum;

        if estimate <= 2.5 * m {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros > 0 {
                (m * (m / zeros as f64).ln()) as usize
            } else {
                estimate as usize
            }
        } else {
            estimate as usize
        }
    }

    /// Merge another HyperLogLog into this one
    pub fn merge(&mut self, other: &HyperLogLogData) {
        for i in 0..Self::NUM_REGISTERS {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
            }
        }
    }
}

impl StreamData {
    pub fn new() -> Self {
        StreamData {
            entries: Vec::new(),
            groups: HashMap::new(),
            last_id: 0,
        }
    }

    /// Generate next stream ID
    pub fn next_id(&mut self) -> String {
        self.last_id += 1;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        format!("{}-{}", timestamp, self.last_id)
    }

    /// Add an entry to the stream
    pub fn add(&mut self, id: Option<String>, fields: HashMap<String, String>) -> String {
        let id = id.unwrap_or_else(|| self.next_id());
        let entry = StreamEntry {
            id: id.clone(),
            fields,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        self.entries.push(entry);
        id
    }

    /// Get entries in range
    pub fn range(&self, start: &str, end: &str, count: Option<usize>) -> Vec<&StreamEntry> {
        let mut result: Vec<_> = self
            .entries
            .iter()
            .filter(|e| e.id.as_str() >= start && e.id.as_str() <= end)
            .collect();
        
        if let Some(n) = count {
            result.truncate(n);
        }
        result
    }
}
