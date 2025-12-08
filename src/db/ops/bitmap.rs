//! Bitmap operations.
//!
//! Redis-compatible bitmap (string-based) operations.

use crate::db::core::DB;
use crate::db::ops::generic::GenericOps;
use crate::db::types::{DataType, Entry};
use std::sync::atomic::Ordering;

/// Bitmap operations trait
pub trait BitmapOps {
    /// Set or clear the bit at offset
    fn setbit(&mut self, key: String, offset: usize, value: bool) -> i64;
    
    /// Get the bit value at offset
    fn getbit(&mut self, key: String, offset: usize) -> i64;
    
    /// Count the number of set bits
    fn bitcount(&mut self, key: String, start: Option<i64>, end: Option<i64>) -> usize;
    
    /// Perform bitwise operations between keys
    fn bitop(&mut self, op: BitOperation, destkey: String, keys: Vec<String>) -> usize;
    
    /// Find first bit set to 0 or 1
    fn bitpos(&mut self, key: String, bit: bool, start: Option<i64>, end: Option<i64>) -> i64;
}

/// Bitwise operation types
#[derive(Debug, Clone, Copy)]
pub enum BitOperation {
    And,
    Or,
    Xor,
    Not,
}

impl BitmapOps for DB {
    fn setbit(&mut self, key: String, offset: usize, value: bool) -> i64 {
        self.check_expiration(&key);

        let byte_index = offset / 8;
        let bit_index = 7 - (offset % 8); // MSB first (Redis compatible)

        // Get or create bitmap
        let old_bit = if let Some(entry) = self.items.get_mut(&key) {
            match &mut entry.value {
                DataType::Bitmap(data) => {
                    // Expand if needed
                    if byte_index >= data.len() {
                        data.resize(byte_index + 1, 0);
                    }
                    let old = (data[byte_index] >> bit_index) & 1;
                    if value {
                        data[byte_index] |= 1 << bit_index;
                    } else {
                        data[byte_index] &= !(1 << bit_index);
                    }
                    old as i64
                }
                DataType::String(s) => {
                    // Convert string to bitmap
                    let mut data: Vec<u8> = s.as_bytes().to_vec();
                    if byte_index >= data.len() {
                        data.resize(byte_index + 1, 0);
                    }
                    let old = (data[byte_index] >> bit_index) & 1;
                    if value {
                        data[byte_index] |= 1 << bit_index;
                    } else {
                        data[byte_index] &= !(1 << bit_index);
                    }
                    entry.value = DataType::Bitmap(data);
                    old as i64
                }
                _ => return 0, // Wrong type
            }
        } else {
            // Create new bitmap
            let mut data = vec![0u8; byte_index + 1];
            if value {
                data[byte_index] |= 1 << bit_index;
            }
            self.items.insert(key, Entry {
                value: DataType::Bitmap(data),
                expires_at: None,
            });
            0
        };

        self.changes_since_save.fetch_add(1, Ordering::Relaxed);
        old_bit
    }

    fn getbit(&mut self, key: String, offset: usize) -> i64 {
        if !self.check_expiration(&key) {
            return 0;
        }

        let byte_index = offset / 8;
        let bit_index = 7 - (offset % 8);

        if let Some(entry) = self.items.get(&key) {
            match &entry.value {
                DataType::Bitmap(data) => {
                    if byte_index < data.len() {
                        ((data[byte_index] >> bit_index) & 1) as i64
                    } else {
                        0
                    }
                }
                DataType::String(s) => {
                    let bytes = s.as_bytes();
                    if byte_index < bytes.len() {
                        ((bytes[byte_index] >> bit_index) & 1) as i64
                    } else {
                        0
                    }
                }
                _ => 0,
            }
        } else {
            0
        }
    }

    fn bitcount(&mut self, key: String, start: Option<i64>, end: Option<i64>) -> usize {
        if !self.check_expiration(&key) {
            return 0;
        }

        if let Some(entry) = self.items.get(&key) {
            let data = match &entry.value {
                DataType::Bitmap(d) => d.as_slice(),
                DataType::String(s) => s.as_bytes(),
                _ => return 0,
            };

            if data.is_empty() {
                return 0;
            }

            let len = data.len() as i64;
            let start = start.map(|s| {
                if s < 0 { (len + s).max(0) } else { s.min(len) }
            }).unwrap_or(0) as usize;
            let end = end.map(|e| {
                if e < 0 { (len + e).max(0) } else { e.min(len - 1) }
            }).unwrap_or(len - 1) as usize;

            if start > end || start >= data.len() {
                return 0;
            }

            data[start..=end.min(data.len() - 1)]
                .iter()
                .map(|b| b.count_ones() as usize)
                .sum()
        } else {
            0
        }
    }

    fn bitop(&mut self, op: BitOperation, destkey: String, keys: Vec<String>) -> usize {
        if keys.is_empty() {
            return 0;
        }

        // Collect all bitmaps
        let mut bitmaps: Vec<Vec<u8>> = Vec::new();
        let mut max_len = 0;

        for key in &keys {
            if let Some(entry) = self.items.get(key) {
                let data = match &entry.value {
                    DataType::Bitmap(d) => d.clone(),
                    DataType::String(s) => s.as_bytes().to_vec(),
                    _ => vec![],
                };
                max_len = max_len.max(data.len());
                bitmaps.push(data);
            } else {
                bitmaps.push(vec![]);
            }
        }

        if max_len == 0 {
            self.items.remove(&destkey);
            return 0;
        }

        // Perform operation
        let mut result = vec![0u8; max_len];

        match op {
            BitOperation::Not => {
                // NOT only uses first key
                if let Some(src) = bitmaps.first() {
                    for (i, &byte) in src.iter().enumerate() {
                        result[i] = !byte;
                    }
                    // Fill remaining with 0xFF (NOT of 0x00)
                    for i in src.len()..max_len {
                        result[i] = 0xFF;
                    }
                }
            }
            BitOperation::And => {
                // Initialize with first bitmap or 0xFF
                if let Some(first) = bitmaps.first() {
                    for (i, r) in result.iter_mut().enumerate() {
                        *r = first.get(i).copied().unwrap_or(0);
                    }
                }
                for bitmap in bitmaps.iter().skip(1) {
                    for (i, r) in result.iter_mut().enumerate() {
                        *r &= bitmap.get(i).copied().unwrap_or(0);
                    }
                }
            }
            BitOperation::Or => {
                for bitmap in &bitmaps {
                    for (i, &byte) in bitmap.iter().enumerate() {
                        result[i] |= byte;
                    }
                }
            }
            BitOperation::Xor => {
                for bitmap in &bitmaps {
                    for (i, &byte) in bitmap.iter().enumerate() {
                        result[i] ^= byte;
                    }
                }
            }
        }

        self.items.insert(destkey, Entry {
            value: DataType::Bitmap(result.clone()),
            expires_at: None,
        });
        self.changes_since_save.fetch_add(1, Ordering::Relaxed);

        result.len()
    }

    fn bitpos(&mut self, key: String, bit: bool, start: Option<i64>, end: Option<i64>) -> i64 {
        if !self.check_expiration(&key) {
            return if bit { -1 } else { 0 };
        }

        if let Some(entry) = self.items.get(&key) {
            let data = match &entry.value {
                DataType::Bitmap(d) => d.as_slice(),
                DataType::String(s) => s.as_bytes(),
                _ => return -1,
            };

            if data.is_empty() {
                return if bit { -1 } else { 0 };
            }

            let len = data.len() as i64;
            let start_byte = start.map(|s| {
                if s < 0 { (len + s).max(0) } else { s.min(len) }
            }).unwrap_or(0) as usize;
            let end_byte = end.map(|e| {
                if e < 0 { (len + e).max(0) } else { e.min(len - 1) }
            }).unwrap_or(len - 1) as usize;

            if start_byte > end_byte || start_byte >= data.len() {
                return -1;
            }

            for (byte_idx, &byte) in data[start_byte..=end_byte.min(data.len() - 1)].iter().enumerate() {
                let target = if bit { byte } else { !byte };
                if target != 0 {
                    // Find the first set bit
                    for bit_idx in 0..8 {
                        if (target >> (7 - bit_idx)) & 1 == 1 {
                            return ((start_byte + byte_idx) * 8 + bit_idx) as i64;
                        }
                    }
                }
            }

            if bit {
                -1
            } else {
                // If searching for 0 and end was specified, return -1
                // If searching for 0 and end was not specified, return first bit after end
                if end.is_some() {
                    -1
                } else {
                    (data.len() * 8) as i64
                }
            }
        } else {
            if bit { -1 } else { 0 }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_setbit_getbit() {
        let mut db = DB::new();
        
        assert_eq!(db.setbit("mykey".to_string(), 7, true), 0);
        assert_eq!(db.getbit("mykey".to_string(), 0), 0);
        assert_eq!(db.getbit("mykey".to_string(), 7), 1);
        
        assert_eq!(db.setbit("mykey".to_string(), 7, false), 1);
        assert_eq!(db.getbit("mykey".to_string(), 7), 0);
    }

    #[test]
    fn test_bitcount() {
        let mut db = DB::new();
        
        db.setbit("mykey".to_string(), 0, true);
        db.setbit("mykey".to_string(), 1, true);
        db.setbit("mykey".to_string(), 2, true);
        
        assert_eq!(db.bitcount("mykey".to_string(), None, None), 3);
    }
}
