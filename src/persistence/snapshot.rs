//! RDB Snapshot persistence.
//!
//! Creates point-in-time snapshots of the database.
//! Supports all data types including Bitmap, Stream, Geo, and HyperLogLog.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::db::types::{DataType, Entry, ZSetData, StreamData, GeoData, HyperLogLogData};
use crate::db::DB;

/// Magic bytes for RDB file - version 02 includes all types
const RDB_MAGIC: &[u8] = b"HEXRDB02";

/// RDB opcodes
mod opcodes {
    pub const EOF: u8 = 0xFF;
    pub const STRING: u8 = 0x00;
    pub const LIST: u8 = 0x01;
    pub const SET: u8 = 0x02;
    pub const ZSET: u8 = 0x03;
    pub const HASH: u8 = 0x04;
    pub const BITMAP: u8 = 0x05;
    pub const STREAM: u8 = 0x06;
    pub const GEO: u8 = 0x07;
    pub const HYPERLOGLOG: u8 = 0x08;
    pub const EXPIRE: u8 = 0xFD;
}

/// Save database to RDB file
pub async fn save<P: AsRef<Path>>(path: P, db: &Arc<RwLock<DB>>) -> io::Result<()> {
    let temp_path = format!("{}.tmp", path.as_ref().display());
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp_path)?;

    let mut writer = BufWriter::new(file);

    // Write magic
    writer.write_all(RDB_MAGIC)?;

    let db_guard = db.read().await;
    let mut saved_count = 0usize;
    let skipped_count = 0usize;

    for (key, entry) in db_guard.items.iter() {
        // Write expiration if exists
        if let Some(expires_at) = entry.expires_at {
            let now = std::time::Instant::now();
            if expires_at > now {
                writer.write_all(&[opcodes::EXPIRE])?;
                let ttl_ms = expires_at.duration_since(now).as_millis() as u64;
                writer.write_all(&ttl_ms.to_le_bytes())?;
            } else {
                // Key has expired, skip it
                continue;
            }
        }

        match &entry.value {
            DataType::String(val) => {
                writer.write_all(&[opcodes::STRING])?;
                write_string(&mut writer, key)?;
                write_string(&mut writer, val)?;
                saved_count += 1;
            }
            DataType::List(list) => {
                writer.write_all(&[opcodes::LIST])?;
                write_string(&mut writer, key)?;
                write_length(&mut writer, list.len())?;
                for item in list {
                    write_string(&mut writer, item)?;
                }
                saved_count += 1;
            }
            DataType::Set(set) => {
                writer.write_all(&[opcodes::SET])?;
                write_string(&mut writer, key)?;
                write_length(&mut writer, set.len())?;
                for member in set {
                    write_string(&mut writer, member)?;
                }
                saved_count += 1;
            }
            DataType::Hash(hash) => {
                writer.write_all(&[opcodes::HASH])?;
                write_string(&mut writer, key)?;
                write_length(&mut writer, hash.len())?;
                for (field, value) in hash {
                    write_string(&mut writer, field)?;
                    write_string(&mut writer, value)?;
                }
                saved_count += 1;
            }
            DataType::ZSet(zset) => {
                writer.write_all(&[opcodes::ZSET])?;
                write_string(&mut writer, key)?;
                write_length(&mut writer, zset.members.len())?;
                for (member, score) in &zset.members {
                    write_string(&mut writer, member)?;
                    writer.write_all(&score.to_le_bytes())?;
                }
                saved_count += 1;
            }
            DataType::Bitmap(data) => {
                writer.write_all(&[opcodes::BITMAP])?;
                write_string(&mut writer, key)?;
                write_length(&mut writer, data.len())?;
                writer.write_all(data)?;
                saved_count += 1;
            }
            DataType::Stream(stream) => {
                // Serialize stream entries
                writer.write_all(&[opcodes::STREAM])?;
                write_string(&mut writer, key)?;
                write_length(&mut writer, stream.entries.len())?;
                for entry in &stream.entries {
                    write_string(&mut writer, &entry.id)?;
                    writer.write_all(&entry.timestamp.to_le_bytes())?;
                    write_length(&mut writer, entry.fields.len())?;
                    for (field, value) in &entry.fields {
                        write_string(&mut writer, field)?;
                        write_string(&mut writer, value)?;
                    }
                }
                // Write last_id as u64
                writer.write_all(&stream.last_id.to_le_bytes())?;
                saved_count += 1;
            }
            DataType::Geo(geo) => {
                writer.write_all(&[opcodes::GEO])?;
                write_string(&mut writer, key)?;
                write_length(&mut writer, geo.locations.len())?;
                for (name, loc) in &geo.locations {
                    write_string(&mut writer, name)?;
                    writer.write_all(&loc.latitude.to_le_bytes())?;
                    writer.write_all(&loc.longitude.to_le_bytes())?;
                }
                saved_count += 1;
            }
            DataType::HyperLogLog(hll) => {
                writer.write_all(&[opcodes::HYPERLOGLOG])?;
                write_string(&mut writer, key)?;
                // Write registers (fixed size array)
                write_length(&mut writer, hll.registers.len())?;
                for &reg in &hll.registers {
                    writer.write_all(&[reg])?;
                }
                saved_count += 1;
            }
        }
    }

    // Write EOF
    writer.write_all(&[opcodes::EOF])?;

    writer.flush()?;
    drop(writer);

    // Atomic rename
    std::fs::rename(&temp_path, path)?;

    info!("RDB snapshot saved: {} keys ({} skipped)", saved_count, skipped_count);
    Ok(())
}

/// Load database from RDB file
pub async fn load<P: AsRef<Path>>(path: P, db: &Arc<RwLock<DB>>) -> io::Result<usize> {
    if !path.as_ref().exists() {
        return Ok(0);
    }

    let file = File::open(&path)?;
    let mut reader = BufReader::new(file);

    // Verify magic
    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?;
    
    // Support both v1 and v2 formats
    if &magic != RDB_MAGIC && &magic != b"HEXRDB01" {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB magic"));
    }
    let is_v2 = &magic == RDB_MAGIC;

    let mut count = 0;
    let mut pending_expire: Option<u64> = None;

    loop {
        let mut opcode = [0u8; 1];
        if reader.read(&mut opcode)? == 0 {
            break;
        }

        match opcode[0] {
            opcodes::EOF => break,
            opcodes::EXPIRE => {
                let mut ttl_bytes = [0u8; 8];
                reader.read_exact(&mut ttl_bytes)?;
                pending_expire = Some(u64::from_le_bytes(ttl_bytes));
            }
            opcodes::STRING => {
                let key = read_string(&mut reader)?;
                let value = read_string(&mut reader)?;

                let mut db_guard = db.write().await;
                let expires_at = pending_expire.map(|ms| {
                    std::time::Instant::now() + std::time::Duration::from_millis(ms)
                });
                db_guard.items.insert(
                    key,
                    Entry {
                        value: DataType::String(value),
                        expires_at,
                    },
                );
                pending_expire = None;
                count += 1;
            }
            opcodes::LIST => {
                let key = read_string(&mut reader)?;
                let len = read_length(&mut reader)?;
                let mut list = Vec::with_capacity(len);
                for _ in 0..len {
                    list.push(read_string(&mut reader)?);
                }

                let mut db_guard = db.write().await;
                let expires_at = pending_expire.map(|ms| {
                    std::time::Instant::now() + std::time::Duration::from_millis(ms)
                });
                db_guard.items.insert(
                    key,
                    Entry {
                        value: DataType::List(list),
                        expires_at,
                    },
                );
                pending_expire = None;
                count += 1;
            }
            opcodes::SET => {
                let key = read_string(&mut reader)?;
                let len = read_length(&mut reader)?;
                let mut set = std::collections::HashSet::with_capacity(len);
                for _ in 0..len {
                    set.insert(read_string(&mut reader)?);
                }

                let mut db_guard = db.write().await;
                let expires_at = pending_expire.map(|ms| {
                    std::time::Instant::now() + std::time::Duration::from_millis(ms)
                });
                db_guard.items.insert(
                    key,
                    Entry {
                        value: DataType::Set(set),
                        expires_at,
                    },
                );
                pending_expire = None;
                count += 1;
            }
            opcodes::HASH => {
                let key = read_string(&mut reader)?;
                let len = read_length(&mut reader)?;
                let mut hash = std::collections::HashMap::with_capacity(len);
                for _ in 0..len {
                    let field = read_string(&mut reader)?;
                    let value = read_string(&mut reader)?;
                    hash.insert(field, value);
                }

                let mut db_guard = db.write().await;
                let expires_at = pending_expire.map(|ms| {
                    std::time::Instant::now() + std::time::Duration::from_millis(ms)
                });
                db_guard.items.insert(
                    key,
                    Entry {
                        value: DataType::Hash(hash),
                        expires_at,
                    },
                );
                pending_expire = None;
                count += 1;
            }
            opcodes::ZSET => {
                let key = read_string(&mut reader)?;
                let len = read_length(&mut reader)?;
                let mut zset = ZSetData::new();
                for _ in 0..len {
                    let member = read_string(&mut reader)?;
                    let mut score_bytes = [0u8; 8];
                    reader.read_exact(&mut score_bytes)?;
                    let score = f64::from_le_bytes(score_bytes);
                    zset.insert(member, score);
                }

                let mut db_guard = db.write().await;
                let expires_at = pending_expire.map(|ms| {
                    std::time::Instant::now() + std::time::Duration::from_millis(ms)
                });
                db_guard.items.insert(
                    key,
                    Entry {
                        value: DataType::ZSet(zset),
                        expires_at,
                    },
                );
                pending_expire = None;
                count += 1;
            }
            opcodes::BITMAP if is_v2 => {
                let key = read_string(&mut reader)?;
                let len = read_length(&mut reader)?;
                let mut data = vec![0u8; len];
                reader.read_exact(&mut data)?;

                let mut db_guard = db.write().await;
                let expires_at = pending_expire.map(|ms| {
                    std::time::Instant::now() + std::time::Duration::from_millis(ms)
                });
                db_guard.items.insert(
                    key,
                    Entry {
                        value: DataType::Bitmap(data),
                        expires_at,
                    },
                );
                pending_expire = None;
                count += 1;
            }
            opcodes::STREAM if is_v2 => {
                let key = read_string(&mut reader)?;
                let entry_count = read_length(&mut reader)?;
                
                let mut stream = StreamData::new();
                for _ in 0..entry_count {
                    let id = read_string(&mut reader)?;
                    let mut ts_bytes = [0u8; 8];
                    reader.read_exact(&mut ts_bytes)?;
                    let timestamp = u64::from_le_bytes(ts_bytes);
                    let field_count = read_length(&mut reader)?;
                    let mut fields = std::collections::HashMap::new();
                    for _ in 0..field_count {
                        let field = read_string(&mut reader)?;
                        let value = read_string(&mut reader)?;
                        fields.insert(field, value);
                    }
                    stream.entries.push(crate::db::types::StreamEntry { id, fields, timestamp });
                }
                // Read last_id as u64
                let mut last_id_bytes = [0u8; 8];
                reader.read_exact(&mut last_id_bytes)?;
                stream.last_id = u64::from_le_bytes(last_id_bytes);

                let mut db_guard = db.write().await;
                let expires_at = pending_expire.map(|ms| {
                    std::time::Instant::now() + std::time::Duration::from_millis(ms)
                });
                db_guard.items.insert(
                    key,
                    Entry {
                        value: DataType::Stream(stream),
                        expires_at,
                    },
                );
                pending_expire = None;
                count += 1;
            }
            opcodes::GEO if is_v2 => {
                let key = read_string(&mut reader)?;
                let loc_count = read_length(&mut reader)?;
                
                let mut geo = GeoData::new();
                for _ in 0..loc_count {
                    let name = read_string(&mut reader)?;
                    let mut lat_bytes = [0u8; 8];
                    let mut lon_bytes = [0u8; 8];
                    reader.read_exact(&mut lat_bytes)?;
                    reader.read_exact(&mut lon_bytes)?;
                    let lat = f64::from_le_bytes(lat_bytes);
                    let lon = f64::from_le_bytes(lon_bytes);
                    geo.locations.insert(name, crate::db::types::GeoLocation {
                        latitude: lat,
                        longitude: lon,
                    });
                }

                let mut db_guard = db.write().await;
                let expires_at = pending_expire.map(|ms| {
                    std::time::Instant::now() + std::time::Duration::from_millis(ms)
                });
                db_guard.items.insert(
                    key,
                    Entry {
                        value: DataType::Geo(geo),
                        expires_at,
                    },
                );
                pending_expire = None;
                count += 1;
            }
            opcodes::HYPERLOGLOG if is_v2 => {
                let key = read_string(&mut reader)?;
                let reg_count = read_length(&mut reader)?;
                
                let mut hll = HyperLogLogData::new();
                if reg_count == hll.registers.len() {
                    for i in 0..reg_count {
                        let mut reg = [0u8; 1];
                        reader.read_exact(&mut reg)?;
                        hll.registers[i] = reg[0];
                    }
                } else {
                    warn!("HyperLogLog register count mismatch, skipping key {}", key);
                    // Skip remaining bytes
                    for _ in 0..reg_count {
                        let mut reg = [0u8; 1];
                        reader.read_exact(&mut reg)?;
                    }
                    pending_expire = None;
                    continue;
                }

                let mut db_guard = db.write().await;
                let expires_at = pending_expire.map(|ms| {
                    std::time::Instant::now() + std::time::Duration::from_millis(ms)
                });
                db_guard.items.insert(
                    key,
                    Entry {
                        value: DataType::HyperLogLog(hll),
                        expires_at,
                    },
                );
                pending_expire = None;
                count += 1;
            }
            _ => {
                error!("Unknown RDB opcode: {} (v2: {})", opcode[0], is_v2);
                return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Unknown opcode: {}", opcode[0])));
            }
        }
    }

    info!("Loaded {} keys from RDB", count);
    Ok(count)
}

// Helper functions for reading/writing

fn write_string<W: Write>(writer: &mut W, s: &str) -> io::Result<()> {
    let bytes = s.as_bytes();
    write_length(writer, bytes.len())?;
    writer.write_all(bytes)?;
    Ok(())
}

fn read_string<R: Read>(reader: &mut R) -> io::Result<String> {
    let len = read_length(reader)?;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn write_length<W: Write>(writer: &mut W, len: usize) -> io::Result<()> {
    let len = len as u32;
    writer.write_all(&len.to_le_bytes())?;
    Ok(())
}

fn read_length<R: Read>(reader: &mut R) -> io::Result<usize> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf) as usize)
}
