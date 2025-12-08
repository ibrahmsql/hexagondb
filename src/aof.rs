use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Write, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use parking_lot::Mutex;
use tracing::{info, error, warn};
use crate::database::DB;
use crate::resp::{RespValue, RespHandler};

/// Append-Only File handler for durability
pub struct Aof {
    file: File,
}

impl Aof {
    /// Create a new AOF handler, opening or creating the file
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path)?;
        
        Ok(Aof { file })
    }

    /// Append a command to the AOF file in RESP format
    pub fn append(&mut self, command: Vec<String>) -> io::Result<()> {
        // Convert command to RESP format
        let resp_args: Vec<RespValue> = command.into_iter()
            .map(|s| RespValue::BulkString(Some(s)))
            .collect();
        
        let resp = RespValue::Array(Some(resp_args));
        let serialized = resp.serialize();
        
        self.file.write_all(serialized.as_bytes())?;
        self.file.flush()?;
        Ok(())
    }

    /// Load and replay commands from AOF file to restore database state
    /// Uses streaming parser to handle large files efficiently
    pub fn load(path: impl AsRef<Path>, db: &Arc<Mutex<DB>>) -> io::Result<()> {
        if !path.as_ref().exists() {
            info!("No AOF file found at {:?}, starting with empty database", path.as_ref());
            return Ok(());
        }

        let file = File::open(&path)?;
        let file_size = file.metadata()?.len();
        
        if file_size == 0 {
            info!("AOF file is empty");
            return Ok(());
        }

        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let mut buffer = Vec::with_capacity(64 * 1024);
        let mut total_read = 0usize;
        let mut count = 0usize;
        let mut errors = 0usize;

        info!("Loading AOF file ({} bytes)...", file_size);

        // Read in chunks and parse incrementally
        loop {
            let mut chunk = [0u8; 8192];
            let bytes_read = reader.read(&mut chunk)?;
            
            if bytes_read == 0 {
                break;
            }
            
            buffer.extend_from_slice(&chunk[..bytes_read]);
            total_read += bytes_read;

            // Process all complete commands in the buffer
            loop {
                match RespHandler::parse_request(&buffer) {
                    Ok(Some((value, len))) => {
                        // Remove parsed bytes from buffer
                        buffer.drain(..len);
                        
                        // Convert RESP value to arguments
                        let args = match value {
                            RespValue::Array(Some(items)) => {
                                items.into_iter().filter_map(|item| {
                                    match item {
                                        RespValue::BulkString(Some(s)) => Some(s),
                                        RespValue::SimpleString(s) => Some(s),
                                        _ => None,
                                    }
                                }).collect::<Vec<String>>()
                            },
                            _ => Vec::new(),
                        };

                        if !args.is_empty() {
                            if let Err(e) = replay_command(&args, db) {
                                warn!("Failed to replay command {:?}: {}", args, e);
                                errors += 1;
                            } else {
                                count += 1;
                            }
                        }
                    },
                    Ok(None) => {
                        // Incomplete command, need more data
                        break;
                    },
                    Err(e) => {
                        error!("Error parsing AOF at offset {}: {}", total_read - buffer.len(), e);
                        errors += 1;
                        // Try to recover by skipping one byte
                        if !buffer.is_empty() {
                            buffer.remove(0);
                        }
                        if errors > 100 {
                            error!("Too many errors, aborting AOF load");
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "Too many parse errors"));
                        }
                    }
                }
            }

            // Progress logging for large files
            if total_read % (10 * 1024 * 1024) == 0 {
                info!("AOF load progress: {} MB / {} MB ({} commands)", 
                    total_read / 1024 / 1024, 
                    file_size / 1024 / 1024,
                    count);
            }
        }

        if !buffer.is_empty() {
            warn!("AOF file has {} trailing bytes that couldn't be parsed", buffer.len());
        }

        info!("Loaded {} commands from AOF ({} errors)", count, errors);
        Ok(())
    }
}

/// Replay a single command on the database
fn replay_command(args: &[String], db: &Arc<Mutex<DB>>) -> Result<(), String> {
    if args.is_empty() {
        return Ok(());
    }

    let cmd = args[0].to_uppercase();
    let mut db_guard = db.lock();
    
    match cmd.as_str() {
        // String commands
        "SET" if args.len() >= 3 => {
            db_guard.set(args[1].clone(), args[2].clone());
        }
        "SETNX" if args.len() >= 3 => {
            db_guard.setnx(args[1].clone(), args[2].clone());
        }
        "SETEX" if args.len() >= 4 => {
            if let Ok(secs) = args[2].parse::<u64>() {
                db_guard.setex(args[1].clone(), secs, args[3].clone());
            }
        }
        "MSET" if args.len() >= 3 && (args.len() - 1) % 2 == 0 => {
            let pairs: Vec<(String, String)> = args[1..]
                .chunks(2)
                .filter_map(|chunk| {
                    if chunk.len() == 2 {
                        Some((chunk[0].clone(), chunk[1].clone()))
                    } else {
                        None
                    }
                })
                .collect();
            db_guard.mset(pairs);
        }
        "APPEND" if args.len() >= 3 => {
            db_guard.append(args[1].clone(), args[2].clone());
        }
        "DEL" if args.len() >= 2 => {
            db_guard.del(args[1].clone());
        }
        "INCR" if args.len() >= 2 => {
            let _ = db_guard.incr(args[1].clone());
        }
        "DECR" if args.len() >= 2 => {
            let _ = db_guard.decr(args[1].clone());
        }
        "INCRBY" if args.len() >= 3 => {
            if let Ok(delta) = args[2].parse::<i64>() {
                let _ = db_guard.incrby(args[1].clone(), delta);
            }
        }
        "DECRBY" if args.len() >= 3 => {
            if let Ok(delta) = args[2].parse::<i64>() {
                let _ = db_guard.decrby(args[1].clone(), delta);
            }
        }
        "INCRBYFLOAT" if args.len() >= 3 => {
            if let Ok(delta) = args[2].parse::<f64>() {
                let _ = db_guard.incrbyfloat(args[1].clone(), delta);
            }
        }
        
        // TTL commands
        "EXPIRE" if args.len() >= 3 => {
            if let Ok(secs) = args[2].parse::<u64>() {
                db_guard.expire(args[1].clone(), secs);
            }
        }
        "EXPIREAT" if args.len() >= 3 => {
            if let Ok(ts) = args[2].parse::<u64>() {
                db_guard.expireat(args[1].clone(), ts);
            }
        }
        "PERSIST" if args.len() >= 2 => {
            db_guard.persist(args[1].clone());
        }
        "RENAME" if args.len() >= 3 => {
            let _ = db_guard.rename(args[1].clone(), args[2].clone());
        }
        
        // List commands
        "LPUSH" if args.len() >= 3 => {
            let values = args[2..].to_vec();
            let _ = db_guard.lpush_safe(args[1].clone(), values);
        }
        "RPUSH" if args.len() >= 3 => {
            let values = args[2..].to_vec();
            let _ = db_guard.rpush(args[1].clone(), values);
        }
        "LPOP" if args.len() >= 2 => {
            let _ = db_guard.lpop(args[1].clone());
        }
        "RPOP" if args.len() >= 2 => {
            let _ = db_guard.rpop(args[1].clone());
        }
        "LSET" if args.len() >= 4 => {
            if let Ok(index) = args[2].parse::<i64>() {
                let _ = db_guard.lset(args[1].clone(), index, args[3].clone());
            }
        }
        "LTRIM" if args.len() >= 4 => {
            if let (Ok(start), Ok(stop)) = (args[2].parse::<i64>(), args[3].parse::<i64>()) {
                db_guard.ltrim(args[1].clone(), start, stop);
            }
        }
        "LREM" if args.len() >= 4 => {
            if let Ok(count) = args[2].parse::<i64>() {
                db_guard.lrem(args[1].clone(), count, args[3].clone());
            }
        }
        
        // Hash commands
        "HSET" if args.len() >= 4 => {
            let _ = db_guard.hset(args[1].clone(), args[2].clone(), args[3].clone());
        }
        "HSETNX" if args.len() >= 4 => {
            let _ = db_guard.hsetnx(args[1].clone(), args[2].clone(), args[3].clone());
        }
        "HMSET" if args.len() >= 4 && (args.len() - 2) % 2 == 0 => {
            let pairs: Vec<(String, String)> = args[2..]
                .chunks(2)
                .filter_map(|chunk| {
                    if chunk.len() == 2 {
                        Some((chunk[0].clone(), chunk[1].clone()))
                    } else {
                        None
                    }
                })
                .collect();
            let _ = db_guard.hmset(args[1].clone(), pairs);
        }
        "HDEL" if args.len() >= 3 => {
            let _ = db_guard.hdel(args[1].clone(), args[2].clone());
        }
        "HINCRBY" if args.len() >= 4 => {
            if let Ok(delta) = args[3].parse::<i64>() {
                let _ = db_guard.hincrby(args[1].clone(), args[2].clone(), delta);
            }
        }
        "HINCRBYFLOAT" if args.len() >= 4 => {
            if let Ok(delta) = args[3].parse::<f64>() {
                let _ = db_guard.hincrbyfloat(args[1].clone(), args[2].clone(), delta);
            }
        }
        
        // Set commands  
        "SADD" if args.len() >= 3 => {
            let members = args[2..].to_vec();
            let _ = db_guard.sadd(args[1].clone(), members);
        }
        "SREM" if args.len() >= 3 => {
            let _ = db_guard.srem(args[1].clone(), args[2].clone());
        }
        "SMOVE" if args.len() >= 4 => {
            db_guard.smove(args[1].clone(), args[2].clone(), args[3].clone());
        }
        "SPOP" if args.len() >= 2 => {
            let count = args.get(2).and_then(|s| s.parse().ok());
            db_guard.spop(args[1].clone(), count);
        }
        
        // Sorted Set commands
        "ZADD" if args.len() >= 4 => {
            let mut members = Vec::new();
            let mut i = 2;
            while i + 1 < args.len() {
                if let Ok(score) = args[i].parse::<f64>() {
                    members.push((score, args[i + 1].clone()));
                    i += 2;
                } else {
                    break;
                }
            }
            if !members.is_empty() {
                let _ = db_guard.zadd(args[1].clone(), members);
            }
        }
        "ZREM" if args.len() >= 3 => {
            let members = args[2..].to_vec();
            let _ = db_guard.zrem(args[1].clone(), members);
        }
        "ZINCRBY" if args.len() >= 4 => {
            if let Ok(incr) = args[2].parse::<f64>() {
                let _ = db_guard.zincrby(args[1].clone(), incr, args[3].clone());
            }
        }
        "ZREMRANGEBYRANK" if args.len() >= 4 => {
            if let (Ok(start), Ok(stop)) = (args[2].parse::<i64>(), args[3].parse::<i64>()) {
                db_guard.zremrangebyrank(args[1].clone(), start, stop);
            }
        }
        "ZREMRANGEBYSCORE" if args.len() >= 4 => {
            if let (Ok(min), Ok(max)) = (args[2].parse::<f64>(), args[3].parse::<f64>()) {
                db_guard.zremrangebyscore(args[1].clone(), min, max);
            }
        }
        
        // Database commands
        "FLUSHDB" | "FLUSHALL" => {
            db_guard.flushdb();
        }
        
        // Skip read-only commands silently
        "GET" | "MGET" | "EXISTS" | "TYPE" | "TTL" | "PTTL" | "KEYS" | "SCAN" |
        "LLEN" | "LRANGE" | "LINDEX" | "LPOS" |
        "HGET" | "HMGET" | "HGETALL" | "HKEYS" | "HVALS" | "HLEN" | "HEXISTS" | "HSTRLEN" | "HSCAN" |
        "SMEMBERS" | "SISMEMBER" | "SCARD" | "SRANDMEMBER" | "SUNION" | "SINTER" | "SDIFF" |
        "ZSCORE" | "ZRANK" | "ZREVRANK" | "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZCARD" | "ZCOUNT" |
        "DBSIZE" | "INFO" | "PING" | "ECHO" | "TIME" => {
            // Read-only commands are not replayed
        }
        
        _ => {
            // Unknown command - log but don't fail
            warn!("Unknown command in AOF: {}", cmd);
        }
    }
    
    Ok(())
}
