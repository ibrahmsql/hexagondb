//! Append-Only File (AOF) persistence.
//!
//! Every write command is logged to the AOF file for durability.
//! On restart, commands are replayed to restore state.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

use crate::db::DB;
use crate::network::resp::RespValue;

/// Append-Only File handler
pub struct Aof {
    file: File,
    fsync_policy: FsyncPolicy,
    last_fsync: std::time::Instant,
}

/// Fsync policies
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FsyncPolicy {
    /// Fsync after every write
    Always,
    /// Fsync once per second
    Everysec,
    /// Let OS handle fsync
    No,
}

impl Aof {
    /// Create a new AOF handler
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path)?;

        Ok(Aof {
            file,
            fsync_policy: FsyncPolicy::Everysec,
            last_fsync: std::time::Instant::now(),
        })
    }

    /// Set fsync policy
    pub fn set_fsync_policy(&mut self, policy: FsyncPolicy) {
        self.fsync_policy = policy;
    }

    /// Append a command to the AOF
    pub fn append(&mut self, command: Vec<String>) -> io::Result<()> {
        // Convert command to RESP format
        let resp_args: Vec<RespValue> = command
            .into_iter()
            .map(|s| RespValue::BulkString(Some(s)))
            .collect();

        let resp = RespValue::Array(Some(resp_args));
        let serialized = resp.serialize();

        self.file.write_all(serialized.as_bytes())?;

        // Apply fsync policy
        match self.fsync_policy {
            FsyncPolicy::Always => {
                self.file.sync_all()?;
            }
            FsyncPolicy::Everysec => {
                if self.last_fsync.elapsed().as_secs() >= 1 {
                    self.file.sync_all()?;
                    self.last_fsync = std::time::Instant::now();
                }
            }
            FsyncPolicy::No => {
                // Let OS handle it
            }
        }

        Ok(())
    }

    /// Force fsync
    pub fn fsync(&mut self) -> io::Result<()> {
        self.file.sync_all()?;
        self.last_fsync = std::time::Instant::now();
        Ok(())
    }

    /// Load and replay AOF file
    pub async fn load<P: AsRef<Path>>(path: P, db: &Arc<RwLock<DB>>) -> io::Result<usize> {
        use crate::db::{GenericOps, HashOps, ListOps, SetOps, StringOps, ZSetOps};
        use crate::network::resp::RespHandler;

        if !path.as_ref().exists() {
            return Ok(0);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        let mut current_pos = 0;
        let mut count = 0;

        while current_pos < buffer.len() {
            match RespHandler::parse_request(&buffer[current_pos..]) {
                Ok(Some((value, len))) => {
                    current_pos += len;

                    // Convert RESP value to arguments
                    let args = match value {
                        RespValue::Array(Some(items)) => items
                            .into_iter()
                            .filter_map(|item| match item {
                                RespValue::BulkString(Some(s)) => Some(s),
                                RespValue::SimpleString(s) => Some(s),
                                _ => None,
                            })
                            .collect::<Vec<String>>(),
                        _ => Vec::new(),
                    };

                    if !args.is_empty() {
                        let cmd = args[0].to_uppercase();
                        let mut db_guard = db.write().await;

                        // Replay write commands
                        match cmd.as_str() {
                            "SET" if args.len() >= 3 => {
                                db_guard.set(args[1].clone(), args[2].clone());
                            }
                            "DEL" if args.len() >= 2 => {
                                db_guard.del(&args[1]);
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
                            "EXPIRE" if args.len() >= 3 => {
                                if let Ok(secs) = args[2].parse::<u64>() {
                                    db_guard.expire(&args[1], secs);
                                }
                            }
                            "PERSIST" if args.len() >= 2 => {
                                db_guard.persist(&args[1]);
                            }
                            "LPUSH" | "RPUSH" if args.len() >= 3 => {
                                let values = args[2..].to_vec();
                                if cmd == "LPUSH" {
                                    let _ = db_guard.lpush(args[1].clone(), values);
                                } else {
                                    let _ = db_guard.rpush(args[1].clone(), values);
                                }
                            }
                            "LPOP" if args.len() >= 2 => {
                                let _ = db_guard.lpop(args[1].clone());
                            }
                            "RPOP" if args.len() >= 2 => {
                                let _ = db_guard.rpop(args[1].clone());
                            }
                            "HSET" if args.len() >= 4 => {
                                let _ = db_guard.hset(
                                    args[1].clone(),
                                    args[2].clone(),
                                    args[3].clone(),
                                );
                            }
                            "HDEL" if args.len() >= 3 => {
                                let _ = db_guard.hdel(args[1].clone(), args[2].clone());
                            }
                            "SADD" if args.len() >= 3 => {
                                let members = args[2..].to_vec();
                                let _ = db_guard.sadd(args[1].clone(), members);
                            }
                            "SREM" if args.len() >= 3 => {
                                let _ = db_guard.srem(args[1].clone(), args[2].clone());
                            }
                            "ZADD" if args.len() >= 4 => {
                                if let Ok(score) = args[2].parse::<f64>() {
                                    let _ = db_guard.zadd(
                                        args[1].clone(),
                                        vec![(score, args[3].clone())],
                                    );
                                }
                            }
                            "ZREM" if args.len() >= 3 => {
                                let _ = db_guard.zrem(args[1].clone(), vec![args[2].clone()]);
                            }
                            _ => {
                                // Unknown or read-only command, skip
                            }
                        }

                        count += 1;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    error!("Error parsing AOF: {}", e);
                    break;
                }
            }
        }

        info!("Loaded {} commands from AOF", count);
        Ok(count)
    }

    /// Rewrite AOF file (compact it)
    pub async fn rewrite<P: AsRef<Path>>(path: P, db: &Arc<RwLock<DB>>) -> io::Result<()> {
        use crate::db::types::DataType;

        let temp_path = format!("{}.tmp", path.as_ref().display());
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;

        let db_guard = db.read().await;

        for (key, entry) in db_guard.items.iter() {
            let commands = match &entry.value {
                DataType::String(val) => {
                    vec![vec!["SET".to_string(), key.clone(), val.clone()]]
                }
                DataType::List(list) => {
                    if !list.is_empty() {
                        let mut cmd = vec!["RPUSH".to_string(), key.clone()];
                        cmd.extend(list.clone());
                        vec![cmd]
                    } else {
                        vec![]
                    }
                }
                DataType::Hash(hash) => {
                    let mut cmds = vec![];
                    for (field, value) in hash {
                        cmds.push(vec![
                            "HSET".to_string(),
                            key.clone(),
                            field.clone(),
                            value.clone(),
                        ]);
                    }
                    cmds
                }
                DataType::Set(set) => {
                    if !set.is_empty() {
                        let mut cmd = vec!["SADD".to_string(), key.clone()];
                        cmd.extend(set.iter().cloned());
                        vec![cmd]
                    } else {
                        vec![]
                    }
                }
                DataType::ZSet(zset) => {
                    let mut cmds = vec![];
                    for (member, score) in &zset.members {
                        cmds.push(vec![
                            "ZADD".to_string(),
                            key.clone(),
                            score.to_string(),
                            member.clone(),
                        ]);
                    }
                    cmds
                }
                _ => vec![],
            };

            for cmd in commands {
                let resp_args: Vec<RespValue> = cmd
                    .into_iter()
                    .map(|s| RespValue::BulkString(Some(s)))
                    .collect();
                let resp = RespValue::Array(Some(resp_args));
                file.write_all(resp.serialize().as_bytes())?;
            }

            // Handle expiration
            if let Some(expires_at) = entry.expires_at {
                let now = std::time::Instant::now();
                if expires_at > now {
                    let ttl = expires_at.duration_since(now).as_secs();
                    let cmd = vec!["EXPIRE".to_string(), key.clone(), ttl.to_string()];
                    let resp_args: Vec<RespValue> = cmd
                        .into_iter()
                        .map(|s| RespValue::BulkString(Some(s)))
                        .collect();
                    let resp = RespValue::Array(Some(resp_args));
                    file.write_all(resp.serialize().as_bytes())?;
                }
            }
        }

        file.sync_all()?;

        // Atomic rename
        std::fs::rename(&temp_path, path)?;

        info!("AOF rewrite completed");
        Ok(())
    }
}
