//! CLI Commands
//!
//! Special CLI commands and help system.

use super::colors::Colors;
use std::io::{self, Write};

/// Print welcome banner
pub fn print_welcome(colors: &Colors) {
    println!(
        "{}{}╔══════════════════════════════════════════════════════════╗{}",
        colors.cyan(), colors.bold(), colors.reset()
    );
    println!(
        "{}{}║              🔷 HexagonDB CLI v0.1.0 🔷                   ║{}",
        colors.cyan(), colors.bold(), colors.reset()
    );
    println!(
        "{}{}║     High-Performance In-Memory Database                  ║{}",
        colors.cyan(), colors.bold(), colors.reset()
    );
    println!(
        "{}{}╚══════════════════════════════════════════════════════════╝{}",
        colors.cyan(), colors.bold(), colors.reset()
    );
    println!();
    println!(
        "{}Type 'help' for commands, 'quit' or 'exit' to exit.{}",
        colors.yellow(), colors.reset()
    );
    println!();
}

/// Print help message
pub fn print_help(colors: &Colors) {
    println!("{}{}HexagonDB CLI Commands:{}", colors.bold(), colors.green(), colors.reset());
    println!();
    
    println!("  {}Connection:{}", colors.yellow(), colors.reset());
    println!("    PING               - Test connection");
    println!("    AUTH <password>    - Authenticate");
    println!("    QUIT               - Close connection");
    println!();
    
    println!("  {}String:{}", colors.yellow(), colors.reset());
    println!("    GET, SET, MGET, MSET, INCR, DECR, APPEND, STRLEN");
    println!();
    
    println!("  {}List:{}", colors.yellow(), colors.reset());
    println!("    LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX");
    println!();
    
    println!("  {}Hash:{}", colors.yellow(), colors.reset());
    println!("    HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HKEYS, HVALS");
    println!();
    
    println!("  {}Set:{}", colors.yellow(), colors.reset());
    println!("    SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SUNION, SINTER");
    println!();
    
    println!("  {}Sorted Set:{}", colors.yellow(), colors.reset());
    println!("    ZADD, ZREM, ZRANGE, ZSCORE, ZRANK, ZCARD, ZINCRBY");
    println!();
    
    println!("  {}Bitmap:{}", colors.yellow(), colors.reset());
    println!("    SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS");
    println!();
    
    println!("  {}Stream:{}", colors.yellow(), colors.reset());
    println!("    XADD, XREAD, XRANGE, XLEN, XTRIM, XDEL");
    println!();
    
    println!("  {}Geo:{}", colors.yellow(), colors.reset());
    println!("    GEOADD, GEODIST, GEORADIUS, GEOPOS, GEOHASH");
    println!();
    
    println!("  {}HyperLogLog:{}", colors.yellow(), colors.reset());
    println!("    PFADD, PFCOUNT, PFMERGE");
    println!();
    
    println!("  {}Key Management:{}", colors.yellow(), colors.reset());
    println!("    KEYS, SCAN, TYPE, DEL, EXISTS, EXPIRE, TTL, RENAME");
    println!();
    
    println!("  {}Server:{}", colors.yellow(), colors.reset());
    println!("    INFO, DBSIZE, FLUSHDB, SAVE, BGSAVE, SLOWLOG, CLIENT");
    println!();
    
    println!("  {}Transactions:{}", colors.yellow(), colors.reset());
    println!("    MULTI, EXEC, DISCARD, WATCH, UNWATCH");
    println!();
    
    println!("  {}Pub/Sub:{}", colors.yellow(), colors.reset());
    println!("    PUBLISH, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE");
    println!();
    
    println!("  {}CLI Special:{}", colors.yellow(), colors.reset());
    println!("    help      - Show this help");
    println!("    clear     - Clear screen");
    println!("    history   - Show command history");
    println!("    quit/exit - Exit CLI");
}

/// Clear the terminal screen
pub fn clear_screen() {
    print!("\x1b[2J\x1b[H");
    io::stdout().flush().ok();
}

/// Print command history
pub fn print_history(history: &[String]) {
    for (i, cmd) in history.iter().enumerate() {
        println!("{:4}) {}", i + 1, cmd);
    }
}
