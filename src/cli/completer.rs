//! Command Completion
//!
//! Auto-completion support for CLI commands.

use rustyline::completion::{Completer, Pair};
use rustyline::Context;

/// All supported commands with their syntax
pub static COMMANDS: &[(&str, &str, &str)] = &[
    // String commands
    ("APPEND", "key value", "Append value to key"),
    ("DECR", "key", "Decrement key"),
    ("DECRBY", "key decrement", "Decrement key by value"),
    ("GET", "key", "Get value of key"),
    ("GETRANGE", "key start end", "Get substring of key"),
    ("GETSET", "key value", "Set key returning old value"),
    ("INCR", "key", "Increment key"),
    ("INCRBY", "key increment", "Increment key by value"),
    ("INCRBYFLOAT", "key increment", "Increment key by float"),
    ("MGET", "key [key ...]", "Get multiple keys"),
    ("MSET", "key value [key value ...]", "Set multiple keys"),
    ("SET", "key value [EX seconds] [PX ms] [NX|XX]", "Set key value"),
    ("SETEX", "key seconds value", "Set key with expiry"),
    ("SETNX", "key value", "Set if not exists"),
    ("SETRANGE", "key offset value", "Overwrite part of string"),
    ("STRLEN", "key", "Get string length"),
    
    // List commands
    ("LINDEX", "key index", "Get element by index"),
    ("LINSERT", "key BEFORE|AFTER pivot value", "Insert element"),
    ("LLEN", "key", "Get list length"),
    ("LPOP", "key [count]", "Remove and get first elements"),
    ("LPUSH", "key value [value ...]", "Prepend values"),
    ("LPUSHX", "key value", "Prepend value if exists"),
    ("LRANGE", "key start stop", "Get range of elements"),
    ("LREM", "key count value", "Remove elements"),
    ("LSET", "key index value", "Set element at index"),
    ("LTRIM", "key start stop", "Trim list"),
    ("RPOP", "key [count]", "Remove and get last elements"),
    ("RPUSH", "key value [value ...]", "Append values"),
    ("RPUSHX", "key value", "Append value if exists"),
    
    // Hash commands
    ("HDEL", "key field [field ...]", "Delete fields"),
    ("HEXISTS", "key field", "Check field exists"),
    ("HGET", "key field", "Get field value"),
    ("HGETALL", "key", "Get all fields and values"),
    ("HINCRBY", "key field increment", "Increment field"),
    ("HINCRBYFLOAT", "key field increment", "Increment field by float"),
    ("HKEYS", "key", "Get all field names"),
    ("HLEN", "key", "Get number of fields"),
    ("HMGET", "key field [field ...]", "Get multiple fields"),
    ("HMSET", "key field value [field value ...]", "Set multiple fields"),
    ("HSCAN", "key cursor [MATCH pattern] [COUNT count]", "Scan fields"),
    ("HSET", "key field value [field value ...]", "Set field(s)"),
    ("HSETNX", "key field value", "Set field if not exists"),
    ("HSTRLEN", "key field", "Get field value length"),
    ("HVALS", "key", "Get all values"),
    
    // Set commands
    ("SADD", "key member [member ...]", "Add members"),
    ("SCARD", "key", "Get set size"),
    ("SDIFF", "key [key ...]", "Get difference"),
    ("SDIFFSTORE", "destination key [key ...]", "Store difference"),
    ("SINTER", "key [key ...]", "Get intersection"),
    ("SINTERSTORE", "destination key [key ...]", "Store intersection"),
    ("SISMEMBER", "key member", "Check membership"),
    ("SMEMBERS", "key", "Get all members"),
    ("SMOVE", "source destination member", "Move member"),
    ("SPOP", "key [count]", "Remove random members"),
    ("SRANDMEMBER", "key [count]", "Get random members"),
    ("SREM", "key member [member ...]", "Remove members"),
    ("SSCAN", "key cursor [MATCH pattern] [COUNT count]", "Scan members"),
    ("SUNION", "key [key ...]", "Get union"),
    ("SUNIONSTORE", "destination key [key ...]", "Store union"),
    
    // Sorted Set commands
    ("ZADD", "key [NX|XX] [GT|LT] [CH] score member [score member ...]", "Add members"),
    ("ZCARD", "key", "Get sorted set size"),
    ("ZCOUNT", "key min max", "Count members in score range"),
    ("ZINCRBY", "key increment member", "Increment member score"),
    ("ZINTERSTORE", "destination numkeys key [key ...] [WEIGHTS weight ...]", "Store intersection"),
    ("ZRANGE", "key start stop [WITHSCORES]", "Get range by index"),
    ("ZRANGEBYSCORE", "key min max [WITHSCORES] [LIMIT offset count]", "Get range by score"),
    ("ZRANK", "key member", "Get member rank"),
    ("ZREM", "key member [member ...]", "Remove members"),
    ("ZREVRANGE", "key start stop [WITHSCORES]", "Get range by index (reverse)"),
    ("ZREVRANGEBYSCORE", "key max min [WITHSCORES] [LIMIT offset count]", "Get range by score (reverse)"),
    ("ZREVRANK", "key member", "Get member rank (reverse)"),
    ("ZSCORE", "key member", "Get member score"),
    ("ZUNIONSTORE", "destination numkeys key [key ...] [WEIGHTS weight ...]", "Store union"),
    
    // Bitmap commands
    ("BITCOUNT", "key [start end]", "Count set bits"),
    ("BITOP", "operation destkey key [key ...]", "Perform bitwise operation"),
    ("BITPOS", "key bit [start] [end]", "Find first bit"),
    ("GETBIT", "key offset", "Get bit value"),
    ("SETBIT", "key offset value", "Set bit value"),
    
    // Stream commands
    ("XADD", "key ID field value [field value ...]", "Add entry"),
    ("XDEL", "key ID [ID ...]", "Delete entries"),
    ("XINFO", "STREAM key", "Get stream info"),
    ("XLEN", "key", "Get stream length"),
    ("XRANGE", "key start end [COUNT count]", "Get range of entries"),
    ("XREAD", "COUNT count STREAMS key [key ...] ID [ID ...]", "Read entries"),
    ("XREVRANGE", "key end start [COUNT count]", "Get range (reverse)"),
    ("XTRIM", "key MAXLEN [~] count", "Trim stream"),
    
    // Geo commands
    ("GEOADD", "key longitude latitude member [longitude latitude member ...]", "Add locations"),
    ("GEODIST", "key member1 member2 [m|km|mi|ft]", "Get distance"),
    ("GEOHASH", "key member [member ...]", "Get geohash"),
    ("GEOPOS", "key member [member ...]", "Get positions"),
    ("GEORADIUS", "key longitude latitude radius m|km|mi|ft [WITHCOORD] [WITHDIST]", "Search by radius"),
    ("GEOSEARCH", "key FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius m|km|mi|ft", "Advanced search"),
    
    // HyperLogLog commands
    ("PFADD", "key element [element ...]", "Add elements"),
    ("PFCOUNT", "key [key ...]", "Count unique elements"),
    ("PFMERGE", "destkey sourcekey [sourcekey ...]", "Merge HLLs"),
    
    // Key commands
    ("COPY", "source destination [REPLACE]", "Copy key"),
    ("DEL", "key [key ...]", "Delete keys"),
    ("EXISTS", "key [key ...]", "Check key existence"),
    ("EXPIRE", "key seconds", "Set key expiry"),
    ("EXPIREAT", "key timestamp", "Set key expiry at timestamp"),
    ("KEYS", "pattern", "Find keys matching pattern"),
    ("PERSIST", "key", "Remove key expiry"),
    ("PEXPIRE", "key milliseconds", "Set key expiry in ms"),
    ("PTTL", "key", "Get key TTL in ms"),
    ("RANDOMKEY", "-", "Get random key"),
    ("RENAME", "key newkey", "Rename key"),
    ("RENAMENX", "key newkey", "Rename if new key doesn't exist"),
    ("SCAN", "cursor [MATCH pattern] [COUNT count] [TYPE type]", "Scan keys"),
    ("TOUCH", "key [key ...]", "Touch keys"),
    ("TTL", "key", "Get key TTL"),
    ("TYPE", "key", "Get key type"),
    ("UNLINK", "key [key ...]", "Delete keys asynchronously"),
    
    // Server commands
    ("AUTH", "password", "Authenticate"),
    ("BGSAVE", "-", "Background save"),
    ("CLIENT", "LIST|KILL|SETNAME [args]", "Client management"),
    ("CONFIG", "GET|SET parameter [value]", "Get/set config"),
    ("DBSIZE", "-", "Get number of keys"),
    ("FLUSHALL", "[ASYNC]", "Delete all keys"),
    ("FLUSHDB", "[ASYNC]", "Delete keys in current DB"),
    ("INFO", "[section]", "Get server info"),
    ("PING", "[message]", "Test connection"),
    ("QUIT", "-", "Close connection"),
    ("SAVE", "-", "Synchronous save"),
    ("SELECT", "index", "Select database"),
    ("SHUTDOWN", "[NOSAVE|SAVE]", "Shutdown server"),
    ("SLOWLOG", "GET|LEN|RESET [count]", "Slow log management"),
    ("TIME", "-", "Get server time"),
    
    // Transaction commands
    ("DISCARD", "-", "Discard transaction"),
    ("EXEC", "-", "Execute transaction"),
    ("MULTI", "-", "Start transaction"),
    ("UNWATCH", "-", "Unwatch all keys"),
    ("WATCH", "key [key ...]", "Watch keys"),
    
    // Pub/Sub commands
    ("PSUBSCRIBE", "pattern [pattern ...]", "Subscribe to patterns"),
    ("PUBLISH", "channel message", "Publish message"),
    ("PUNSUBSCRIBE", "[pattern [pattern ...]]", "Unsubscribe from patterns"),
    ("SUBSCRIBE", "channel [channel ...]", "Subscribe to channels"),
    ("UNSUBSCRIBE", "[channel [channel ...]]", "Unsubscribe from channels"),
    
    // Replication commands
    ("REPLICAOF", "host port", "Set master"),
    ("SLAVEOF", "host port | NO ONE", "Set master (deprecated)"),
];

/// Command completer
pub struct CommandCompleter;

impl Completer for CommandCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let line_up_to_cursor = &line[..pos];
        let words: Vec<&str> = line_up_to_cursor.split_whitespace().collect();
        
        // If empty or completing first word
        if words.is_empty() || (words.len() == 1 && !line_up_to_cursor.ends_with(' ')) {
            let prefix = words.first().map(|s| s.to_uppercase()).unwrap_or_default();
            let matches: Vec<Pair> = COMMANDS
                .iter()
                .filter(|(cmd, _, _)| cmd.starts_with(&prefix))
                .map(|(cmd, args, desc)| Pair {
                    display: format!("{:<20} {:<40} # {}", cmd, args, desc),
                    replacement: format!("{} ", cmd),
                })
                .collect();
            
            let start = line_up_to_cursor.rfind(' ').map(|i| i + 1).unwrap_or(0);
            return Ok((start, matches));
        }
        
        Ok((pos, vec![]))
    }
}

/// Get command help text
pub fn get_command_help(cmd: &str) -> Option<String> {
    let cmd_upper = cmd.to_uppercase();
    COMMANDS
        .iter()
        .find(|(c, _, _)| *c == cmd_upper)
        .map(|(cmd, args, desc)| format!("{} {} - {}", cmd, args, desc))
}
