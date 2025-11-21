# HexagonDB

HexagonDB is a fast, in-memory key-value database written in Rust.  
It supports concurrent client connections and provides Redis-like commands over TCP.

>Use with caution: this project is in early development and not yet stable.

## Features

- **In-memory key-value storage** - Fast data access with HashMap backend
- **Concurrent client support** - Multiple simultaneous connections with thread pooling
- **Thread-safe operations** - Safe concurrent access with Arc<Mutex>
- **Structured logging** - Tracing integration for observability
- **Rich command set** - GET, SET, DEL, EXISTS, KEYS, INCR, DECR

## Installation

```bash
cargo build --release
```

## Usage

Start the server:

```bash
cargo run --release
```

The server will listen on `127.0.0.1:2112`.

Connect with a TCP client (e.g., `nc`):

```bash
nc localhost 2112
```

## Commands

### Basic Commands
- `GET key` - Get value of a key, returns `(nil)` if not found
- `SET key value` - Set key to value, returns `+OK`
- `DEL key` - Delete a key, returns `:1`
- `EXISTS key` - Check if key exists, returns `:1` or `:0`

### Pattern Matching
- `KEYS pattern` - Find keys matching pattern (`*` for all, `prefix*` for prefix match)

### Counters
- `INCR key` - Increment integer value by 1 (creates if not exists)
- `DECR key` - Decrement integer value by 1 (creates if not exists)

### Example Session

```
SET counter 10
+OK
INCR counter
:11
GET counter
11
EXISTS counter
:1
KEYS *
counter
DEL counter
:1
```

## Architecture

- **Thread-per-connection** - Each client runs in a dedicated thread
- **Shared database access** - Database wrapped in Arc<Mutex<>> for thread safety
- **Structured logging** - Tracing framework for debugging and monitoring
- **Custom error types** - Comprehensive error handling

## License

MIT
