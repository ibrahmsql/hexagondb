//! CLI Configuration and Arguments
//!
//! Command-line argument parsing and configuration.

use clap::Parser;

/// HexagonDB CLI - Command line interface for HexagonDB
#[derive(Parser, Debug, Clone)]
#[command(name = "hexagondb-cli")]
#[command(author = "HexagonDB Contributors")]
#[command(version = "0.1.0")]
#[command(about = "Interactive CLI for HexagonDB", long_about = None)]
pub struct CliArgs {
    /// Server hostname
    #[arg(short = 'h', long, default_value = "127.0.0.1")]
    pub host: String,

    /// Server port
    #[arg(short, long, default_value_t = 6379)]
    pub port: u16,

    /// Password for authentication
    #[arg(short = 'a', long)]
    pub password: Option<String>,

    /// Database number
    #[arg(short = 'n', long, default_value_t = 0)]
    pub db: u8,

    /// Execute command and exit
    #[arg(short = 'c', long)]
    pub command: Option<String>,

    /// Read commands from stdin (pipe mode)
    #[arg(short = 'x', long)]
    pub pipe: bool,

    /// Number of times to repeat the command
    #[arg(short = 'r', long, default_value_t = 1)]
    pub repeat: u32,

    /// Interval between commands in seconds
    #[arg(short = 'i', long, default_value_t = 0.0)]
    pub interval: f64,

    /// Enable raw output mode (no formatting)
    #[arg(long)]
    pub raw: bool,

    /// Enable verbose output
    #[arg(short = 'v', long)]
    pub verbose: bool,

    /// Disable colors
    #[arg(long)]
    pub no_color: bool,

    /// Connection timeout in seconds
    #[arg(long, default_value_t = 5)]
    pub timeout: u64,
}

impl CliArgs {
    /// Get server address string
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
