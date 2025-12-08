//! HexagonDB CLI Library
//!
//! Modular CLI client for HexagonDB.

pub mod client;
pub mod colors;
pub mod commands;
pub mod completer;
pub mod config;
pub mod highlighter;
pub mod hinter;
pub mod output;
pub mod parser;
pub mod repl;

// Re-export main helper for editors
pub use rustyline;
