//! Database module for HexagonDB.
//!
//! Contains the core database structure, data types, and all operations.

pub mod core;
pub mod ops;
pub mod pubsub;
pub mod types;

// Re-export main types and traits
pub use core::DB;
pub use ops::generic::GenericOps;
pub use ops::hash::HashOps;
pub use ops::list::ListOps;
pub use ops::set::SetOps;
pub use ops::string::StringOps;
pub use ops::zset::ZSetOps;
pub use ops::bitmap::BitmapOps;
pub use ops::stream::StreamOps;
pub use ops::geo::GeoOps;
pub use ops::hyperloglog::HyperLogLogOps;
pub use types::{DataType, Entry};
