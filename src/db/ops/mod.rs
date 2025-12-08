//! Database operations module.
//!
//! All operations are organized into traits:
//! - GenericOps: Key management, expiration
//! - StringOps: String operations
//! - ListOps: List operations  
//! - HashOps: Hash operations
//! - SetOps: Set operations
//! - ZSetOps: Sorted set operations
//! - BitmapOps: Bitmap operations
//! - StreamOps: Stream (Kafka-like) operations
//! - GeoOps: Geospatial operations
//! - HyperLogLogOps: Probabilistic cardinality estimation

pub mod generic;
pub mod hash;
pub mod list;
pub mod set;
pub mod string;
pub mod zset;
pub mod bitmap;
pub mod stream;
pub mod geo;
pub mod hyperloglog;
