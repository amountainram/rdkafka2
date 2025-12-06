//! A safe Rust wrapper around the C library `librdkafka`.
//!
//! ## Clients
//!
//! - [`crate::client::NativeClient`] - A low-level client that provides direct access to the
//!   underlying `librdkafka` functionality.
//! - [`crate::client::AdminClient`] - A client for performing administrative operations
//!   on Kafka clusters.
//!
//! ## Producer
//!
//! - [`crate::producer::Producer`] - A high-level Kafka producer.
//!
//! ## Configuration
//!
//! Clients, producers, and consumers share the same configuration which is a mirror of
//! `librdkafka`'s configuration options. See [`crate::config::NativeClientConfig`]
//! and [`crate::config::ClientConfig`] for details.

pub use error::{KafkaError, Result};
pub use log::RDKafkaLogLevel;
pub use ptr::IntoOpaque;
pub use rdkafka2_sys::bindings;
pub use util::{Shutdown, Timeout, rdkafka_version};

pub mod client;
pub mod config;
pub mod error;
mod log;
pub mod message;
pub mod partitions;
pub mod producer;
mod ptr;
pub mod topic;
mod util;
