//! # Introduction
//! Simple crate, which wraps redis a few types into Rust structures. These structures
//! are destined to be used in inter-process or service-to-service communication.


pub mod cache;
pub mod queue;
pub mod stream;
pub mod helpers;
pub mod error;


use r2d2::{Pool, PooledConnection};
use redis::Client;
use std::time::Duration;

// re-exports:
/// Simple cache, based on redis hash. May be used by multiple processes.
pub use cache::Cache;
/// Task queue. Contains read and write variants. Based on redis list.
pub use queue::{ReadQueue, WriteQueue};
/// Event stream based on redis streams.
pub use stream::{ReadStream, WriteStream};

/// Type alias for [`Pool`](Pool) with [`Client`](Client), which is used widely in this crate.
pub type RedisPool = Pool<Client>;
/// Alias for connection, which may be got from pool.
pub type RedisConnection = PooledConnection<Client>;

/// Alias for specifying timeouts in this crate.
pub type Timeout = Duration;
/// Sometimes timeouts are optional, and [`None`](None) may be used instead of specified timeout. 
/// This type alias cover this possibility.
pub type OptionalTimeout = Option<Duration>;

/// Type alias for "Time To Live" function parameters in this crate.
pub type Ttl = Duration;
/// Optional ttl, similar to [`OptionalTimeout`](OptionalTimeout)
pub type OptionalTtl = Option<Duration>;
