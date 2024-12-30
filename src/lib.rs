pub mod cache;
pub mod error;
pub mod helpers;
pub mod queue;
pub mod stream;

use r2d2::{Pool, PooledConnection};
use redis::Client;
use std::time::Duration;

// re-exports:
pub use cache::Cache;
pub use queue::{ReadQueue, WriteQueue};
pub use stream::{ReadStream, WriteStream};

pub type RedisPool = Pool<Client>;
pub type RedisConnection = PooledConnection<Client>;

pub type Timeout = Duration;
pub type OptionalTimeout = Option<Duration>;

pub type Ttl = Duration;
pub type OptionalTtl = Option<Duration>;
