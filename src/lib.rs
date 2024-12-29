pub mod cache;
pub mod helpers;
pub mod queue;
pub mod stream;

use r2d2::{Pool, PooledConnection};
use redis::Client;
use std::num::NonZeroU32;

// desired actions:
// - request drivers -> queue
// - read drivers -> cache
// - get tasks -> queue
// - return tasks result -> event

// desired data types:
// - task queue -> redis list
// - cache, ttl, waitFor with timeout -> redis hash
// - event stream with max_size -> stream

pub type RedisPool = Pool<Client>;
pub type RedisConnection = PooledConnection<Client>;
pub type Timeout = Option<NonZeroU32>;
pub type Ttl = Option<NonZeroU32>;
