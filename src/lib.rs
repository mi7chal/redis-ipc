pub mod queue;
pub mod helpers;

use r2d2::{Pool, PooledConnection};
use redis::Client;
use std::num::NonZeroU32;

// desired actions:
// - request drivers -> queue
// - read drivers -> cache
// - get tasks -> queue
// - return tasks result -> stream

// desired data types:
// - task queue
// - cache, ttl, waitFor with timeout
// - event stream 

pub type RedisPool = Pool<Client>;
pub type RedisConnection = PooledConnection<Client>;
pub type Timeout = Option<NonZeroU32>;