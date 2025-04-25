use crate::error::{IpcError, IpcErrorKind};
use crate::{ OptionalTimeout, OptionalTtl, RedisPool, Timeout};
use redis::{Commands, ExpireOption};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread;
use std::time;

/// Wrapper struct for elements in cache. 
#[derive(Serialize, Deserialize)]
pub struct CacheElement<ElementContent> {
    timestamp: u128,
    content: ElementContent,
}

impl<ElementContent> CacheElement<ElementContent> {
    /// Creates a new `CacheElement`. `timestamp` param should be unix timestamp.
    pub fn new(timestamp: u128, content: ElementContent) -> Self {
        Self { timestamp, content }
    }

    /// Getter for timestamp field
    pub fn get_timestamp_128(&self) -> u128 {
        self.timestamp
    }

    /// Getter for content field
    pub fn get_content(&self) -> &ElementContent {
        &self.content
    }
}

/// Shared cache based on redis hash.
#[derive(Clone)]
pub struct Cache<ElementContent: Serialize + DeserializeOwned> {
    /// Configured [`Pool`](r2d2::Pool) with [`Client`](redis::Client)
    pool: RedisPool,
    /// Cache name
    name: Arc<String>,
    /// Time to live for elements in cache. It is shared for every element.
    ttl: OptionalTtl,
    /// phantom to specify type of elements in cache
    phantom: PhantomData<ElementContent>,
    /// timeout for reading operation in milliseconds
    read_timeout: Timeout,
}

impl<ElementContent: Serialize + DeserializeOwned> Cache<ElementContent> {
    /// Creates new cache, using existing pool.
    ///
    /// # Arguments
    ///
    /// * pool - configured [`RedisPool`](RedisPool)
    /// * name - cache name, will be used as redis hash name
    /// * ttl - time to live for every new cache element (in ms)
    /// * read_timeout - timeout for reading operations (in ms)
    pub fn new(
        pool: RedisPool,
        name: &str,
        ttl: OptionalTtl,
        read_timeout: OptionalTimeout,
    ) -> Self {
        // maps None as 0, because redis uses 0 as infinite timeout
        let read_timeout = read_timeout.unwrap_or(time::Duration::ZERO);

        Self {
            pool,
            name: Arc::new(name.to_string()),
            ttl,
            read_timeout,
            phantom: PhantomData,
        }
    }

    /// Returns a cache element or error if not exists
    pub fn get(&self, field: &str) -> Result<Option<CacheElement<ElementContent>>, IpcError> {
        let mut conn = self.pool.get()?;

        let element = conn.hget::<&str, &str, Option<String>>(&self.name, field)?;
        
        Ok(
            if let Some(element) = element {
                let parsed = serde_json::from_str::<CacheElement<ElementContent>>(&element)?;
                Some(parsed)
            } else {
                None
            }
        )
    }

    /// Returns (blocking) a cache element with given name, or error if timeouts.
    pub fn b_get(&self, field: &str) -> Result<CacheElement<ElementContent>, IpcError> {
        let start_time = time::Instant::now();
        let sleep_duration = time::Duration::from_millis(50);

        loop {
            let elem = self.get(field);

            if let Ok(Some(elem)) = elem {
                return Ok(elem);
            }

            if !self.read_timeout.is_zero() && start_time.elapsed() >= self.read_timeout {
                return Err(IpcError::new(IpcErrorKind::Timeout, "Request timed out."));
            }

            thread::sleep(sleep_duration);
        }
    }

    /// Sets given cache field to the element or returns error on failure.
    pub fn set(&self, field: &str, value: &ElementContent) -> Result<(), IpcError> {
        let mut conn = self.pool.get()?;

        let element = CacheElement::new(timestamp_u128_now()?, value);

        let json = serde_json::to_string(&element)?;

        let _ = conn.hset::<&str, &str, &str, ()>(&self.name, field, &json)?;

        // optionally sets expiration
        if let Some(ttl) = self.ttl {
            // ttl set for max i64 value, if `Duration` was too big
            let ttl = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);

            let _ =
                conn.hexpire::<&str, &str, Vec<i8>>(&self.name, ttl, ExpireOption::NONE, field)?;
        }

        Ok(())
    }

    /// Checks if cache element with given name exists. Returns error on failure.
    pub fn exists(&self, field: &str) -> Result<bool, IpcError> {
        let mut conn = self.pool.get()?;

        let result = conn.hexists::<&str, &str, u8>(&self.name, field)?;

        Ok(result != 0)
    }

    /// Deletes cache field by given key. Returns error on failure.
    pub fn delete(&self, field: &str) -> Result<(), IpcError> {
        let mut conn = self.pool.get()?;

        conn.hdel::<&str, &str, ()>(&self.name, field)?;

        Ok(())
    }
}

/// Returns current 128 bit unix timestamp
fn timestamp_u128_now() -> Result<u128, time::SystemTimeError> {
    Ok(time::SystemTime::now()
        .duration_since(time::UNIX_EPOCH)?
        .as_millis())
}
