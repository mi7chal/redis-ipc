use crate::{RedisPool, Timeout, Ttl};
use redis::{Commands, ExpireOption};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use std::thread;
use std::time;

/// Shared cache based on redis hash map.
///
pub struct Cache<CacheElement: Serialize + DeserializeOwned> {
    /// Configured [`Pool`](r2d2::Pool) with [`Client`](redis::Client)
    pool: RedisPool,
    /// Cache name
    name: Arc<String>,
    /// Time to live for elemens in cache. It is shared for every element.
    ttl: Ttl,
    phantom: PhantomData<CacheElement>,
    read_timeout: u32,
}

impl<CacheElement: Serialize + DeserializeOwned> Clone for Cache<CacheElement> {
    fn clone(&self) -> Cache<CacheElement> {
        Self {
            pool: self.pool.clone(),
            name: Arc::clone(&self.name),
            ttl: self.ttl,
            read_timeout: self.read_timeout,
            phantom: self.phantom,
        }
    }
}

impl<CacheElement: Serialize + DeserializeOwned> Cache<CacheElement> {
    /// Creates new cache, using existing pool.
    ///
    /// # Arguments
    ///
    /// * pool - confiugred [`Pool`](r2d2::Pool) with [`Client`](redis::Client)
    /// * name - cache name, will be used as redis hash name
    /// * ttl - time to live for every new cache element
    pub fn new(pool: RedisPool, name: String, ttl: Ttl, read_timeout: Timeout) -> Self {
        // maps None as 0, because redis uses 0 as infinite timeout
        let read_timeout: u32 = read_timeout.map_or(0, |t| t.get());

        Self {
            pool,
            name: Arc::new(name),
            ttl,
            read_timeout,
            phantom: PhantomData,
        }
    }

    /// Returns a cache elemnent or error if not exists
    pub fn get(&self, field: &str) -> Result<CacheElement, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        let element = conn.hget::<&str, &str, String>(&self.name, field)?;

        Ok(serde_json::from_str::<CacheElement>(&element)?)
    }

    /// Returns (blocking) a cache element with given name, or error if timeouts.
    pub fn b_get(&self, field: &str) -> Result<CacheElement, Box<dyn Error>> {
        let timeout = time::Duration::from_millis(self.read_timeout as u64);
        let start_time = time::Instant::now();
        let sleep_duration = time::Duration::from_millis(50);

        loop {
            let elem = self.get(field);

            if let Ok(elem) = elem {
                return Ok(elem);
            }

            if !timeout.is_zero() && start_time.elapsed() >= timeout {
                return Err(Box::new(io::Error::new(io::ErrorKind::TimedOut, "Timeout")));
            }

            thread::sleep(sleep_duration);
        }
    }

    /// Sets given cache field to the element or returns error on failure.
    pub fn set(&self, field: &str, value: &CacheElement) -> Result<(), Box<dyn Error>> {
        // todo set ttl
        let mut conn = self.pool.get()?;

        let json = serde_json::to_string(&value)?;

        let _ = conn.hset::<&str, &str, &str, ()>(&self.name, field, &json)?;

        if let Some(ttl) = self.ttl {
            let _ = conn.hexpire::<&str, &str, Vec<i8>>(
                &self.name,
                ttl.get() as i64,
                ExpireOption::NONE,
                field,
            )?;
        }

        Ok(())
    }

    /// Checks if cache element with given name exists. Returns error on failure.
    pub fn exists(&self, field: &str) -> Result<bool, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        let result = conn.hexists::<&str, &str, u8>(&self.name, field)?;

        Ok(result != 0)
    }
}
