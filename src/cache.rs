use crate::{RedisPool, Ttl, Timeout};
use std::fmt;
use std::error::Error;
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use std::sync::Once;
use std::marker::PhantomData;
use redis::Commands;
use std::io;
use std::thread;
use std::time;
use std::sync::Arc;

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
}

impl<CacheElement: Serialize + DeserializeOwned> Clone for Cache<CacheElement> {
	fn clone(&self) -> Cache<CacheElement> {
        Self {
        	pool: self.pool.clone(),
        	name: Arc::clone(&self.name),
        	ttl: self.ttl,
        	phantom: self.phantom,
        }
    }
}

impl<CacheElement: Serialize + DeserializeOwned> Cache <CacheElement> {
	/// Creates new cache, using existing pool.
	/// 
	/// # Arguments
	/// 
	/// * pool - confiugred [`Pool`](r2d2::Pool) with [`Client`](redis::Client)
	/// * name - cache name, will be used as redis hash name
	/// * ttl - time to live for every new cache element
	pub fn new(pool: RedisPool, name: String, ttl: Ttl) -> Self {
		Self {
			pool,
			name: Arc::new(name),
			ttl,
			phantom: PhantomData
		}
	}

	/// Returns a cache elemnent or error if not exists
	pub fn get(&self, field: &str) -> Result<CacheElement, Box<dyn Error>> {
		let mut conn = self.pool.get()?;

		let element = conn.hget::<&str, &str, String>(&self.name, field)?;

		Ok(serde_json::from_str::<CacheElement>(&element)?)
	}

	/// Blockingly returns a cache element with given name, or error if timeouts.
	pub fn b_get(&self, field: &str, timeout: Timeout) -> Result<CacheElement, Box<dyn Error>> {
		let timeout = time::Duration::from_millis(timeout.map_or(0, |t| t.get()) as u64);
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
		let mut conn = self.pool.get()?;

		let json = serde_json::to_string(&value)?;

		let _ = conn.hset::<&str, &str, &str, ()>(&self.name, field, &json)?;

		Ok(())
	}

	/// Checks if cache element with given name exists. Returns error on failure.
	pub fn exists(&self, field: &str) -> Result<bool, Box<dyn Error>> {
		let mut conn = self.pool.get()?;

		let result = conn.hexists::<&str, &str, u8>(&self.name, field)?;

		Ok(result != 0)
	}
}