use crate::{RedisPool, Timeout};

pub struct Cache {
	pool: RedisPool,
	name: String,
}

impl Cache {
	fn new(pool: RedisPool, name: String) -> Self {
		Self {
			pool,
			name
		}
	}

	fn get(&self, key: String) -> Option(CacheResult) {

	}

	// todo timeout
	fn waitFor(&self, key: String, timeout: Timeout) -> Option(CacheResult) {

	}

	fn request(&self, key: String) {

	}
}