mod common;
use redis_ipc::cache::Cache;
use redis_ipc::{Ttl, Timeout};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::num::NonZeroU32;
use std::time;
use std::thread;


#[test]
fn random_element_should_not_exist() {
	let name = common::random_string(10);

	// 12h in ms
	let h12 = NonZeroU32::new(42_200_000);

	// ttl 12h
	let cache: Cache<String> = build_cache(name.clone(), h12.clone(), h12);

	let field = common::random_string(5);

	let exists = cache.exists(&field).expect("Element existence check failed.");

	assert!(!exists);
}


#[test]
fn element_set_get() {
	let name = common::random_string(10);

	// 12h in ms
	let h12 = NonZeroU32::new(42_200_000);

	// ttl 12h
	let cache: Cache<String> = build_cache(name.clone(), h12.clone(), h12);

	let field = common::random_string(5);

	let value = common::random_string(5);

	let _ = cache.set(&field, &value);

	let field_val = cache.get(&field).expect("Cannot get cache value");

	assert_eq!(value, field_val);
}

#[test]
fn element_set_exists() {
	let name = common::random_string(10);

	// 12h in ms
	let h12 = NonZeroU32::new(42_200_000);

	// ttl 12h
	let cache: Cache<String> = build_cache(name.clone(), h12.clone(), h12);

	let field = common::random_string(5);

	let value = common::random_string(5);

	let _ = cache.set(&field, &value);

	let exists = cache.exists(&field).expect("Cannot check value existence");

	assert!(exists);
}

#[test]
fn element_b_get() {
	let name = common::random_string(10);

	// 12h in ms
	let h12 = NonZeroU32::new(42_200_000);

	// ttl 12h
	let cache: Cache<String> = build_cache(name.clone(), h12.clone(), h12);

	let field = common::random_string(5);

	let value = common::random_string(5);

	let cache_clone = cache.clone();
	let field_clone = field.clone();
	let value_clone = value.clone();

	// todo get rid of this clone()
	let handler = thread::spawn(move || {
    	let res = cache_clone.b_get(&field_clone).unwrap();

    	assert_eq!(res, value_clone);
	});

	thread::sleep(time::Duration::from_millis(1000));

	let _ = cache.set(&field, &value);

	handler.join().unwrap();
}

// ** Helpers **
fn build_cache<CacheElement: Serialize + DeserializeOwned>(name: String, ttl: Ttl, timeout: Timeout) -> Cache<CacheElement> {
	let pool = common::build_pool();

	Cache::new(pool, name, ttl, timeout)
}