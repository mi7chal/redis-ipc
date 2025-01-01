mod common;
use redis_ipc::cache::Cache;
use redis_ipc::{Ttl, Timeout};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::Duration;
use std::thread;


#[test]
fn random_element_should_not_exist() {
	let name = common::random_string(10);

	let ttl = Duration::from_secs(15);
	let timeout = ttl.clone();

	let cache: Cache<String> = build_cache(&name, ttl, timeout);

	let field = common::random_string(5);

	let exists = cache.exists(&field).expect("Element existence check failed.");

	assert!(!exists);
}


#[test]
fn element_set_get() {
	let name = common::random_string(10);

	let ttl = Duration::from_secs(15);
	let timeout = ttl.clone();

	let cache: Cache<String> = build_cache(&name, ttl, timeout);

	let field = common::random_string(5);

	let value = common::random_string(5);

	let _ = cache.set(&field, &value);

	let field_val = cache.get(&field).unwrap().unwrap();

	assert_eq!(&value, field_val.get_content());
}

#[test]
fn non_existing_get() {
	let name = common::random_string(10);

	let ttl = Duration::from_secs(15);
	let timeout = ttl.clone();

	let cache: Cache<String> = build_cache(&name, ttl, timeout);

	let field = common::random_string(5);
	

	let element = cache.get(&field).expect("Cache element get error");

	assert!(element.is_none());
}

#[test]
fn element_set_exists() {
	let name = common::random_string(10);

	let ttl = Duration::from_secs(15);
	let timeout = ttl.clone();

	let cache: Cache<String> = build_cache(&name, ttl, timeout);

	let field = common::random_string(5);

	let value = common::random_string(5);

	let _ = cache.set(&field, &value);

	let exists = cache.exists(&field).expect("Cannot check value existence");

	assert!(exists);
}

#[test]
fn element_b_get() {
	let name = common::random_string(10);

	let ttl = Duration::from_secs(15);
	let timeout = ttl.clone();

	let cache: Cache<String> = build_cache(&name, ttl, timeout);

	let field = common::random_string(5);

	let value = common::random_string(5);

	let cache_clone = cache.clone();
	let field_clone = field.clone();
	let value_clone = value.clone();

	// todo get rid of this clone()
	let handler = thread::spawn(move || {
    	let res = cache_clone.b_get(&field_clone).unwrap();

    	assert_eq!(res.get_content(), &value_clone);
	});

	thread::sleep(Duration::from_secs(1));

	let _ = cache.set(&field, &value);

	handler.join().unwrap();
}

// ** Helpers **
fn build_cache<CacheElement: Serialize + DeserializeOwned>(name: &str, ttl: Ttl, timeout: Timeout) -> Cache<CacheElement> {
	let pool = common::build_pool();

	Cache::new(pool, name, Some(ttl), Some(timeout))
}