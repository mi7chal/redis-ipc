use redis_ipc::{RedisPool, helpers};
use rand::{distributions::Alphanumeric, Rng};
use std::env;
use std::sync::Once;

static INIT: Once = Once::new();

pub fn build_pool() -> RedisPool {
    INIT.call_once(|| {
        let _ = dotenvy::dotenv();
    });

    let url = env::var("REDIS_URL").expect("Env REDIS_URL not found");
    let pool = helpers::connect(url).expect("Redis pool cannot be built.");

    pool
}

pub fn random_string(len: u8) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len as usize)
        .map(char::from)
        .collect()
}