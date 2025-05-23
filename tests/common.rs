use redis_ipc::{RedisPool, helpers};
use rand::{distr::Alphanumeric, Rng};
use std::env;
use std::sync::Once;
use serde::{Deserialize, Serialize};

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
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len as usize)
        .map(char::from)
        .collect()
}

pub fn build_test_message() -> TestMessage {
    TestMessage {
        title: String::from("Hello test!"),
    }
}

// test model
/// Example message which is used for test purposes of messaging.
///
/// # Implements
/// It implements PartialEq, so it may be used to compare in assertion
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TestMessage {
    pub title: String,
}

impl PartialEq for TestMessage {
    fn eq(&self, other: &Self) -> bool {
        self.title == other.title
    }
}