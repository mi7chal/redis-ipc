use redis_ipc::{RedisPool, Timeout, helpers};
use redis_ipc::queue::{WriteQueue, ReadQueue};
use std::env;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use std::sync::Once;
use rand::{distributions::Alphanumeric, Rng};
use std::num::NonZeroU32;

static INIT: Once = Once::new();

/// Checks only if publishing to write queue doesn't produce any errors
/// DO NOT checks if queue message is actually published!
#[test]
fn publishes_to_write_queue() {
    let queue_name = random_string(10);
    let mut queue = build_write_queue::<TestQueueMessageContent>(&queue_name);

    let msg = TestQueueMessageContent {
        title: String::from("Hello 1"),
    };

    let _ = queue.publish(&msg);
}

/// Checks if `ReadQueue::b_next()` returns error when queue is empty and timeout happens.
/// 
/// Please be aware that this test should NOT ever panic. It may panic
/// during queue initialization but that means failure. It should only
/// end up with queue read error. 
#[test]
fn read_queue_timeouts() {
    let queue_name = random_string(10);

    // 1s timeout
    let mut queue = build_read_queue::<TestQueueMessageContent>(&queue_name, NonZeroU32::new(1000));

    let res = queue.b_next();

    assert!(res.is_err())
}


/// Checks if `ReadQueue::next()` returns error when queue is empty.
/// 
/// Please be aware that this test should NOT ever panic. It may panic
/// during queue initialization but that means failure. It should only
/// end up with queue read error. 
#[test]
fn read_queue_error_on_empty() {
    let queue_name = random_string(10);

    // 1s timeout
    let mut queue = build_read_queue::<TestQueueMessageContent>(&queue_name, NonZeroU32::new(1000));

    let res = queue.next();

    assert!(res.is_err())
}

/// Checks if read queue and write queue communicates with each other.
/// 
/// Also tests if send message is equal to the sent one.
#[test]
fn write_and_read_queues_communicate() {
    let queue_name = random_string(10);

    let mut write_queue = build_write_queue::<TestQueueMessageContent>(&queue_name);
    let mut read_queue = build_read_queue::<TestQueueMessageContent>(&queue_name,  NonZeroU32::new(60000));

    let msg = TestQueueMessageContent {
        title: String::from("Queue test"),
    };

    let _ = write_queue.publish(&msg).expect("Cannot publish");

    let response = read_queue.b_next().expect("Response error");

    assert_eq!(response.get_content(), &msg);
}


// *Test helpers*

/// Example message which is used for test purposes of queue.
///
/// # Implements
/// It implements PartialEq, so it may be used to compare in assertion
#[derive(Deserialize, Serialize, Debug)]
struct TestQueueMessageContent {
    pub title: String,
}

impl PartialEq for TestQueueMessageContent {
    fn eq(&self, other: &Self) -> bool {
        self.title == other.title
    }
}

fn random_string(len: u8) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len as usize)
        .map(char::from)
        .collect()
}

fn build_write_queue<'a, MessageContent: Serialize>(name: &'a str) -> WriteQueue<'a, MessageContent> {
    let pool = build_pool();
    
    WriteQueue::build(pool, &name)
}

fn build_read_queue<'a, MessageContent: DeserializeOwned>(name: &'a str, timeout: Timeout) -> ReadQueue<'a, MessageContent> {
    let pool = build_pool();

    // timeout 60s
    ReadQueue::build(pool, &name, timeout)
}

fn build_pool() -> RedisPool {
    INIT.call_once(|| {
        let _ = dotenvy::dotenv();
    });

    let url = env::var("REDIS_URL").expect("Env REDIS_URL not found");
    let pool = helpers::connect(url).expect("Redis pool cannot be built.");

    pool
}