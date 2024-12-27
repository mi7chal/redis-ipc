use redis_ipc::RedisPool;
use redis_ipc::queue::{WriteQueue, ReadQueue};
use std::env;
use std::error::Error;
use redis::{Client};
use r2d2::{Pool};

#[test]
fn write_and_read_queues_communicate() {
    let mut write_queue = build_write_queue();
    let mut read_queue = build_read_queue();

    let message = String::from("Queue test");

    write_queue.publish(message.clone()).expect("Cannot publish");

    let response = read_queue.b_next().expect("Response error");

    println!("{:?}", message);

    assert_eq!(response.get_content(), &message);
}

fn build_write_queue() -> WriteQueue<'static, String> {
    dotenvy::dotenv();
    let url = env::var("REDIS_URL").expect("Env REDIS_URL not found");
    let pool = connect(url).expect("Redis pool cannot be built.");

    WriteQueue::build(pool, "my_test_queue")
}

fn build_read_queue() -> ReadQueue<'static, String> {
    dotenvy::dotenv();
    let url = env::var("REDIS_URL").expect("Env REDIS_URL not found");
    let pool = connect(url).expect("Redis pool cannot be built.");

    ReadQueue::build(pool, "my_test_queue", None)
}


fn connect(redis_url: String) -> Result<RedisPool, Box<dyn Error>> {
    let client = Client::open(redis_url)?;
    let pool = Pool::builder().build(client)?;
    Ok(pool)
}