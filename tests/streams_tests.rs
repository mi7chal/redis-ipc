mod common;

use common::TestMessage;
use redis_ipc::{Timeout};
use redis_ipc::stream::{WriteStream, ReadStream};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::num::NonZeroU32;
use std::thread;
use std::time::Duration;


/// Tests only if stream publish works without any failure. Doesn't read message
#[test]
fn publishes_message() {
    let name = common::random_string(10);
    let mut stream = build_write_stream::<TestMessage>(name);

    let msg = TestMessage {
        title: String::from("Hello 1"),
    };

    let _ = stream.publish(&msg);
}

#[test]
fn timeout_on_empty() {
    let name = common::random_string(10);

    // 1s timeout
    let mut stream = build_read_stream::<TestMessage>(name, NonZeroU32::new(1000));

    let res = stream.b_next();

    assert!(res.is_err())
}

#[test]
fn last_empty_error() {
    let name = common::random_string(10);

    // 1s timeout
    let mut stream = build_read_stream::<TestMessage>(name, NonZeroU32::new(1000));

    let res = stream.last();

    assert!(res.is_err())
}

#[test]
fn publishes_and_last_communicate() {
     let name = common::random_string(10);

    let mut write_stream = build_write_stream::<TestMessage>(name.clone());
    let mut read_stream = build_read_stream::<TestMessage>(name,  NonZeroU32::new(10000));

    let msg = common::build_test_message();
    let _ = write_stream.publish(&msg).expect("Cannot publish");

    thread::sleep(Duration::from_secs(5));

    let response = read_stream.last().expect("Response error");

    assert_eq!(response.get_content(), &msg);
}

#[test]
fn publishes_and_b_next_communicate() {
    let name = common::random_string(10);

    let write_stream = build_write_stream::<TestMessage>(name.clone());
    let read_stream = build_read_stream::<TestMessage>(name,  NonZeroU32::new(15_000));

    let msg = common::build_test_message();
    let msg_clone = msg.clone();

    let handler = thread::spawn(move || {
        thread::sleep(Duration::from_secs(3));
        
        write_stream.publish(&msg_clone).expect("Message can't be published");
    });

    let res = read_stream.b_next().expect("Cannot read stream message.");

    handler.join().unwrap();

    assert_eq!(res.get_content(), &msg);
}


// **helpers**s
fn build_write_stream<'a, MessageContent: Serialize>(name: String) -> WriteStream<MessageContent> {
    let pool = common::build_pool();
    
    WriteStream::new(pool, name, 1024)
}

fn build_read_stream<'a, MessageContent: DeserializeOwned>(name: String, timeout: Timeout) -> ReadStream<MessageContent> {
    let pool = common::build_pool();

    // timeout 60s
    ReadStream::new(pool, name, timeout)
}