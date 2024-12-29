use crate::{RedisPool, Timeout};
use redis::streams::{StreamMaxlen, StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::Commands;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
// needs:
// universal - closure might not work here
//
//
// pub/sub vs streams:
//
// pub/sub:
// callback (closure) based, connection must be persisted
//
// stream:
// messages are cached
// contains map (not a string), which may be difficult to decode and encode
// each reply has a deep, complicated structure

// read options:
// - last
// - b_next

const CONTENT_FIELD: &str = "content";

pub type StreamId = (u64, u64);

pub struct StreamMessage<MessageContent> {
    id: StreamId,
    content: MessageContent,
}

impl<MessageContent> StreamMessage<MessageContent> {
    pub fn new(id: StreamId, content: MessageContent) -> Self {
        Self { id, content }
    }

    pub fn get_content(&self) -> &MessageContent {
        &self.content
    }

    pub fn get_id(&self) -> StreamId {
        self.id
    }
}

pub struct ReadStream<MessageContent: DeserializeOwned> {
    pool: RedisPool,
    name: Arc<String>,
    timeout: u32,
    last_id: Arc<Mutex<StreamId>>,
    phantom: PhantomData<MessageContent>,
}

impl<MessageContent: DeserializeOwned> ReadStream<MessageContent> {
    pub fn new(pool: RedisPool, name: String, timeout: Timeout) -> Self {
        let last_id = Arc::new(Mutex::new((0, 0)));
        let timeout: u32 = timeout.map_or(0, |t| t.get());

        Self {
            name: Arc::new(name),
            pool,
            last_id,
            timeout,
            phantom: PhantomData,
        }
    }

    /// Returns current length of the stream
    pub fn len(&self) -> Result<u32, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        let res = conn.xlen::<&str, u32>(&self.name)?;

        Ok(res)
    }

    pub fn last(&self) -> Result<StreamMessage<MessageContent>, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        let res = conn
            .xrevrange_count::<&str, &str, &str, u8, StreamRangeReply>(&self.name, "+", "-", 1)?;

        let msg = parse_single_range_reply(res)?;

        Ok(msg)
    }

    pub fn b_next(&self) -> Result<StreamMessage<MessageContent>, Box<dyn Error + '_>> {
        let mut conn = self.pool.get()?;

        let id = {
            let last_id = self.last_id.lock()?;

            if *last_id == (0, 0) {
                String::from("$")
            } else {
                stringify_id(&last_id)
            }
        };

        let opts = StreamReadOptions::default()
            .count(1)
            .block(self.timeout as usize);

        let res =
            conn.xread_options::<&str, &str, StreamReadReply>(&[&self.name], &[&id], &opts)?;

        let msg = parse_single_read_reply(res)?;

        if let Ok(mut last_id) = self.last_id.lock() {
            *last_id = msg.get_id();
        }

        Ok(msg)
    }
}

pub struct WriteStream<MessageContent: Serialize> {
    pool: RedisPool,
    name: Arc<String>,
    max_size: usize,
    phantom: PhantomData<MessageContent>,
}

impl<MessageContent: Serialize> WriteStream<MessageContent> {
    pub fn new(pool: RedisPool, name: String, max_size: u32) -> Self {
        Self {
            name: Arc::new(name),
            pool,
            max_size: max_size as usize,
            phantom: PhantomData,
        }
    }

    pub fn publish(&self, message: &MessageContent) -> Result<StreamId, Box<dyn Error>> {
        let json = serde_json::to_string(message)?;

        let mut conn = self.pool.get()?;

        let res = conn.xadd_maxlen::<&str, u8, &str, &str, String>(
            &self.name,
            StreamMaxlen::Approx(self.max_size),
            b'*',
            &[(CONTENT_FIELD, &json)],
        )?;

        let id = parse_id(&res)?;

        Ok(id)
    }
}

// Stringifies redis id tuple to format `<millisecondsTime>-<sequenceNumber>`.
fn stringify_id(id: &StreamId) -> String {
    format!("{}-{}", id.0, id.1)
}

// Parses redis stream id from str to tuple
fn parse_id(id_str: &str) -> Result<StreamId, io::Error> {
    let parts = id_str.split('-');

    let values: Vec<&str> = parts.take(2).collect();

    // Id should have only two parts
    if let (Ok(timestamp), Ok(seq)) = (values[0].parse(), values[1].parse()) {
        return Ok((timestamp, seq));
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Invalid id string. Please provide \"<millisecondsTime>-<sequenceNumber>\".",
    ))
}

/// Helper function, Parses stream read reply, which meets requirements below:
/// - Is from one stream (has one key),
/// - Has only one stream entry (one id),
/// - Only entry has a field named content, which can be parsed to `<MessageContent>` generic argument
// Todo add tests
fn parse_single_read_reply<MessageContent: DeserializeOwned>(rep: StreamReadReply) -> Result<StreamMessage<MessageContent>, io::Error> {
    let stream_key = rep.keys.get(0).cloned().ok_or(io::Error::new(
        io::ErrorKind::InvalidData,
        "Redis message empty.",
    ))?;

    let message = stream_key.ids.get(0).cloned().ok_or(io::Error::new(
        io::ErrorKind::InvalidData,
        "Redis message has no ids.",
    ))?;

    let id = parse_id(&message.id)?;

    let content: String = message.get(CONTENT_FIELD).ok_or(io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid message.",
    ))?;

    let content = serde_json::from_str::<MessageContent>(&content).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Message content can't be parsed.",
        )
    })?;

    Ok(StreamMessage::new(id, content))
}

/// Helper function, Parses stream range reply, which meets requirements below:
/// - Has only one stream entry (one id),
/// - Only entry has a field named content, which can be parsed to `<MessageContent>` generic argument
// Todo add tests
fn parse_single_range_reply<MessageContent: DeserializeOwned>(rep: StreamRangeReply) -> Result<StreamMessage<MessageContent>, io::Error> {
    let message = rep.ids.get(0).cloned().ok_or(io::Error::new(
        io::ErrorKind::InvalidData,
        "Redis message has no ids.",
    ))?;

    let id = parse_id(&message.id)?;

    let content: String = message.get(CONTENT_FIELD).ok_or(io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid message.",
    ))?;

    let content = serde_json::from_str::<MessageContent>(&content).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Message content can't be parsed.",
        )
    })?;

    Ok(StreamMessage::new(id, content))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
    fn stream_id_decoding() {
        let example = "123456-789102";

        let result = parse_id(example).unwrap();

        assert_eq!(result, (123456, 789102));
    }

    #[test]
    #[should_panic]
    fn stream_id_decoding_fails_on_too_short() {
        // improper id
        let example = "123456";

        let _ = parse_id(example).unwrap();
    }

    #[test]
    #[should_panic]
    fn stream_id_decoding_fails_on_too_bg_num() {
        // to big number id
        let example = "999999999999999999999999-123";

        let _ = parse_id(example).unwrap();
    }
}
