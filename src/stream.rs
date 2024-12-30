use crate::error::{IpcError, IpcErrorKind};
use crate::{OptionalTimeout, RedisPool, Timeout};
use redis::streams::{StreamMaxlen, StreamRangeReply, StreamReadOptions, StreamReadReply};
use redis::Commands;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time;

/// Actual message content in redis streams is send in only one field as a string, this is the name
/// of this field.
const CONTENT_FIELD: &str = "content";

/// Lighter and more robust way of storing rust stream message id.
///
/// According to [official redis docs](https://redis.io/docs/latest/develop/data-types/streams/)
/// id is stored in format: `<millisecondsTime>-<sequenceNumber>`, where `<millisecondsTime>`
/// and `<sequenceNumber>` are unsigned 64-bit integers.
pub type StreamId = (u64, u64);

/// Stream message wrapper
pub struct StreamMessage<MessageContent> {
    /// Message id
    id: StreamId,
    /// Custom message content
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

/// Structured projected in order to read messages from stream synchronously one by one.
/// Messages are cached, connection is not blocked unless `b_next()` is called.
#[derive(Clone)]
pub struct ReadStream<MessageContent: DeserializeOwned> {
    /// configured [`Pool`](r2d2::Pool) with redis [`Client`](redis::Client)
    pool: RedisPool,
    /// Stream name, used in redis stream
    name: Arc<String>,
    /// Timeout duration, 0 if no timeout
    timeout: Timeout,
    /// Id of the last read message
    last_id: Arc<Mutex<StreamId>>,
    /// Phantom for message type
    phantom: PhantomData<MessageContent>,
}

impl<MessageContent: DeserializeOwned> ReadStream<MessageContent> {
    pub fn new(pool: RedisPool, name: &str, timeout: OptionalTimeout) -> Self {
        let last_id = Arc::new(Mutex::new((0, 0)));
        let timeout = timeout.unwrap_or(time::Duration::ZERO);

        Self {
            name: Arc::new(name.to_string()),
            pool,
            last_id,
            timeout,
            phantom: PhantomData,
        }
    }

    /// Returns current length of the stream
    pub fn len(&self) -> Result<u32, IpcError> {
        let mut conn = self.pool.get()?;

        let res = conn.xlen::<&str, u32>(&self.name)?;

        Ok(res)
    }

    /// Returns last message in stream. For now returns error if there's no messages, but it will change in the future.
    pub fn last(&self) -> Result<StreamMessage<MessageContent>, IpcError> {
        let mut conn = self.pool.get()?;

        let res = conn
            .xrevrange_count::<&str, &str, &str, u8, StreamRangeReply>(&self.name, "+", "-", 1)?;

        let msg = parse_single_range_reply(res)?;

        Ok(msg)
    }

    /// Reads next message in stream. Blocks thread if not available. Waits indefinitely
    //// or returns error after [`ReadStream::timeout`](ReadStream::timeout) if it was set.
    ///
    /// Message is queried based on last id read or if not available first message added after this method call
    /// will be returned.
    pub fn b_next(&self) -> Result<StreamMessage<MessageContent>, IpcError> {
        let mut conn = self.pool.get()?;

        let id = {
            let last_id = self.last_id.lock()?;

            if *last_id == (0, 0) {
                // "$" is redis symbol, for first message after xread()
                String::from("$")
            } else {
                stringify_id(&last_id)
            }
        };

        let timeout = usize::try_from(self.timeout.as_millis()).unwrap_or(usize::MAX);

        let opts = StreamReadOptions::default().count(1).block(timeout);

        let res =
            conn.xread_options::<&str, &str, StreamReadReply>(&[&self.name], &[&id], &opts)?;

        let msg = parse_single_read_reply(res)?;

        if let Ok(mut last_id) = self.last_id.lock() {
            *last_id = msg.get_id();
        }

        Ok(msg)
    }
}

/// Writes stream based on redis streams. It can publish single messages, which can be later read using [`ReadStream`](ReadStream).
///
///
#[derive(Clone)]
pub struct WriteStream<MessageContent: Serialize> {
    /// configured [`Pool`](r2d2::Pool) with redis [`Client`](redis::Client)
    pool: RedisPool,
    /// Stream name, used in redis stream
    name: Arc<String>,
    /// Max size of stream. Stream will be trimmed to this size
    max_size: usize,
    /// Phantom for message content type
    phantom: PhantomData<MessageContent>,
}

impl<MessageContent: Serialize> WriteStream<MessageContent> {
    pub fn new(pool: RedisPool, name: &str, max_size: u32) -> Self {
        Self {
            name: Arc::new(name.to_string()),
            pool,
            max_size: max_size as usize,
            phantom: PhantomData,
        }
    }

    /// Publishes message on stream. Returns message id or error if publishing was unsuccessful
    /// or result is unknown.
    pub fn publish(&self, message: &MessageContent) -> Result<StreamId, IpcError> {
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

// Stringifies redis id tuple to format `<millisecondsTime>-<sequenceNumber>`. See [`StreamId`](StreamId).
fn stringify_id(id: &StreamId) -> String {
    format!("{}-{}", id.0, id.1)
}

// Parses redis stream id from str to tuple. See [`StreamId`](StreamId).
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
fn parse_single_read_reply<MessageContent: DeserializeOwned>(
    rep: StreamReadReply,
) -> Result<StreamMessage<MessageContent>, IpcError> {
    let stream_key = rep.keys.get(0).cloned().ok_or(IpcError::new(
        IpcErrorKind::InvalidData,
        "Redis message empty.",
    ))?;

    let message = stream_key.ids.get(0).cloned().ok_or(IpcError::new(
        IpcErrorKind::InvalidData,
        "Redis message has no ids.",
    ))?;

    let id = parse_id(&message.id)?;

    let content: String = message
        .get(CONTENT_FIELD)
        .ok_or(IpcError::new(IpcErrorKind::InvalidData, "Invalid message."))?;

    let content = serde_json::from_str::<MessageContent>(&content).map_err(|_| {
        IpcError::new(
            IpcErrorKind::InvalidData,
            "Message content can't be parsed.",
        )
    })?;

    Ok(StreamMessage::new(id, content))
}

/// Helper function, Parses stream range reply, which meets requirements below:
/// - Has only one stream entry (one id),
/// - Only entry has a field named content, which can be parsed to `<MessageContent>` generic argument
// Todo add tests
fn parse_single_range_reply<MessageContent: DeserializeOwned>(
    rep: StreamRangeReply,
) -> Result<StreamMessage<MessageContent>, IpcError> {
    let message = rep.ids.get(0).cloned().ok_or(IpcError::new(
        IpcErrorKind::InvalidData,
        "Redis message has no ids.",
    ))?;

    let id = parse_id(&message.id)?;

    let content: String = message
        .get(CONTENT_FIELD)
        .ok_or(IpcError::new(IpcErrorKind::InvalidData, "Invalid message."))?;

    let content = serde_json::from_str::<MessageContent>(&content).map_err(|_| {
        IpcError::new(
            IpcErrorKind::InvalidData,
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
