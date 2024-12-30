use crate::error::{IpcError, IpcErrorKind};
use crate::{OptionalTimeout, RedisPool, Timeout};
use redis::Commands;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Error as SerdeJsonError;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Wrapper struct for messages in [`WriteQueue`](WriteQueue).
#[derive(Serialize)]
pub struct WriteQueueMessage<MessageContent: Serialize> {
    /// Message id
    uuid: String,
    /// Custom content
    content: MessageContent,
}

impl<MessageContent: Serialize> WriteQueueMessage<MessageContent> {
    pub fn new(uuid: String, content: MessageContent) -> WriteQueueMessage<MessageContent> {
        Self { uuid, content }
    }

    pub fn get_uuid(&self) -> &str {
        &self.uuid
    }

    pub fn get_content(&self) -> &MessageContent {
        &self.content
    }
}

/// Wrapper for messages in [`ReadQueue`](ReadQueue).
#[derive(Deserialize)]
pub struct ReadQueueMessage<MessageContent> {
    uuid: String,
    content: MessageContent,
}

impl<MessageContent: DeserializeOwned> ReadQueueMessage<MessageContent> {
    /// Deserializes string and builds message from it.
    ///
    /// # Errors
    /// Returns [`Error`](serde_json::Error) produced by [`serde_json::from_str()](serde_json::from_str)
    pub fn from_str(message: String) -> Result<ReadQueueMessage<MessageContent>, SerdeJsonError> {
        Ok(serde_json::from_str::<ReadQueueMessage<MessageContent>>(
            &message,
        )?)
    }

    pub fn get_uuid(&self) -> &str {
        &self.uuid
    }

    pub fn get_content(&self) -> &MessageContent {
        &self.content
    }
}

/// Queue dedicated for writing tasks only.
///
/// For reading use ReadQueue
#[derive(Clone)]
pub struct WriteQueue<MessageContent: Serialize> {
    /// configured [`Pool`](r2d2::Pool) with redis [`Client`](redis::Client)
    pool: RedisPool,
    /// queue name
    name: Arc<String>,
    /// phantom indicating message type of queue instance
    phantom: PhantomData<MessageContent>,
}

impl<MessageContent: Serialize> WriteQueue<MessageContent> {
    /// Builds [`ReadQueue`](ReadQueue) with given name
    ///
    /// # Arguments
    ///
    /// * pool - configured [`Pool`](r2d2::Pool) with redis [`Client`](redis::Client)
    /// * name - queue name, will be used as redis list name
    pub fn new(pool: RedisPool, name: &str) -> Self {
        Self {
            name: Arc::new(name.to_string()),
            pool,
            phantom: PhantomData,
        }
    }

    /// Publishes task to the queue. Uses queue name, which may be accessed using `WriteQueue::get_name(&self)`
    ///
    /// # Errors
    ///
    /// Returns [`r2d2::Error`](r2d2::Error) when getting connection fails. See [`Pool::get()`](r2d2::Pool::get)
    ///
    /// Returns [`RedisError`](redis::RedisError) when pushing to queue fails.
    ///
    /// Returns [`serde_json::Error`](serde_json::Error) when stringify json fails. See [`serde_json::to_string()`](serde_json::to_string)
    pub fn publish(&mut self, message_content: &MessageContent) -> Result<(), IpcError> {
        let message = WriteQueueMessage::new(Uuid::new_v4().to_string(), message_content);

        let json = serde_json::to_string(&message)?;

        let mut conn = self.pool.get()?;

        conn.lpush::<&str, &str, ()>(&self.name, &json)?;

        Ok(())
    }

    /// Queue name getter.
    pub fn get_name(&self) -> &str {
        &self.name
    }
}

/// Read only task queue. It is based on redis list.
///
/// For writing use `WriteQueue`
#[derive(Clone)]
pub struct ReadQueue<MessageContent: DeserializeOwned> {
    /// configured [`Pool`](r2d2::Pool) with redis [`Client`](redis::Client)
    pool: RedisPool,
    /// blocking requests timeout
    timeout: Timeout,
    /// queue name
    name: Arc<String>,
    /// phantom indicating message type of queue instance
    phantom: PhantomData<MessageContent>,
}

impl<MessageContent: DeserializeOwned> ReadQueue<MessageContent> {
    /// Builds a queue with given timeout and name.
    ///
    /// # Arguments
    ///
    /// * pool - configured r2d2 pool with redis connection
    /// * name - queue name, will be used as redis list name
    /// * timeout - blocking requests timeout in milliseconds or None for infinite timeout
    pub fn new(pool: RedisPool, name: &str, timeout: OptionalTimeout) -> Self {
        // maps None as 0, because redis uses 0 as infinite timeout
        let timeout = timeout.unwrap_or(Duration::ZERO);

        Self {
            name: Arc::new(name.to_string()),
            pool,
            timeout,
            phantom: PhantomData,
        }
    }

    /// Returns the next message in queue or error if it wasn't found
    ///
    /// # Errors
    /// Returns [`r2d2::Error`](r2d2::Error) when getting connection fails. See [`Pool::get()`](r2d2::Pool::get)
    ///
    /// Returns [`RedisError`](redis::RedisError) when reading fails or there is no object available.
    ///
    /// Returns [`serde_json::Error`](serde_json::Error) produced by [`serde_json::from_str()](serde_json::from_str)
    /// when json parsing fails
    pub fn next(&mut self) -> Result<ReadQueueMessage<MessageContent>, IpcError> {
        let mut conn = self.pool.get()?;

        let msg = conn.rpop::<&str, String>(&self.name, NonZeroUsize::new(1))?;

        Ok(ReadQueueMessage::from_str(msg)?)
    }

    /// Blocking read next message from queue. If no message is available blocks thread and waits for timeout or indefinitely.
    /// When timeout exceeds, error is returned.
    ///
    /// # Errors
    ///
    /// Returns [`r2d2::Error`](r2d2::Error) when getting connection fails. See [`Pool::get()`](r2d2::Pool::get)
    ///
    /// Returns [`RedisError`](redis::RedisError) when reading fails or timeout exceeds
    ///
    /// Returns [`Error`](std::io::Error) when redis returns invalid data. It is not possible (theoretically).
    ///
    /// Returns [`serde_json::Error`](serde_json::Error) produced by [`serde_json::from_str()](serde_json::from_str)
    /// when json parsing fails
    pub fn b_next(&mut self) -> Result<ReadQueueMessage<MessageContent>, IpcError> {
        let mut conn = self.pool.get()?;

        // return type of redis blocking pop is ["queue_name", "queue_elem"], br_pop takes timeout in float (seconds) 0.0 timeout is infinite
        let res = conn.brpop::<&str, Vec<String>>(&self.name, self.timeout.as_secs_f64())?;

        let msg = res.get(1).cloned().ok_or(IpcError::new(
            IpcErrorKind::InvalidData,
            "Invalid redis message.",
        ))?;

        Ok(ReadQueueMessage::from_str(msg)?)
    }
}

impl<MessageContent: DeserializeOwned> Iterator for ReadQueue<MessageContent> {
    type Item = ReadQueueMessage<MessageContent>;

    /// Returns first message which can be read.
    ///
    /// # Warning
    /// This method loops infinitely and will never return None.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let res = self.b_next();
            if res.is_ok() {
                return res.ok();
            }
        }
    }
}