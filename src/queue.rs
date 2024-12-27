use std::num::{NonZeroUsize};
use crate::{RedisPool, Timeout};
use serde::{Serialize, Deserialize};
use serde_json::{Error as SerdeJsonError};
use serde::de::DeserializeOwned;
use std::error::Error;
use redis::{Commands};
use std::io::{Error as IOError, ErrorKind as IOErrorKind};
use uuid::Uuid;
use std::marker::PhantomData;

/// Wrapper struct for messages in [`WriteQueue`](crate::queue::WriteQueue).
#[derive(Serialize)]
pub struct WriteQueueMessage<MessageContent: Serialize> {
    /// Message id
    uuid: String,
    /// Custom content
    content: MessageContent,
}

impl<MessageContent: Serialize> WriteQueueMessage<MessageContent> {
    pub fn new(uuid: String, content: MessageContent) -> WriteQueueMessage<MessageContent>{
        Self {
            uuid,
            content,
        }
    }

    pub fn get_uuid(&self) -> &str {
        &self.uuid
    }

    pub fn get_content(&self) -> &MessageContent {
        &self.content
    }
}

/// Wrapper for messages in [`ReadQueue`](crate::queue::ReadQueue).
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
        Ok(serde_json::from_str::<ReadQueueMessage<MessageContent>>(&message)?)
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
pub struct WriteQueue<'a, MessageContent: Serialize> {
    /// configured [`Pool`](r2d2::Pool) with redis [`Client`](redis::Client)
    pool: RedisPool,
    /// queue name
    name: &'a str,
    /// phantom indicating message type of queue instance
    phantom: PhantomData<MessageContent>,
}

impl<'a, MessageContent: Serialize> WriteQueue<'a, MessageContent> {
    /// Builds [`ReadQueue`](ReadQueue) with given name
    /// 
    /// # Arguments 
    /// 
    /// * pool - configured [`Pool`](r2d2::Pool) with redis [`Client`](redis::Client)
    /// * name - queue name, will be used as redis list name
    pub fn build(pool: RedisPool, name: &'a str,) -> Self {
        Self {
            name,
            pool,
            phantom: PhantomData,
        }
    }

    /// Publishes string message directly to the used res channel. This method is only a local helper.
    ///
    /// # Errors
    /// 
    /// Returns [`r2d2::Error`](r2d2::Error) when getting connection fails. See [`Pool::get()`](r2d2::Pool::get)
    /// 
    /// Returns [`RedisError`](redis::RedisError) when pushing to queue fails
    fn publish_str(&mut self, response: &str) -> Result<(), Box<dyn Error>> {
        let mut conn = self.pool.get()?;
        conn.lpush::<&str, &str, ()>(self.name, response)?;

        Ok(())
    }

    /// Publishes task to the queue. Uses queue name, which may be accessed using `WriteQueue::get_name(&self)`
    ///
    /// # Errors
    /// 
    /// Returns [`r2d2::Error`](r2d2::Error) when getting connection fails. See [`Pool::get()`](r2d2::Pool::get)
    /// 
    /// Returns [`RedisError`](redis::RedisError) when pushing to queue fails.
    ///
    /// Returns [`serde_json::Error`](serde_json::Error) when stringifying json fails. See [`serde_json::to_string()`](serde_json::to_string)
    pub fn publish(&mut self, message_content: &MessageContent) -> Result<(), Box<dyn Error>> {
        let message = WriteQueueMessage::new(Uuid::new_v4().to_string(), message_content);

        let json = serde_json::to_string(&message)?;

        self.publish_str(&json)
    }

    /// Queue name getter.
    pub fn get_name(&self) -> &'a str {
        self.name
    }
}

/// Read only queue. 

/// # Timeout
/// This queue has timeout, which is used only in blocking operations. After this timeout,
/// operation returns error.
/// 
/// For writing use `WriteQueue`
pub struct ReadQueue<'a, MessageContent: DeserializeOwned> {
    /// configured [`Pool`](r2d2::Pool) with redis [`Client`](redis::Client)
	pool: RedisPool,
    /// blocking rquests timeout
	timeout: u32,
    /// queue name
    name: &'a str,
    /// phantom indicating message type of queue instance
    phantom: PhantomData<MessageContent>,
}

impl<'a, MessageContent: DeserializeOwned> ReadQueue<'a, MessageContent> {

    /// Builds a queue with given timeout and name.
    /// 
    /// # Arguments 
    /// 
    /// * pool - configured r2d2 pool with redis connection
    /// * name - queue name, will be used as redis list name
    /// * timeout - blocking requests timeout in milliseconds or None for infinite timeout
	pub fn build(pool: RedisPool, name: &'a str, timeout: Timeout) -> Self {
		// maps None as 0, because redis uses 0 as infinite timeout
		let timeout: u32 = timeout.map_or(0, |t| t.get());

        Self {
            name,
            pool,
            timeout,
            phantom: PhantomData
        }
    }

    /// Returns the next message in queue or error if it wasn't found
    ///
    /// # Errors
    /// Returns [`r2d2::Error`](r2d2::Error) when getting connection fails. See [`Pool::get()`](r2d2::Pool::get)
    /// 
    /// Returns [`RedisError`](redis::RedisError) when reading fails or there is no object available.
    fn next_str(&mut self) -> Result<String, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        Ok(conn.rpop::<&str, String>(self.name, NonZeroUsize::new(1))?)
    }

    /// Blocking read next message from queue. If no message is available blocks thread and waits for timeout or indefinitely.
    ///
    /// # Errors
    /// 
    /// Returns [`r2d2::Error`](r2d2::Error) when getting connection fails. See [`Pool::get()`](r2d2::Pool::get)
    /// 
    /// Returns [`RedisError`](redis::RedisError) when reading fails or timeout exceeds
    /// 
    /// Returns [`Error`](std::io::Error) when redis returns invalid data. It is not possible (theoretically).
    fn next_b_str(&mut self) -> Result<String, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        let timeout = ms_to_float_s(self.timeout);
         // return type of redis blocking pop is ["queue_name", "queue_elem"], 0.0 timeout is infinite
        let res = conn.brpop::<&str, Vec<String>>(self.name, timeout)?;

        res.get(1)
            .map(|s| s.clone())
            .ok_or(IOError::new(IOErrorKind::InvalidData, "Invalid redis message.").into())
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
    pub fn next(&mut self) -> Result<ReadQueueMessage<MessageContent>, Box<dyn Error>> {
        let msg = self.next_str()?;

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
    pub fn b_next(&mut self) -> Result<ReadQueueMessage<MessageContent>, Box<dyn Error>> {
        let msg = self.next_b_str()?;

        Ok(ReadQueueMessage::from_str(msg)?)
    }
}

impl<'a, MessageContent: DeserializeOwned> Iterator for ReadQueue<'a, MessageContent> {
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

/// Converts miliseconds in u32 to seconds in f64
fn ms_to_float_s(ms: u32) -> f64 {
    (ms as f64) / 1000.0
}
