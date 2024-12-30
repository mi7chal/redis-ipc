use std::num::{NonZeroUsize};
use crate::{RedisPool, Timeout};
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use std::error::Error;
use redis::{Commands};
use std::io::{Error as IOError, ErrorKind as IOErrorKind};
use uuid::Uuid;
use std::marker::PhantomData;

/// Wrapper for messages in queue
#[derive(Serialize)]
pub struct WriteQueueMessage<MessageContent: Serialize> {
    uuid: String,
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

/// Wrapper for messages in queue
#[derive(Deserialize)]
pub struct ReadQueueMessage<MessageContent> {
    uuid: String,
    content: MessageContent,
}

impl<MessageContent: DeserializeOwned> ReadQueueMessage<MessageContent> {
    pub fn from_str(message: String) -> Result<ReadQueueMessage<MessageContent>, Box<dyn Error>> {
        Ok(serde_json::from_str::<ReadQueueMessage<MessageContent>>(&message)?)
    }

    pub fn get_uuid(&self) -> &str {
        &self.uuid
    }

    pub fn get_content(&self) -> &MessageContent {
        &self.content
    }
}



/// Queue dedicated for adding tasks only.
///
/// For reading use ReadQueue
pub struct WriteQueue<'a, MessageContent: Serialize> {
    pool: RedisPool,
    name: &'a str,
    phantom: PhantomData<MessageContent>,
}

impl<'a, MessageContent: Serialize> WriteQueue<'a, MessageContent> {
    pub fn build(pool: RedisPool, name: &'a str,) -> Self {
        Self {
            name,
            pool,
            phantom: PhantomData,
        }
    }

    /// Publishes string message directly to the used res channel. This method is only a local helper.
    fn publish_str(&mut self, response: String) -> Result<(), Box<dyn Error>> {
        let mut conn = self.pool.get()?;
        conn.lpush::<&str, String, ()>(self.name, response)?;

        Ok(())
    }

    /// Publishes message (probably queue action result) on res channel.
    pub fn publish(&mut self, message_content: MessageContent) -> Result<(), Box<dyn Error>> {
        let message = WriteQueueMessage::new(Uuid::new_v4().to_string(), message_content);

        let json = serde_json::to_string(&message)?;

        self.publish_str(json)
    }
}

/// Queue for reading tasks.
///
pub struct ReadQueue<'a, MessageContent: DeserializeOwned> {
	pool: RedisPool,
	timeout: u32,
    name: &'a str,
    phantom: PhantomData<MessageContent>,
}

impl<'a, MessageContent: DeserializeOwned> ReadQueue<'a, MessageContent> {
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

    fn next_str(&mut self) -> Result<String, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        Ok(conn.rpop::<&str, String>(self.name, NonZeroUsize::new(1))?)
    }

    fn next_b_str(&mut self) -> Result<String, Box<dyn Error>> {
        let mut conn = self.pool.get()?;

        let timeout = ms_to_float_s(self.timeout);
         // return type of redis blocking pop is ["queue_name", "queue_elem"], 0.0 timeout is infinite
        let res = conn.brpop::<&str, Vec<String>>(self.name, timeout)?;

        res.get(1)
            .map(|s| s.clone())
            .ok_or(IOError::new(IOErrorKind::InvalidData, "Invalid redis message.").into())
    }

    /// Returns next message on queue or error if it wasn't found
    pub fn next(&mut self) -> Result<ReadQueueMessage<MessageContent>, Box<dyn Error>> {
        let msg = self.next_str()?;

        ReadQueueMessage::from_str(msg)
    }

    /// Returns next message (blocking) or error if timeout happens.
    pub fn b_next(&mut self) -> Result<ReadQueueMessage<MessageContent>, Box<dyn Error>> {
        let msg = self.next_b_str()?;

        ReadQueueMessage::from_str(msg)
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
