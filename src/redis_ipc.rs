use redis::{Client, Commands};
use r2d2::{Pool, PooledConnection, Error as R2D2Error};
use std::error::Error;
use serde::{Deserialize, Serialize};
use std::io::{Error as IOError, ErrorKind};
use std::num::{NonZeroU32, NonZeroUsize};
use uuid::Uuid;
use crate::{RedisPool, RedisConnection};

// todo add uuid checking (parsing), we don't want a mess with ids


pub enum MessageStatus {
    Success,
    Error,
}

#[derive(Serialize, Deserialize)]
pub struct DuplexMessage<MessageContent> {
    uuid: String,
    status: MessageStatus,
    content: MessageContent,
}

impl<MessageContent> DuplexMessage<MessageContent> {
    pub fn new(uuid: String, content: MessageContent, status: MessageStatus) -> Self {
        Self {
            uuid,
            status,
            content
        }
    }
}

/// This structure is a custom implementation of redis request-response communication based on redis list. It uses redis-rs crate. 
/// RedisDuplex is destined to be used with simple messages only. If you need advanced solution, do not use it!! 
/// Messages use uuids in order to identify them,
/// 
///
/// # Channels creation
/// Communication uses two channels based on given name: `{given_name}:request` and `{given_name}:response`. It allows for two 
/// way communication and implementing it more efficiently.
///
/// # UUID message identification
/// This structure uses randomly generated UUID v4, in order to connect request with response. 
/// 
/// Please have in mind that there are extremely low mathematical chances of duplicating two uuids (especially v4), 
/// but message will be just ignored then. This case should be handled by caller. In the future implementing some other version of uuid
/// may be considered in order to prevent this scenario.
pub struct RedisDuplex {
    pool: Pool<Client>,
    name: String,
}

impl RedisDuplex {
    pub fn new(pool: RedisPool, name: String) -> Self {
        Self {
            pool,
            name,
        }
    }

    fn get_request_channel_name(&self) -> String {
        format!("{}:request", self.name)
    }

    fn get_response_channel_name(&self) -> String {
        format!("{}:response", self.name)
    }

    fn get_connection(&self) -> Result<RedisConnection, R2D2Error>  {
        self.pool.get()
    }

     fn respond_str(&mut self, response: String) -> Result<(), Box<dyn Error>> {
        let mut conn = self.get_connection()?;
        conn.lpush::<&str, &str, ()>(self.get_response_channel_name().as_str(), response.as_str())?;

        Ok(())
    }

    pub fn respond<MessageContent: Serialize>(&mut self, message_content: MessageContent, status: MessageStatus, target_uuid: String) -> Result<(), Box<dyn Error>> {
        let message = build_response_message(message_content, status, target_uuid);

        let json = serde_json::to_string(&message)?;

        self.respond_str(json)
    }

    fn next_str(&mut self) -> Result<String, Box<dyn Error>> {
        let mut conn = self.get_connection()?;

        conn.rpop::<&str, String>(self.get_request_channel_name().as_str(), NonZeroUsize::new(1)).into()
    }

    fn next_b_str(&mut self, timeout: Option<f64>) -> Result<String, Box<dyn Error>> {
        let mut conn = self.get_connection()?;
         // return type of redis blocking pop is ["queue_name", "queue_elem"], 0.0 timeout is infinite
        let res = conn.brpop::<&str, Vec<String>>(self.get_request_channel_name().as_str(), timeout.unwrap_or(0.0))?;

        res.get(1)
            .map(|s| s.clone())
            .ok_or(IOError::new(ErrorKind::InvalidData, "Invalid redis message.").into())
    }

    pub fn next<MessageContent: Deserialize>(&mut self) -> Result<DuplexMessage<MessageContent>, Box<dyn Error>> {
        let msg = self.next_str()?;

        parse_message(msg)
    }

    pub fn b_next<MessageContent: Deserialize>(&mut self, timeout: Option<f64>) -> Result<DuplexMessage<MessageContent>, Box<dyn Error>> {
        let msg = self.next_b_str(timeout)?;

        parse_message(msg)
    }
}

impl<MessageContent> Iterator for RedisDuplex {
    type Item = DuplexMessage<MessageContent>;

    fn next(&mut self) -> Option<Self::Item> {
        self.b_next(None).ok()
    }
}

fn build_response_message<MessageContent: Serialize>(content: MessageContent, status: MessageStatus, uuid: String) -> DuplexMessage<MessageContent> {
     DuplexMessage {
        uuid, 
        content,
        status
    }
}


fn parse_message<MessageContent: Deserialize>(message: String) -> Result<DuplexMessage<MessageContent>, Box<dyn Error>> {
    let msg = serde_json::from_str::<DuplexMessage<MessageContent>>(message.as_str())?;

    Ok(msg)
}



pub struct RedisIpcPool {
    duplex: RedisDuplex,
    timeout: Option<NonZeroU32>,
}


impl RedisIpcPool {
    pub fn build(pool: RedisPool, timeout: Option<NonZeroU32>) -> Self {
        Self {
            pool: duplex,
            timeout
        }
    }

    pub fn get_connection(&self) -> Result<RedisIpcChannelConnection, dyn Error> {}

}

impl Clone for RedisIpcPool {
    fn clone(&self) -> Self {
        todo!()
    }
}

struct RedisIpcChannelConnection;

impl RedisIpcChannelConnection {

}
