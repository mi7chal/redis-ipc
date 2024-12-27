use r2d2::Pool;
use redis::Client;
use std::error::Error;
use crate::RedisPool;

pub fn connect(redis_url: String) -> Result<RedisPool, Box<dyn Error>> {
    let client = Client::open(redis_url)?;
    let pool = Pool::builder().build(client)?;
    Ok(pool)
}