use r2d2::Pool;
use redis::Client;
use std::error::Error;
use crate::RedisPool;

/// Creates [`RedisPool`](crate::RedisPool) using given url. 
/// 
/// # Errors
/// 
/// Returns [`RedisError`](redis::RedisError) when cannot connect to redis server.
/// 
/// Returns [`r2d2::Error`](r2d2::Error) when pool creation fails.
/// 
/// # Examples
/// ```
/// # use redis_ipc::helpers::connect;
/// # use std::env;
///
/// # let _ = dotenvy::dotenv();
/// # let url = env::var("REDIS_URL").expect("Env REDIS_URL not found");
///
/// let pool = connect(url).expect("Redis pool cannot be built.");
/// let connection = pool.get().expect("Cannot extract connection!");
/// // Connection is ready to use!
///
/// ```
pub fn connect(redis_url: String) -> Result<RedisPool, Box<dyn Error>> {
    let client = Client::open(redis_url)?;
    let pool = Pool::builder().build(client)?;
    Ok(pool)
}
    
