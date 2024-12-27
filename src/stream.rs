use crate::RedisPool;

pub struct ReadStream {
	pool: RedisPool,
}

impl ReadStream {

}

pub struct WriteStream<'a> {
	pool: RedisPool,
	name: &'a str
}

impl WriteStream {
	pub fn new(pool: RedisPool, name: &'a str) -> Self {
		Self {
			name,
			pool
		}
	}

	pub fn publish(&self) -> StreamResult {

	}

	pub fn publish_err(&self) -> StreamResult {
		
	}
}

// todo move to streams
// #[derive(Serialize, Deserialize)]
// pub struct ErrorMessage<MessageContent> {
//     uuid: String,
//     code: String,
//     message: String,
//     content: MessageContent,
// }

// impl<MessageContent> ErrorMessage<MessageContent> {
// 	pub fn new<MessageContent>(uuid: String, code: String, message: String, content: MessageContent) -> Self<MessageContent> {
// 		Self {
// 			uuid,
// 			code,
// 			message,
// 			content,
// 		}
// 	}
// }