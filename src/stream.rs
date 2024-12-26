use crate::RedisPool;

pub struct ReadStream {
	pool: RedisPool,
}

impl ReadStream {

}

pub struct WriteStream {
	pool: RedisPool,
}

impl WriteStream {
	fn new(pool: RedisPool, name: String) -> Self {
		Self {
			name,
			pool
		}
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