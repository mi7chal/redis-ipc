# Redis IPC in Rust
Simple crate which provides a few data structures useful in inter-process or service-to-service communication. 
Redis-ipc synchronously implements a few basic data structures using redis. These structures may 
be handy and efficient for simple purposes. If you need advanced or sophisticated solution this crate 
is probably not for you.

This crate is made in order to provide ipc which is:
- lightweight
- simple
- efficient

## Introduction
Every data structure uses `r2d2::Pool` with `redis::Client` in order to manage connections. Blocking and non-blocking
read operations are available. Structures also contains name, which is used as redis key. It must be the same for two
streams, two queues etc. in order to communicate with each other. It may be treated as an id.

Structures allow to set timeout, which is used in blocking operation. Thread is blocked maximum for this timeout
and when response isn't ready after timout, error is returned.

Also, ttl (time to live) is available for cache.

## Data structures
For now available structures are: task queue, cache and event stream. Each data structure may be used with custom data
type which is passed as a generic argument.

### Task queue
It provides task management based on redis list. Multiple clients may publish and consume tasks, but one task is consumed only by one 
client. Please be aware that when task is popped from queue and execution is disrupted the task is lost. 

In order to publish tasks use `WriteQueue` and for reading use `ReadQueue`. One client can't consume its own tasks.

### Cache
Cache provides temporary storage for data. It may be shared between clients, but usage with single client is also
possible. It provides saving data, blocking and non-blocking reading. Blocking reading blocks thread until element
appears or timeout happens.

### Event stream
It allows for synchronous exchanging events between processes or services. New event can be accessed with a blocking 
method and existing ones can be accessed with a non-blocking one.

Event streaming is based on redis streams, which are used for events caching. Maximum size of stream can be specified.

# Todo
- [ ] Differentiating empty results from error results,