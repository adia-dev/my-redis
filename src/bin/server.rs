use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use mini_redis::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};

type Db = Mutex<HashMap<String, Bytes>>;
type ArcDb = Arc<Db>;
type ShardedDb = Arc<Vec<Db>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let _db: ArcDb = Arc::new(Mutex::new(HashMap::new()));
    let sharded_db = new_sharded_db(10);

    println!("Server up and running on port 6379 !");

    loop {
        let (socket, sockaddr) = listener.accept().await.unwrap();
        let sharded_db = sharded_db.clone();

        println!("Accepting a client: {}", sockaddr.port());

        println!("{:#?}", sharded_db);
        tokio::spawn(async move {
            handle_client(socket, sharded_db).await;
        });
    }
}

async fn handle_client(socket: TcpStream, sharded_db: ShardedDb) {
    use mini_redis::Command::{self, Get, Set};

    // Connection, provided by `mini-redis`, handles parsing frames from
    // the socket
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let shard_hash = get_shard_hash(cmd.key());
                let mut shard = sharded_db[(shard_hash as usize) % sharded_db.len()]
                    .lock()
                    .unwrap();
                shard.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let shard_hash = get_shard_hash(cmd.key());
                let shard = sharded_db[(shard_hash as usize) % sharded_db.len()]
                    .lock()
                    .unwrap();
                if let Some(value) = shard.get(cmd.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }
}

fn new_sharded_db(num_shards: usize) -> ShardedDb {
    let mut db = Vec::with_capacity(num_shards);

    for _ in 0..num_shards {
        db.push(Mutex::new(HashMap::new()));
    }

    Arc::new(db)
}

fn get_shard_hash(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();

    key.hash(&mut hasher);

    hasher.finish()
}
