use std::path::PathBuf;
use std::sync::Arc;

use crunch_common::env::parse_env;
use crunch_engine::engine::Engine;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    env_logger::init();
    let port: u16 = parse_env("kv", None, "port", 6210);
    let path: PathBuf = parse_env("kv", None, "path", "./data".into());
    let engine = Arc::new(RwLock::new(Engine::new(path).unwrap()));
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                log::warn!("connection error: {error}");
                continue;
            },
        };
        tokio::task::spawn(handle_client(engine.clone(), stream));
    }
}

enum Command {
    Get,
    Set,
    Delete,
}

impl Command {
    fn from_u8_opt(indicator: u8) -> Option<Self> {
        match indicator {
            1 => Some(Self::Get),
            2 => Some(Self::Set),
            3 => Some(Self::Delete),
            _ => None,
        }
    }
}

// TODO: Don't unwrap, and don't swallow errors.
async fn handle_client(
    engine: Arc<RwLock<Engine>>,
    mut stream: TcpStream,
) -> Result<(), io::Error> {
    loop {
        let indicator = stream.read_u8().await?;
        let Some(command) = Command::from_u8_opt(indicator) else {
            continue;
        };
        match command {
            Command::Get => {
                let key_size = stream.read_u32().await?;
                let mut key_bytes = vec![0; key_size as usize];
                stream.read_exact(&mut key_bytes).await?;
                let key = std::str::from_utf8(&key_bytes).unwrap();

                let value = engine.read().await.get(key).unwrap();
                match value {
                    Some(value) => {
                        stream.write_u8(1).await?;
                        stream.write_all(value.as_bytes()).await?;
                    },
                    None => {
                        stream.write_u8(0).await?;
                    },
                }
            },
            Command::Set => {
                let key_size = stream.read_u32().await?;
                let mut key_bytes = vec![0; key_size as usize];
                stream.read_exact(&mut key_bytes).await?;
                let key = std::str::from_utf8(&key_bytes).unwrap();

                let value_size = stream.read_u32().await?;
                let mut value_bytes = vec![0; value_size as usize];
                stream.read_exact(&mut value_bytes).await?;
                let value = std::str::from_utf8(&value_bytes).unwrap();

                let result = engine.write().await.set(key, value);
                let output = if result.is_ok() { 1 } else { 0 };
                stream.write_u8(output).await?;
            },
            Command::Delete => {
                let key_size = stream.read_u32().await?;
                let mut key_bytes = vec![0; key_size as usize];
                stream.read_exact(&mut key_bytes).await?;
                let key = std::str::from_utf8(&key_bytes).unwrap();

                let result = engine.write().await.delete(key);
                let output = if result.is_ok() { 1 } else { 0 };
                stream.write_u8(output).await?;
            },
        }
    }
}
