use std::path::PathBuf;
use std::sync::Arc;

use crunch_common::env::parse_env;
use crunch_engine::engine::Engine;
use protocol::Command;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

mod protocol;

#[tokio::main]
async fn main() {
    env_logger::init();
    let port: u16 = parse_env("kv", None, "port", 6210);
    let path: PathBuf = parse_env("kv", None, "path", "./data".into());
    let engine = Arc::new(RwLock::new(Engine::new(path).unwrap()));
    let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    log::info!("CrunchKV server listening on port {port}");
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

// TODO: Don't unwrap, and don't swallow errors.
async fn handle_client(engine: Arc<RwLock<Engine>>, stream: TcpStream) -> Result<(), io::Error> {
    let mut stream = protocol::Stream(stream);
    loop {
        let Some(command) = stream.read_command_indicator().await? else {
            continue;
        };
        match command {
            Command::Get => {
                let data = stream.read_data().await?;
                let key = std::str::from_utf8(&data).unwrap();
                log::trace!("GET {key}");
                match engine.read().await.get(key).unwrap() {
                    Some(value) => {
                        log::trace!("got {key} = {value}");
                        stream.write_success().await?;
                        stream.write_data(value.as_bytes()).await?;
                    },
                    None => {
                        log::trace!("{key} not found");
                        stream.write_outcome(2).await?;
                    },
                }
            },
            Command::Set => {
                let key = stream.read_data().await?;
                let val = stream.read_data().await?;
                let key = std::str::from_utf8(&key).unwrap();
                let val = std::str::from_utf8(&val).unwrap();
                log::trace!("SET {key}={val}");
                match engine.write().await.set(key, val) {
                    Ok(_) => stream.write_success().await?,
                    Err(_) => stream.write_failure().await?,
                }
            },
            Command::Delete => {
                let data = stream.read_data().await?;
                let key = std::str::from_utf8(&data).unwrap();
                log::trace!("DELETE {key}");
                match engine.write().await.delete(key) {
                    Ok(_) => stream.write_success().await?,
                    Err(_) => stream.write_failure().await?,
                }
            },
        }
    }
}
