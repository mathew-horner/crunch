use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub enum Command {
    Get,
    Set,
    Delete,
}

impl Command {
    pub fn from_u8_opt(indicator: u8) -> Option<Self> {
        match indicator {
            1 => Some(Self::Get),
            2 => Some(Self::Set),
            3 => Some(Self::Delete),
            _ => None,
        }
    }
}

pub async fn read_command_indicator(stream: &mut TcpStream) -> Result<Option<Command>, io::Error> {
    let indicator = stream.read_u8().await?;
    Ok(Command::from_u8_opt(indicator))
}

pub async fn read_data(stream: &mut TcpStream) -> Result<Vec<u8>, io::Error> {
    let size = stream.read_u32().await?;
    let mut bytes = vec![0; size as usize];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

pub async fn write_success(stream: &mut TcpStream) -> Result<(), io::Error> {
    stream.write_u8(1).await?;
    Ok(())
}

pub async fn write_failure(stream: &mut TcpStream) -> Result<(), io::Error> {
    stream.write_u8(0).await?;
    Ok(())
}

pub async fn write_data(stream: &mut TcpStream, data: &[u8]) -> Result<(), io::Error> {
    // TODO: Bounds check this.
    let size = data.len() as u32;
    stream.write_u32(size).await?;
    stream.write_all(data).await?;
    Ok(())
}
