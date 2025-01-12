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

pub struct Stream(pub TcpStream);

impl Stream {
    pub async fn read_command_indicator(&mut self) -> Result<Option<Command>, io::Error> {
        let indicator = self.0.read_u8().await?;
        Ok(Command::from_u8_opt(indicator))
    }

    pub async fn read_data(&mut self) -> Result<Vec<u8>, io::Error> {
        let size = self.0.read_u32().await?;
        let mut bytes = vec![0; size as usize];
        self.0.read_exact(&mut bytes).await?;
        Ok(bytes)
    }

    pub async fn write_success(&mut self) -> Result<(), io::Error> {
        self.0.write_u8(1).await?;
        Ok(())
    }

    pub async fn write_failure(&mut self) -> Result<(), io::Error> {
        self.0.write_u8(0).await?;
        Ok(())
    }

    pub async fn write_data(&mut self, data: &[u8]) -> Result<(), io::Error> {
        // TODO: Bounds check this.
        let size = data.len() as u32;
        self.0.write_u32(size).await?;
        self.0.write_all(data).await?;
        Ok(())
    }
}
