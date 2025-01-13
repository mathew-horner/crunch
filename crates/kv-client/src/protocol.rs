use std::io::{Read, Write};
use std::net::TcpStream;

use anyhow::{anyhow, Result};

#[repr(u8)]
enum Command {
    Get = 1,
    Set,
    Delete,
}

pub struct Stream(pub TcpStream);

impl Stream {
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.write_indicator(Command::Get)?;
        self.write_data(key)?;
        match self.read_outcome()? {
            1 => Ok(Some(self.read_data()?)),
            2 => Ok(None),
            _ => Err(anyhow!("operation failed")),
        }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_indicator(Command::Set)?;
        self.write_data(key)?;
        self.write_data(value)?;
        self.assert_success()
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write_indicator(Command::Delete)?;
        self.write_data(key)?;
        self.assert_success()
    }

    fn assert_success(&mut self) -> Result<()> {
        match self.read_outcome()? {
            1 => Ok(()),
            _ => Err(anyhow!("operation failed")),
        }
    }

    fn write_indicator(&mut self, command: Command) -> Result<()> {
        self.0.write_all(&[command as u8])?;
        Ok(())
    }

    fn write_data(&mut self, data: &[u8]) -> Result<()> {
        let size = data.len() as u32;
        self.0.write_all(&size.to_be_bytes())?;
        self.0.write_all(data)?;
        Ok(())
    }

    fn read_outcome(&mut self) -> Result<u8> {
        let mut outcome = [0; 1];
        self.0.read_exact(&mut outcome)?;
        Ok(outcome[0])
    }

    fn read_data(&mut self) -> Result<Vec<u8>> {
        let mut size = [0; 4];
        self.0.read_exact(&mut size)?;
        let size = u32::from_be_bytes(size);
        let mut data = vec![0; size as usize];
        self.0.read_exact(&mut data)?;
        Ok(data)
    }
}
