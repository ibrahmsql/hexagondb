//! RESP Client
//!
//! TCP client for RESP protocol communication.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// RESP protocol client
pub struct RespClient {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl RespClient {
    /// Connect to a HexagonDB server
    pub fn connect(host: &str, port: u16, timeout_secs: u64) -> io::Result<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr)?;
        
        stream.set_read_timeout(Some(Duration::from_secs(timeout_secs)))?;
        stream.set_write_timeout(Some(Duration::from_secs(timeout_secs)))?;
        
        let reader = BufReader::new(stream.try_clone()?);
        Ok(RespClient { stream, reader })
    }

    /// Send a command and get response
    pub fn send_command(&mut self, parts: &[&str]) -> io::Result<RespResponse> {
        // Build RESP array
        let mut cmd = format!("*{}\r\n", parts.len());
        for part in parts {
            cmd.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
        }

        self.stream.write_all(cmd.as_bytes())?;
        self.stream.flush()?;

        self.read_response()
    }

    /// Read a RESP response
    fn read_response(&mut self) -> io::Result<RespResponse> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        
        if line.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "Connection closed",
            ));
        }

        let line = line.trim_end();
        
        match line.chars().next() {
            Some('+') => Ok(RespResponse::Simple(line[1..].to_string())),
            Some('-') => Ok(RespResponse::Error(line[1..].to_string())),
            Some(':') => {
                let num: i64 = line[1..].parse().unwrap_or(0);
                Ok(RespResponse::Integer(num))
            }
            Some('$') => self.read_bulk_string(&line[1..]),
            Some('*') => self.read_array(&line[1..]),
            _ => Ok(RespResponse::Simple(line.to_string())),
        }
    }

    fn read_bulk_string(&mut self, len_str: &str) -> io::Result<RespResponse> {
        let len: i64 = len_str.parse().unwrap_or(-1);
        
        if len < 0 {
            return Ok(RespResponse::Null);
        }

        let mut data = vec![0u8; len as usize + 2];
        self.reader.read_exact(&mut data)?;
        
        let s = String::from_utf8_lossy(&data[..len as usize]).to_string();
        Ok(RespResponse::Bulk(s))
    }

    fn read_array(&mut self, len_str: &str) -> io::Result<RespResponse> {
        let len: i64 = len_str.parse().unwrap_or(-1);
        
        if len < 0 {
            return Ok(RespResponse::Null);
        }

        let mut items = Vec::with_capacity(len as usize);
        for _ in 0..len {
            items.push(self.read_response()?);
        }
        
        Ok(RespResponse::Array(items))
    }

    /// Check if connection is alive
    pub fn ping(&mut self) -> bool {
        matches!(self.send_command(&["PING"]), Ok(RespResponse::Simple(s)) if s == "PONG")
    }
}

/// RESP response types
#[derive(Debug, Clone)]
pub enum RespResponse {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(String),
    Array(Vec<RespResponse>),
    Null,
}

impl RespResponse {
    /// Check if response is an error
    pub fn is_error(&self) -> bool {
        matches!(self, RespResponse::Error(_))
    }

    /// Check if response is null
    pub fn is_null(&self) -> bool {
        matches!(self, RespResponse::Null)
    }

    /// Get error message if error
    pub fn error_message(&self) -> Option<&str> {
        if let RespResponse::Error(msg) = self {
            Some(msg)
        } else {
            None
        }
    }
}
