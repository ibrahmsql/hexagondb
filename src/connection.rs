use std::net::TcpStream;
use std::io::{Read, Write};
use tracing::{error, debug};
use crate::{interpreter, resp::{RespHandler, RespValue}};

/// Buffer for accumulating incomplete commands across reads
struct CommandBuffer {
    data: Vec<u8>,
}

impl CommandBuffer {
    fn new() -> Self {
        CommandBuffer {
            data: Vec::with_capacity(8192),
        }
    }

    /// Append new data to the buffer
    fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    /// Consume processed bytes from the front of the buffer
    fn consume(&mut self, count: usize) {
        self.data.drain(..count);
    }

    /// Get current buffer contents
    fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Check if buffer is empty
    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Clear the buffer
    fn clear(&mut self) {
        self.data.clear();
    }
}

pub fn handle_client(mut stream: TcpStream, interpreter: &mut interpreter::Interpreter) {
    let mut read_buffer = [0u8; 4096];
    let mut cmd_buffer = CommandBuffer::new();

    loop {
        let n = match stream.read(&mut read_buffer) {
            Ok(0) => return, // Connection closed
            Ok(n) => n,
            Err(e) => {
                error!("Error reading from stream: {}", e);
                return;
            }
        };

        // Append new data to command buffer
        cmd_buffer.append(&read_buffer[..n]);

        // Process all complete commands in the buffer
        loop {
            match RespHandler::parse_request(cmd_buffer.as_slice()) {
                Ok(Some((value, len))) => {
                    // Successfully parsed a command
                    cmd_buffer.consume(len);
                    
                    // Convert RESP value to arguments
                    let args = match value {
                        RespValue::Array(Some(items)) => {
                            items.into_iter().filter_map(|item| {
                                match item {
                                    RespValue::BulkString(Some(s)) => Some(s),
                                    RespValue::SimpleString(s) => Some(s),
                                    _ => None,
                                }
                            }).collect()
                        },
                        _ => Vec::new(),
                    };

                    if args.is_empty() {
                        continue;
                    }

                    let response = interpreter.exec_args(args);
                    let response_bytes = response.serialize();
                    
                    if let Err(e) = stream.write_all(response_bytes.as_bytes()) {
                        error!("Error sending response: {}", e);
                        return;
                    }
                },
                Ok(None) => {
                    // Incomplete command - wait for more data
                    // The buffer retains partial data for the next read
                    debug!("Incomplete command, waiting for more data ({} bytes buffered)", cmd_buffer.data.len());
                    break;
                },
                Err(e) => {
                    error!("Protocol error: {}", e);
                    let err_resp = RespValue::Error(format!("Protocol error: {}", e));
                    let _ = stream.write_all(err_resp.serialize().as_bytes());
                    // Clear buffer on protocol error to allow recovery
                    cmd_buffer.clear();
                    return;
                }
            }
        }

        // Prevent buffer from growing too large (DoS protection)
        if cmd_buffer.data.len() > 64 * 1024 * 1024 {
            error!("Command buffer too large, closing connection");
            let err_resp = RespValue::Error("ERR request too large".to_string());
            let _ = stream.write_all(err_resp.serialize().as_bytes());
            return;
        }
    }
}