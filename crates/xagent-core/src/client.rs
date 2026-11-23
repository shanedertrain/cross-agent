//! A minimal client of the daemon socket: one connection, one request line and one response line
//! at a time. The CLI and the MCP server (which runs inside the daemon) both speak through it.

use std::path::Path;

use crate::protocol::{Call, Request, Response};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use xagent_proto::{Error, ErrorCode};

#[derive(Debug)]
pub struct Client {
    stream: BufReader<UnixStream>,
    next_id: u64,
}

impl Client {
    pub async fn connect(socket: &Path) -> Result<Self, Error> {
        let stream = UnixStream::connect(socket).await.map_err(|err| {
            Error::new(
                ErrorCode::DaemonUnavailable,
                format!(
                    "cannot reach the xagent daemon at {}: {err}",
                    socket.display()
                ),
            )
        })?;
        Ok(Self {
            stream: BufReader::new(stream),
            next_id: 1,
        })
    }

    pub async fn call(&mut self, call: Call) -> Result<serde_json::Value, Error> {
        let lost = |err: std::io::Error| {
            Error::new(
                ErrorCode::DaemonUnavailable,
                format!("daemon connection lost: {err}"),
            )
        };
        let request = Request {
            id: self.next_id,
            call,
        };
        self.next_id += 1;
        let mut line = serde_json::to_vec(&request).expect("requests serialize");
        line.push(b'\n');
        self.stream.get_mut().write_all(&line).await.map_err(lost)?;
        let mut reply = String::new();
        let n = self.stream.read_line(&mut reply).await.map_err(lost)?;
        if n == 0 {
            return Err(Error::new(
                ErrorCode::DaemonUnavailable,
                "daemon closed the connection",
            ));
        }
        let response: Response = serde_json::from_str(&reply).map_err(|err| {
            Error::new(
                ErrorCode::Internal,
                format!("malformed daemon response: {err}"),
            )
        })?;
        response.into_result()
    }
}
