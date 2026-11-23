//! `xagent mcp`: the stdio relay an agent client launches as its MCP server. It sends the launch
//! context in one hello line, waits for the daemon's answer, then copies bytes both ways; the
//! daemon serves MCP on the other end.

use std::path::Path;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use xagent_mcp::{Ack, Hello};
use xagent_proto::{Client, Error, ErrorCode};

fn unavailable(socket: &Path, err: &std::io::Error) -> Error {
    Error::new(
        ErrorCode::DaemonUnavailable,
        format!(
            "cannot reach the xagent daemon's MCP socket {}: {err}",
            socket.display()
        ),
    )
}

/// Relays stdio to the daemon until either side closes.
pub async fn relay(socket: &Path, client: Client) -> Result<(), Error> {
    let hello = Hello::from_process(client).map_err(|err| {
        Error::new(
            ErrorCode::InvalidArgument,
            format!("no working directory: {err}"),
        )
    })?;
    let stream = UnixStream::connect(socket)
        .await
        .map_err(|e| unavailable(socket, &e))?;
    let (read, mut write) = stream.into_split();
    let mut line = serde_json::to_vec(&hello).expect("hello serializes");
    line.push(b'\n');
    write
        .write_all(&line)
        .await
        .map_err(|e| unavailable(socket, &e))?;
    let mut reader = BufReader::new(read);
    let mut answer = String::new();
    reader
        .read_line(&mut answer)
        .await
        .map_err(|e| unavailable(socket, &e))?;
    let ack: Ack = serde_json::from_str(&answer).map_err(|_| {
        Error::new(
            ErrorCode::DaemonUnavailable,
            "the daemon closed the MCP connection during the hello",
        )
    })?;
    if !ack.ok {
        return Err(ack
            .error
            .unwrap_or_else(|| Error::new(ErrorCode::Internal, "hello refused")));
    }

    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let upstream = async {
        // Client closed stdin: half-close so the daemon ends the session.
        let _ = tokio::io::copy(&mut stdin, &mut write).await;
        let _ = write.shutdown().await;
        std::future::pending::<()>().await;
    };
    let downstream = async {
        let _ = tokio::io::copy(&mut reader, &mut stdout).await;
        let _ = stdout.flush().await;
    };
    // The daemon closing its end ends the relay, whatever stdin is doing.
    tokio::select! {
        () = upstream => {}
        () = downstream => {}
    }
    Ok(())
}
