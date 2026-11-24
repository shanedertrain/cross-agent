//! The MCP server the xagent daemon hosts.
//!
//! The daemon listens on its own Unix socket (`mcp.sock`, next to the IPC socket, owner-only).
//! Clients launch `xagent mcp --client <c>`, a relay that copies stdio to that socket after one
//! hello line with its launch context ([`Hello`]). Each connection is one MCP session served by
//! rmcp, with its own conversation identity, bound at its first tool call and released when the
//! connection closes.
//!
//! The tools act through the daemon's IPC protocol on its own socket, so MCP and the CLI share
//! one session API and a session's lifetime is exactly its connection's.

mod antigravity;
mod codex;
mod hello;
mod server;

use std::fs;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use rmcp::ServiceExt;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, info, warn};
use xagent_proto::{Error, ErrorCode};

pub use hello::{Ack, CONTEXT_ENV, Hello, MAX_HELLO_BYTES};
pub use server::INSTRUCTIONS;

use crate::hello::Launch;
use crate::server::Connection;

/// What the MCP listener needs from the daemon's configuration.
#[derive(Debug, Clone)]
pub struct Settings {
    /// The daemon's IPC socket, through which each connection opens its session.
    pub ipc_socket: PathBuf,
    /// The daemon user's home: default location of `~/.claude` and `~/.codex`.
    pub home: PathBuf,
}

/// Binds the MCP socket in an owner-only directory, refusing to replace a live one.
pub async fn bind(path: &Path) -> std::io::Result<UnixListener> {
    if let Some(dir) = path.parent() {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    }
    if fs::symlink_metadata(path).is_ok() {
        if UnixStream::connect(path).await.is_ok() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AddrInUse,
                format!("another xagent daemon serves MCP on {}", path.display()),
            ));
        }
        fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    info!(socket = %path.display(), "mcp listening");
    Ok(listener)
}

/// Accepts MCP connections of the socket owner until the task is dropped.
pub async fn serve(listener: UnixListener, settings: Settings) {
    let Some(owner) = listener
        .local_addr()
        .ok()
        .and_then(|a| a.as_pathname().map(Path::to_owned))
        .and_then(|p| fs::metadata(p).ok())
        .map(|meta| meta.uid())
    else {
        warn!("cannot stat the mcp socket; not serving MCP");
        return;
    };
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(err) => {
                warn!(error = %err, "mcp accept failed");
                continue;
            }
        };
        match stream.peer_cred() {
            Ok(cred) if cred.uid() == owner => {
                tokio::spawn(handle(stream, settings.clone()));
            }
            Ok(cred) => warn!(uid = cred.uid(), "rejected mcp peer of another user"),
            Err(err) => warn!(error = %err, "cannot read mcp peer credentials"),
        }
    }
}

/// Reads the hello line; `Err` carries the refusal to send back.
async fn read_hello(
    reader: &mut BufReader<impl tokio::io::AsyncRead + Unpin>,
) -> Result<Hello, Error> {
    let mut line = Vec::new();
    let limit = u64::try_from(MAX_HELLO_BYTES + 1).expect("hello limit fits u64");
    let n = reader
        .take(limit)
        .read_until(b'\n', &mut line)
        .await
        .map_err(|err| Error::new(ErrorCode::InvalidArgument, format!("reading hello: {err}")))?;
    if n == 0 || line.len() > MAX_HELLO_BYTES || line.last() != Some(&b'\n') {
        return Err(Error::new(
            ErrorCode::InvalidArgument,
            "expected one hello line of at most 64 KiB",
        ));
    }
    serde_json::from_slice(&line).map_err(|err| {
        Error::new(
            ErrorCode::InvalidArgument,
            format!("malformed hello: {err}"),
        )
    })
}

async fn handle(stream: UnixStream, settings: Settings) {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    let hello = read_hello(&mut reader).await;
    let ack = match &hello {
        Ok(_) => Ack {
            ok: true,
            error: None,
        },
        Err(err) => Ack {
            ok: false,
            error: Some(err.clone()),
        },
    };
    let mut line = serde_json::to_vec(&ack).expect("acks serialize");
    line.push(b'\n');
    if let Err(err) = write.write_all(&line).await {
        debug!(error = %err, "mcp client left during the hello");
        return;
    }
    let hello = match hello {
        Ok(hello) => hello,
        Err(err) => {
            warn!(error = %err, "refused an mcp connection");
            return;
        }
    };
    debug!(client = %hello.client, cwd = %hello.cwd.display(), "mcp connection");
    let launch = Launch::new(&hello, &settings.home);
    let connection = Connection::new(settings.ipc_socket, launch, hello.client, hello.cwd.clone());
    connection.open().await;
    // The reader keeps any bytes buffered past the hello line.
    match connection.clone().serve((reader, write)).await {
        Ok(running) => {
            if let Err(err) = running.waiting().await {
                warn!(error = %err, "mcp session task failed");
            }
        }
        Err(err) => debug!(error = %err, "mcp initialization failed"),
    }
    connection.close().await;
}
