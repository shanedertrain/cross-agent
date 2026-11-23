//! `xagent mcp`: the stdio relay an agent client launches as its MCP server. It sends the launch
//! context in one hello line, waits for the daemon's answer, then relays newline-delimited
//! JSON-RPC both ways; the daemon serves MCP on the other end.
//!
//! The relay outlives the daemon. When the daemon goes away (a restart, an upgrade), the relay
//! answers the calls left in flight and any made meanwhile with an error, reconnects, and replays
//! the client's `initialize` handshake so the client never notices beyond those errors.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, Stdout};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::time::{Instant, sleep_until, timeout};
use xagent_mcp::{Ack, Hello};
use xagent_proto::{Client, Error, ErrorCode};

/// First and longest wait between reconnect attempts.
const RETRY_FIRST: Duration = Duration::from_millis(100);
const RETRY_MAX: Duration = Duration::from_secs(2);
/// How long a reconnect waits for the daemon to answer the replayed `initialize`.
const REPLAY_TIMEOUT: Duration = Duration::from_secs(10);
/// JSON-RPC server error code for "the daemon is not connected".
const UNAVAILABLE: i64 = -32000;

fn unavailable(socket: &Path, err: &std::io::Error) -> Error {
    Error::new(
        ErrorCode::DaemonUnavailable,
        format!(
            "cannot reach the xagent daemon's MCP socket {}: {err}",
            socket.display()
        ),
    )
}

/// The parts of a JSON-RPC message the relay looks at.
#[derive(Deserialize)]
struct Frame {
    id: Option<Value>,
    method: Option<String>,
}

impl Frame {
    fn parse(line: &str) -> Option<Self> {
        serde_json::from_str(line).ok()
    }
}

/// One connection to the daemon, past its hello.
struct Daemon {
    lines: Lines<BufReader<OwnedReadHalf>>,
    write: OwnedWriteHalf,
}

impl Daemon {
    async fn connect(socket: &Path, hello: &Hello) -> Result<Self, Error> {
        let stream = UnixStream::connect(socket)
            .await
            .map_err(|e| unavailable(socket, &e))?;
        let (read, mut write) = stream.into_split();
        let mut line = serde_json::to_vec(hello).expect("hello serializes");
        line.push(b'\n');
        write
            .write_all(&line)
            .await
            .map_err(|e| unavailable(socket, &e))?;
        let mut lines = BufReader::new(read).lines();
        let answer = lines
            .next_line()
            .await
            .map_err(|e| unavailable(socket, &e))?
            .unwrap_or_default();
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
        Ok(Self { lines, write })
    }

    async fn send(&mut self, line: &str) -> std::io::Result<()> {
        self.write.write_all(line.as_bytes()).await?;
        self.write.write_all(b"\n").await
    }

    /// Replays the client's handshake on a fresh connection; the daemon's answer to
    /// `initialize` was already given to the client once and is dropped.
    async fn replay(&mut self, init: &str, initialized: Option<&str>) -> std::io::Result<()> {
        let id = Frame::parse(init).and_then(|f| f.id);
        self.send(init).await?;
        let answered = async {
            while let Some(line) = self.lines.next_line().await? {
                if Frame::parse(&line).is_some_and(|f| f.method.is_none() && f.id == id) {
                    return Ok(());
                }
            }
            Err::<(), std::io::Error>(std::io::ErrorKind::UnexpectedEof.into())
        };
        timeout(REPLAY_TIMEOUT, answered)
            .await
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))??;
        if let Some(initialized) = initialized {
            self.send(initialized).await?;
        }
        Ok(())
    }
}

/// What the relay remembers about the client's side of the session.
#[derive(Default)]
struct Session {
    initialize: Option<String>,
    initialized: Option<String>,
    /// Requests sent to the daemon and not answered yet, keyed by their id's JSON text.
    pending: HashMap<String, Value>,
}

impl Session {
    fn note_client(&mut self, line: &str, frame: &Frame) {
        match frame.method.as_deref() {
            Some("initialize") => self.initialize = Some(line.to_owned()),
            Some("notifications/initialized") => self.initialized = Some(line.to_owned()),
            _ => {}
        }
    }
}

async fn emit(stdout: &mut Stdout, line: &str) -> std::io::Result<()> {
    stdout.write_all(line.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await
}

async fn refuse(stdout: &mut Stdout, id: &Value, message: &str) -> std::io::Result<()> {
    let error = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": UNAVAILABLE, "message": message },
    });
    emit(stdout, &error.to_string()).await
}

/// Relays stdio to the daemon until the client closes stdin. The first connection must succeed;
/// after that the relay reconnects whenever the daemon goes away.
pub async fn relay(socket: &Path, client: Client) -> Result<(), Error> {
    let hello = Hello::from_process(client).map_err(|err| {
        Error::new(
            ErrorCode::InvalidArgument,
            format!("no working directory: {err}"),
        )
    })?;
    let mut daemon = Some(Daemon::connect(socket, &hello).await?);
    let mut session = Session::default();
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    let mut retry_wait = RETRY_FIRST;
    let mut retry_at = Instant::now();
    let io_err = |e: std::io::Error| Error::new(ErrorCode::Internal, format!("relay stdio: {e}"));

    loop {
        tokio::select! {
            line = stdin.next_line() => {
                let Some(line) = line.map_err(io_err)? else {
                    // Client closed stdin: half-close and let the daemon finish its answers.
                    if let Some(mut d) = daemon {
                        let _ = d.write.shutdown().await;
                        while let Ok(Some(line)) = d.lines.next_line().await {
                            emit(&mut stdout, &line).await.map_err(io_err)?;
                        }
                    }
                    return Ok(());
                };
                let frame = Frame::parse(&line);
                if let Some(frame) = &frame {
                    session.note_client(&line, frame);
                }
                let request_id = frame.and_then(|f| f.method.and(f.id));
                let sent = match &mut daemon {
                    Some(d) => d.send(&line).await.is_ok(),
                    None => false,
                };
                match (sent, request_id) {
                    (true, Some(id)) => {
                        session.pending.insert(id.to_string(), id);
                    }
                    (false, Some(id)) => {
                        refuse(&mut stdout, &id, "the xagent daemon is restarting; retry shortly")
                            .await
                            .map_err(io_err)?;
                    }
                    _ => {}
                }
            }
            line = async { daemon.as_mut().expect("guarded").lines.next_line().await },
                if daemon.is_some() =>
            {
                if let Ok(Some(line)) = line {
                    if let Some(Frame { id: Some(id), method: None }) = Frame::parse(&line) {
                        session.pending.remove(&id.to_string());
                    }
                    emit(&mut stdout, &line).await.map_err(io_err)?;
                    continue;
                }
                tracing::warn!("xagent daemon connection closed; reconnecting");
                daemon = None;
                for (_, id) in session.pending.drain() {
                    refuse(&mut stdout, &id, "the xagent daemon restarted during this call; retry it")
                        .await
                        .map_err(io_err)?;
                }
                retry_wait = RETRY_FIRST;
                retry_at = Instant::now() + retry_wait;
            }
            () = sleep_until(retry_at), if daemon.is_none() => {
                let reconnected = match Daemon::connect(socket, &hello).await {
                    Ok(mut d) => match &session.initialize {
                        Some(init) => d
                            .replay(init, session.initialized.as_deref())
                            .await
                            .is_ok()
                            .then_some(d),
                        None => Some(d),
                    },
                    Err(_) => None,
                };
                if reconnected.is_some() {
                    tracing::info!("reconnected to the xagent daemon");
                    daemon = reconnected;
                } else {
                    retry_wait = (retry_wait * 2).min(RETRY_MAX);
                    retry_at = Instant::now() + retry_wait;
                }
            }
        }
    }
}
