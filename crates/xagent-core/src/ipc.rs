//! The local IPC server: a Unix socket in an owner-only directory, peers checked by uid.

use std::fs;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};
use xagent_proto::{Error, ErrorCode};

use crate::daemon::Daemon;
use crate::protocol::{Call, MAX_LINE_BYTES, Request, Response};
use crate::session::{EndReason, Session};
use crate::windows::WindowId;

/// Binds the daemon socket, refusing to replace a live one.
pub(crate) async fn bind(path: &Path) -> std::io::Result<UnixListener> {
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
                format!("another xagent daemon is listening on {}", path.display()),
            ));
        }
        fs::remove_file(path)?;
    }
    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    info!(socket = %path.display(), "ipc listening");
    Ok(listener)
}

/// Accepts connections until the listener fails; each connection runs in its own task.
pub(crate) async fn serve(daemon: Arc<Daemon>, listener: UnixListener) {
    let owner = match fs::metadata(&daemon.config.socket) {
        Ok(meta) => meta.uid(),
        Err(err) => {
            error!(error = %err, "cannot stat the daemon socket");
            return;
        }
    };
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(err) => {
                warn!(error = %err, "ipc accept failed");
                continue;
            }
        };
        match stream.peer_cred() {
            Ok(cred) if cred.uid() == owner => {}
            Ok(cred) => {
                warn!(uid = cred.uid(), "rejected ipc peer of another user");
                continue;
            }
            Err(err) => {
                warn!(error = %err, "cannot read ipc peer credentials");
                continue;
            }
        }
        tokio::spawn(Connection::new(Arc::clone(&daemon), stream).run());
    }
}

/// One client connection; it becomes a session with `session_open`.
struct Connection {
    daemon: Arc<Daemon>,
    stream: BufReader<UnixStream>,
    session: Option<Arc<Session>>,
    window: Option<WindowId>,
}

impl Connection {
    fn new(daemon: Arc<Daemon>, stream: UnixStream) -> Self {
        Self {
            daemon,
            stream: BufReader::new(stream),
            session: None,
            window: None,
        }
    }

    async fn run(mut self) {
        if let Err(err) = self.serve().await {
            debug!(error = %err, "ipc connection ended");
        }
        if let Some(session) = self.session.take() {
            self.daemon
                .close_session(&session, EndReason::Disconnect)
                .await;
        }
        if let Some(window) = self.window.take() {
            self.daemon.close_window(window);
        }
    }

    async fn serve(&mut self) -> std::io::Result<()> {
        let mut line = Vec::new();
        loop {
            line.clear();
            let limit = u64::try_from(MAX_LINE_BYTES + 1).expect("line limit fits u64");
            let n = (&mut self.stream)
                .take(limit)
                .read_until(b'\n', &mut line)
                .await?;
            if n == 0 {
                return Ok(());
            }
            if line.len() > MAX_LINE_BYTES {
                let err = Error::new(ErrorCode::InvalidArgument, "request too large");
                return self.reply(Response::new(0, Err(err))).await;
            }
            let response = match serde_json::from_slice::<Request>(&line) {
                Ok(request) => Response::new(request.id, self.dispatch(request.call).await),
                Err(err) => Response::new(
                    0,
                    Err(Error::new(
                        ErrorCode::InvalidArgument,
                        format!("malformed request: {err}"),
                    )),
                ),
            };
            self.reply(response).await?;
        }
    }

    async fn reply(&mut self, response: Response) -> std::io::Result<()> {
        let mut bytes = serde_json::to_vec(&response).expect("responses serialize");
        bytes.push(b'\n');
        self.stream.get_mut().write_all(&bytes).await
    }

    async fn dispatch(&mut self, call: Call) -> Result<serde_json::Value, Error> {
        let daemon = Arc::clone(&self.daemon);
        if call.needs_session() && self.session.is_none() {
            return Err(Error::new(
                ErrorCode::NotBound,
                "no agent identity bound; call session_open first",
            ));
        }
        match call {
            Call::Ping => Ok(serde_json::json!({
                "pong": true, "host_id": daemon.config.host_id, "run_id": daemon.run_id,
            })),
            Call::Status => daemon.status(),
            Call::WindowOpen(params) => {
                if self.window.is_some() || self.session.is_some() {
                    return Err(Error::new(
                        ErrorCode::InvalidArgument,
                        "window_open must be the connection's first call, once",
                    ));
                }
                self.window = Some(daemon.open_window(params)?);
                Ok(serde_json::json!({ "ok": true }))
            }
            Call::SessionOpen(params) => {
                if self.session.is_some() {
                    return Err(Error::new(
                        ErrorCode::InvalidArgument,
                        "this connection is already bound to a conversation",
                    ));
                }
                if params.cwd.as_ref().is_some_and(|cwd| !cwd.is_absolute()) {
                    return Err(Error::new(
                        ErrorCode::InvalidArgument,
                        "cwd must be an absolute path",
                    ));
                }
                let endpoint = self.window.and_then(|w| daemon.window_endpoint(w));
                let session = daemon
                    .open_session(
                        params.conversation,
                        params.client,
                        params.cwd.as_deref(),
                        endpoint,
                    )
                    .await?;
                let whoami = Daemon::whoami(&session);
                self.session = Some(session);
                Ok(serde_json::to_value(whoami).expect("whoami serializes"))
            }
            Call::Whoami => {
                let session = self.bound();
                Ok(serde_json::to_value(Daemon::whoami(session)).expect("whoami serializes"))
            }
            Call::InboxStatus => daemon.inbox_status(self.bound()).await,
            Call::InboxList(params) => daemon.inbox_list(self.bound(), params).await,
            Call::MessageRead(params) => daemon.message_read(self.bound(), params),
            Call::DmSend(params) => daemon.dm_send(self.bound(), params).await,
            Call::StopCheck(params) => daemon.stop_check(&params),
            Call::SendmessageMirror(params) => Box::pin(daemon.sendmessage_mirror(params)).await,
            Call::SessionUpdate(params) => daemon.session_update(self.bound(), params).await,
            Call::AgentList(params) => daemon.agent_list(self.bound(), params).await,
            Call::TopicCreate(params) => daemon.topic_create(self.bound(), params).await,
            Call::TopicList(params) => daemon.topic_list(self.bound(), params).await,
            Call::TopicSubscribe(params) => daemon.topic_subscribe(self.bound(), params).await,
            Call::TopicUnsubscribe(params) => daemon.topic_unsubscribe(self.bound(), params).await,
            Call::TopicSubscriptions => daemon.topic_subscriptions(self.bound()).await,
            Call::TopicHistory(params) => daemon.topic_history(self.bound(), params).await,
            Call::MessageSend(params) => daemon.message_send(self.bound(), params).await,
            Call::MessageReply(params) => daemon.message_reply(self.bound(), params).await,
            Call::ThreadRead(params) => daemon.thread_read(self.bound(), &params),
            Call::Gc(params) => daemon.gc(params.dry_run).await,
            Call::Retention(params) => daemon.retention(params).await,
            Call::Recover(params) => daemon.recover(params).await,
        }
    }

    fn bound(&self) -> &Session {
        self.session
            .as_deref()
            .expect("dispatch checks the session before session-scoped calls")
    }
}
