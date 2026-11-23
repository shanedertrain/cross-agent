//! `xagent`: the cross-agent messaging daemon and its command-line client.

mod client_config;
mod config;
mod doctor;
mod hook;
mod mcp;

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use xagent_core::protocol::{
    Call, DmSend, Gc, InboxList, MessageRead, MessageReply, MessageSend, Recover, Retention,
    SessionOpen, ThreadRead, TopicCreate, TopicHistory, TopicList, TopicRef,
};
use xagent_proto::{AgentId, Channel, Client as ClientKind, Conversation, Error, Kind, Priority};

use crate::config::Config;
use xagent_core::client::Client;

#[derive(Debug, Parser)]
#[command(
    name = "xagent",
    version,
    about = "Cross-agent messaging over NATS JetStream"
)]
struct Cli {
    /// Config file (default: `$XAGENT_CONFIG`, then `~/.config/xagent/config.toml`).
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the daemon (logs JSON to stderr).
    Daemon,
    /// Daemon health: NATS, JetStream, sessions, outbox.
    Status,
    /// Check installation and connectivity: config, NATS, daemon, wake transports.
    Doctor,
    /// Print the MCP entry and hooks stanza for a client's configuration.
    ClientConfig {
        #[arg(long)]
        client: client_config::ClientName,
    },
    /// Client hook (reads the hook JSON on stdin; never fails the client).
    Hook {
        #[arg(value_enum)]
        name: hook::Hook,
    },
    /// MCP server for an agent client (stdio): relays to the daemon, which serves the tools.
    Mcp {
        /// claude-code (or claude), codex, dsh.
        #[arg(long)]
        client: ClientKind,
    },
    /// Open a session for a conversation and show its identity.
    Whoami(SessionArgs),
    /// Unread counters of a conversation's inbox.
    Inbox(SessionArgs),
    /// Inbox headers (no bodies), oldest first.
    List {
        #[command(flatten)]
        session: SessionArgs,
        /// Include messages already read.
        #[arg(long)]
        all: bool,
        #[arg(long)]
        limit: Option<u32>,
        /// Only messages of this topic.
        #[arg(long)]
        topic: Option<String>,
        /// Only topic messages (chat) or only DMs (dm).
        #[arg(long, value_parser = parse_channel)]
        channel: Option<Channel>,
    },
    /// Read messages by id (marks them read).
    Read {
        #[command(flatten)]
        session: SessionArgs,
        #[arg(required = true)]
        ids: Vec<String>,
        /// Leave the messages unread.
        #[arg(long)]
        keep_unread: bool,
    },
    /// Send a direct message to an agent id.
    Dm {
        #[command(flatten)]
        session: SessionArgs,
        /// Recipient agent id (see `whoami`).
        #[arg(long)]
        to: AgentId,
        #[arg(long)]
        summary: String,
        #[arg(long, default_value = "")]
        body: String,
        #[arg(long, default_value = "message", value_parser = parse_kind)]
        kind: Kind,
        #[arg(long, default_value = "normal", value_parser = parse_priority)]
        priority: Priority,
    },
    /// Topics: create, list, subscribe, unsubscribe, subscriptions, history.
    Topic {
        #[command(subcommand)]
        command: TopicCommand,
    },
    /// Post a message to a topic.
    Send {
        #[command(flatten)]
        session: SessionArgs,
        #[arg(long)]
        topic: String,
        #[command(flatten)]
        content: ContentArgs,
    },
    /// Reply to a message in its thread (its topic, or the other party of a DM).
    Reply {
        #[command(flatten)]
        session: SessionArgs,
        /// The message replied to.
        id: String,
        #[command(flatten)]
        content: ContentArgs,
    },
    /// Read a thread in order (marks it read).
    Thread {
        #[command(flatten)]
        session: SessionArgs,
        thread_id: String,
        #[arg(long)]
        limit: Option<u32>,
        /// Leave the messages unread.
        #[arg(long)]
        keep_unread: bool,
    },
    /// Delete local copies older than this host's retention (`[retention] local_days`, capped by
    /// the broker's).
    Gc {
        /// Report what would be deleted.
        #[arg(long)]
        dry_run: bool,
    },
    /// Show message retention: the broker's (shared by every host) and this host's local copies.
    Retention {
        /// Set the broker's retention to this many days, for every host. The broker deletes
        /// messages older than that at once.
        #[arg(long)]
        days: Option<u32>,
    },
    /// Rebuild inboxes from JetStream: re-read the last N hours as unread.
    Recover {
        #[arg(long)]
        hours: u32,
        /// Agent id to recover (repeatable); default: every agent last registered from this host.
        #[arg(long = "agent")]
        agents: Vec<AgentId>,
    },
}

#[derive(Debug, Subcommand)]
enum TopicCommand {
    /// Register a new topic.
    Create {
        #[command(flatten)]
        session: SessionArgs,
        name: String,
        #[arg(long, default_value = "")]
        description: String,
    },
    /// List topics (marks the ones subscribed).
    List {
        #[command(flatten)]
        session: SessionArgs,
        /// Only this topic and its children.
        #[arg(long)]
        prefix: Option<String>,
        /// Include archived topics.
        #[arg(long)]
        archived: bool,
    },
    /// Subscribe from now on (earlier messages are not delivered; see `history`).
    Subscribe {
        #[command(flatten)]
        session: SessionArgs,
        topic: String,
    },
    Unsubscribe {
        #[command(flatten)]
        session: SessionArgs,
        topic: String,
    },
    /// The conversation's topic subscriptions.
    Subscriptions(SessionArgs),
    /// The newest messages of a topic (headers; bodies with --body).
    History {
        #[command(flatten)]
        session: SessionArgs,
        topic: String,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        body: bool,
    },
}

/// The content of a message.
#[derive(Debug, Args)]
struct ContentArgs {
    #[arg(long)]
    summary: String,
    #[arg(long, default_value = "")]
    body: String,
    #[arg(long, default_value = "message", value_parser = parse_kind)]
    kind: Kind,
    #[arg(long, default_value = "normal", value_parser = parse_priority)]
    priority: Priority,
}

/// The conversation a session-scoped command acts as.
#[derive(Debug, Args)]
struct SessionArgs {
    /// The client's native conversation id.
    #[arg(long)]
    conversation: Conversation,
    /// claude-code (or claude), codex, dsh.
    #[arg(long)]
    client: ClientKind,
    /// The conversation's working directory (joins its project's root topic).
    #[arg(long)]
    cwd: Option<PathBuf>,
}

impl ContentArgs {
    fn send(self, topic: String) -> Call {
        Call::MessageSend(MessageSend {
            topic,
            summary: self.summary,
            body: self.body,
            kind: self.kind,
            priority: self.priority,
            mentions: Vec::new(),
            refs: Vec::new(),
        })
    }

    fn reply(self, message_id: String) -> Call {
        Call::MessageReply(MessageReply {
            message_id,
            summary: self.summary,
            body: self.body,
            kind: self.kind,
            priority: self.priority,
            mentions: Vec::new(),
            refs: Vec::new(),
        })
    }
}

fn parse_kind(s: &str) -> Result<Kind, String> {
    match Kind::from(s) {
        Kind::Other(other) => Err(format!("unknown kind {other:?}")),
        known => Ok(known),
    }
}

fn parse_channel(s: &str) -> Result<Channel, String> {
    serde_json::from_value(serde_json::Value::String(s.to_owned())).map_err(|e| e.to_string())
}

fn parse_priority(s: &str) -> Result<Priority, String> {
    serde_json::from_value(serde_json::Value::String(s.to_owned())).map_err(|e| e.to_string())
}

/// Opens a session for `session`, runs `call` as it, and closes it.
async fn as_session(
    config: &Config,
    session: SessionArgs,
    call: Option<Call>,
) -> Result<serde_json::Value, Error> {
    let mut client = Client::connect(&config.socket()).await?;
    let whoami = client
        .call(Call::SessionOpen(SessionOpen {
            conversation: session.conversation,
            client: session.client,
            cwd: session
                .cwd
                .map(std::path::absolute)
                .transpose()
                .map_err(|err| {
                    Error::new(
                        xagent_proto::ErrorCode::InvalidArgument,
                        format!("cwd: {err}"),
                    )
                })?,
        }))
        .await?;
    match call {
        Some(call) => client.call(call).await,
        None => Ok(whoami),
    }
}

/// One call that needs no session.
async fn call(config: &Config, call: Call) -> Result<serde_json::Value, Error> {
    Client::connect(&config.socket()).await?.call(call).await
}

async fn run_client(config: &Config, command: Command) -> Result<serde_json::Value, Error> {
    match command {
        Command::Daemon
        | Command::Doctor
        | Command::ClientConfig { .. }
        | Command::Hook { .. }
        | Command::Mcp { .. } => unreachable!("handled by main"),
        Command::Status => call(config, Call::Status).await,
        Command::Whoami(session) => as_session(config, session, None).await,
        Command::Inbox(session) => as_session(config, session, Some(Call::InboxStatus)).await,
        Command::List {
            session,
            all,
            limit,
            topic,
            channel,
        } => {
            let call = Call::InboxList(InboxList {
                unread_only: !all,
                limit,
                topic,
                channel,
            });
            as_session(config, session, Some(call)).await
        }
        Command::Read {
            session,
            ids,
            keep_unread,
        } => {
            let call = Call::MessageRead(MessageRead {
                ids,
                mark_read: !keep_unread,
            });
            as_session(config, session, Some(call)).await
        }
        Command::Dm {
            session,
            to,
            summary,
            body,
            kind,
            priority,
        } => {
            let call = Call::DmSend(DmSend {
                recipient: to,
                summary,
                body,
                kind,
                priority,
                mentions: Vec::new(),
                refs: Vec::new(),
            });
            as_session(config, session, Some(call)).await
        }
        Command::Topic { command } => run_topic(config, command).await,
        Command::Send {
            session,
            topic,
            content,
        } => as_session(config, session, Some(content.send(topic))).await,
        Command::Reply {
            session,
            id,
            content,
        } => as_session(config, session, Some(content.reply(id))).await,
        Command::Thread {
            session,
            thread_id,
            limit,
            keep_unread,
        } => {
            let call = Call::ThreadRead(ThreadRead {
                thread_id,
                limit,
                mark_read: !keep_unread,
            });
            as_session(config, session, Some(call)).await
        }
        Command::Gc { dry_run } => call(config, Call::Gc(Gc { dry_run })).await,
        Command::Retention { days } => call(config, Call::Retention(Retention { days })).await,
        Command::Recover { hours, agents } => {
            call(config, Call::Recover(Recover { hours, agents })).await
        }
    }
}

async fn run_topic(config: &Config, command: TopicCommand) -> Result<serde_json::Value, Error> {
    let (session, call) = match command {
        TopicCommand::Create {
            session,
            name,
            description,
        } => (
            session,
            Call::TopicCreate(TopicCreate { name, description }),
        ),
        TopicCommand::List {
            session,
            prefix,
            archived,
        } => (
            session,
            Call::TopicList(TopicList {
                prefix,
                include_archived: archived,
            }),
        ),
        TopicCommand::Subscribe { session, topic } => {
            (session, Call::TopicSubscribe(TopicRef { topic }))
        }
        TopicCommand::Unsubscribe { session, topic } => {
            (session, Call::TopicUnsubscribe(TopicRef { topic }))
        }
        TopicCommand::Subscriptions(session) => (session, Call::TopicSubscriptions),
        TopicCommand::History {
            session,
            topic,
            limit,
            body,
        } => (
            session,
            Call::TopicHistory(TopicHistory {
                topic,
                limit,
                include_body: body,
            }),
        ),
    };
    as_session(config, session, Some(call)).await
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing the SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("installing the SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

async fn run_daemon(config: &Config) -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let daemon = config.daemon()?;
    let mcp_socket = config.mcp_socket();
    let listener = xagent_mcp::bind(&mcp_socket).await?;
    let mcp = tokio::spawn(xagent_mcp::serve(
        listener,
        xagent_mcp::Settings {
            ipc_socket: daemon.socket.clone(),
            home: config.home().to_owned(),
        },
    ));
    let outcome = xagent_core::run(daemon, shutdown_signal()).await;
    mcp.abort();
    if let Err(err) = std::fs::remove_file(&mcp_socket) {
        tracing::error!(error = %err, "removing the mcp socket failed");
    }
    outcome?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let config = match Config::load(cli.config.as_deref()) {
        Ok(config) => config,
        // A hook never fails its client, whatever is wrong with the configuration.
        Err(_) if matches!(cli.command, Command::Hook { .. }) => return ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("xagent: {err:#}");
            return ExitCode::FAILURE;
        }
    };
    if let Command::Mcp { client } = cli.command {
        // Exit at once: the relay's stdin reader is a blocking thread the runtime would wait for.
        std::process::exit(match mcp::relay(&config.mcp_socket(), client).await {
            Ok(()) => 0,
            Err(err) => {
                eprintln!("xagent mcp: {err}");
                1
            }
        });
    }
    if matches!(cli.command, Command::Daemon) {
        return match run_daemon(&config).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("xagent daemon: {err:#}");
                ExitCode::FAILURE
            }
        };
    }
    match cli.command {
        Command::Hook { name } => {
            hook::run(&config, name).await;
            return ExitCode::SUCCESS;
        }
        Command::Doctor => {
            return if doctor::run(&config).await {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            };
        }
        Command::ClientConfig { client } => {
            let exe = std::env::current_exe()
                .map_or_else(|_| "xagent".to_owned(), |path| path.display().to_string());
            println!("{}", client_config::render(client, &exe));
            return ExitCode::SUCCESS;
        }
        _ => {}
    }
    match run_client(&config, cli.command).await {
        Ok(result) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&result).expect("JSON values serialize")
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("{}", serde_json::json!({ "error": err }));
            ExitCode::FAILURE
        }
    }
}
