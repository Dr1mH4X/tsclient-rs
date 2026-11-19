use std::fmt;
use std::time::{Duration, Instant};

use crate::errors::Error;
use crate::types::PluginCommandTarget;

pub const PROTOCOL_VERSION: u32 = 1;
pub const IDENTITY_PLUGIN: &str = "tsclient:identity";
pub const COMMANDS_PLUGIN: &str = "tsclient:commands";
pub const CAP_CHAT_SLASH_COMMANDS: &str = "chat:slash_commands";

pub const MAX_PAYLOAD_BYTES: usize = 1024;
pub const MAX_IDENTITY_FIELD_CHARS: usize = 32;
pub const MAX_CAPABILITIES: usize = 16;
pub const MAX_BOT_COMMANDS: usize = 16;
pub const MAX_COMMAND_PREFIX_CHARS: usize = 8;
pub const MAX_COMMAND_NAME_CHARS: usize = 24;
pub const MAX_COMMAND_ARGS_CHARS: usize = 32;
pub const MAX_COMMAND_DESC_CHARS: usize = 64;

pub const ANNOUNCE_ACK_COOLDOWN: Duration = Duration::from_secs(10);
pub const QUERY_ACK_COOLDOWN: Duration = Duration::from_secs(5);
pub const COMMANDS_ACK_COOLDOWN: Duration = Duration::from_secs(5);
pub const SELF_ANNOUNCE_DEBOUNCE: Duration = Duration::from_secs(2);
pub const SELF_ANNOUNCE_RETRY_DELAY: Duration = Duration::from_millis(500);
pub const DEFAULT_QUERY_COOLDOWN: Duration = Duration::from_secs(30);
pub const DEFAULT_COMMANDS_FETCH_TTL: Duration = Duration::from_secs(15);
pub const DEFAULT_PROBE_INTERVAL: Duration = Duration::from_millis(900);
pub const DEFAULT_PROBE_INITIAL_DELAY: Duration = Duration::from_millis(1500);
pub const DEFAULT_MAX_AUTO_DISCOVER_PEERS: usize = 64;

pub const ACK_GLOBAL_BURST: f64 = 4.0;
pub const ACK_GLOBAL_RATE_PER_SEC: f64 = 2.0;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClientKind {
    FlSpeak,
    Bot,
    Custom(String),
}

impl ClientKind {
    pub fn parse(raw: &str) -> Option<Self> {
        let cleaned: String = raw
            .trim()
            .chars()
            .filter(|c| !c.is_control() && !c.is_whitespace())
            .take(MAX_IDENTITY_FIELD_CHARS)
            .collect();
        if cleaned.is_empty() {
            return None;
        }
        match cleaned.as_str() {
            "flspeak" => Some(Self::FlSpeak),
            "bot" => Some(Self::Bot),
            _ => Some(Self::Custom(cleaned)),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::FlSpeak => "flspeak",
            Self::Bot => "bot",
            Self::Custom(other) => other.as_str(),
        }
    }
}

impl fmt::Display for ClientKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
    pub client_type: ClientKind,
    pub app_name: String,
    pub app_version: String,
    pub capabilities: Vec<String>,
}

impl PeerIdentity {
    pub fn new(
        client_type: ClientKind,
        app_name: impl Into<String>,
        app_version: impl Into<String>,
    ) -> Self {
        Self {
            client_type,
            app_name: app_name.into(),
            app_version: app_version.into(),
            capabilities: Vec::new(),
        }
    }

    pub fn with_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.capabilities = capabilities.into_iter().map(Into::into).collect();
        self
    }

    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotCommand {
    pub name: String,
    pub args: Option<String>,
    pub desc: String,
}

impl BotCommand {
    pub fn new(name: impl Into<String>, desc: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            args: None,
            desc: desc.into(),
        }
    }

    pub fn with_args(mut self, args: impl Into<String>) -> Self {
        self.args = Some(args.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BotCommandList {
    pub command_prefix: Option<String>,
    pub commands: Vec<BotCommand>,
}

impl BotCommandList {
    pub fn new(commands: Vec<BotCommand>) -> Self {
        Self {
            command_prefix: None,
            commands,
        }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.command_prefix = Some(prefix.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenProtocolConfig {
    pub identity: PeerIdentity,
    pub commands: Option<BotCommandList>,
    pub cache_peers: bool,
    pub reply_to_bots: bool,
    pub announce_on_join: bool,
    pub server_announce_on_join: bool,
    pub auto_discover_peers: bool,
    pub probe_interval: Duration,
    pub probe_initial_delay: Duration,
    pub max_auto_discover_peers: usize,
    pub query_cooldown: Duration,
    pub commands_fetch_ttl: Duration,
}

impl OpenProtocolConfig {
    pub fn new(identity: PeerIdentity) -> Self {
        Self {
            identity,
            commands: None,
            cache_peers: true,
            reply_to_bots: true,
            announce_on_join: true,
            server_announce_on_join: false,
            auto_discover_peers: false,
            probe_interval: DEFAULT_PROBE_INTERVAL,
            probe_initial_delay: DEFAULT_PROBE_INITIAL_DELAY,
            max_auto_discover_peers: DEFAULT_MAX_AUTO_DISCOVER_PEERS,
            query_cooldown: DEFAULT_QUERY_COOLDOWN,
            commands_fetch_ttl: DEFAULT_COMMANDS_FETCH_TTL,
        }
    }

    pub fn with_commands(mut self, commands: BotCommandList) -> Self {
        self.commands = Some(commands);
        self
    }

    pub fn with_commands_opt(mut self, commands: Option<BotCommandList>) -> Self {
        self.commands = commands;
        self
    }

    pub fn with_cache_peers(mut self, enabled: bool) -> Self {
        self.cache_peers = enabled;
        self
    }

    pub fn with_reply_to_bots(mut self, enabled: bool) -> Self {
        self.reply_to_bots = enabled;
        self
    }

    pub fn with_announce_on_join(mut self, enabled: bool) -> Self {
        self.announce_on_join = enabled;
        self
    }

    pub fn with_server_announce_on_join(mut self, enabled: bool) -> Self {
        self.server_announce_on_join = enabled;
        self
    }

    pub fn with_auto_discover_peers(mut self, enabled: bool) -> Self {
        self.auto_discover_peers = enabled;
        self
    }

    pub fn with_probe_interval(mut self, interval: Duration) -> Self {
        self.probe_interval = interval;
        self
    }

    pub fn with_probe_initial_delay(mut self, delay: Duration) -> Self {
        self.probe_initial_delay = delay;
        self
    }

    pub fn with_max_auto_discover_peers(mut self, max_peers: usize) -> Self {
        self.max_auto_discover_peers = max_peers;
        self
    }

    pub fn with_query_cooldown(mut self, cooldown: Duration) -> Self {
        self.query_cooldown = cooldown;
        self
    }

    pub fn with_commands_fetch_ttl(mut self, ttl: Duration) -> Self {
        self.commands_fetch_ttl = ttl;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CooldownAction {
    IdentityAnnounce,
    IdentityQuery,
    CommandsList,
}

impl CooldownAction {
    pub fn cooldown(self) -> Duration {
        match self {
            Self::IdentityAnnounce => ANNOUNCE_ACK_COOLDOWN,
            Self::IdentityQuery => QUERY_ACK_COOLDOWN,
            Self::CommandsList => COMMANDS_ACK_COOLDOWN,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendReceipt {
    Announce {
        attempt_at: Instant,
        seq_at_start: u64,
        allow_failure_retry: bool,
    },
    ServerBeacon,
    Ack {
        peer: String,
        action: CooldownAction,
        acquired_at: Instant,
    },
    Query {
        peer: String,
        acquired_at: Instant,
    },
    ProbeQuery {
        peer_uid: String,
        acquired_at: Instant,
    },
    CommandsFetch {
        peer_uid: String,
        started_at: Instant,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendOutcome {
    Succeeded,
    Failed { flood: bool },
}

impl SendOutcome {
    pub fn from_result(result: &Result<(), Error>) -> Self {
        match result {
            Ok(()) => Self::Succeeded,
            Err(err) => Self::Failed {
                flood: err.is_flood_error(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundPluginCommand {
    pub plugin: &'static str,
    pub data: String,
    pub target: PluginCommandTarget,
    pub target_id: Option<i32>,
    pub receipt: SendReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerToken {
    AnnounceTrailing,
    AnnounceRetry { expected_seq: u64 },
    ProbeNext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerRequest {
    pub delay: Duration,
    pub token: TimerToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenProtocolEvent {
    PeerIdentityUpdated {
        peer_uid: String,
        clid: i32,
        identity: PeerIdentity,
    },
    PeerCommandsUpdated {
        peer_uid: String,
        clid: i32,
        commands: BotCommandList,
    },
    ProbeHaltedByFlood,
    SendFailed {
        plugin: &'static str,
        target: PluginCommandTarget,
        target_id: Option<i32>,
        error: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineEffects {
    pub outbound: Vec<OutboundPluginCommand>,
    pub timers: Vec<TimerRequest>,
    pub events: Vec<OpenProtocolEvent>,
}
