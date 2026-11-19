//! OpenProtocol (`v1`) 支持模块（需启用 `open-protocol` feature）。
//!
//! 提供两层接口：
//! - 无 IO 核心层（[`OpenProtocolEngine`]、编解码与限流器），适合嵌入单线程 Actor；
//! - 异步托管层（[`OpenProtocolHandle`]），一行挂载到 `Arc<Client>` 自动处理收发与防刷限流。

mod codec;
mod engine;
mod limiter;
mod service;
mod types;

pub use codec::{
    InboundMessage, fits_wire_budget, parse_inbound_message, sanitize_bot_command_list,
    sanitize_bot_commands, sanitize_command_prefix, sanitize_identity, wire_command_len,
};
pub use engine::OpenProtocolEngine;
pub use limiter::{
    AckRateLimiter, AnnounceAttemptMode, AnnounceDecision, AnnounceState, InflightCommands,
    QueryRateLimiter,
};
pub use service::OpenProtocolHandle;
pub use types::{
    ACK_GLOBAL_BURST, ACK_GLOBAL_RATE_PER_SEC, ANNOUNCE_ACK_COOLDOWN, BotCommand, BotCommandList,
    CAP_CHAT_SLASH_COMMANDS, COMMANDS_ACK_COOLDOWN, COMMANDS_PLUGIN, ClientKind, CooldownAction,
    DEFAULT_COMMANDS_FETCH_TTL, DEFAULT_QUERY_COOLDOWN, EngineEffects, IDENTITY_PLUGIN,
    MAX_BOT_COMMANDS, MAX_CAPABILITIES, MAX_COMMAND_ARGS_CHARS, MAX_COMMAND_DESC_CHARS,
    MAX_COMMAND_NAME_CHARS, MAX_COMMAND_PREFIX_CHARS, MAX_IDENTITY_FIELD_CHARS, MAX_PAYLOAD_BYTES,
    OpenProtocolConfig, OpenProtocolEvent, OutboundPluginCommand, PROTOCOL_VERSION, PeerIdentity,
    QUERY_ACK_COOLDOWN, SELF_ANNOUNCE_DEBOUNCE, SELF_ANNOUNCE_RETRY_DELAY, SendReceipt,
    TimerRequest, TimerToken,
};
