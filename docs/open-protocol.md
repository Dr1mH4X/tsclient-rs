# OpenProtocol Specification (`v1`)

OpenProtocol is a lightweight, JSON-based application-layer convention built on top of TeamSpeak's `PluginCommand` (`send_plugin_command` / `notifyplugincmd`) mechanism. It enables TeamSpeak clients and bots powered by `tsclient-rs` to discover each other's identities, capabilities, and available slash commands without requiring server-side plugins or out-of-band services.

## Transport

- **Mechanism**: TeamSpeak `PluginCommand` (`tsclient_rs::send_plugin_command` and `client.on_plugin_command`)
- **Encoding**: Compact UTF-8 JSON object
- **Versioning**: Every payload includes `"v": 1`
- **Plugin Channels**:
  - `tsclient:identity` — Peer identity announcement, unicast query, and acknowledgement
  - `tsclient:commands` — On-demand bot slash command discovery
- **Self-Filtering**: Receivers **MUST** ignore incoming `PluginCommandEvent`s where `event.invoker_id` equals their own `client_id`.

---

## 1. Identity Discovery (`tsclient:identity`)

### Overview

1. **Channel Broadcast (`announce`)**: When a client joins a channel (on initial connect or after moving channels), it broadcasts an `announce` payload to `PluginCommandTarget::CurrentChannel`.
2. **One-Way Server Beacon (`announce` with `"noreply": true`)**: Optionally, once on initial join, a client or bot may send a single `announce` payload with `"noreply": true` to `PluginCommandTarget::Server`. Receivers update their identity cache without replying `announce_ack`, preventing reply storms across large servers. `noreply` applies only to `announce` and defaults to `false` (omitted when `false`); explicit `query` requests always permit `announce_ack` replies.
3. **Unicast Query (`query`)**: When a client needs the identity of a specific peer (for example, when opening a direct message window with an unrecognized peer, or via a paced background discovery queue), it sends a `query` payload to `PluginCommandTarget::Client` targeting that peer's `client_id`.
4. **Unicast Acknowledgement (`announce_ack`)**: Upon receiving a normal `announce` (`noreply` omitted or `false`) or a `query`, a peer replies with an `announce_ack` payload to `PluginCommandTarget::Client` targeting `event.invoker_id` (subject to per-peer cooldowns).

### Payloads

#### `announce` / `announce_ack`

```json
{
  "v": 1,
  "action": "announce",
  "client_type": "bot",
  "app_name": "TeamSpeakClaw",
  "app_version": "0.5.0",
  "capabilities": ["chat:slash_commands"]
}
```

| Field | Type | Description |
| --- | --- | --- |
| `v` | `u32` | Protocol version (`1`) |
| `action` | `string` | `"announce"` (broadcast) or `"announce_ack"` (unicast reply) |
| `client_type` | `string` | Client category, e.g. `"flspeak"`, `"bot"` |
| `app_name` | `string` | Human-readable application name |
| `app_version` | `string` | Application version string |
| `capabilities` | `string[]?` | Optional list of capability tags supported by this peer (omitted when empty) |
| `noreply` | `bool?` | `announce` only: when `true`, receivers update cache without sending `announce_ack` (default `false`, omitted when `false`) |

#### `query`

```json
{
  "v": 1,
  "action": "query"
}
```

### Known Capabilities

| Capability | Meaning |
| --- | --- |
| `chat:slash_commands` | Peer exposes bot slash commands via `tsclient:commands` |

---

## 2. Bot Slash Command Discovery (`tsclient:commands`)

When a peer advertises `"chat:slash_commands"` in its `capabilities`, clients may query its supported commands on demand.

### Request (`list_req`)

Sent via `PluginCommandTarget::Client` to the bot's `client_id`:

```json
{
  "v": 1,
  "action": "list_req"
}
```

### Response (`list_ack`)

Sent via `PluginCommandTarget::Client` back to `event.invoker_id`:

```json
{
  "v": 1,
  "action": "list_ack",
  "command_prefix": "!",
  "commands": [
    {
      "name": "replay",
      "args": "[seconds] [@nickname]",
      "desc": "Replay recent voice segments"
    },
    {
      "name": "skip",
      "desc": "Skip current track"
    }
  ]
}
```

| Field | Type | Description |
| --- | --- | --- |
| `v` | `u32` | Protocol version (`1`) |
| `action` | `string` | `"list_req"` or `"list_ack"` |
| `command_prefix` | `string?` | Optional command trigger prefix declared by the bot (e.g. `"!"`, `"/"`); omitted or empty defaults to `"/"` |
| `commands` | `object[]` | List of command descriptors |
| `commands[].name` | `string` | Command name without leading prefix (e.g. without `/` or `!`) |
| `commands[].args` | `string?` | Optional argument hint (omitted when absent or empty) |
| `commands[].desc` | `string?` | Short command description (receivers default omitted values to `""`) |

---

## 3. Rate Limiting & Defensive Limits

To avoid flooding the TeamSpeak server or UI with redundant plugin commands:

- **Self-announce debounce**: At least **2 seconds** between channel `announce` broadcasts.
- **Per-peer reply cooldowns** (keyed by `invoker_uid`, falling back to `invoker_id` if UID is empty):
  - `tsclient:identity` `announce` → `announce_ack`: **10 seconds**
  - `tsclient:identity` `query` → `announce_ack`: **5 seconds**
  - `tsclient:commands` `list_req` → `list_ack`: **5 seconds**
- **Cross-channel background discovery limits**:
  - Background directory probing must pace unicast `query` requests (e.g. 1 peer per `800ms–1s`), probe each peer UID at most once per connection, respect a server size cap (default `<= 64` peers), remove queued peers immediately upon receiving an inbound `announce` or `announce_ack`, and halt/clear the queue immediately if the server returns an anti-flood error (`id=524`).
- **Field truncation & packet budget**:
  - Keep both the raw JSON payload and the full TS3-escaped `plugincmd` wire line strictly under the TeamSpeak packet limit (`< 1024` bytes).
  - Receivers should sanitize and truncate untrusted peer fields (strip control characters, strip accidental leading prefixes from `commands[].name`, max 32 chars per identity string field, max 16 capabilities, max 12–16 commands, `command_prefix` ≤ 8 chars, `name` ≤ 24 chars, `args` ≤ 32 chars, `desc` ≤ 64 chars).

---

## 4. `tsclient-rs` Integration (`open-protocol` feature)

Enable the optional `open-protocol` feature in `Cargo.toml`:

```toml
tsclient-rs = { version = "0.2.10", features = ["open-protocol"] }
```

### Managed Bot / Client Integration (`OpenProtocolHandle`)

```rust
use tsclient_rs::open_protocol::{
    BotCommand, BotCommandList, ClientKind, OpenProtocolConfig, OpenProtocolHandle, PeerIdentity,
};
use tsclient_rs::{Client, ClientOptions, generateIdentity};

let mut client = Client::new(
    generateIdentity(8),
    "127.0.0.1:9987".to_string(),
    "MyBot".to_string(),
    ClientOptions::default(),
);

let config = OpenProtocolConfig::new(PeerIdentity::new(
    ClientKind::Bot,
    "MyBot",
    env!("CARGO_PKG_VERSION"),
))
.with_commands(
    BotCommandList::new(vec![
        BotCommand::new("replay", "Replay recent voice segments")
            .with_args("[seconds] [@nickname]"),
    ])
    .with_prefix("!"),
);

// Automatically handles self-filtering, announce debounce, ack cooldowns,
// global anti-flood token bucket, wire-budget encoding, and list_req replies.
let open_protocol = OpenProtocolHandle::attach(&client, config);

client.connect().await?;
client.wait_connected(None).await?;
```

### Embedded Sans-IO Core (`OpenProtocolEngine`)

Single-threaded actors or custom state loops can drive [`OpenProtocolEngine`](../src/open_protocol/engine.rs) directly via synchronous `on_connected`, `on_self_moved`, `on_plugin_command`, `request_query_peer`, `request_fetch_commands`, `on_timer`, and `on_send_outcome` calls.

### Low-Level Manual Transport (Without `open-protocol` Feature)

```rust
use std::sync::Arc;
use tsclient_rs::{Event, PluginCommandTarget, send_plugin_command};

client.on_plugin_command(Arc::new(|event| {
    if let Event::PluginCommand(cmd) = event {
        if cmd.name == "tsclient:identity" {
            // Parse cmd.data, ignore self (cmd.invoker_id == self_id),
            // and reply with PluginCommandTarget::Client + Some(cmd.invoker_id).
        }
    }
}));
```

## Reference Implementations

- [FlSpeak](https://github.com/Dr1mH4X/FlSpeak) (Client)
- [TeamSpeakClaw](https://github.com/Dr1mH4X/TeamSpeakClaw) (Bot)
