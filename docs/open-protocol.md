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
2. **Unicast Query (`query`)**: When a client needs the identity of a specific peer (for example, when opening a direct message window with an unrecognized peer), it sends a `query` payload to `PluginCommandTarget::Client` targeting that peer's `client_id`.
3. **Unicast Acknowledgement (`announce_ack`)**: Upon receiving an `announce` or `query`, a peer replies with an `announce_ack` payload to `PluginCommandTarget::Client` targeting `event.invoker_id` (subject to per-peer cooldowns).

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
| `capabilities` | `string[]` | Optional list of capability tags supported by this peer |

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
| `commands` | `object[]` | List of command descriptors |
| `commands[].name` | `string` | Command name without leading `/` or `!` |
| `commands[].args` | `string?` | Optional argument hint (omitted when absent) |
| `commands[].desc` | `string` | Short command description |

---

## 3. Rate Limiting & Defensive Limits

To avoid flooding the TeamSpeak server or UI with redundant plugin commands:

- **Self-announce debounce**: At least **2 seconds** between channel `announce` broadcasts.
- **Per-peer reply cooldowns** (keyed by `invoker_uid`, falling back to `invoker_id` if UID is empty):
  - `tsclient:identity` `announce` → `announce_ack`: **10 seconds**
  - `tsclient:identity` `query` → `announce_ack`: **5 seconds**
  - `tsclient:commands` `list_req` → `list_ack`: **5 seconds**
- **Field truncation & packet budget**:
  - Keep serialized payloads well under the TeamSpeak packet limit (`< 1024` bytes).
  - Receivers should sanitize and truncate untrusted peer fields (e.g., max 32 chars per identity string field, max 16 capabilities, max 12–16 commands, `name` ≤ 24 chars, `args` ≤ 32 chars, `desc` ≤ 64 chars).

---

## 4. `tsclient-rs` Integration Example

```rust
use tsclient_rs::{Client, PluginCommandTarget, send_plugin_command};

// Broadcast identity to the current channel
let payload = serde_json::json!({
    "v": 1,
    "action": "announce",
    "client_type": "bot",
    "app_name": "MyBot",
    "app_version": env!("CARGO_PKG_VERSION"),
    "capabilities": ["chat:slash_commands"],
});

send_plugin_command(
    &client,
    "tsclient:identity",
    &payload.to_string(),
    PluginCommandTarget::CurrentChannel,
    None,
)
.await?;

// Listen for incoming OpenProtocol messages
client.on_plugin_command(|event| {
    if event.name == "tsclient:identity" {
        // Parse event.data, ignore self (event.invoker_id == self_id),
        // and reply with PluginCommandTarget::Client + Some(event.invoker_id).
    }
});
```

## Reference Implementations

- [FlSpeak](https://github.com/Dr1mH4X/FlSpeak) (Client)
- [TeamSpeakClaw](https://github.com/Dr1mH4X/TeamSpeakClaw) (Bot)
