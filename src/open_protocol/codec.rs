use serde::{Deserialize, Serialize};

use crate::api::build_plugin_command_line;
use crate::types::PluginCommandTarget;

use super::types::{
    BotCommand, BotCommandList, CAP_CHAT_SLASH_COMMANDS, COMMANDS_PLUGIN, ClientKind,
    IDENTITY_PLUGIN, MAX_BOT_COMMANDS, MAX_CAPABILITIES, MAX_COMMAND_ARGS_CHARS,
    MAX_COMMAND_DESC_CHARS, MAX_COMMAND_NAME_CHARS, MAX_COMMAND_PREFIX_CHARS,
    MAX_IDENTITY_FIELD_CHARS, MAX_PAYLOAD_BYTES, PROTOCOL_VERSION, PeerIdentity,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct IdentityWirePayload {
    v: u32,
    action: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    client_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    app_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    app_version: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    capabilities: Vec<String>,
    /// 仅作用于 `announce`：为 `true` 时通知接收方只更新缓存、不回发 `announce_ack`。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    noreply: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BotCommandWire {
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    args: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    desc: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CommandsWirePayload {
    v: u32,
    action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    command_prefix: Option<String>,
    #[serde(default)]
    commands: Vec<BotCommandWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ActionOnlyPayload {
    v: u32,
    action: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundMessage {
    IdentityAnnounce {
        identity: Option<PeerIdentity>,
        noreply: bool,
    },
    IdentityQuery,
    IdentityAnnounceAck(PeerIdentity),
    CommandsListReq,
    CommandsListAck(BotCommandList),
}

fn truncate_chars(s: &str, max_chars: usize) -> String {
    s.chars().take(max_chars).collect()
}

fn strip_control_chars(s: &str) -> String {
    s.chars().filter(|c| !c.is_control()).collect()
}

fn strip_ws_and_control(s: &str) -> String {
    s.chars()
        .filter(|c| !c.is_control() && !c.is_whitespace())
        .collect()
}

fn sanitize_capabilities<'a, I>(caps: I) -> Vec<String>
where
    I: IntoIterator<Item = &'a String>,
{
    let mut out = Vec::new();
    for raw in caps {
        let cleaned = truncate_chars(&strip_ws_and_control(raw.trim()), MAX_IDENTITY_FIELD_CHARS);
        if cleaned.is_empty() || out.contains(&cleaned) {
            continue;
        }
        out.push(cleaned);
        if out.len() >= MAX_CAPABILITIES {
            break;
        }
    }
    out
}

pub fn sanitize_identity(identity: &PeerIdentity) -> PeerIdentity {
    let client_type = ClientKind::parse(identity.client_type.as_str())
        .unwrap_or_else(|| ClientKind::Custom("custom".to_string()));
    PeerIdentity {
        client_type,
        app_name: truncate_chars(
            &strip_control_chars(identity.app_name.trim()),
            MAX_IDENTITY_FIELD_CHARS,
        ),
        app_version: truncate_chars(
            &strip_control_chars(identity.app_version.trim()),
            MAX_IDENTITY_FIELD_CHARS,
        ),
        capabilities: sanitize_capabilities(&identity.capabilities),
    }
}

pub(crate) fn effective_identity(
    identity: &PeerIdentity,
    commands: Option<&BotCommandList>,
) -> PeerIdentity {
    let mut sanitized = sanitize_identity(identity);
    let has_commands = commands.is_some_and(|list| !list.commands.is_empty());
    sanitized
        .capabilities
        .retain(|cap| cap != CAP_CHAT_SLASH_COMMANDS);
    if has_commands && sanitized.capabilities.len() < MAX_CAPABILITIES {
        sanitized
            .capabilities
            .push(CAP_CHAT_SLASH_COMMANDS.to_string());
    }
    sanitized
}

pub fn sanitize_command_prefix(raw: Option<&str>) -> Option<String> {
    let cleaned = truncate_chars(&strip_ws_and_control(raw?.trim()), MAX_COMMAND_PREFIX_CHARS);
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn sanitize_command_name(raw: &str, declared_prefix: Option<&str>) -> String {
    let mut name = strip_ws_and_control(raw.trim());
    if let Some(prefix) = declared_prefix {
        while let Some(rest) = name.strip_prefix(prefix) {
            name = rest.to_string();
        }
    }
    let stripped = name.trim_start_matches(['/', '!']);
    truncate_chars(stripped, MAX_COMMAND_NAME_CHARS)
}

pub fn sanitize_bot_commands(commands: Vec<BotCommand>) -> Vec<BotCommand> {
    sanitize_bot_commands_with_prefix(commands, None)
}

fn sanitize_bot_commands_with_prefix(
    commands: Vec<BotCommand>,
    declared_prefix: Option<&str>,
) -> Vec<BotCommand> {
    let mut out = Vec::new();
    for cmd in commands {
        let name = sanitize_command_name(&cmd.name, declared_prefix);
        if name.is_empty() {
            continue;
        }
        let args = cmd
            .args
            .map(|a| truncate_chars(&strip_control_chars(a.trim()), MAX_COMMAND_ARGS_CHARS))
            .filter(|a| !a.is_empty());
        let desc = truncate_chars(
            &strip_control_chars(cmd.desc.trim()),
            MAX_COMMAND_DESC_CHARS,
        );
        out.push(BotCommand { name, args, desc });
        if out.len() >= MAX_BOT_COMMANDS {
            break;
        }
    }
    out
}

pub fn sanitize_bot_command_list(list: BotCommandList) -> BotCommandList {
    let command_prefix = sanitize_command_prefix(list.command_prefix.as_deref());
    let commands = sanitize_bot_commands_with_prefix(list.commands, command_prefix.as_deref());
    BotCommandList {
        command_prefix,
        commands,
    }
}

/// 计算指定载荷构造为完整 TS3 `plugincmd` 转义命令后的线缆字节数。
pub fn wire_command_len(
    plugin: &str,
    data: &str,
    target: PluginCommandTarget,
    target_id: Option<i32>,
) -> Option<usize> {
    build_plugin_command_line(plugin, data, target, target_id)
        .ok()
        .map(|line| line.len())
}

/// 判断 JSON 原文与 TS3 转义后线缆命令是否均严格小于 `MAX_PAYLOAD_BYTES` (1024)。
pub fn fits_wire_budget(
    plugin: &str,
    data: &str,
    target: PluginCommandTarget,
    target_id: Option<i32>,
) -> bool {
    if data.len() >= MAX_PAYLOAD_BYTES {
        return false;
    }
    wire_command_len(plugin, data, target, target_id).is_some_and(|len| len < MAX_PAYLOAD_BYTES)
}

pub(crate) fn encode_identity_announce(
    identity: &PeerIdentity,
    noreply: bool,
    target: PluginCommandTarget,
    target_id: Option<i32>,
) -> Option<String> {
    encode_identity_payload("announce", identity, noreply, target, target_id)
}

pub(crate) fn encode_identity_ack(identity: &PeerIdentity, target_id: i32) -> Option<String> {
    // noreply 仅允许用于 announce，ack 固定为 false。
    encode_identity_payload(
        "announce_ack",
        identity,
        false,
        PluginCommandTarget::Client,
        Some(target_id),
    )
}

fn encode_identity_payload(
    action: &str,
    identity: &PeerIdentity,
    noreply: bool,
    target: PluginCommandTarget,
    target_id: Option<i32>,
) -> Option<String> {
    let sanitized = sanitize_identity(identity);
    let payload = IdentityWirePayload {
        v: PROTOCOL_VERSION,
        action: action.to_string(),
        client_type: sanitized.client_type.as_str().to_string(),
        app_name: sanitized.app_name,
        app_version: sanitized.app_version,
        capabilities: sanitized.capabilities,
        noreply: action == "announce" && noreply,
    };
    let json = serde_json::to_string(&payload).ok()?;
    fits_wire_budget(IDENTITY_PLUGIN, &json, target, target_id).then_some(json)
}

pub(crate) fn encode_query_message() -> String {
    serde_json::to_string(&ActionOnlyPayload {
        v: PROTOCOL_VERSION,
        action: "query",
    })
    .unwrap_or_else(|_| r#"{"v":1,"action":"query"}"#.to_string())
}

pub(crate) fn encode_commands_req() -> String {
    serde_json::to_string(&ActionOnlyPayload {
        v: PROTOCOL_VERSION,
        action: "list_req",
    })
    .unwrap_or_else(|_| r#"{"v":1,"action":"list_req"}"#.to_string())
}

pub(crate) fn encode_commands_ack(list: &BotCommandList, target_id: i32) -> Option<String> {
    let sanitized = sanitize_bot_command_list(list.clone());
    let mut wire_cmds: Vec<BotCommandWire> = sanitized
        .commands
        .into_iter()
        .map(|cmd| BotCommandWire {
            name: cmd.name,
            args: cmd.args,
            desc: cmd.desc,
        })
        .collect();

    loop {
        let payload = CommandsWirePayload {
            v: PROTOCOL_VERSION,
            action: "list_ack".to_string(),
            command_prefix: sanitized.command_prefix.clone(),
            commands: wire_cmds.clone(),
        };
        let json = serde_json::to_string(&payload).ok()?;
        if fits_wire_budget(
            COMMANDS_PLUGIN,
            &json,
            PluginCommandTarget::Client,
            Some(target_id),
        ) {
            return Some(json);
        }
        if wire_cmds.pop().is_none() {
            return None;
        }
    }
}

pub fn parse_inbound_message(plugin: &str, data: &str) -> Option<InboundMessage> {
    match plugin {
        IDENTITY_PLUGIN => {
            let payload: IdentityWirePayload = serde_json::from_str(data).ok()?;
            if payload.v != PROTOCOL_VERSION {
                return None;
            }
            match payload.action.as_str() {
                "announce" => {
                    let parsed = parse_wire_identity(&payload);
                    Some(InboundMessage::IdentityAnnounce {
                        identity: parsed,
                        noreply: payload.noreply,
                    })
                }
                "announce_ack" => {
                    parse_wire_identity(&payload).map(InboundMessage::IdentityAnnounceAck)
                }
                // query 忽略 noreply，确保显式定向查询总是允许按冷却正常回包。
                "query" => Some(InboundMessage::IdentityQuery),
                _ => None,
            }
        }
        COMMANDS_PLUGIN => {
            let payload: CommandsWirePayload = serde_json::from_str(data).ok()?;
            if payload.v != PROTOCOL_VERSION {
                return None;
            }
            match payload.action.as_str() {
                "list_req" => Some(InboundMessage::CommandsListReq),
                "list_ack" => {
                    let commands = payload
                        .commands
                        .into_iter()
                        .map(|cmd| BotCommand {
                            name: cmd.name,
                            args: cmd.args,
                            desc: cmd.desc,
                        })
                        .collect();
                    Some(InboundMessage::CommandsListAck(sanitize_bot_command_list(
                        BotCommandList {
                            command_prefix: payload.command_prefix,
                            commands,
                        },
                    )))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn parse_wire_identity(payload: &IdentityWirePayload) -> Option<PeerIdentity> {
    let client_type = ClientKind::parse(&payload.client_type)?;
    Some(sanitize_identity(&PeerIdentity {
        client_type,
        app_name: payload.app_name.clone(),
        app_version: payload.app_version.clone(),
        capabilities: payload.capabilities.clone(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_omits_empty_capabilities_and_false_noreply_and_roundtrips() {
        let id = PeerIdentity::new(ClientKind::FlSpeak, "FlSpeak", "0.1.14");
        let normal_json =
            encode_identity_announce(&id, false, PluginCommandTarget::CurrentChannel, None)
                .expect("encode identity");
        assert!(!normal_json.contains("capabilities"));
        assert!(!normal_json.contains("noreply"));
        assert_eq!(
            parse_inbound_message(IDENTITY_PLUGIN, &normal_json),
            Some(InboundMessage::IdentityAnnounce {
                identity: Some(id.clone()),
                noreply: false,
            })
        );

        let beacon_json = encode_identity_announce(&id, true, PluginCommandTarget::Server, None)
            .expect("encode beacon");
        assert!(beacon_json.contains(r#""noreply":true"#));
        assert_eq!(
            parse_inbound_message(IDENTITY_PLUGIN, &beacon_json),
            Some(InboundMessage::IdentityAnnounce {
                identity: Some(id.clone()),
                noreply: true,
            })
        );

        let ack_json = encode_identity_ack(&id, 7).expect("encode ack");
        assert!(!ack_json.contains("noreply"));

        // 即使收到带 noreply 的 query 报文，也必须解析为标准 IdentityQuery
        assert_eq!(
            parse_inbound_message(
                IDENTITY_PLUGIN,
                r#"{"v":1,"action":"query","noreply":true}"#
            ),
            Some(InboundMessage::IdentityQuery)
        );
    }

    #[test]
    fn effective_identity_syncs_slash_command_capability() {
        let base = PeerIdentity::new(ClientKind::Bot, "MyBot", "1.0.0")
            .with_capabilities(["chat:slash_commands", "custom:cap"]);
        let without_cmds = effective_identity(&base, None);
        assert_eq!(without_cmds.capabilities, vec!["custom:cap"]);

        let empty_cmds = BotCommandList::default();
        let with_empty = effective_identity(&base, Some(&empty_cmds));
        assert_eq!(with_empty.capabilities, vec!["custom:cap"]);

        let cmds = BotCommandList::new(vec![BotCommand::new("replay", "Replay")]);
        let with_cmds = effective_identity(&base, Some(&cmds));
        assert_eq!(
            with_cmds.capabilities,
            vec!["custom:cap", "chat:slash_commands"]
        );
    }

    #[test]
    fn command_sanitization_strips_controls_prefixes_and_defaults() {
        let json = r#"{"v":1,"action":"list_ack","command_prefix":"  ! \n","commands":[{"name":"/ping"},{"name":"!play","args":" <song>\t","desc":"Play\nsong"},{"name":"   "}]}"#;
        let Some(InboundMessage::CommandsListAck(list)) =
            parse_inbound_message(COMMANDS_PLUGIN, json)
        else {
            panic!("expected CommandsListAck");
        };
        assert_eq!(list.command_prefix.as_deref(), Some("!"));
        assert_eq!(list.commands.len(), 2);
        assert_eq!(list.commands[0].name, "ping");
        assert_eq!(list.commands[0].args, None);
        assert_eq!(list.commands[0].desc, "");
        assert_eq!(list.commands[1].name, "play");
        assert_eq!(list.commands[1].args.as_deref(), Some("<song>"));
        assert_eq!(list.commands[1].desc, "Playsong");
    }

    #[test]
    fn wire_budget_accounts_for_ts3_escaping_and_trims_overflow_commands() {
        let many_spaced: Vec<BotCommand> = (0..16)
            .map(|i| {
                BotCommand::new(format!("cmd_{i}"), "a / b | c \\ d repeat ".repeat(3))
                    .with_args("x y z / w")
            })
            .collect();
        let list = BotCommandList::new(many_spaced).with_prefix("!");
        let json = encode_commands_ack(&list, 65535).expect("fits after trimming");
        assert!(fits_wire_budget(
            COMMANDS_PLUGIN,
            &json,
            PluginCommandTarget::Client,
            Some(65535),
        ));
        let Some(InboundMessage::CommandsListAck(decoded)) =
            parse_inbound_message(COMMANDS_PLUGIN, &json)
        else {
            panic!("expected decoded CommandsListAck");
        };
        assert!(!decoded.commands.is_empty());
        assert!(decoded.commands.len() < 16);
    }

    #[test]
    fn parser_rejects_unsupported_protocol_version() {
        assert_eq!(
            parse_inbound_message(
                IDENTITY_PLUGIN,
                r#"{"v":2,"action":"announce","client_type":"bot"}"#,
            ),
            None
        );
        assert_eq!(
            parse_inbound_message(COMMANDS_PLUGIN, r#"{"v":0,"action":"list_req"}"#),
            None
        );
    }
}
