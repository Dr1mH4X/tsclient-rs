//! High-level API — mirrors `teamspeak-js/src/api.ts`

use std::collections::HashMap;

use crate::Client;
use crate::Error;
use crate::command::{build_command, build_command_ordered, unescape};
use crate::directory::{parse_bool, parse_channel, parse_channel_info};
use crate::helpers;
use crate::types::*;

/// Default timeout (ms) for commands that modify server state.
const CMD_TIMEOUT_MS: u64 = 10_000;

/// Shorter timeout (ms) for read-only queries.
const QUERY_TIMEOUT_MS: u64 = 5_000;

/// Send a text message to a client (target_mode=1), channel (2), or server (3).
pub async fn send_text_message(
    client: &Client,
    target_mode: i32,
    target_id: u64,
    message: &str,
) -> Result<(), Error> {
    let cmd = build_command_ordered(
        "sendtextmessage",
        &[
            ("targetmode", &target_mode.to_string()),
            ("target", &target_id.to_string()),
            ("msg", message),
        ],
    );
    client.send_command_no_wait(&cmd).await
}

/// Move a client to a different channel.
pub async fn client_move(
    client: &Client,
    clid: i32,
    channel_id: u64,
    password: &str,
) -> Result<(), Error> {
    let mut params = vec![("clid", clid.to_string()), ("cid", channel_id.to_string())];
    if !password.is_empty() {
        params.push(("cpw", password.to_string()));
    }
    let cmd = build_command_ordered(
        "clientmove",
        &params
            .iter()
            .map(|(k, v)| (*k, v.as_str()))
            .collect::<Vec<_>>(),
    );
    client.exec_command(&cmd, CMD_TIMEOUT_MS).await
}

/// Send a poke message to a client.
pub async fn poke(client: &Client, clid: i32, message: &str) -> Result<(), Error> {
    let cmd = build_command_ordered(
        "clientpoke",
        &[("clid", &clid.to_string()), ("msg", message)],
    );
    client.exec_command(&cmd, CMD_TIMEOUT_MS).await
}

/// Fetch raw clientinfo for a given clid.
pub async fn get_client_info(client: &Client, clid: i32) -> Result<HashMap<String, String>, Error> {
    let data = client
        .exec_command_with_response(&format!("clientinfo clid={clid}"), QUERY_TIMEOUT_MS)
        .await?;
    data.into_iter()
        .next()
        .ok_or_else(|| Error::Teamspeak(format!("no data returned for client {clid}")))
}

/// Fetch detailed configuration information for a channel via `channelinfo`.
pub async fn get_channel_info(client: &Client, channel_id: u64) -> Result<ChannelInfo, Error> {
    let cmd = build_command_ordered("channelinfo", &[("cid", &channel_id.to_string())]);
    let data = client
        .exec_command_with_response(&cmd, QUERY_TIMEOUT_MS)
        .await?;
    let row = data
        .first()
        .ok_or_else(|| Error::Teamspeak(format!("no data returned for channel {channel_id}")))?;
    Ok(parse_channel_info(channel_id, row))
}

/// Fetch a channel's description via `channelgetdescription`.
pub async fn get_channel_description(client: &Client, channel_id: u64) -> Result<String, Error> {
    let cid = channel_id.to_string();
    let cmd = build_command_ordered("channelgetdescription", &[("cid", &cid)]);
    let data = client
        .exec_command_with_response(&cmd, QUERY_TIMEOUT_MS)
        .await?;
    data.into_iter()
        .find(|row| {
            row.get("cid").map(String::as_str) == Some(cid.as_str())
                && row.contains_key("channel_description")
        })
        .and_then(|mut row| row.remove("channel_description"))
        .ok_or_else(|| Error::Teamspeak(format!("no data returned for channel {channel_id}")))
}

/// List all channels on the server.
pub async fn list_channels(client: &Client) -> Result<Vec<ChannelInfo>, Error> {
    let data = client
        .exec_command_with_response("channellist -icon", QUERY_TIMEOUT_MS)
        .await?;
    Ok(data.iter().filter_map(parse_channel).collect())
}

/// List all clients currently connected to the server.
pub async fn list_clients(client: &Client) -> Result<Vec<ClientInfo>, Error> {
    let data = client
        .exec_command_with_response("clientlist -uid -away -voice -groups", QUERY_TIMEOUT_MS)
        .await?;
    Ok(data
        .into_iter()
        .map(|item| {
            let groups_str = item.get("client_servergroups").cloned().unwrap_or_default();
            let server_groups = if groups_str.is_empty() {
                Vec::new()
            } else {
                groups_str.split(',').map(|s| s.to_string()).collect()
            };
            ClientInfo {
                id: helpers::parse_i10(item.get("clid").map(|s| s.as_str()).unwrap_or("0")),
                nickname: unescape(
                    item.get("client_nickname")
                        .map(|s| s.as_str())
                        .unwrap_or(""),
                ),
                uid: item
                    .get("client_unique_identifier")
                    .cloned()
                    .unwrap_or_default(),
                channel_id: helpers::parse_u64(item.get("cid").map(|s| s.as_str()).unwrap_or("0")),
                r#type: helpers::parse_i10(
                    item.get("client_type").map(|s| s.as_str()).unwrap_or("0"),
                ),
                server_groups,
                away: parse_bool(item.get("client_away")),
                away_message: item.get("client_away_message").cloned(),
                input_muted: parse_bool(item.get("client_input_muted")),
                output_muted: parse_bool(item.get("client_output_muted")),
                channel_commander: parse_bool(item.get("client_is_channel_commander")),
            }
        })
        .collect())
}

/// Kick a client from the server.
///
/// `reason` controls whether the kick removes from channel (`KickReason::Channel`)
/// or from the entire server (`KickReason::Server`).
pub async fn client_kick(
    client: &Client,
    clid: i32,
    reason: KickReason,
    reasonmsg: &str,
) -> Result<(), Error> {
    let reasonid: i32 = reason.into();
    let cmd = build_command_ordered(
        "clientkick",
        &[
            ("clid", &clid.to_string()),
            ("reasonid", &reasonid.to_string()),
            ("reasonmsg", reasonmsg),
        ],
    );
    client.exec_command(&cmd, CMD_TIMEOUT_MS).await
}

/// Ban a client with a given duration (seconds) and reason.
pub async fn ban_client(
    client: &Client,
    clid: i32,
    time: u64,
    banreason: &str,
) -> Result<(), Error> {
    let cmd = build_command_ordered(
        "banclient",
        &[
            ("clid", &clid.to_string()),
            ("time", &time.to_string()),
            ("banreason", banreason),
        ],
    );
    client.exec_command(&cmd, CMD_TIMEOUT_MS).await
}

/// Update client properties via `clientupdate`.
///
/// Example: `client_update(client, &[("client_description", "hello world")]).await`
pub async fn client_update(client: &Client, params: &[(&str, &str)]) -> Result<(), Error> {
    let cmd = build_command_ordered("clientupdate", params);
    client.exec_command(&cmd, CMD_TIMEOUT_MS).await
}

/// Delete files on the server.
pub async fn file_transfer_delete_file(
    client: &Client,
    channel_id: u64,
    paths: &[String],
) -> Result<(), Error> {
    if paths.is_empty() {
        return Ok(());
    }
    let path_str = paths.join("|");
    let mut params = HashMap::new();
    params.insert("cid".to_string(), channel_id.to_string());
    params.insert("cpw".to_string(), String::new());
    params.insert("name".to_string(), path_str);
    let cmd = build_command("ftdeletefile", params);
    client.exec_command(&cmd, CMD_TIMEOUT_MS).await
}

/// 图标文件位于频道 0，文件名使用无符号 CRC-32 十进制值。
pub fn icon_file_name(icon_id: u32) -> String {
    format!("/icon_{icon_id}")
}

/// 下载图标；调用者提供目标主机和允许的最大字节数。
pub async fn download_icon(
    client: &Client,
    host: &str,
    icon_id: u32,
    max_bytes: usize,
) -> Result<Vec<u8>, Error> {
    if icon_id == 0 {
        return Err(Error::FileTransfer("icon ID must be non-zero".into()));
    }
    let info = client
        .file_transfer_init_download(0, &icon_file_name(icon_id), "")
        .await?;
    if info.size == 0 || info.size > max_bytes as u64 {
        return Err(Error::FileTransfer(format!(
            "invalid icon size {} (limit {max_bytes})",
            info.size
        )));
    }
    let mut data = Vec::new();
    crate::transfer::download_file_data(host, &info, &mut data).await?;
    Ok(data)
}

#[cfg(test)]
mod icon_tests {
    use super::*;

    #[test]
    fn icon_file_names_use_unsigned_decimal_ids() {
        assert_eq!(icon_file_name(3009859579), "/icon_3009859579");
        assert_eq!(icon_file_name(u32::MAX), "/icon_4294967295");
    }
}
