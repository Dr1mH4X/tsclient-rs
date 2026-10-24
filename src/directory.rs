//! Server directory rows and channel notifications.

use std::collections::HashMap;

use crate::types::{ChannelInfo, ClientInfo, DirectorySnapshot};

/// Parse one `channellist` row. Command values have already been unescaped.
pub fn parse_channel(params: &HashMap<String, String>) -> Option<ChannelInfo> {
    let id = parse_u64(params.get("cid")?);
    if id == 0 {
        return None;
    }
    Some(ChannelInfo {
        id,
        parent_id: parse_u64(params.get("pid").or_else(|| params.get("cpid"))?),
        order: params
            .get("channel_order")
            .or_else(|| params.get("order"))
            .map(|value| parse_u64(value))
            .unwrap_or(0),
        name: params.get("channel_name").cloned().unwrap_or_default(),
        description: params
            .get("channel_topic")
            .or_else(|| params.get("channel_description"))
            .cloned()
            .unwrap_or_default(),
        password_protected: parse_bool(params.get("channel_flag_password")),
        max_clients: params
            .get("channel_maxclients")
            .and_then(|value| value.parse::<u16>().ok()),
        max_clients_limited: parse_bool(params.get("channel_flag_maxclients_unlimited"))
            .map(|unlimited| !unlimited),
        codec: params
            .get("channel_codec")
            .and_then(|value| value.parse::<i32>().ok()),
    })
}

/// Apply a channel notification to the live directory.
pub fn apply_channel_notification(
    name: &str,
    params: &HashMap<String, String>,
    channels: &mut HashMap<u64, ChannelInfo>,
) -> bool {
    let Some(id) = params.get("cid").map(|value| parse_u64(value)) else {
        return false;
    };
    if id == 0 {
        return false;
    }

    match name {
        "notifychannelcreated" => {
            let Some(channel) = parse_channel(params) else {
                return false;
            };
            channels.insert(channel.id, channel);
            true
        }
        "notifychanneledited" => {
            let Some(current) = channels.get_mut(&id) else {
                return false;
            };
            if let Some(value) = params.get("channel_name") {
                current.name.clone_from(value);
            }
            if let Some(value) = params
                .get("channel_topic")
                .or_else(|| params.get("channel_description"))
            {
                current.description.clone_from(value);
            }
            if let Some(value) = params.get("cpid").or_else(|| params.get("pid")) {
                current.parent_id = parse_u64(value);
            }
            if let Some(value) = params
                .get("channel_order")
                .or_else(|| params.get("order"))
            {
                current.order = parse_u64(value);
            }
            if let Some(value) = parse_bool(params.get("channel_flag_password")) {
                current.password_protected = Some(value);
            }
            if let Some(value) = params
                .get("channel_maxclients")
                .and_then(|value| value.parse::<u16>().ok())
            {
                current.max_clients = Some(value);
            }
            if let Some(value) = parse_bool(params.get("channel_flag_maxclients_unlimited")) {
                current.max_clients_limited = Some(!value);
            }
            if let Some(value) = params
                .get("channel_codec")
                .and_then(|value| value.parse::<i32>().ok())
            {
                current.codec = Some(value);
            }
            true
        }
        "notifychannelmoved" => {
            let Some(current) = channels.get_mut(&id) else {
                return false;
            };
            if let Some(value) = params.get("cpid").or_else(|| params.get("pid")) {
                current.parent_id = parse_u64(value);
            }
            if let Some(value) = params
                .get("channel_order")
                .or_else(|| params.get("order"))
            {
                current.order = parse_u64(value);
            }
            true
        }
        "notifychanneldeleted" => channels.remove(&id).is_some(),
        _ => false,
    }
}

/// Parse one `channelclientlist` row.
pub fn parse_client(params: &HashMap<String, String>) -> Option<ClientInfo> {
    let id = params
        .get("clid")
        .and_then(|value| value.parse::<u16>().ok())
        .map(i32::from)?;
    if id == 0 {
        return None;
    }
    let server_groups = params
        .get("client_servergroups")
        .filter(|value| !value.is_empty())
        .map(|value| value.split(',').map(ToOwned::to_owned).collect())
        .unwrap_or_default();
    Some(ClientInfo {
        id,
        nickname: params
            .get("client_nickname")
            .cloned()
            .unwrap_or_default(),
        uid: params
            .get("client_unique_identifier")
            .cloned()
            .unwrap_or_default(),
        channel_id: params
            .get("cid")
            .or_else(|| params.get("ctid"))
            .map(|value| parse_u64(value))
            .unwrap_or(0),
        r#type: params
            .get("client_type")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(0),
        server_groups,
    })
}

pub(crate) fn snapshot(
    channels: &HashMap<u64, ChannelInfo>,
    clients: &HashMap<i32, ClientInfo>,
) -> DirectorySnapshot {
    DirectorySnapshot {
        channels: channels.values().cloned().collect(),
        clients: clients.values().cloned().collect(),
    }
}

fn parse_u64(value: &str) -> u64 {
    value.parse().unwrap_or(0)
}

fn parse_bool(value: Option<&String>) -> Option<bool> {
    match value.map(String::as_str) {
        Some("0") => Some(false),
        Some("1") => Some(true),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn channellist_row_keeps_tree_order_topic_and_public_flags() {
        let channel = parse_channel(&row(&[
            ("cid", "42"),
            ("pid", "1"),
            ("channel_order", "17"),
            ("channel_name", "Lobby Room"),
            ("channel_topic", "A|B"),
            ("channel_flag_password", "1"),
            ("channel_maxclients", "25"),
            ("channel_flag_maxclients_unlimited", "0"),
            ("channel_codec", "5"),
        ]))
        .expect("channel");

        assert_eq!(channel.id, 42);
        assert_eq!(channel.parent_id, 1);
        assert_eq!(channel.order, 17);
        assert_eq!(channel.name, "Lobby Room");
        assert_eq!(channel.description, "A|B");
        assert_eq!(channel.password_protected, Some(true));
        assert_eq!(channel.max_clients, Some(25));
        assert_eq!(channel.max_clients_limited, Some(true));
        assert_eq!(channel.codec, Some(5));
    }

    #[test]
    fn channel_notifications_update_and_delete_live_rows() {
        let mut channels = HashMap::from([(
            1,
            parse_channel(&row(&[
                ("cid", "1"),
                ("pid", "0"),
                ("channel_name", "Lobby"),
            ]))
            .expect("channel"),
        )]);

        assert!(apply_channel_notification(
            "notifychanneledited",
            &row(&[
                ("cid", "1"),
                ("channel_name", "New Lobby"),
                ("channel_topic", "new"),
                ("channel_order", "9"),
                ("channel_flag_password", "1"),
            ]),
            &mut channels,
        ));
        let channel = channels.get(&1).expect("updated channel");
        assert_eq!(channel.name, "New Lobby");
        assert_eq!(channel.description, "new");
        assert_eq!(channel.order, 9);
        assert_eq!(channel.password_protected, Some(true));

        assert!(apply_channel_notification(
            "notifychannelmoved",
            &row(&[("cid", "1"), ("cpid", "2"), ("channel_order", "4")]),
            &mut channels,
        ));
        assert_eq!(channels[&1].parent_id, 2);
        assert_eq!(channels[&1].order, 4);

        assert!(apply_channel_notification(
            "notifychanneldeleted",
            &row(&[("cid", "1")]),
            &mut channels,
        ));
        assert!(channels.is_empty());
    }

    #[test]
    fn channelclientlist_row_uses_the_channel_not_the_former_channel() {
        let client = parse_client(&row(&[
            ("clid", "7"),
            ("cid", "42"),
            ("client_nickname", "Alice"),
            ("client_unique_identifier", "uid"),
            ("client_servergroups", "6,9"),
            ("client_type", "0"),
        ]))
        .expect("client");

        assert_eq!(client.id, 7);
        assert_eq!(client.channel_id, 42);
        assert_eq!(client.server_groups, vec!["6", "9"]);
    }
}
