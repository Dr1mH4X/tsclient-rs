//! Server directory rows and channel notifications.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;

use crate::types::{ChannelInfo, ClientInfo, DirectorySnapshot};

const MAX_DIRECTORY_ENTRIES: usize = u16::MAX as usize;
const MAX_DIRECTORY_BYTES: usize = 32 * 1024 * 1024;

/// The bounded cache behind the live directory.
#[derive(Default)]
pub(crate) struct DirectoryCache {
    channels: HashMap<u64, ChannelInfo>,
    children: HashMap<u64, Vec<u64>>,
    clients: HashMap<i32, ClientInfo>,
    channel_bytes: usize,
    client_bytes: usize,
    welcome_complete: bool,
}

impl DirectoryCache {
    pub(crate) fn client(&self, id: i32) -> Option<&ClientInfo> {
        self.clients.get(&id)
    }

    pub(crate) fn insert_channel(&mut self, mut channel: ChannelInfo) -> bool {
        if self.welcome_complete {
            if channel.parent_id != 0
                && (channel.parent_id == channel.id
                    || !self.channels.contains_key(&channel.parent_id))
            {
                channel.parent_id = 0;
            }
        }
        let previous = self.channels.get(&channel.id);
        if previous.is_none() && self.channels.len() >= MAX_DIRECTORY_ENTRIES {
            return false;
        }
        let previous_bytes = previous.map_or(0, channel_size);
        let next_bytes = self.channel_bytes - previous_bytes + channel_size(&channel);
        if next_bytes > MAX_DIRECTORY_BYTES || previous == Some(&channel) {
            return false;
        }
        let previous_parent = previous.map(|channel| channel.parent_id);
        let parent_id = channel.parent_id;
        let predecessor = channel.order;
        let channel_id = channel.id;
        self.channel_bytes = next_bytes;
        if let Some(parent_id) = previous_parent {
            self.unindex_channel(parent_id, channel_id);
        }
        self.channels.insert(channel_id, channel);
        self.index_channel(parent_id, channel_id, predecessor);
        true
    }

    pub(crate) fn insert_client(&mut self, client: ClientInfo) -> bool {
        let previous = self.clients.get(&client.id);
        if previous.is_none() && self.clients.len() >= MAX_DIRECTORY_ENTRIES {
            return false;
        }
        let previous_bytes = previous.map_or(0, client_size);
        let next_bytes = self.client_bytes - previous_bytes + client_size(&client);
        if next_bytes > MAX_DIRECTORY_BYTES || previous == Some(&client) {
            return false;
        }
        self.client_bytes = next_bytes;
        self.clients.insert(client.id, client);
        true
    }

    pub(crate) fn remove_client(&mut self, id: i32) -> bool {
        let Some(client) = self.clients.remove(&id) else {
            return false;
        };
        self.client_bytes -= client_size(&client);
        true
    }

    pub(crate) fn move_client(&mut self, id: i32, channel_id: u64) -> bool {
        let Some(client) = self.clients.get_mut(&id) else {
            return false;
        };
        if client.channel_id == channel_id {
            return false;
        }
        client.channel_id = channel_id;
        true
    }

    fn update_channel(&mut self, id: u64, update: impl FnOnce(&mut ChannelInfo) -> bool) -> bool {
        let Some(current) = self.channels.get(&id) else {
            return false;
        };
        let current_bytes = channel_size(current);
        let current_parent = current.parent_id;
        let current_order = current.order;
        let mut next = current.clone();
        if !update(&mut next) {
            return false;
        }
        let next_bytes = self.channel_bytes - current_bytes + channel_size(&next);
        if next_bytes > MAX_DIRECTORY_BYTES {
            return false;
        }
        let mut parent_id = next.parent_id;
        if self.welcome_complete && parent_id != 0 && !self.channels.contains_key(&parent_id) {
            parent_id = 0;
        }
        if self.welcome_complete && parent_id != 0 && self.is_within(id, parent_id) {
            parent_id = current_parent;
        }
        next.parent_id = parent_id;
        let predecessor = next.order;
        let reorder = current_parent != parent_id || current_order != predecessor;
        if reorder {
            self.unindex_channel(current_parent, id);
        }
        self.channel_bytes = next_bytes;
        self.channels.insert(id, next);
        if reorder {
            self.index_channel(parent_id, id, predecessor);
        }
        true
    }

    fn update_client(&mut self, id: i32, update: impl FnOnce(&mut ClientInfo) -> bool) -> bool {
        let Some(current) = self.clients.get(&id) else {
            return false;
        };
        let mut next = current.clone();
        if !update(&mut next) {
            return false;
        }
        let next_bytes = self.client_bytes - client_size(current) + client_size(&next);
        if next_bytes > MAX_DIRECTORY_BYTES {
            return false;
        }
        self.client_bytes = next_bytes;
        self.clients.insert(id, next);
        true
    }

    pub(crate) fn remove_channel_tree(&mut self, id: u64) -> bool {
        let Some(root_parent) = self.channels.get(&id).map(|channel| channel.parent_id) else {
            return false;
        };
        self.unindex_channel(root_parent, id);

        let mut stack = vec![id];
        let mut removed = HashSet::new();
        while let Some(channel_id) = stack.pop() {
            if !removed.insert(channel_id) {
                continue;
            }
            if let Some(descendants) = self.children.remove(&channel_id) {
                stack.extend(descendants);
            }
        }

        let mut removed_bytes = 0;
        for channel_id in &removed {
            if let Some(channel) = self.channels.remove(channel_id) {
                removed_bytes += channel_size(&channel);
            }
        }
        self.channel_bytes -= removed_bytes;
        true
    }

    pub(crate) fn snapshot(&self) -> DirectorySnapshot {
        let mut clients: Vec<_> = self.clients.values().cloned().collect();
        clients.sort_by_key(|client| client.id);
        DirectorySnapshot {
            channels: ordered_channels_from_index(&self.channels, &self.children),
            clients,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.channels.clear();
        self.children.clear();
        self.clients.clear();
        self.channel_bytes = 0;
        self.client_bytes = 0;
        self.welcome_complete = false;
    }

    pub(crate) fn finish_welcome(&mut self) {
        let channel_ids: Vec<u64> = self.channels.keys().copied().collect();
        for channel_id in channel_ids {
            let Some(parent_id) = self.channels.get(&channel_id).map(|channel| channel.parent_id)
            else {
                continue;
            };
            if parent_id != 0
                && (parent_id == channel_id || !self.channels.contains_key(&parent_id))
            {
                if let Some(channel) = self.channels.get_mut(&channel_id) {
                    channel.parent_id = 0;
                }
            }
        }
        self.children = self
            .channels
            .values()
            .fold(HashMap::new(), |mut children, channel| {
                children
                    .entry(channel.parent_id)
                    .or_insert_with(Vec::new)
                    .push(channel.id);
                children
            });
        let parents: Vec<u64> = self.children.keys().copied().collect();
        for parent_id in parents {
            let Some(ids) = self.children.remove(&parent_id) else {
                continue;
            };
            self.children
                .insert(parent_id, ordered_sibling_ids(ids, &self.channels));
            self.settle_channel_orders(parent_id);
        }
        self.welcome_complete = true;
    }

    fn index_channel(&mut self, parent_id: u64, channel_id: u64, predecessor: u64) {
        {
            let siblings = self.children.entry(parent_id).or_default();
            let at = if predecessor == 0 {
                0
            } else {
                siblings
                    .iter()
                    .position(|sibling| *sibling == predecessor)
                    .map_or(siblings.len(), |index| index + 1)
            };
            siblings.insert(at, channel_id);
        }
        if self.welcome_complete {
            self.settle_channel_orders(parent_id);
        }
    }

    fn unindex_channel(&mut self, parent_id: u64, channel_id: u64) {
        let empty = if let Some(children) = self.children.get_mut(&parent_id) {
            children.retain(|sibling| *sibling != channel_id);
            children.is_empty()
        } else {
            false
        };
        if empty {
            self.children.remove(&parent_id);
        } else if self.welcome_complete {
            self.settle_channel_orders(parent_id);
        }
    }

    fn settle_channel_orders(&mut self, parent_id: u64) {
        let Some(children) = self.children.get(&parent_id).cloned() else {
            return;
        };
        let mut predecessor = 0;
        for channel_id in children {
            if let Some(channel) = self.channels.get_mut(&channel_id) {
                channel.order = predecessor;
                predecessor = channel_id;
            }
        }
    }

    fn is_within(&self, channel_id: u64, ancestor_id: u64) -> bool {
        let mut current = ancestor_id;
        for _ in 0..self.channels.len() {
            if current == channel_id {
                return true;
            }
            let Some(channel) = self.channels.get(&current) else {
                return false;
            };
            if channel.parent_id == 0 {
                return false;
            }
            current = channel.parent_id;
        }
        false
    }
}

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
    directory: &mut DirectoryCache,
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
            directory.insert_channel(channel)
        }
        "notifychanneledited" => directory.update_channel(id, |current| {
            let mut changed = false;
            if let Some(value) = params.get("channel_name") {
                if current.name.as_str() != value {
                    current.name = value.clone();
                    changed = true;
                }
            }
            if let Some(value) = params
                .get("channel_topic")
                .or_else(|| params.get("channel_description"))
            {
                if current.description.as_str() != value {
                    current.description = value.clone();
                    changed = true;
                }
            }
            if let Some(value) = params
                .get("channel_order")
                .or_else(|| params.get("order"))
            {
                let order = parse_u64(value);
                if current.order != order {
                    current.order = order;
                    changed = true;
                }
            }
            if let Some(value) = parse_bool(params.get("channel_flag_password")) {
                if current.password_protected != Some(value) {
                    current.password_protected = Some(value);
                    changed = true;
                }
            }
            if let Some(value) = params
                .get("channel_maxclients")
                .and_then(|value| value.parse::<u16>().ok())
            {
                if current.max_clients != Some(value) {
                    current.max_clients = Some(value);
                    changed = true;
                }
            }
            if let Some(value) = parse_bool(params.get("channel_flag_maxclients_unlimited")) {
                let limited = !value;
                if current.max_clients_limited != Some(limited) {
                    current.max_clients_limited = Some(limited);
                    changed = true;
                }
            }
            if let Some(value) = params
                .get("channel_codec")
                .and_then(|value| value.parse::<i32>().ok())
            {
                if current.codec != Some(value) {
                    current.codec = Some(value);
                    changed = true;
                }
            }
            changed
        }),
        "notifychannelmoved" => directory.update_channel(id, |current| {
            let mut changed = false;
            if let Some(value) = params.get("cpid").or_else(|| params.get("pid")) {
                let parent_id = parse_u64(value);
                if current.parent_id != parent_id {
                    current.parent_id = parent_id;
                    changed = true;
                }
            }
            if let Some(value) = params
                .get("channel_order")
                .or_else(|| params.get("order"))
            {
                let order = parse_u64(value);
                if current.order != order {
                    current.order = order;
                    changed = true;
                }
            }
            changed
        }),
        "notifychanneldeleted" => directory.remove_channel_tree(id),
        _ => false,
    }
}

/// Parse one `channelclientlist` row.
pub fn parse_client(params: &HashMap<String, String>) -> Option<ClientInfo> {
    let id = parse_client_id(params)?;
    let server_groups = parse_server_groups(
        params
            .get("client_servergroups")
            .map(String::as_str)
            .unwrap_or(""),
    );
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

/// Apply the fields a `notifyclientupdated` row carries to a known client.
pub(crate) fn apply_client_notification(
    name: &str,
    params: &HashMap<String, String>,
    directory: &mut DirectoryCache,
) -> bool {
    if name != "notifyclientupdated" {
        return false;
    }
    let Some(id) = parse_client_id(params) else {
        return false;
    };
    if directory.client(id).is_none() {
        return false;
    }
    let server_groups = params
        .get("client_servergroups")
        .map(|value| parse_server_groups(value));
    let channel_id = params
        .get("cid")
        .or_else(|| params.get("ctid"))
        .map(|value| parse_u64(value));
    let client_type = params
        .get("client_type")
        .and_then(|value| value.parse::<i32>().ok());

    directory.update_client(id, |current| {
        let mut changed = false;
        if let Some(value) = params.get("client_nickname") {
            if current.nickname.as_str() != value {
                current.nickname = value.clone();
                changed = true;
            }
        }
        if let Some(value) = params.get("client_unique_identifier") {
            if current.uid.as_str() != value {
                current.uid = value.clone();
                changed = true;
            }
        }
        if let Some(value) = &server_groups {
            if current.server_groups != *value {
                current.server_groups = value.clone();
                changed = true;
            }
        }
        if let Some(value) = channel_id {
            if current.channel_id != value {
                current.channel_id = value;
                changed = true;
            }
        }
        if let Some(value) = client_type {
            if current.r#type != value {
                current.r#type = value;
                changed = true;
            }
        }
        changed
    })
}

fn parse_u64(value: &str) -> u64 {
    value.parse().unwrap_or(0)
}

fn parse_client_id(params: &HashMap<String, String>) -> Option<i32> {
    let id = params
        .get("clid")
        .and_then(|value| value.parse::<u16>().ok())
        .map(i32::from)?;
    (id != 0).then_some(id)
}

fn parse_server_groups(value: &str) -> Vec<String> {
    if value.is_empty() {
        return Vec::new();
    }
    value.split(',').map(ToOwned::to_owned).collect()
}

fn parse_bool(value: Option<&String>) -> Option<bool> {
    match value.map(String::as_str) {
        Some("0") => Some(false),
        Some("1") => Some(true),
        _ => None,
    }
}

fn channel_size(channel: &ChannelInfo) -> usize {
    size_of::<ChannelInfo>() + channel.name.len() + channel.description.len()
}

fn client_size(client: &ClientInfo) -> usize {
    size_of::<ClientInfo>()
        + client.nickname.len()
        + client.uid.len()
        + client
            .server_groups
            .iter()
            .map(|group| size_of::<String>() + group.len())
            .sum::<usize>()
}

fn ordered_channels_from_index(
    channels: &HashMap<u64, ChannelInfo>,
    children: &HashMap<u64, Vec<u64>>,
) -> Vec<ChannelInfo> {
    let mut ordered = Vec::with_capacity(channels.len());
    let mut seen = HashSet::new();
    let mut stack: Vec<u64> = children
        .get(&0)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .rev()
        .collect();
    while let Some(channel_id) = stack.pop() {
        if !seen.insert(channel_id) {
            continue;
        }
        if let Some(channel) = channels.get(&channel_id) {
            ordered.push(channel.clone());
        }
        if let Some(descendants) = children.get(&channel_id) {
            stack.extend(descendants.iter().rev().copied());
        }
    }

    let mut leftovers: Vec<u64> = channels
        .keys()
        .copied()
        .filter(|channel_id| !seen.contains(channel_id))
        .collect();
    leftovers.sort_unstable();
    for channel_id in leftovers {
        if let Some(mut channel) = channels.get(&channel_id).cloned() {
            channel.parent_id = 0;
            ordered.push(channel);
        }
    }
    ordered
}

fn ordered_sibling_ids(
    mut ids: Vec<u64>,
    channels: &HashMap<u64, ChannelInfo>,
) -> Vec<u64> {
    ids.sort_unstable();
    let mut by_predecessor: HashMap<u64, Vec<u64>> = HashMap::new();
    for channel_id in ids {
        let predecessor = channels
            .get(&channel_id)
            .map(|channel| channel.order)
            .unwrap_or(0);
        by_predecessor
            .entry(predecessor)
            .or_default()
            .push(channel_id);
    }

    let mut ordered = Vec::new();
    let mut predecessor = 0;
    while let Some(mut following) = by_predecessor.remove(&predecessor) {
        following.sort_unstable();
        for channel_id in following {
            predecessor = channel_id;
            ordered.push(channel_id);
        }
    }

    let mut remaining: Vec<_> = by_predecessor.into_values().flatten().collect();
    remaining.sort_by_key(|channel_id| {
        (
            channels
                .get(channel_id)
                .map(|channel| channel.order)
                .unwrap_or(0),
            *channel_id,
        )
    });
    ordered.extend(remaining);
    ordered
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
        let mut directory = DirectoryCache::default();
        assert!(directory.insert_channel(
            parse_channel(&row(&[
                ("cid", "1"),
                ("pid", "0"),
                ("channel_name", "Lobby"),
            ]))
            .expect("channel"),
        ));

        assert!(apply_channel_notification(
            "notifychanneledited",
            &row(&[
                ("cid", "1"),
                ("channel_name", "New Lobby"),
                ("channel_topic", "new"),
                ("channel_order", "9"),
                ("cpid", "9"),
                ("channel_flag_password", "1"),
            ]),
            &mut directory,
        ));
        let channel = directory.channels.get(&1).expect("updated channel");
        assert_eq!(channel.name, "New Lobby");
        assert_eq!(channel.description, "new");
        assert_eq!(channel.order, 9);
        assert_eq!(channel.parent_id, 0);
        assert_eq!(channel.password_protected, Some(true));

        assert!(apply_channel_notification(
            "notifychannelmoved",
            &row(&[("cid", "1"), ("cpid", "2"), ("channel_order", "4")]),
            &mut directory,
        ));
        assert_eq!(directory.channels[&1].parent_id, 2);
        assert_eq!(directory.channels[&1].order, 4);

        assert!(apply_channel_notification(
            "notifychanneldeleted",
            &row(&[("cid", "1")]),
            &mut directory,
        ));
        assert!(directory.snapshot().channels.is_empty());
    }

    #[test]
    fn deleting_a_channel_removes_every_descendant() {
        let mut directory = DirectoryCache::default();
        for (id, parent) in [(1, 0), (2, 1), (3, 2), (4, 0), (5, 0)] {
            assert!(directory.insert_channel(
                parse_channel(&row(&[
                    ("cid", &id.to_string()),
                    ("pid", &parent.to_string()),
                ]))
                .expect("channel"),
            ));
        }
        assert!(apply_channel_notification(
            "notifychannelmoved",
            &row(&[("cid", "4"), ("cpid", "1")]),
            &mut directory,
        ));

        assert!(apply_channel_notification(
            "notifychanneldeleted",
            &row(&[("cid", "1")]),
            &mut directory,
        ));
        let snapshot = directory.snapshot();
        assert_eq!(
            snapshot
                .channels
                .iter()
                .map(|channel| channel.id)
                .collect::<Vec<_>>(),
            vec![5]
        );
        assert_eq!(
            directory.children.get(&0).map(Vec::as_slice),
            Some(&[5][..])
        );
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

    #[test]
    fn client_updates_preserve_fields_the_notification_omits() {
        let mut directory = DirectoryCache::default();
        assert!(directory.insert_client(
            parse_client(&row(&[
                ("clid", "7"),
                ("cid", "42"),
                ("client_nickname", "Alice"),
                ("client_unique_identifier", "uid"),
                ("client_servergroups", "6,9"),
                ("client_type", "0"),
            ]))
            .expect("client"),
        ));

        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "7"), ("client_nickname", "Alicia")]),
            &mut directory,
        ));
        let client = directory.client(7).expect("client");
        assert_eq!(client.nickname, "Alicia");
        assert_eq!(client.uid, "uid");
        assert_eq!(client.channel_id, 42);
        assert_eq!(client.server_groups, vec!["6", "9"]);

        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "7"), ("client_servergroups", "")]),
            &mut directory,
        ));
        assert!(directory.client(7).expect("client").server_groups.is_empty());
    }

    #[test]
    fn snapshot_orders_channels_as_a_tree_and_clients_by_id() {
        let mut directory = DirectoryCache::default();
        for (id, parent, order) in [
            (6, 2, 5),
            (3, 0, 2),
            (5, 2, 0),
            (2, 0, 1),
            (7, 2, 6),
            (1, 0, 0),
        ] {
            assert!(directory.insert_channel(
                parse_channel(&row(&[
                    ("cid", &id.to_string()),
                    ("pid", &parent.to_string()),
                    ("channel_order", &order.to_string()),
                ]))
                .expect("channel"),
            ));
        }
        for id in [9, 4, 7] {
            assert!(directory.insert_client(ClientInfo {
                id,
                nickname: format!("client-{id}"),
                uid: String::new(),
                channel_id: 1,
                r#type: 0,
                server_groups: Vec::new(),
            }));
        }
        directory.finish_welcome();

        let snapshot = directory.snapshot();
        assert_eq!(
            snapshot
                .channels
                .iter()
                .map(|channel| channel.id)
                .collect::<Vec<_>>(),
            vec![1, 2, 5, 6, 7, 3]
        );
        assert_eq!(
            snapshot
                .clients
                .iter()
                .map(|client| client.id)
                .collect::<Vec<_>>(),
            vec![4, 7, 9]
        );
    }

    #[test]
    fn live_channel_creation_keeps_siblings_in_server_order() {
        let mut directory = DirectoryCache::default();
        for (id, order) in [(1, 0), (2, 1), (3, 2)] {
            assert!(directory.insert_channel(
                parse_channel(&row(&[
                    ("cid", &id.to_string()),
                    ("pid", "0"),
                    ("channel_order", &order.to_string()),
                ]))
                .expect("channel"),
            ));
        }
        directory.finish_welcome();

        assert!(apply_channel_notification(
            "notifychannelcreated",
            &row(&[
                ("cid", "4"),
                ("cpid", "0"),
                ("channel_order", "1"),
            ]),
            &mut directory,
        ));
        let channels = directory.snapshot().channels;
        assert_eq!(
            channels
                .iter()
                .map(|channel| channel.id)
                .collect::<Vec<_>>(),
            vec![1, 4, 2, 3]
        );
        assert_eq!(
            channels
                .iter()
                .map(|channel| channel.order)
                .collect::<Vec<_>>(),
            vec![0, 1, 4, 2]
        );
    }

    #[test]
    fn directory_cache_rejects_entries_past_its_byte_budget() {
        let mut directory = DirectoryCache::default();
        let channel = ChannelInfo {
            id: 1,
            parent_id: 0,
            order: 0,
            name: String::new(),
            description: "x".repeat(MAX_DIRECTORY_BYTES),
            password_protected: None,
            max_clients: None,
            max_clients_limited: None,
            codec: None,
        };

        assert!(!directory.insert_channel(channel));
        assert!(directory.snapshot().channels.is_empty());
    }
}
