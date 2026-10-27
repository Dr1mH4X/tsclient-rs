//! Server directory rows and channel notifications.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;

use crate::types::{ChannelInfo, ClientInfo, DirectorySnapshot};

const MAX_DIRECTORY_ENTRIES: usize = u16::MAX as usize;
const MAX_DIRECTORY_BYTES: usize = 32 * 1024 * 1024;
/// Bound on clients with buffered welcome updates; past it the updates are
/// dropped and the affected flags stay stale until the next client update.
const MAX_PENDING_CLIENT_UPDATES: usize = 1024;

/// The bounded cache behind the live directory.
#[derive(Default)]
pub(crate) struct DirectoryCache {
    channels: HashMap<u64, ChannelInfo>,
    children: HashMap<u64, Vec<u64>>,
    clients: HashMap<i32, ClientInfo>,
    channel_bytes: usize,
    client_bytes: usize,
    welcome_complete: bool,
    truncated: bool,
    /// `notifyclientupdated` rows for clients the welcome has not introduced
    /// yet, coalesced to one patch per client: a `channelclientlist` row is a
    /// snapshot from before the rows were sent, so an update that arrives
    /// first carries newer state than the row that follows it; the row path
    /// replays the patch on insert. Dropped once the welcome finishes.
    pending_client_updates: HashMap<i32, HashMap<String, String>>,
    /// Server group adds/deletes for unknown welcome clients, coalesced per
    /// client to the last operation per group id. Replayed after the update
    /// patch.
    pending_group_ops: HashMap<i32, HashMap<String, bool>>,
    /// Clients a live event removed, welcome or live: a left view cannot be
    /// undone by a late `channelclientlist` row still in flight.
    departed_clients: HashSet<i32>,
}

impl DirectoryCache {
    pub(crate) fn client(&self, id: i32) -> Option<&ClientInfo> {
        self.clients.get(&id)
    }

    pub(crate) fn truncated(&self) -> bool {
        self.truncated
    }

    /// Buffer a `notifyclientupdated` row for a client the welcome has not
    /// introduced yet, so the client's `channelclientlist` row can replay it.
    /// Updates for an already buffered id always merge — the cap bounds
    /// distinct clients only; a dropped new id sets `truncated`, so the
    /// snapshot reports the loss.
    fn buffer_pending_client_update(&mut self, id: i32, params: &HashMap<String, String>) {
        if self.welcome_complete {
            return;
        }
        match self.pending_client_updates.get_mut(&id) {
            Some(patch) => patch.extend(params.clone()),
            None => {
                if self.pending_client_updates.len() >= MAX_PENDING_CLIENT_UPDATES {
                    self.truncated = true;
                    return;
                }
                self.pending_client_updates.insert(id, params.clone());
            }
        }
    }

    /// Remove and return the update patch buffered for `id`.
    pub(crate) fn take_pending_client_updates(
        &mut self,
        id: i32,
    ) -> Option<HashMap<String, String>> {
        self.pending_client_updates.remove(&id)
    }

    /// Buffer a server group add/delete for a client the welcome has not
    /// introduced yet; the last operation per group id wins.
    fn buffer_pending_group_op(&mut self, id: i32, sgid: String, add: bool) {
        if self.welcome_complete {
            return;
        }
        match self.pending_group_ops.get_mut(&id) {
            Some(ops) => {
                ops.insert(sgid, add);
            }
            None => {
                if self.pending_group_ops.len() >= MAX_PENDING_CLIENT_UPDATES {
                    self.truncated = true;
                    return;
                }
                self.pending_group_ops.insert(id, HashMap::from([(sgid, add)]));
            }
        }
    }

    /// Remove and return the group operations buffered for `id`.
    pub(crate) fn take_pending_group_ops(&mut self, id: i32) -> HashMap<String, bool> {
        self.pending_group_ops.remove(&id).unwrap_or_default()
    }

    /// Drop buffered welcome state for a client whose payload is newer (an
    /// enterview) or that is gone (a left view).
    pub(crate) fn discard_pending_welcome_state(&mut self, id: i32) {
        self.pending_client_updates.remove(&id);
        self.pending_group_ops.remove(&id);
    }

    /// Whether a live event removed this client, so a late welcome row must
    /// not resurrect it.
    pub(crate) fn client_departed(&self, id: i32) -> bool {
        self.departed_clients.contains(&id)
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
            self.truncated = true;
            return false;
        }
        let previous_bytes = previous.map_or(0, channel_size);
        let next_bytes = self.channel_bytes - previous_bytes + channel_size(&channel);
        if next_bytes > MAX_DIRECTORY_BYTES {
            self.truncated = true;
            return false;
        }
        if previous == Some(&channel) {
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
            self.truncated = true;
            return false;
        }
        let previous_bytes = previous.map_or(0, client_size);
        let next_bytes = self.client_bytes - previous_bytes + client_size(&client);
        if next_bytes > MAX_DIRECTORY_BYTES {
            self.truncated = true;
            return false;
        }
        if previous == Some(&client) {
            return false;
        }
        self.client_bytes = next_bytes;
        self.clients.insert(client.id, client);
        true
    }

    pub(crate) fn remove_client(&mut self, id: i32) -> bool {
        self.discard_pending_welcome_state(id);
        // A client seen leaving must not come back through a welcome or
        // query row still in flight; enterview reinsertion is unaffected.
        if self.departed_clients.len() < MAX_DIRECTORY_ENTRIES {
            self.departed_clients.insert(id);
        }
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
            self.truncated = true;
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
            self.truncated = true;
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
            truncated: self.truncated,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.channels.clear();
        self.children.clear();
        self.clients.clear();
        self.channel_bytes = 0;
        self.client_bytes = 0;
        self.welcome_complete = false;
        self.truncated = false;
        self.pending_client_updates.clear();
        self.pending_group_ops.clear();
        self.departed_clients.clear();
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
        self.pending_client_updates.clear();
        self.pending_group_ops.clear();
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
    parse_channel_row(params, false)
}

fn parse_channel_notification(params: &HashMap<String, String>) -> Option<ChannelInfo> {
    parse_channel_row(params, true)
}

fn parse_channel_row(
    params: &HashMap<String, String>,
    require_parent: bool,
) -> Option<ChannelInfo> {
    let id = parse_u64(params.get("cid")?);
    if id == 0 {
        return None;
    }
    let parent = params.get("pid").or_else(|| params.get("cpid"));
    if require_parent && parent.is_none() {
        return None;
    }
    Some(ChannelInfo {
        id,
        parent_id: parent.map(|value| parse_u64(value)).unwrap_or(0),
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
        icon_id: params.get("channel_icon_id").and_then(|value| parse_icon_id(value)),
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
            let Some(mut channel) = parse_channel_notification(params) else {
                return false;
            };
            if channel.icon_id.is_none() {
                channel.icon_id = directory.channels.get(&id).and_then(|current| current.icon_id);
            }
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
            if let Some(value) = params.get("channel_icon_id").and_then(|value| parse_icon_id(value)) {
                if current.icon_id != Some(value) {
                    current.icon_id = Some(value);
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
        away: parse_bool(params.get("client_away")),
        away_message: params.get("client_away_message").cloned(),
        input_muted: parse_bool(params.get("client_input_muted")),
        output_muted: parse_bool(params.get("client_output_muted")),
        channel_commander: parse_bool(params.get("client_is_channel_commander")),
    })
}

/// Apply the fields a `notifyclientupdated` row carries to a known client.
/// Returns the updated record when the entry changed, `None` otherwise —
/// unknown clients' `notifyclientupdated` rows are buffered for the welcome
/// replay instead of applied.
pub(crate) fn apply_client_notification(
    name: &str,
    params: &HashMap<String, String>,
    directory: &mut DirectoryCache,
) -> Option<ClientInfo> {
    let Some(id) = parse_client_id(params) else {
        return None;
    };
    if directory.client(id).is_none() {
        if name == "notifyclientupdated" {
            directory.buffer_pending_client_update(id, params);
        } else if name == "notifyservergroupclientadded"
            || name == "notifyservergroupclientdeleted"
        {
            if let Some(group_id) = params.get("sgid").filter(|value| !value.is_empty()) {
                directory.buffer_pending_group_op(
                    id,
                    group_id.clone(),
                    name == "notifyservergroupclientadded",
                );
            }
        }
        return None;
    }
    if name == "notifyservergroupclientadded" || name == "notifyservergroupclientdeleted" {
        let Some(group_id) = params.get("sgid").filter(|value| !value.is_empty()) else {
            return None;
        };
        let add = name == "notifyservergroupclientadded";
        let changed = directory.update_client(id, |current| {
            if add {
                if current
                    .server_groups
                    .iter()
                    .any(|group| group == group_id)
                {
                    return false;
                }
                current.server_groups.push(group_id.clone());
            } else {
                let before = current.server_groups.len();
                current.server_groups.retain(|group| group != group_id);
                if current.server_groups.len() == before {
                    return false;
                }
            }
            true
        });
        if !changed {
            return None;
        }
        return directory.client(id).cloned();
    }
    if name != "notifyclientupdated" {
        return None;
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

    let changed = directory.update_client(id, |current| {
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
        if let Some(value) = parse_bool(params.get("client_away")) {
            if current.away != Some(value) {
                current.away = Some(value);
                changed = true;
            }
        }
        if let Some(value) = params.get("client_away_message") {
            if current.away_message.as_deref() != Some(value.as_str()) {
                current.away_message = Some(value.clone());
                changed = true;
            }
        }
        if let Some(value) = parse_bool(params.get("client_input_muted")) {
            if current.input_muted != Some(value) {
                current.input_muted = Some(value);
                changed = true;
            }
        }
        if let Some(value) = parse_bool(params.get("client_output_muted")) {
            if current.output_muted != Some(value) {
                current.output_muted = Some(value);
                changed = true;
            }
        }
        if let Some(value) = parse_bool(params.get("client_is_channel_commander")) {
            if current.channel_commander != Some(value) {
                current.channel_commander = Some(value);
                changed = true;
            }
        }
        changed
    });
    if changed {
        directory.client(id).cloned()
    } else {
        None
    }
}

fn parse_icon_id(value: &str) -> Option<u32> {
    value.parse::<u32>().ok().or_else(|| {
        // i_icon_id 也会以同一组位的有符号形式出现在协议里。
        value.parse::<i32>().ok().map(|id| id as u32)
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

pub(crate) fn parse_bool(value: Option<&String>) -> Option<bool> {
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
        + client.away_message.as_deref().map_or(0, str::len)
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
    fn welcome_updates_coalesce_per_client() {
        let mut directory = DirectoryCache::default();
        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "7"), ("client_input_muted", "1"), ("client_away", "1")]),
            &mut directory,
        ).is_none());
        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "7"), ("client_input_muted", "0"), ("client_away_message", "brb")]),
            &mut directory,
        ).is_none());
        assert!(directory.client(7).is_none());

        let patch = directory.take_pending_client_updates(7).expect("patch");
        assert_eq!(patch.len(), 4); // clid + the three state keys
        assert_eq!(patch.get("clid").map(String::as_str), Some("7"));
        assert_eq!(
            patch.get("client_input_muted").map(String::as_str),
            Some("0")
        );
        assert_eq!(patch.get("client_away").map(String::as_str), Some("1"));
        assert_eq!(
            patch.get("client_away_message").map(String::as_str),
            Some("brb")
        );
        assert!(directory.take_pending_client_updates(7).is_none());

        // Non-update notifications for unknown clients are not buffered.
        assert!(apply_client_notification(
            "notifyclientmoved",
            &row(&[("clid", "7"), ("ctid", "3")]),
            &mut directory,
        ).is_none());
        assert!(directory.take_pending_client_updates(7).is_none());

        // Once the welcome finishes, updates for unknown clients are dropped.
        directory.finish_welcome();
        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "9"), ("client_away", "1")]),
            &mut directory,
        ).is_none());
        assert!(directory.take_pending_client_updates(9).is_none());
    }

    #[test]
    fn pending_group_ops_coalesce_per_client() {
        let mut directory = DirectoryCache::default();
        // The last operation per group id wins; different ids are independent.
        assert!(apply_client_notification(
            "notifyservergroupclientadded",
            &row(&[("clid", "12"), ("sgid", "8")]),
            &mut directory,
        )
        .is_none());
        assert!(apply_client_notification(
            "notifyservergroupclientdeleted",
            &row(&[("clid", "12"), ("sgid", "8")]),
            &mut directory,
        )
        .is_none());
        assert!(apply_client_notification(
            "notifyservergroupclientadded",
            &row(&[("clid", "12"), ("sgid", "9")]),
            &mut directory,
        )
        .is_none());

        let ops = directory.take_pending_group_ops(12);
        assert_eq!(ops.len(), 2);
        assert_eq!(ops.get("8"), Some(&false));
        assert_eq!(ops.get("9"), Some(&true));
        assert!(directory.take_pending_group_ops(12).is_empty());

        // Once the welcome finishes, buffered group operations are dropped.
        assert!(apply_client_notification(
            "notifyservergroupclientadded",
            &row(&[("clid", "13"), ("sgid", "8")]),
            &mut directory,
        )
        .is_none());
        directory.finish_welcome();
        assert!(directory.take_pending_group_ops(13).is_empty());
    }

    #[test]
    fn pending_update_overflow_sets_truncated_but_keeps_buffered_clients() {
        let mut directory = DirectoryCache::default();
        for id in 1..=MAX_PENDING_CLIENT_UPDATES as i32 {
            let clid = id.to_string();
            assert!(apply_client_notification(
                "notifyclientupdated",
                &row(&[("clid", &clid), ("client_away", "1")]),
                &mut directory,
            ).is_none());
        }
        assert!(!directory.truncated());

        // A new id past the cap is dropped, and the snapshot reports the loss.
        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "20000"), ("client_away", "1")]),
            &mut directory,
        ).is_none());
        assert!(directory.take_pending_client_updates(20000).is_none());
        assert!(directory.truncated());

        // An already buffered id keeps merging past the cap.
        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "1"), ("client_input_muted", "1")]),
            &mut directory,
        ).is_none());
        let patch = directory.take_pending_client_updates(1).expect("patch");
        assert_eq!(
            patch.get("client_input_muted").map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn channellist_row_defaults_a_missing_parent_but_notifications_do_not() {
        let row = row(&[("cid", "42"), ("channel_name", "Lobby")]);
        assert_eq!(parse_channel(&row).expect("channel").parent_id, 0);

        let mut directory = DirectoryCache::default();
        assert!(!apply_channel_notification(
            "notifychannelcreated",
            &row,
            &mut directory,
        ));
        assert!(directory.snapshot().channels.is_empty());
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
        ).is_some());
        let client = directory.client(7).expect("client");
        assert_eq!(client.nickname, "Alicia");
        assert_eq!(client.uid, "uid");
        assert_eq!(client.channel_id, 42);
        assert_eq!(client.server_groups, vec!["6", "9"]);
        assert_eq!(client.away, None);
        assert_eq!(client.input_muted, None);

        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "7"), ("client_input_muted", "1")]),
            &mut directory,
        ).is_some());
        let client = directory.client(7).expect("client");
        assert_eq!(client.input_muted, Some(true));
        assert_eq!(client.output_muted, None);
        assert_eq!(client.nickname, "Alicia");
        assert_eq!(client.channel_id, 42);

        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "7"), ("client_input_muted", "0")]),
            &mut directory,
        ).is_some());
        assert_eq!(
            directory.client(7).expect("client").input_muted,
            Some(false)
        );

        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "7"), ("client_away", "1")]),
            &mut directory,
        ).is_some());
        assert_eq!(directory.client(7).expect("client").away, Some(true));

        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "7"), ("client_servergroups", "")]),
            &mut directory,
        ).is_some());
        assert!(directory.client(7).expect("client").server_groups.is_empty());

        assert!(apply_client_notification(
            "notifyservergroupclientadded",
            &row(&[("clid", "7"), ("sgid", "8")]),
            &mut directory,
        ).is_some());
        assert!(apply_client_notification(
            "notifyservergroupclientadded",
            &row(&[("clid", "7"), ("sgid", "8")]),
            &mut directory,
        ).is_none());
        assert_eq!(
            directory.client(7).expect("client").server_groups,
            vec!["8"]
        );
        assert!(apply_client_notification(
            "notifyservergroupclientdeleted",
            &row(&[("clid", "7"), ("sgid", "8")]),
            &mut directory,
        ).is_some());
        assert!(directory.client(7).expect("client").server_groups.is_empty());

        // A groups-only update leaves previously set flags alone.
        assert!(apply_client_notification(
            "notifyclientupdated",
            &row(&[("clid", "7"), ("client_servergroups", "6")]),
            &mut directory,
        ).is_some());
        let client = directory.client(7).expect("client");
        assert_eq!(client.server_groups, vec!["6"]);
        assert_eq!(client.input_muted, Some(false));
        assert_eq!(client.away, Some(true));
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
                away: None,
                away_message: None,
                input_muted: None,
                output_muted: None,
                channel_commander: None,
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
            icon_id: None,
        };

        assert!(!directory.insert_channel(channel));
        let snapshot = directory.snapshot();
        assert!(snapshot.channels.is_empty());
        assert!(snapshot.truncated);
    }

    #[test]
    fn channel_icons_keep_unsigned_signed_and_missing_ids() {
        for (wire, expected) in [
            ("0", Some(0)),
            ("4294967295", Some(u32::MAX)),
            ("-1", Some(u32::MAX)),
            ("-1285107717", Some(3009859579)),
            ("4294967296", None),
            ("-2147483649", None),
            ("invalid", None),
        ] {
            let params = HashMap::from([
                ("cid".to_string(), "1".to_string()),
                ("channel_icon_id".to_string(), wire.to_string()),
            ]);
            assert_eq!(parse_channel(&params).expect("channel").icon_id, expected);
        }
        let params = HashMap::from([("cid".to_string(), "1".to_string())]);
        assert_eq!(parse_channel(&params).expect("channel").icon_id, None);
    }

    #[test]
    fn repeated_channel_creation_keeps_an_omitted_icon_and_accepts_explicit_zero() {
        let mut directory = DirectoryCache::default();
        let mut params = HashMap::from([
            ("cid".to_string(), "1".to_string()),
            ("cpid".to_string(), "0".to_string()),
            ("channel_name".to_string(), "before".to_string()),
            ("channel_icon_id".to_string(), "-1".to_string()),
        ]);
        assert!(apply_channel_notification("notifychannelcreated", &params, &mut directory));
        params.remove("channel_icon_id");
        params.insert("channel_name".to_string(), "after".to_string());
        assert!(apply_channel_notification("notifychannelcreated", &params, &mut directory));
        let channel = directory.snapshot().channels.remove(0);
        assert_eq!(channel.name, "after");
        assert_eq!(channel.icon_id, Some(u32::MAX));
        params.insert("channel_icon_id".to_string(), "0".to_string());
        assert!(apply_channel_notification("notifychannelcreated", &params, &mut directory));
        assert_eq!(directory.snapshot().channels[0].icon_id, Some(0));
    }

    #[test]
    fn channel_icon_updates_preserve_omitted_values_and_clear_with_zero() {
        let mut directory = DirectoryCache::default();
        let mut params = HashMap::from([
            ("cid".to_string(), "1".to_string()),
            ("cpid".to_string(), "0".to_string()),
            ("channel_icon_id".to_string(), "-1".to_string()),
        ]);
        assert!(apply_channel_notification("notifychannelcreated", &params, &mut directory));
        assert_eq!(directory.snapshot().channels[0].icon_id, Some(u32::MAX));
        params.remove("channel_icon_id");
        assert!(!apply_channel_notification("notifychanneledited", &params, &mut directory));
        assert_eq!(directory.snapshot().channels[0].icon_id, Some(u32::MAX));
        params.insert("channel_icon_id".to_string(), "0".to_string());
        assert!(apply_channel_notification("notifychanneledited", &params, &mut directory));
        assert_eq!(directory.snapshot().channels[0].icon_id, Some(0));
    }
}
