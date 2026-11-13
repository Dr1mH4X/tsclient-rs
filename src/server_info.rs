//! 虚拟服务器信息解析、缓存合并与计数器过滤。

use std::collections::HashMap;

use crate::directory::parse_icon_id;
use crate::types::ServerInfo;

const SERVER_PROPERTY_PREFIX: &str = "virtualserver_";

/// `servergetvariables` 与 `serverinfo` 中随时间/在线人数自行变化的计数器字段；
/// 这些字段不保留在本地缓存中，避免离线后失真或触发无效更新事件。
const VOLATILE_COUNTERS: &[&str] = &[
    "virtualserver_clientsonline",
    "virtualserver_queryclientsonline",
    "virtualserver_channelsonline",
    "virtualserver_uptime",
    "virtualserver_client_connections",
    "virtualserver_query_client_connections",
    "virtualserver_month_bytes_downloaded",
    "virtualserver_month_bytes_uploaded",
    "virtualserver_total_bytes_downloaded",
    "virtualserver_total_bytes_uploaded",
    "virtualserver_total_packetloss_speech",
    "virtualserver_total_packetloss_keepalive",
    "virtualserver_total_packetloss_control",
    "virtualserver_total_packetloss_total",
    "virtualserver_total_ping",
];

pub(crate) fn is_volatile_counter(key: &str) -> bool {
    VOLATILE_COUNTERS.contains(&key)
}

/// 从 `initserver` 提取初始 `virtualserver_*` 属性（过滤客户端自身字段与易变计数器）。
pub(crate) fn extract_server_properties(
    params: &HashMap<String, String>,
    include_counters: bool,
) -> HashMap<String, String> {
    params
        .iter()
        .filter(|(k, _)| {
            k.starts_with(SERVER_PROPERTY_PREFIX) && (include_counters || !is_volatile_counter(k))
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// 将 `params` 中的 `virtualserver_*` 属性合并进 `target`，返回是否发生实际变化。
pub(crate) fn merge_server_properties(
    target: &mut HashMap<String, String>,
    params: &HashMap<String, String>,
    include_counters: bool,
) -> bool {
    let mut changed = false;
    for (key, value) in params {
        if !key.starts_with(SERVER_PROPERTY_PREFIX)
            || (!include_counters && is_volatile_counter(key))
        {
            continue;
        }
        if target.get(key) != Some(value) {
            target.insert(key.clone(), value.clone());
            changed = true;
        }
    }
    changed
}

/// 从已解码（`parse_command` 已完成 `unescape`）的键值表构建 [`ServerInfo`]。
pub(crate) fn build_server_info(raw: &HashMap<String, String>) -> ServerInfo {
    ServerInfo {
        name: raw.get("virtualserver_name").cloned().unwrap_or_default(),
        unique_identifier: raw
            .get("virtualserver_unique_identifier")
            .cloned()
            .unwrap_or_default(),
        welcome_message: raw
            .get("virtualserver_welcomemessage")
            .cloned()
            .unwrap_or_default(),
        icon_id: raw
            .get("virtualserver_icon_id")
            .and_then(|value| parse_icon_id(value)),
        max_clients: raw
            .get("virtualserver_maxclients")
            .and_then(|value| value.trim().parse::<u32>().ok()),
        hostbanner_gfx_url: raw
            .get("virtualserver_hostbanner_gfx_url")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        platform: raw
            .get("virtualserver_platform")
            .cloned()
            .unwrap_or_default(),
        version: raw
            .get("virtualserver_version")
            .cloned()
            .unwrap_or_default(),
        raw: raw.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn extract_keeps_only_virtualserver_fields() {
        let input = params(&[
            ("aclid", "3"),
            ("client_nickname", "Me"),
            ("virtualserver_name", "Mine"),
            ("virtualserver_maxclients", "32"),
            ("virtualserver_clientsonline", "5"),
        ]);
        let seeded = extract_server_properties(&input, true);
        assert_eq!(seeded.len(), 3);
        assert!(!seeded.contains_key("aclid"));
        assert!(!seeded.contains_key("client_nickname"));

        let filtered = extract_server_properties(&input, false);
        assert_eq!(filtered.len(), 2);
        assert!(!filtered.contains_key("virtualserver_clientsonline"));
    }

    #[test]
    fn merge_detects_no_op_and_filters_counters() {
        let mut target = params(&[
            ("virtualserver_name", "Before"),
            ("virtualserver_welcomemessage", "Hello"),
        ]);
        assert!(!merge_server_properties(
            &mut target,
            &params(&[
                ("virtualserver_name", "Before"),
                ("virtualserver_uptime", "900"),
                ("invokerid", "1"),
            ]),
            false,
        ));
        assert!(!target.contains_key("virtualserver_uptime"));
        assert!(!target.contains_key("invokerid"));

        assert!(merge_server_properties(
            &mut target,
            &params(&[
                ("virtualserver_welcomemessage", "Changed"),
                ("virtualserver_clientsonline", "10"),
            ]),
            false,
        ));
        assert_eq!(
            target
                .get("virtualserver_welcomemessage")
                .map(String::as_str),
            Some("Changed")
        );
        assert!(!target.contains_key("virtualserver_clientsonline"));
    }

    #[test]
    fn build_server_info_parses_icon_id_and_optional_fields() {
        let info = build_server_info(&params(&[
            ("virtualserver_name", "TeamSpeak ]I[ Server"),
            ("virtualserver_unique_identifier", "abc123="),
            ("virtualserver_welcomemessage", "Welcome\\nLine2"),
            ("virtualserver_icon_id", "-1528016466"),
            ("virtualserver_maxclients", "64"),
            (
                "virtualserver_hostbanner_gfx_url",
                " https://example.com/a.png ",
            ),
            ("virtualserver_platform", "Linux"),
            ("virtualserver_version", "3.13.8"),
        ]));
        assert_eq!(info.name, "TeamSpeak ]I[ Server");
        assert_eq!(info.unique_identifier, "abc123=");
        assert_eq!(info.welcome_message, "Welcome\\nLine2");
        assert_eq!(info.icon_id, Some(2766950830));
        assert_eq!(info.max_clients, Some(64));
        assert_eq!(
            info.hostbanner_gfx_url.as_deref(),
            Some("https://example.com/a.png")
        );
        assert_eq!(info.platform, "Linux");
        assert_eq!(info.version, "3.13.8");
        assert_eq!(info.get_raw("virtualserver_maxclients"), Some("64"));

        let zero_icon = build_server_info(&params(&[
            ("virtualserver_icon_id", "0"),
            ("virtualserver_hostbanner_gfx_url", "   "),
        ]));
        assert_eq!(zero_icon.icon_id, Some(0));
        assert_eq!(zero_icon.hostbanner_gfx_url, None);

        let missing_icon = build_server_info(&params(&[("virtualserver_icon_id", "bad")]));
        assert_eq!(missing_icon.icon_id, None);
    }
}
