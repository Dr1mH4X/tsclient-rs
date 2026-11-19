use std::collections::HashMap;
use std::time::Instant;

use crate::types::{PluginCommandEvent, PluginCommandTarget};

use super::codec::{
    InboundMessage, effective_identity, encode_commands_ack, encode_commands_req,
    encode_identity_ack, encode_identity_announce, encode_query_message, parse_inbound_message,
    sanitize_bot_command_list,
};
use super::limiter::{
    AckRateLimiter, AnnounceAttemptMode, AnnounceDecision, AnnounceState, InflightCommands,
    ProbeQueue, QueryRateLimiter,
};
use super::types::{
    BotCommandList, COMMANDS_PLUGIN, ClientKind, CooldownAction, EngineEffects, IDENTITY_PLUGIN,
    OpenProtocolConfig, OpenProtocolEvent, OutboundPluginCommand, PeerIdentity,
    SELF_ANNOUNCE_RETRY_DELAY, SendOutcome, SendReceipt, TimerRequest, TimerToken,
};

/// 纯同步、无 IO 的 OpenProtocol 协议引擎，可嵌入单线程 Actor 或被异步托管层驱动。
pub struct OpenProtocolEngine {
    config: OpenProtocolConfig,
    self_clid: Option<i32>,
    self_channel_id: u64,
    server_beacon_sent: bool,
    announce_state: AnnounceState,
    ack_limiter: AckRateLimiter,
    query_limiter: QueryRateLimiter,
    inflight_commands: InflightCommands,
    probe_queue: ProbeQueue,
    peer_identities: HashMap<String, PeerIdentity>,
    peer_commands: HashMap<String, BotCommandList>,
}

impl OpenProtocolEngine {
    pub fn new(mut config: OpenProtocolConfig, now: Instant) -> Self {
        config.commands = config.commands.map(sanitize_bot_command_list);
        let query_cooldown = config.query_cooldown;
        let fetch_ttl = config.commands_fetch_ttl;
        let probe_interval = config.probe_interval;
        let probe_initial_delay = config.probe_initial_delay;
        let max_auto_discover_peers = config.max_auto_discover_peers;
        Self {
            config,
            self_clid: None,
            self_channel_id: 0,
            server_beacon_sent: false,
            announce_state: AnnounceState::default(),
            ack_limiter: AckRateLimiter::new(now),
            query_limiter: QueryRateLimiter::new(query_cooldown),
            inflight_commands: InflightCommands::new(fetch_ttl),
            probe_queue: ProbeQueue::new(
                probe_interval,
                probe_initial_delay,
                max_auto_discover_peers,
            ),
            peer_identities: HashMap::new(),
            peer_commands: HashMap::new(),
        }
    }

    pub fn effective_identity(&self) -> PeerIdentity {
        effective_identity(&self.config.identity, self.config.commands.as_ref())
    }

    pub fn local_commands(&self) -> Option<&BotCommandList> {
        self.config.commands.as_ref()
    }

    pub fn set_commands(&mut self, commands: Option<BotCommandList>) {
        self.config.commands = commands.map(sanitize_bot_command_list);
    }

    pub fn self_client_id(&self) -> Option<i32> {
        self.self_clid
    }

    pub fn set_self_client_id(&mut self, clid: Option<i32>) {
        self.self_clid = clid;
    }

    pub fn self_channel_id(&self) -> u64 {
        self.self_channel_id
    }

    pub fn peer_identity(&self, uid: &str) -> Option<&PeerIdentity> {
        self.peer_identities.get(uid)
    }

    pub fn peer_commands(&self, uid: &str) -> Option<&BotCommandList> {
        self.peer_commands.get(uid)
    }

    pub fn peer_identities(&self) -> &HashMap<String, PeerIdentity> {
        &self.peer_identities
    }

    pub fn peer_commands_map(&self) -> &HashMap<String, BotCommandList> {
        &self.peer_commands
    }

    pub fn on_connected(&mut self, clid: i32, channel_id: u64, now: Instant) -> EngineEffects {
        self.self_clid = Some(clid);
        self.self_channel_id = channel_id;
        if channel_id == 0 {
            return EngineEffects::default();
        }
        let mut effects = if self.config.announce_on_join {
            self.begin_announce(AnnounceAttemptMode::Normal, true, now)
        } else {
            EngineEffects::default()
        };
        self.maybe_enqueue_server_beacon(&mut effects);
        effects
    }

    pub fn on_self_moved(&mut self, channel_id: u64, now: Instant) -> EngineEffects {
        if channel_id == 0 {
            self.self_channel_id = 0;
            return EngineEffects::default();
        }
        let mut effects = EngineEffects::default();
        if !(channel_id == self.self_channel_id && self.announce_state.has_attempted()) {
            self.self_channel_id = channel_id;
            if self.config.announce_on_join {
                effects = self.begin_announce(AnnounceAttemptMode::Normal, true, now);
            }
        }
        self.maybe_enqueue_server_beacon(&mut effects);
        effects
    }

    pub fn sync_directory_peers(
        &mut self,
        peers: &[(i32, &str, u64)],
        now: Instant,
    ) -> EngineEffects {
        let mut effects = EngineEffects::default();
        if !self.config.auto_discover_peers || self.self_clid.is_none() {
            return effects;
        }
        if let Some(timer) = self.probe_queue.sync_peers(
            self.self_clid,
            self.self_channel_id,
            peers,
            &self.peer_identities,
            now,
        ) {
            effects.timers.push(timer);
        }
        effects
    }

    pub fn on_peer_joined(&mut self, clid: i32, uid: &str, now: Instant) -> EngineEffects {
        let mut effects = EngineEffects::default();
        if !self.config.auto_discover_peers || self.self_clid.is_none() {
            return effects;
        }
        if let Some(timer) =
            self.probe_queue
                .enqueue_joined(clid, uid, self.self_clid, &self.peer_identities, now)
        {
            effects.timers.push(timer);
        }
        effects
    }

    pub fn on_peer_left(&mut self, clid: i32) {
        self.probe_queue.remove_clid(clid);
    }

    pub fn announce_now(&mut self, now: Instant) -> EngineEffects {
        if self.self_channel_id == 0 {
            return EngineEffects::default();
        }
        self.begin_announce(AnnounceAttemptMode::Normal, true, now)
    }

    pub fn on_timer(&mut self, token: TimerToken, now: Instant) -> EngineEffects {
        match token {
            TimerToken::AnnounceTrailing => {
                if self.self_channel_id == 0 {
                    return EngineEffects::default();
                }
                self.begin_announce(AnnounceAttemptMode::Trailing, true, now)
            }
            TimerToken::AnnounceRetry { expected_seq } => {
                if self.self_channel_id != 0
                    && self.announce_state.should_run_failure_retry(expected_seq)
                {
                    self.begin_announce(AnnounceAttemptMode::Normal, false, now)
                } else {
                    EngineEffects::default()
                }
            }
            TimerToken::ProbeNext => self.step_probe_queue(now),
        }
    }

    pub fn on_plugin_command(
        &mut self,
        event: &PluginCommandEvent,
        resolved_uid: Option<&str>,
        now: Instant,
    ) -> EngineEffects {
        let mut effects = EngineEffects::default();
        if self.self_clid == Some(event.invoker_id) {
            return effects;
        }
        let Some(message) = parse_inbound_message(&event.name, &event.data) else {
            return effects;
        };

        let peer_uid = if !event.invoker_uid.is_empty() {
            event.invoker_uid.as_str()
        } else {
            resolved_uid.filter(|u| !u.is_empty()).unwrap_or("")
        };
        let cooldown_key = if !peer_uid.is_empty() {
            peer_uid.to_string()
        } else {
            format!("clid:{}", event.invoker_id)
        };

        match message {
            InboundMessage::IdentityAnnounce {
                identity: maybe_identity,
                noreply,
            } => {
                let mut is_bot = false;
                if let Some(identity) = maybe_identity {
                    is_bot = identity.client_type == ClientKind::Bot;
                    self.record_peer_identity(peer_uid, event.invoker_id, identity, &mut effects);
                }
                if !noreply && (!is_bot || self.config.reply_to_bots) {
                    self.enqueue_identity_ack(
                        &cooldown_key,
                        event.invoker_id,
                        CooldownAction::IdentityAnnounce,
                        now,
                        &mut effects,
                    );
                }
            }
            InboundMessage::IdentityQuery => {
                self.enqueue_identity_ack(
                    &cooldown_key,
                    event.invoker_id,
                    CooldownAction::IdentityQuery,
                    now,
                    &mut effects,
                );
            }
            InboundMessage::IdentityAnnounceAck(identity) => {
                self.record_peer_identity(peer_uid, event.invoker_id, identity, &mut effects);
            }
            InboundMessage::CommandsListReq => {
                if let Some(ref list) = self.config.commands
                    && let Some(data) = encode_commands_ack(list, event.invoker_id)
                    && self.ack_limiter.try_acquire(
                        &cooldown_key,
                        CooldownAction::CommandsList,
                        now,
                    )
                {
                    effects.outbound.push(OutboundPluginCommand {
                        plugin: COMMANDS_PLUGIN,
                        data,
                        target: PluginCommandTarget::Client,
                        target_id: Some(event.invoker_id),
                        receipt: SendReceipt::Ack {
                            peer: cooldown_key,
                            action: CooldownAction::CommandsList,
                            acquired_at: now,
                        },
                    });
                }
            }
            InboundMessage::CommandsListAck(list) => {
                if !peer_uid.is_empty() {
                    self.inflight_commands.complete(peer_uid);
                    if self.config.cache_peers {
                        self.peer_commands
                            .insert(peer_uid.to_string(), list.clone());
                    }
                }
                effects.events.push(OpenProtocolEvent::PeerCommandsUpdated {
                    peer_uid: peer_uid.to_string(),
                    clid: event.invoker_id,
                    commands: list,
                });
            }
        }

        effects
    }

    pub fn request_query_peer(
        &mut self,
        clid: i32,
        peer_uid: Option<&str>,
        now: Instant,
    ) -> EngineEffects {
        let mut effects = EngineEffects::default();
        if self.self_clid.is_none() || self.self_clid == Some(clid) {
            return effects;
        }
        let clean_uid = peer_uid.filter(|u| !u.is_empty());
        if let Some(uid) = clean_uid {
            self.probe_queue.mark_discovered(uid);
        }
        let key = clean_uid
            .map(str::to_string)
            .unwrap_or_else(|| format!("clid:{clid}"));
        if !self.query_limiter.try_acquire(&key, now) {
            return effects;
        }
        effects.outbound.push(OutboundPluginCommand {
            plugin: IDENTITY_PLUGIN,
            data: encode_query_message(),
            target: PluginCommandTarget::Client,
            target_id: Some(clid),
            receipt: SendReceipt::Query {
                peer: key,
                acquired_at: now,
            },
        });
        effects
    }

    pub fn request_fetch_commands(
        &mut self,
        clid: i32,
        peer_uid: &str,
        now: Instant,
    ) -> EngineEffects {
        let mut effects = EngineEffects::default();
        if self.self_clid.is_none()
            || self.self_clid == Some(clid)
            || peer_uid.is_empty()
            || self.peer_commands.contains_key(peer_uid)
            || !self.inflight_commands.try_begin(peer_uid, now)
        {
            return effects;
        }
        effects.outbound.push(OutboundPluginCommand {
            plugin: COMMANDS_PLUGIN,
            data: encode_commands_req(),
            target: PluginCommandTarget::Client,
            target_id: Some(clid),
            receipt: SendReceipt::CommandsFetch {
                peer_uid: peer_uid.to_string(),
                started_at: now,
            },
        });
        effects
    }

    pub fn on_send_outcome(&mut self, receipt: SendReceipt, outcome: SendOutcome) -> EngineEffects {
        let mut effects = EngineEffects::default();
        let flood = match outcome {
            SendOutcome::Succeeded => {
                if matches!(receipt, SendReceipt::Announce { .. }) {
                    self.announce_state.record_success();
                }
                return effects;
            }
            SendOutcome::Failed { flood } => flood,
        };

        if flood && !self.probe_queue.is_halted() {
            self.probe_queue.halt_for_flood();
            effects.events.push(OpenProtocolEvent::ProbeHaltedByFlood);
        }

        match receipt {
            SendReceipt::Announce {
                attempt_at,
                seq_at_start,
                allow_failure_retry,
            } => {
                let should_retry = self
                    .announce_state
                    .rollback_failure(attempt_at, seq_at_start);
                if allow_failure_retry && should_retry && !flood {
                    effects.timers.push(TimerRequest {
                        delay: SELF_ANNOUNCE_RETRY_DELAY,
                        token: TimerToken::AnnounceRetry {
                            expected_seq: seq_at_start,
                        },
                    });
                }
            }
            SendReceipt::ServerBeacon => {}
            SendReceipt::Ack {
                peer,
                action,
                acquired_at,
            } => {
                self.ack_limiter.rollback(&peer, action, acquired_at);
            }
            SendReceipt::Query { peer, acquired_at } => {
                self.query_limiter.rollback(&peer, acquired_at);
            }
            SendReceipt::ProbeQuery {
                peer_uid,
                acquired_at,
            } => {
                if flood {
                    self.query_limiter.rollback(&peer_uid, acquired_at);
                }
            }
            SendReceipt::CommandsFetch {
                peer_uid,
                started_at,
            } => {
                self.inflight_commands.rollback(&peer_uid, started_at);
            }
        }
        effects
    }

    pub fn reset(&mut self, now: Instant) {
        self.self_clid = None;
        self.self_channel_id = 0;
        self.server_beacon_sent = false;
        self.peer_identities.clear();
        self.peer_commands.clear();
        self.announce_state.reset();
        self.ack_limiter.reset(now);
        self.query_limiter.reset();
        self.inflight_commands.reset();
        self.probe_queue.reset();
    }

    fn maybe_enqueue_server_beacon(&mut self, effects: &mut EngineEffects) {
        if !self.config.server_announce_on_join
            || self.server_beacon_sent
            || self.self_channel_id == 0
        {
            return;
        }
        self.server_beacon_sent = true;
        let identity = self.effective_identity();
        if let Some(data) =
            encode_identity_announce(&identity, true, PluginCommandTarget::Server, None)
        {
            effects.outbound.push(OutboundPluginCommand {
                plugin: IDENTITY_PLUGIN,
                data,
                target: PluginCommandTarget::Server,
                target_id: None,
                receipt: SendReceipt::ServerBeacon,
            });
        }
    }

    fn step_probe_queue(&mut self, now: Instant) -> EngineEffects {
        let mut effects = EngineEffects::default();
        if !self.config.auto_discover_peers || self.self_clid.is_none() {
            return effects;
        }
        let (next, next_timer) = self.probe_queue.pop_next_ready(&self.peer_identities, now);
        if let Some(timer) = next_timer {
            effects.timers.push(timer);
        }
        if let Some((clid, uid)) = next
            && self.query_limiter.try_acquire(&uid, now)
        {
            effects.outbound.push(OutboundPluginCommand {
                plugin: IDENTITY_PLUGIN,
                data: encode_query_message(),
                target: PluginCommandTarget::Client,
                target_id: Some(clid),
                receipt: SendReceipt::ProbeQuery {
                    peer_uid: uid,
                    acquired_at: now,
                },
            });
        }
        effects
    }

    fn begin_announce(
        &mut self,
        mode: AnnounceAttemptMode,
        allow_failure_retry: bool,
        now: Instant,
    ) -> EngineEffects {
        let mut effects = EngineEffects::default();
        let identity = self.effective_identity();
        let Some(data) =
            encode_identity_announce(&identity, false, PluginCommandTarget::CurrentChannel, None)
        else {
            return effects;
        };

        match self.announce_state.try_begin(mode, now) {
            AnnounceDecision::SendNow {
                attempt_at,
                seq_at_start,
            } => {
                effects.outbound.push(OutboundPluginCommand {
                    plugin: IDENTITY_PLUGIN,
                    data,
                    target: PluginCommandTarget::CurrentChannel,
                    target_id: None,
                    receipt: SendReceipt::Announce {
                        attempt_at,
                        seq_at_start,
                        allow_failure_retry,
                    },
                });
            }
            AnnounceDecision::ScheduleTrailing { wait } => {
                effects.timers.push(TimerRequest {
                    delay: wait,
                    token: TimerToken::AnnounceTrailing,
                });
            }
            AnnounceDecision::AlreadyScheduled => {}
        }
        effects
    }

    fn enqueue_identity_ack(
        &mut self,
        cooldown_key: &str,
        invoker_id: i32,
        action: CooldownAction,
        now: Instant,
        effects: &mut EngineEffects,
    ) {
        let identity = self.effective_identity();
        let Some(data) = encode_identity_ack(&identity, invoker_id) else {
            return;
        };
        if !self.ack_limiter.try_acquire(cooldown_key, action, now) {
            return;
        }
        effects.outbound.push(OutboundPluginCommand {
            plugin: IDENTITY_PLUGIN,
            data,
            target: PluginCommandTarget::Client,
            target_id: Some(invoker_id),
            receipt: SendReceipt::Ack {
                peer: cooldown_key.to_string(),
                action,
                acquired_at: now,
            },
        });
    }

    fn record_peer_identity(
        &mut self,
        peer_uid: &str,
        clid: i32,
        identity: PeerIdentity,
        effects: &mut EngineEffects,
    ) {
        if !peer_uid.is_empty() {
            self.probe_queue.mark_discovered(peer_uid);
            if self.config.cache_peers {
                self.peer_identities
                    .insert(peer_uid.to_string(), identity.clone());
            }
        }
        effects.events.push(OpenProtocolEvent::PeerIdentityUpdated {
            peer_uid: peer_uid.to_string(),
            clid,
            identity,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::open_protocol::types::{BotCommand, CAP_CHAT_SLASH_COMMANDS};

    fn bot_config() -> OpenProtocolConfig {
        OpenProtocolConfig::new(PeerIdentity::new(ClientKind::Bot, "TestBot", "1.0.0"))
            .with_commands(
                BotCommandList::new(vec![BotCommand::new("replay", "Replay voice")])
                    .with_prefix("!"),
            )
    }

    #[test]
    fn connected_and_duplicate_move_do_not_double_announce() {
        let t0 = Instant::now();
        let mut engine =
            OpenProtocolEngine::new(bot_config().with_server_announce_on_join(true), t0);

        let e1 = engine.on_connected(7, 5, t0);
        assert_eq!(e1.outbound.len(), 2);
        assert_eq!(e1.outbound[0].target, PluginCommandTarget::CurrentChannel);
        assert!(!e1.outbound[0].data.contains("noreply"));
        assert_eq!(e1.outbound[1].target, PluginCommandTarget::Server);
        assert!(e1.outbound[1].data.contains(r#""noreply":true"#));

        // 同频道重复通知不触发额外广播或全服公告
        let e2 = engine.on_self_moved(5, t0 + Duration::from_millis(100));
        assert!(e2.outbound.is_empty());
        assert!(e2.timers.is_empty());

        // 真正切换频道只调度同频道尾随广播，不再重发全服单向公告
        let e3 = engine.on_self_moved(6, t0 + Duration::from_millis(200));
        assert!(e3.outbound.is_empty());
        assert_eq!(e3.timers.len(), 1);
        assert_eq!(e3.timers[0].token, TimerToken::AnnounceTrailing);
    }

    #[test]
    fn announce_failure_schedules_single_retry_only() {
        let t0 = Instant::now();
        let mut engine = OpenProtocolEngine::new(bot_config(), t0);
        let e1 = engine.on_connected(7, 5, t0);
        let receipt = e1.outbound[0].receipt.clone();

        let retry = engine.on_send_outcome(receipt, SendOutcome::Failed { flood: false });
        assert_eq!(retry.timers.len(), 1);
        let TimerToken::AnnounceRetry { expected_seq } = retry.timers[0].token else {
            panic!("expected AnnounceRetry timer");
        };

        let e2 = engine.on_timer(
            TimerToken::AnnounceRetry { expected_seq },
            t0 + Duration::from_millis(500),
        );
        assert_eq!(e2.outbound.len(), 1);
        let second_failure = engine.on_send_outcome(
            e2.outbound[0].receipt.clone(),
            SendOutcome::Failed { flood: false },
        );
        assert!(second_failure.timers.is_empty());
    }

    #[test]
    fn noreply_announce_caches_without_ack_while_query_still_replies() {
        let t0 = Instant::now();
        let mut engine = OpenProtocolEngine::new(bot_config(), t0);
        engine.on_connected(7, 5, t0);

        let beacon = PluginCommandEvent {
            name: IDENTITY_PLUGIN.to_string(),
            data: serde_json::json!({
                "v": 1,
                "action": "announce",
                "client_type": "flspeak",
                "app_name": "FlSpeak",
                "app_version": "0.1.14",
                "noreply": true
            })
            .to_string(),
            invoker_id: 21,
            invoker_name: "Alice".to_string(),
            invoker_uid: "alice-uid".to_string(),
        };
        let eff = engine.on_plugin_command(&beacon, None, t0);
        assert!(eff.outbound.is_empty());
        assert_eq!(
            engine
                .peer_identity("alice-uid")
                .map(|i| i.app_name.as_str()),
            Some("FlSpeak")
        );

        // 即使 query 报文被伪造携带 noreply: true，也必须正常回发 announce_ack
        let query = PluginCommandEvent {
            name: IDENTITY_PLUGIN.to_string(),
            data: r#"{"v":1,"action":"query","noreply":true}"#.to_string(),
            invoker_id: 21,
            invoker_name: "Alice".to_string(),
            invoker_uid: "alice-uid".to_string(),
        };
        let q_eff = engine.on_plugin_command(&query, None, t0);
        assert_eq!(q_eff.outbound.len(), 1);
        assert_eq!(q_eff.outbound[0].target, PluginCommandTarget::Client);
        assert_eq!(q_eff.outbound[0].target_id, Some(21));
    }

    #[test]
    fn self_echo_is_ignored_and_list_req_replies_with_ack() {
        let t0 = Instant::now();
        let mut engine = OpenProtocolEngine::new(bot_config(), t0);
        engine.on_connected(7, 5, t0);

        let self_event = PluginCommandEvent {
            name: IDENTITY_PLUGIN.to_string(),
            data: r#"{"v":1,"action":"announce","client_type":"bot"}"#.to_string(),
            invoker_id: 7,
            invoker_name: "TestBot".to_string(),
            invoker_uid: "self-uid".to_string(),
        };
        let self_effects = engine.on_plugin_command(&self_event, None, t0);
        assert!(self_effects.outbound.is_empty());
        assert!(self_effects.events.is_empty());

        let req_event = PluginCommandEvent {
            name: COMMANDS_PLUGIN.to_string(),
            data: r#"{"v":1,"action":"list_req"}"#.to_string(),
            invoker_id: 42,
            invoker_name: "Alice".to_string(),
            invoker_uid: "alice-uid".to_string(),
        };
        let req_effects = engine.on_plugin_command(&req_event, None, t0);
        assert_eq!(req_effects.outbound.len(), 1);
        assert_eq!(req_effects.outbound[0].plugin, COMMANDS_PLUGIN);
        assert_eq!(req_effects.outbound[0].target, PluginCommandTarget::Client);
        assert_eq!(req_effects.outbound[0].target_id, Some(42));
        assert!(
            req_effects.outbound[0]
                .data
                .contains(r#""command_prefix":"!""#)
        );
    }

    #[test]
    fn cross_channel_probe_queue_cancels_on_announce_and_trips_on_flood() {
        let t0 = Instant::now();
        let config =
            OpenProtocolConfig::new(PeerIdentity::new(ClientKind::FlSpeak, "FlSpeak", "0.1.14"))
                .with_auto_discover_peers(true);
        let mut engine = OpenProtocolEngine::new(config, t0);
        engine.on_connected(3, 1, t0);

        let sync = engine.sync_directory_peers(
            &[
                (3, "self-uid", 1),
                (12, "bot-uid", 8),
                (13, "peer-a", 9),
                (14, "peer-b", 10),
            ],
            t0,
        );
        assert_eq!(sync.timers.len(), 1);
        assert_eq!(sync.timers[0].token, TimerToken::ProbeNext);

        // 排队期间收到 peer-a 的 noreply 全服公告后，缓存更新且从待探测队列剔除
        let beacon = PluginCommandEvent {
            name: IDENTITY_PLUGIN.to_string(),
            data: r#"{"v":1,"action":"announce","client_type":"flspeak","app_name":"FlSpeak","app_version":"0.1.14","noreply":true}"#.to_string(),
            invoker_id: 13,
            invoker_name: "PeerA".to_string(),
            invoker_uid: "peer-a".to_string(),
        };
        let b_eff = engine.on_plugin_command(&beacon, None, t0 + Duration::from_millis(200));
        assert!(b_eff.outbound.is_empty());

        // 定时器首次触发：探测 bot-uid，并跳过已被剔除的 peer-a，再排下一次 peer-b
        let t1 = t0 + Duration::from_millis(1500);
        let p1 = engine.on_timer(TimerToken::ProbeNext, t1);
        assert_eq!(p1.outbound.len(), 1);
        assert_eq!(p1.outbound[0].target_id, Some(12));
        assert_eq!(p1.timers.len(), 1);

        // 遇到服务端防洪错误 524：立即熔断并清空队列，后续定时器不发任何包
        let halt = engine.on_send_outcome(
            p1.outbound[0].receipt.clone(),
            SendOutcome::Failed { flood: true },
        );
        assert_eq!(halt.events, vec![OpenProtocolEvent::ProbeHaltedByFlood]);

        let p2 = engine.on_timer(TimerToken::ProbeNext, t1 + Duration::from_millis(900));
        assert!(p2.outbound.is_empty());
        assert!(p2.timers.is_empty());
    }

    #[test]
    fn peer_discovery_caching_query_cooldown_and_inflight_ttl() {
        let t0 = Instant::now();
        let config =
            OpenProtocolConfig::new(PeerIdentity::new(ClientKind::FlSpeak, "FlSpeak", "0.1.14"));
        let mut engine = OpenProtocolEngine::new(config, t0);
        engine.on_connected(3, 1, t0);

        let bot_announce = PluginCommandEvent {
            name: IDENTITY_PLUGIN.to_string(),
            data: serde_json::json!({
                "v": 1,
                "action": "announce",
                "client_type": "bot",
                "app_name": "TeamSpeakClaw",
                "app_version": "0.5.0",
                "capabilities": [CAP_CHAT_SLASH_COMMANDS]
            })
            .to_string(),
            invoker_id: 12,
            invoker_name: "ClawBot".to_string(),
            invoker_uid: "bot-uid".to_string(),
        };
        let eff = engine.on_plugin_command(&bot_announce, None, t0);
        assert_eq!(eff.events.len(), 1);
        assert_eq!(
            engine.peer_identity("bot-uid").map(|i| i.app_name.as_str()),
            Some("TeamSpeakClaw")
        );

        let q1 = engine.request_query_peer(20, None, t0);
        assert_eq!(q1.outbound.len(), 1);
        let q2 = engine.request_query_peer(20, None, t0 + Duration::from_secs(5));
        assert!(q2.outbound.is_empty());
        let q3 = engine.request_query_peer(20, None, t0 + Duration::from_secs(31));
        assert_eq!(q3.outbound.len(), 1);

        let f1 = engine.request_fetch_commands(12, "bot-uid", t0);
        assert_eq!(f1.outbound.len(), 1);
        let f2 = engine.request_fetch_commands(12, "bot-uid", t0 + Duration::from_secs(5));
        assert!(f2.outbound.is_empty());
        let f3 = engine.request_fetch_commands(12, "bot-uid", t0 + Duration::from_secs(16));
        assert_eq!(f3.outbound.len(), 1);

        let list_ack = PluginCommandEvent {
            name: COMMANDS_PLUGIN.to_string(),
            data: r#"{"v":1,"action":"list_ack","command_prefix":"!","commands":[{"name":"replay","desc":"Replay"}]}"#.to_string(),
            invoker_id: 12,
            invoker_name: "ClawBot".to_string(),
            invoker_uid: "bot-uid".to_string(),
        };
        let ack_eff = engine.on_plugin_command(&list_ack, None, t0 + Duration::from_secs(17));
        assert_eq!(ack_eff.events.len(), 1);
        assert!(engine.peer_commands("bot-uid").is_some());
        assert!(
            engine
                .request_fetch_commands(12, "bot-uid", t0 + Duration::from_secs(18))
                .outbound
                .is_empty()
        );
    }
}
