use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tokio::sync::broadcast;

use crate::client::{Client, ClientStatus, ClientWeak};
use crate::types::Event;

use super::engine::OpenProtocolEngine;
use super::types::{
    BotCommandList, EngineEffects, OpenProtocolConfig, OpenProtocolEvent, PeerIdentity,
    SendOutcome, SendReceipt, TimerToken,
};

struct ServiceState {
    epoch: u64,
    engine: OpenProtocolEngine,
}

/// 面向常规异步应用（Bot/客户端）的 OpenProtocol 一键挂载句柄。
pub struct OpenProtocolHandle {
    client: ClientWeak,
    state: Mutex<ServiceState>,
    events: broadcast::Sender<OpenProtocolEvent>,
}

impl OpenProtocolHandle {
    /// 将 OpenProtocol 自动挂载到 `Client`，支持在 `connect()` 前后任意时机调用。
    pub fn attach(client: &Client, config: OpenProtocolConfig) -> Arc<Self> {
        let (events, _) = broadcast::channel(64);
        let weak_client = client.weak_handle();
        let handle = Arc::new(Self {
            client: weak_client.clone(),
            state: Mutex::new(ServiceState {
                epoch: 0,
                engine: OpenProtocolEngine::new(config, Instant::now()),
            }),
            events,
        });

        Self::register_handlers(client, &weak_client, &handle);

        if client.status() == ClientStatus::Connected {
            let clid = client.client_id();
            let channel_id = client.channel_id();
            if clid > 0 {
                handle.handle_connected(clid, channel_id);
            }
        }

        handle
    }

    pub fn effective_identity(&self) -> PeerIdentity {
        self.state.lock().unwrap().engine.effective_identity()
    }

    pub fn local_commands(&self) -> Option<BotCommandList> {
        self.state.lock().unwrap().engine.local_commands().cloned()
    }

    pub fn set_commands(&self, commands: Option<BotCommandList>) {
        self.state.lock().unwrap().engine.set_commands(commands);
    }

    pub fn announce(self: &Arc<Self>) {
        let (epoch, effects) = {
            let mut guard = self.state.lock().unwrap();
            let epoch = guard.epoch;
            let effects = guard.engine.announce_now(Instant::now());
            (epoch, effects)
        };
        self.dispatch_effects(epoch, effects);
    }

    pub fn query_peer(self: &Arc<Self>, clid: i32, peer_uid: Option<&str>) {
        let (epoch, effects) = {
            let mut guard = self.state.lock().unwrap();
            let epoch = guard.epoch;
            let effects = guard
                .engine
                .request_query_peer(clid, peer_uid, Instant::now());
            (epoch, effects)
        };
        self.dispatch_effects(epoch, effects);
    }

    pub fn fetch_commands(self: &Arc<Self>, clid: i32, peer_uid: &str) {
        let (epoch, effects) = {
            let mut guard = self.state.lock().unwrap();
            let epoch = guard.epoch;
            let effects = guard
                .engine
                .request_fetch_commands(clid, peer_uid, Instant::now());
            (epoch, effects)
        };
        self.dispatch_effects(epoch, effects);
    }

    pub fn peer_identity(&self, uid: &str) -> Option<PeerIdentity> {
        self.state
            .lock()
            .unwrap()
            .engine
            .peer_identity(uid)
            .cloned()
    }

    pub fn peer_commands(&self, uid: &str) -> Option<BotCommandList> {
        self.state
            .lock()
            .unwrap()
            .engine
            .peer_commands(uid)
            .cloned()
    }

    pub fn all_peer_identities(&self) -> HashMap<String, PeerIdentity> {
        self.state.lock().unwrap().engine.peer_identities().clone()
    }

    pub fn all_peer_commands(&self) -> HashMap<String, BotCommandList> {
        self.state
            .lock()
            .unwrap()
            .engine
            .peer_commands_map()
            .clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OpenProtocolEvent> {
        self.events.subscribe()
    }

    fn register_handlers(client: &Client, weak_client: &ClientWeak, handle: &Arc<Self>) {
        let weak_handle = Arc::downgrade(handle);
        let client_ref = weak_client.clone();
        client.on_connected(Arc::new(move || {
            let Some(handle) = weak_handle.upgrade() else {
                return;
            };
            let Some(client) = client_ref.upgrade() else {
                return;
            };
            let clid = client.client_id();
            if clid > 0 {
                handle.handle_connected(clid, client.channel_id());
            }
        }));

        let weak_handle = Arc::downgrade(handle);
        let client_ref = weak_client.clone();
        client.on_directory_snapshot(Arc::new(move |event| {
            let Event::DirectorySnapshot(snapshot) = event else {
                return;
            };
            let Some(handle) = weak_handle.upgrade() else {
                return;
            };
            let Some(client) = client_ref.upgrade() else {
                return;
            };
            let clid = client.client_id();
            if clid <= 0 {
                return;
            }
            let channel_id = snapshot
                .clients
                .iter()
                .find(|c| c.id == clid)
                .map(|c| c.channel_id)
                .unwrap_or_else(|| client.channel_id());
            let peers: Vec<(i32, &str, u64)> = snapshot
                .clients
                .iter()
                .map(|c| (c.id, c.uid.as_str(), c.channel_id))
                .collect();
            handle.handle_directory_snapshot(clid, channel_id, &peers);
        }));

        let weak_handle = Arc::downgrade(handle);
        client.on_client_enter(Arc::new(move |event| {
            let Event::ClientEnter(info) = event else {
                return;
            };
            let Some(handle) = weak_handle.upgrade() else {
                return;
            };
            handle.handle_peer_joined(info.id, &info.uid);
        }));

        let weak_handle = Arc::downgrade(handle);
        client.on_client_leave(Arc::new(move |event| {
            let Event::ClientLeave(ev) = event else {
                return;
            };
            let Some(handle) = weak_handle.upgrade() else {
                return;
            };
            handle.state.lock().unwrap().engine.on_peer_left(ev.id);
        }));

        let weak_handle = Arc::downgrade(handle);
        let client_ref = weak_client.clone();
        client.on_client_moved(Arc::new(move |event| {
            let Event::ClientMoved(ev) = event else {
                return;
            };
            let Some(handle) = weak_handle.upgrade() else {
                return;
            };
            let Some(client) = client_ref.upgrade() else {
                return;
            };
            let clid = client.client_id();
            if clid > 0 && ev.id == clid {
                handle.handle_self_moved(clid, ev.target_channel_id);
            }
        }));

        let plugin_handle = Arc::clone(handle);
        let client_ref = weak_client.clone();
        client.on_plugin_command(Arc::new(move |event| {
            let Event::PluginCommand(ev) = event else {
                return;
            };
            let handle = &plugin_handle;
            let client = client_ref.upgrade();
            let resolved_uid = if ev.invoker_uid.is_empty() {
                client.as_ref().and_then(|c| {
                    c.directory_snapshot().and_then(|snap| {
                        snap.clients
                            .into_iter()
                            .find(|peer| peer.id == ev.invoker_id && !peer.uid.is_empty())
                            .map(|peer| peer.uid)
                    })
                })
            } else {
                None
            };
            let live_clid = client.as_ref().map(|c| c.client_id()).filter(|&id| id > 0);
            let (epoch, effects) = {
                let mut guard = handle.state.lock().unwrap();
                if guard.engine.self_client_id().is_none() && live_clid.is_some() {
                    guard.engine.set_self_client_id(live_clid);
                }
                let epoch = guard.epoch;
                let effects =
                    guard
                        .engine
                        .on_plugin_command(&ev, resolved_uid.as_deref(), Instant::now());
                (epoch, effects)
            };
            handle.dispatch_effects(epoch, effects);
        }));

        let weak_handle = Arc::downgrade(handle);
        client.on_disconnected(Arc::new(move |_| {
            let Some(handle) = weak_handle.upgrade() else {
                return;
            };
            let mut guard = handle.state.lock().unwrap();
            guard.epoch = guard.epoch.wrapping_add(1);
            guard.engine.reset(Instant::now());
        }));
    }

    fn handle_connected(self: &Arc<Self>, clid: i32, channel_id: u64) {
        let (epoch, effects) = {
            let mut guard = self.state.lock().unwrap();
            let epoch = guard.epoch;
            let effects = guard.engine.on_connected(clid, channel_id, Instant::now());
            (epoch, effects)
        };
        self.dispatch_effects(epoch, effects);
    }

    fn handle_directory_snapshot(
        self: &Arc<Self>,
        clid: i32,
        channel_id: u64,
        peers: &[(i32, &str, u64)],
    ) {
        let (epoch, ready_effects, probe_effects) = {
            let mut guard = self.state.lock().unwrap();
            let now = Instant::now();
            let epoch = guard.epoch;
            let ready_effects = if guard.engine.self_client_id().is_none() {
                guard.engine.on_connected(clid, channel_id, now)
            } else if channel_id != 0 && guard.engine.self_channel_id() == 0 {
                guard.engine.on_self_moved(channel_id, now)
            } else {
                EngineEffects::default()
            };
            let probe_effects = guard.engine.sync_directory_peers(peers, now);
            (epoch, ready_effects, probe_effects)
        };
        self.dispatch_effects(epoch, ready_effects);
        self.dispatch_effects(epoch, probe_effects);
    }

    fn handle_peer_joined(self: &Arc<Self>, clid: i32, uid: &str) {
        let (epoch, effects) = {
            let mut guard = self.state.lock().unwrap();
            let epoch = guard.epoch;
            let effects = guard.engine.on_peer_joined(clid, uid, Instant::now());
            (epoch, effects)
        };
        self.dispatch_effects(epoch, effects);
    }

    fn handle_self_moved(self: &Arc<Self>, clid: i32, channel_id: u64) {
        let (epoch, effects) = {
            let mut guard = self.state.lock().unwrap();
            if guard.engine.self_client_id().is_none() {
                guard.engine.set_self_client_id(Some(clid));
            }
            let epoch = guard.epoch;
            let effects = guard.engine.on_self_moved(channel_id, Instant::now());
            (epoch, effects)
        };
        self.dispatch_effects(epoch, effects);
    }

    fn handle_timer(self: &Arc<Self>, epoch: u64, token: TimerToken) {
        let effects = {
            let mut guard = self.state.lock().unwrap();
            if guard.epoch != epoch {
                return;
            }
            guard.engine.on_timer(token, Instant::now())
        };
        self.dispatch_effects(epoch, effects);
    }

    fn handle_send_outcome(
        self: &Arc<Self>,
        epoch: u64,
        receipt: SendReceipt,
        outcome: SendOutcome,
    ) {
        let effects = {
            let mut guard = self.state.lock().unwrap();
            if guard.epoch != epoch {
                return;
            }
            guard.engine.on_send_outcome(receipt, outcome)
        };
        self.dispatch_effects(epoch, effects);
    }

    fn dispatch_effects(self: &Arc<Self>, epoch: u64, effects: EngineEffects) {
        for event in effects.events {
            let _ = self.events.send(event);
        }

        for cmd in effects.outbound {
            let weak_client = self.client.clone();
            let weak_handle = Arc::downgrade(self);
            tokio::spawn(async move {
                let Some(client) = weak_client.upgrade() else {
                    return;
                };
                let result = client
                    .send_plugin_command(cmd.plugin, &cmd.data, cmd.target, cmd.target_id)
                    .await;
                let Some(handle) = weak_handle.upgrade() else {
                    return;
                };
                let outcome = SendOutcome::from_result(&result);
                if let Err(err) = result {
                    let _ = handle.events.send(OpenProtocolEvent::SendFailed {
                        plugin: cmd.plugin,
                        target: cmd.target,
                        target_id: cmd.target_id,
                        error: err.to_string(),
                    });
                }
                handle.handle_send_outcome(epoch, cmd.receipt, outcome);
            });
        }

        for timer in effects.timers {
            let weak_handle = Arc::downgrade(self);
            tokio::spawn(async move {
                tokio::time::sleep(timer.delay).await;
                if let Some(handle) = weak_handle.upgrade() {
                    handle.handle_timer(epoch, timer.token);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::open_protocol::types::{BotCommand, COMMANDS_PLUGIN, ClientKind, IDENTITY_PLUGIN};
    use crate::types::ClientOptions;

    #[tokio::test]
    async fn handle_attaches_before_connect_and_answers_list_req() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "TestBot".to_string(),
            ClientOptions::default(),
        );

        let sent_cmds = Arc::new(Mutex::new(Vec::<String>::new()));
        client.install_open_protocol_test_transport(Arc::clone(&sent_cmds));

        let config =
            OpenProtocolConfig::new(PeerIdentity::new(ClientKind::Bot, "ExampleBot", "0.1.0"))
                .with_commands(
                    BotCommandList::new(vec![BotCommand::new("ping", "Ping bot")]).with_prefix("!"),
                );

        let handle = OpenProtocolHandle::attach(&client, config);
        assert!(
            handle
                .effective_identity()
                .has_capability(super::super::types::CAP_CHAT_SLASH_COMMANDS)
        );

        client.inject_open_protocol_test_line(
            7,
            r#"notifyplugincmd name=tsclient:commands data={"v":1,"action":"list_req"} invokerid=42 invokername=Alice invokeruid=alice-uid"#,
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !sent_cmds.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("list_ack command sent");

        let lines = sent_cmds.lock().unwrap().clone();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains(&format!("name={COMMANDS_PLUGIN}")));
        assert!(lines[0].contains("targetmode=2 target=42"));
        assert!(lines[0].contains("ping"));
    }

    #[tokio::test]
    async fn handle_caches_peer_identity_and_emits_broadcast_event() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "FlSpeak".to_string(),
            ClientOptions::default(),
        );

        let sent_cmds = Arc::new(Mutex::new(Vec::<String>::new()));
        client.install_open_protocol_test_transport(sent_cmds);

        let config =
            OpenProtocolConfig::new(PeerIdentity::new(ClientKind::FlSpeak, "FlSpeak", "0.1.14"));
        let handle = OpenProtocolHandle::attach(&client, config);
        let mut rx = handle.subscribe();

        client.inject_open_protocol_test_line(
            3,
            r#"notifyplugincmd name=tsclient:identity data={"v":1,"action":"announce_ack","client_type":"bot","app_name":"Claw","app_version":"0.5.0","capabilities":["chat:slash_commands"]} invokerid=12 invokername=Claw invokeruid=bot-uid"#,
        );

        let evt = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("event timeout")
            .expect("event received");
        assert!(matches!(
            evt,
            OpenProtocolEvent::PeerIdentityUpdated {
                ref peer_uid,
                clid: 12,
                ..
            } if peer_uid == "bot-uid"
        ));
        assert_eq!(
            handle.peer_identity("bot-uid").map(|i| i.app_name),
            Some("Claw".to_string())
        );
        assert_eq!(IDENTITY_PLUGIN, "tsclient:identity");

        let weak = Arc::downgrade(&handle);
        drop(handle);
        assert!(weak.upgrade().is_some());
        drop(client);
        for _ in 0..10 {
            if weak.upgrade().is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(weak.upgrade().is_none());
    }
}
