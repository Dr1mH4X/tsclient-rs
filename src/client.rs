//! Client lifecycle & connection management — mirrors `teamspeak-js/src/client.ts`

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::Error;
use crate::command::parse_command;
use crate::commands::{self, CommandTracker};
use crate::connection_info::build_set_connection_info;
use crate::crypto::{Crypt, Identity};
use crate::directory as server_directory;
use crate::discovery::Resolver;
use crate::events::{build_command_chain, build_event_chain};
use crate::helpers::{is_auto_nickname_match, split_command_rows};
use crate::notifications::{NotificationResult, handle_notification};
use crate::throttle::CommandThrottle;
use crate::transfer::{self, FileTransferTracker, FtNotification};
use crate::transport::{
    Packet, PacketHandler, PacketLoss, PacketSender, PacketType, RoundTrip, TrafficSnapshot,
};
pub use crate::types::AbortSignal;
pub use crate::types::ClientStatus;
use crate::types::*;

// ---- Internal helpers -----------------------------------------------------------

#[derive(Clone)]
pub(crate) struct ClientInitOptions {
    pub(crate) server_password: String,
    pub(crate) default_channel: String,
    pub(crate) default_channel_password: String,
}

impl From<&ClientOptions> for ClientInitOptions {
    fn from(opts: &ClientOptions) -> Self {
        Self {
            server_password: opts.server_password.clone().unwrap_or_default(),
            default_channel: opts.default_channel.clone().unwrap_or_default(),
            default_channel_password: opts.default_channel_password.clone().unwrap_or_default(),
        }
    }
}

const DIRECTORY_PUBLISH_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Default)]
struct DirectoryPublication {
    dirty: bool,
    next_publish: Option<tokio::time::Instant>,
}

impl DirectoryPublication {
    fn mark(&mut self) {
        self.dirty = true;
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.dirty
            .then(|| self.next_publish.unwrap_or_else(tokio::time::Instant::now))
    }

    fn take_due(&mut self, now: tokio::time::Instant) -> bool {
        if !self.dirty || self.next_publish.is_some_and(|next| now < next) {
            return false;
        }
        self.dirty = false;
        self.next_publish = Some(now + DIRECTORY_PUBLISH_INTERVAL);
        true
    }
}

struct OrderedDirectoryHandler {
    handler: EventHandler,
    pending: Arc<Mutex<Option<DirectorySnapshot>>>,
    wake: Arc<Notify>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl OrderedDirectoryHandler {
    fn new(handler: EventHandler) -> Self {
        Self {
            handler,
            pending: Arc::new(Mutex::new(None)),
            wake: Arc::new(Notify::new()),
            shutdown: None,
        }
    }

    fn enqueue(&mut self, snapshot: DirectorySnapshot) {
        if self.shutdown.is_none() {
            self.start();
        }
        {
            // Full snapshots are state: a slow handler keeps only the newest one.
            *self.pending.lock().unwrap() = Some(snapshot);
        }
        self.wake.notify_one();
    }

    fn start(&mut self) {
        let handler = Arc::clone(&self.handler);
        let pending = Arc::clone(&self.pending);
        let wake = Arc::clone(&self.wake);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    _ = wake.notified() => {}
                }
                let snapshot = { pending.lock().unwrap().take() };
                if let Some(snapshot) = snapshot {
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        handler(Event::DirectorySnapshot(snapshot));
                    }));
                }
            }
        });
        self.shutdown = Some(shutdown_tx);
    }
}

/// Cap on updates queued per handler; past it the oldest update is dropped.
/// Each event carries the full record, so consumers still converge, and the
/// coalesced `DirectorySnapshot` event carries the whole state regardless.
const MAX_QUEUED_CLIENT_UPDATES: usize = 256;

/// Cap on plugin commands queued per serial handler so bursts stay ordered
/// without unbounded memory growth.
const MAX_QUEUED_PLUGIN_COMMANDS: usize = 1024;

/// Delivers state updates to one handler in dispatch order: a drain task
/// pops a bounded FIFO, so an older record can never overtake a newer one
/// and a slow handler cannot accumulate unbounded work.
struct SerialEventHandler {
    handler: EventHandler,
    pending: Arc<Mutex<VecDeque<Event>>>,
    wake: Arc<Notify>,
    max_queued: usize,
    shutdown: Option<oneshot::Sender<()>>,
}

impl SerialEventHandler {
    fn new(handler: EventHandler) -> Self {
        Self::with_capacity(handler, MAX_QUEUED_CLIENT_UPDATES)
    }

    fn with_capacity(handler: EventHandler, max_queued: usize) -> Self {
        Self {
            handler,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            wake: Arc::new(Notify::new()),
            max_queued,
            shutdown: None,
        }
    }

    fn enqueue(&mut self, event: Event) {
        if self.shutdown.is_none() {
            self.start();
        }
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.len() >= self.max_queued {
                pending.pop_front();
            }
            pending.push_back(event);
        }
        self.wake.notify_one();
    }

    fn start(&mut self) {
        let handler = Arc::clone(&self.handler);
        let pending = Arc::clone(&self.pending);
        let wake = Arc::clone(&self.wake);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => break,
                    _ = wake.notified() => {}
                }
                loop {
                    if matches!(
                        shutdown_rx.try_recv(),
                        Ok(_) | Err(oneshot::error::TryRecvError::Closed)
                    ) {
                        return;
                    }
                    let next = pending.lock().unwrap().pop_front();
                    let Some(event) = next else { break };
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        handler(event);
                    }));
                }
            }
        });
        self.shutdown = Some(shutdown_tx);
    }
}

/// Cap on raw notifications queued per handler; past it the queue resets
/// and signals an overflow event instead of dropping individual signaling packets.
const MAX_QUEUED_RAW_NOTIFICATIONS: usize = 256;

/// Delivers raw notifications to one handler in strict dispatch order:
/// a drain task pops a bounded FIFO. When queue reaches 256, it clears
/// pending items and delivers `Event::NotificationOverflow`.
struct SerialRawNotificationHandler {
    handler: EventHandler,
    pending: Arc<Mutex<VecDeque<Event>>>,
    wake: Arc<Notify>,
    shutdown: Option<oneshot::Sender<()>>,
    logger: Arc<dyn Logger>,
}

impl SerialRawNotificationHandler {
    fn new(handler: EventHandler, logger: Arc<dyn Logger>) -> Self {
        Self {
            handler,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            wake: Arc::new(Notify::new()),
            shutdown: None,
            logger,
        }
    }

    fn enqueue(&mut self, event: Event) {
        if self.shutdown.is_none() {
            self.start();
        }
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.len() >= MAX_QUEUED_RAW_NOTIFICATIONS {
                self.logger.warn(
                    "raw notification queue full (256); resetting queue and signaling overflow",
                    &[],
                );
                pending.clear();
                pending.push_back(Event::NotificationOverflow {
                    queue: "raw_notification".to_string(),
                });
            } else {
                pending.push_back(event);
            }
        }
        self.wake.notify_one();
    }

    fn start(&mut self) {
        let handler = Arc::clone(&self.handler);
        let pending = Arc::clone(&self.pending);
        let wake = Arc::clone(&self.wake);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => break,
                    _ = wake.notified() => {}
                }
                loop {
                    if matches!(
                        shutdown_rx.try_recv(),
                        Ok(_) | Err(oneshot::error::TryRecvError::Closed)
                    ) {
                        return;
                    }
                    let next = pending.lock().unwrap().pop_front();
                    let Some(event) = next else { break };
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        handler(event);
                    }));
                }
            }
        });
        self.shutdown = Some(shutdown_tx);
    }
}

enum LifecycleJob {
    Connected(Vec<Arc<dyn Fn() + Send + Sync>>),
    Disconnected {
        handlers: Vec<EventHandler>,
        err: Option<Error>,
        done: watch::Sender<bool>,
    },
}

/// Serializes `connected` and `disconnected` listener callbacks so a slow
/// disconnect callback from an earlier session can never overtake a reconnect.
struct LifecycleDispatcher {
    tx: mpsc::UnboundedSender<LifecycleJob>,
    _shutdown: oneshot::Sender<()>,
}

impl LifecycleDispatcher {
    fn spawn() -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<LifecycleJob>();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            loop {
                let job = tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => break,
                    maybe = rx.recv() => match maybe {
                        Some(job) => job,
                        None => break,
                    },
                };
                match job {
                    LifecycleJob::Connected(handlers) => {
                        for h in handlers {
                            let _ = catch_unwind(AssertUnwindSafe(|| {
                                h();
                            }));
                        }
                    }
                    LifecycleJob::Disconnected {
                        handlers,
                        err,
                        done,
                    } => {
                        for h in handlers {
                            let e = err.clone();
                            let _ = catch_unwind(AssertUnwindSafe(|| {
                                h(Event::Disconnected(e));
                            }));
                        }
                        let _ = done.send(true);
                    }
                }
            }
        });
        Self {
            tx,
            _shutdown: shutdown_tx,
        }
    }

    fn dispatch(&mut self, job: LifecycleJob) {
        let _ = self.tx.send(job);
    }
}

// ---- Event handler storage ------------------------------------------------------

struct EventHandlers {
    text_message: Vec<EventHandler>,
    client_enter: Vec<EventHandler>,
    client_updated: Vec<SerialEventHandler>,
    client_leave: Vec<EventHandler>,
    client_moved: Vec<EventHandler>,
    directory_snapshot: Vec<OrderedDirectoryHandler>,
    server_info_updated: Vec<SerialEventHandler>,
    server_edited: Vec<EventHandler>,
    poked: Vec<EventHandler>,
    own_permissions_changed: Vec<EventHandler>,
    voice_data: Vec<SerialEventHandler>,
    connected: Vec<Arc<dyn Fn() + Send + Sync>>,
    disconnected: Vec<EventHandler>,
    kicked: Vec<EventHandler>,
    plugin_command: Vec<SerialEventHandler>,
    raw_notification: Vec<SerialRawNotificationHandler>,
}

impl EventHandlers {
    fn new() -> Self {
        Self {
            text_message: Vec::new(),
            client_enter: Vec::new(),
            client_updated: Vec::new(),
            client_leave: Vec::new(),
            client_moved: Vec::new(),
            directory_snapshot: Vec::new(),
            server_info_updated: Vec::new(),
            server_edited: Vec::new(),
            poked: Vec::new(),
            own_permissions_changed: Vec::new(),
            voice_data: Vec::new(),
            connected: Vec::new(),
            disconnected: Vec::new(),
            kicked: Vec::new(),
            plugin_command: Vec::new(),
            raw_notification: Vec::new(),
        }
    }
}

// ---- ClientInner (all private mutable state) ------------------------------------

pub(crate) struct ClientInner {
    pub(crate) crypt: Crypt,
    pub(crate) logger: Arc<dyn Logger>,
    pub(crate) nickname: String,
    pub(crate) clid: i32,
    pub(crate) identity: Identity,
    pub(crate) addr: String,
    pub(crate) client_init_options: ClientInitOptions,
    pub(crate) status: ClientStatus,
    pub(crate) connection_epoch: u64,
    pub(crate) cmd_track: CommandTracker,
    own_permissions: HashMap<u32, i32>,
    pub(crate) ft_track: FileTransferTracker,
    pub(crate) directory: server_directory::DirectoryCache,
    pub(crate) directory_ready: bool,
    directory_publication: DirectoryPublication,
    server_properties: Option<HashMap<String, String>>,
    pub(crate) connected_wakers: Vec<(u64, oneshot::Sender<()>)>,
    directory_wakers: Vec<(u64, oneshot::Sender<()>)>,
    next_waker_id: u64,
    event_handlers: EventHandlers,
    lifecycle: Option<LifecycleDispatcher>,
    pending_disconnect_done: Option<watch::Receiver<bool>>,
    defer_events: bool,
    deferred_events: Vec<Event>,
    pub(crate) cmd_middlewares: Vec<Box<dyn CommandMiddleware>>,
    pub(crate) event_middlewares: Vec<Box<dyn EventMiddleware>>,
    pub(crate) packet_rx: mpsc::UnboundedReceiver<Packet>,
    pub(crate) close_rx: mpsc::UnboundedReceiver<Option<Error>>,
    pub(crate) close_tx: mpsc::UnboundedSender<Option<Error>>,
    /// Set when error 3329 is received — signals the bg task to perform
    /// a deferred disconnect, matching JS `setImmediate(() => this.disconnect())`.
    pub(crate) pending_disconnect: bool,
    self_weak: Weak<Mutex<ClientInner>>,
    throttle: Arc<tokio::sync::Mutex<CommandThrottle>>,
    final_cmd_handler: Arc<Mutex<CommandHandler>>,
}

impl ClientInner {
    fn new(
        crypt: Crypt,
        logger: Arc<dyn Logger>,
        nickname: String,
        identity: Identity,
        addr: String,
        client_init_options: ClientInitOptions,
        cmd_middlewares: Vec<Box<dyn CommandMiddleware>>,
        event_middlewares: Vec<Box<dyn EventMiddleware>>,
        close_tx: mpsc::UnboundedSender<Option<Error>>,
        self_weak: Weak<Mutex<ClientInner>>,
        throttle: Arc<tokio::sync::Mutex<CommandThrottle>>,
        final_cmd_handler: Arc<Mutex<CommandHandler>>,
    ) -> Self {
        let (_, packet_rx) = mpsc::unbounded_channel();
        let (_, close_rx) = mpsc::unbounded_channel();
        Self {
            crypt,
            logger,
            nickname,
            clid: 0,
            identity,
            addr,
            client_init_options,
            status: ClientStatus::Disconnected,
            connection_epoch: 0,
            cmd_track: CommandTracker::new(),
            own_permissions: HashMap::new(),
            ft_track: FileTransferTracker::new(),
            directory: server_directory::DirectoryCache::default(),
            directory_ready: false,
            directory_publication: DirectoryPublication::default(),
            server_properties: None,
            connected_wakers: Vec::new(),
            directory_wakers: Vec::new(),
            next_waker_id: 0,
            event_handlers: EventHandlers::new(),
            lifecycle: None,
            pending_disconnect_done: None,
            defer_events: false,
            deferred_events: Vec::new(),
            cmd_middlewares,
            event_middlewares,
            packet_rx,
            close_rx,
            close_tx,
            pending_disconnect: false,
            self_weak,
            throttle,
            final_cmd_handler,
        }
    }

    // ---- Packet processing -------------------------------------------------------

    fn process_packet(&mut self, sender: &PacketSender, p: Packet) {
        let p_type = p.type_flagged & 0x0f;
        if p_type == 8
        /* Init1 */
        {
            if self.crypt.crypto_init_complete {
                return;
            }
            match crate::handshake::process_init1(&mut self.crypt, Some(&p.data)) {
                Ok(Some(response)) => {
                    sender.set_crypt(self.crypt.clone());
                    sender.send_packet(PacketType::Init1, response, 0);
                }
                _ => {}
            }
            return;
        }

        if (p_type == 0 /* Voice */ || p_type == 1/* VoiceWhisper */) && p.data.len() >= 5 {
            self.handle_voice_packet(&p.data, p_type == 1, p.received_at);
            return;
        }

        if (p_type == 2 /* Command */ || p_type == 3/* CommandLow */) && !p.data.is_empty() {
            if let Ok(s) = String::from_utf8(p.data) {
                self.handle_command_lines(&s, sender);
            }
        }
    }

    fn handle_voice_packet(
        &mut self,
        payload: &[u8],
        whisper: bool,
        received_at: std::time::Instant,
    ) {
        if self.event_handlers.voice_data.is_empty() && self.event_middlewares.is_empty() {
            return;
        }

        if payload.len() < 5 {
            return;
        }

        let seq = u16::from_be_bytes([payload[0], payload[1]]);
        let client_id = u16::from_be_bytes([payload[2], payload[3]]) as i32;
        if client_id == self.clid {
            return;
        }

        let codec = payload[4] as i32;
        let data = bytes::Bytes::copy_from_slice(&payload[5..]);

        let voice_data = VoiceData {
            client_id,
            codec,
            data,
            seq,
            whisper,
            received_at,
        };

        self.dispatch_event(Event::VoiceData(voice_data));
    }

    fn handle_command_lines(&mut self, s: &str, sender: &PacketSender) {
        if s.is_empty() {
            return;
        }
        for line in s.replace('\0', "\n").split('\n') {
            let trimmed = line.trim_end_matches('\r');
            if trimmed.is_empty() {
                continue;
            }
            self.handle_command_str(trimmed, sender);
        }
    }

    fn handle_command_str(&mut self, s: &str, sender: &PacketSender) {
        let mut raw_notification: Option<(String, Vec<HashMap<String, String>>)> = None;
        for row in split_command_rows(s) {
            let cmd = match parse_command(&row) {
                Some(c) => c,
                None => continue,
            };

            if cmd.name.starts_with("notify") {
                self.cmd_track
                    .buffer_notification(&cmd.name, cmd.params.clone());
                if cmd.name == "notifyconnectioninforequest" {
                    self.answer_connection_info_request(sender);
                }
                if cmd.name == "notifyclientneededpermissions"
                    && self.status != ClientStatus::Disconnected
                {
                    if let Some(id) = cmd.params.get("permid").and_then(|v| v.parse::<u32>().ok()) {
                        match cmd
                            .params
                            .get("permvalue")
                            .and_then(|v| v.parse::<i32>().ok())
                        {
                            Some(value) => {
                                self.own_permissions.insert(id, value);
                            }
                            None => {
                                self.own_permissions.remove(&id);
                            }
                        }
                    }
                    self.dispatch_event(Event::OwnPermissionsChanged);
                }
                let was_truncated = self.directory.truncated();
                let channel_changed = server_directory::apply_channel_notification(
                    &cmd.name,
                    &cmd.params,
                    &mut self.directory,
                );
                let updated_client = server_directory::apply_client_notification(
                    &cmd.name,
                    &cmd.params,
                    &mut self.directory,
                );
                let result = handle_notification(
                    &cmd.name,
                    &cmd.params,
                    self.clid,
                    &mut self.directory,
                    &self.nickname,
                );
                match result {
                    NotificationResult::RawNotification { name, items } => {
                        raw_notification
                            .get_or_insert_with(|| (name, Vec::new()))
                            .1
                            .extend(items);
                    }
                    other => {
                        self.process_notification_result(other, sender);
                    }
                }
                let client_changed = updated_client.is_some();
                if let Some(info) = updated_client {
                    self.dispatch_event(Event::ClientUpdated(info));
                }
                if channel_changed
                    || client_changed
                    || (!was_truncated && self.directory.truncated())
                {
                    self.mark_directory_changed();
                }
                continue;
            }

            match cmd.name.as_str() {
                "clientinitiv" => {
                    crate::handshake::handle_handshake_init_iv(self, sender, &cmd.params);
                    sender.set_crypt(self.crypt.clone());
                }
                "initivexpand2" => {
                    crate::handshake::handle_handshake_expand2(self, sender, &cmd.params);
                }
                "initserver" => {
                    crate::handshake::handle_init_server(self, sender, &cmd.params);
                }
                "channellist" => {
                    if !self.directory_ready {
                        if let Some(channel) = server_directory::parse_channel(&cmd.params) {
                            self.directory.insert_channel(channel);
                        }
                    }
                    self.cmd_track.buffer(cmd.params.clone());
                }
                "channelclientlist" => {
                    let was_truncated = self.directory.truncated();
                    if let Some(client) = server_directory::parse_client(&cmd.params) {
                        let id = client.id;
                        let mut changed = false;
                        // A row is a snapshot from before it was sent: live
                        // events own the state of clients they introduced or
                        // removed, and a buffered welcome patch replays over a
                        // fresh insert only.
                        if !self.directory.client_departed(id)
                            && self.directory.client(id).is_none()
                        {
                            changed = self.directory.insert_client(client);
                            if let Some(params) = self.directory.take_pending_client_updates(id) {
                                if let Some(info) = server_directory::apply_client_notification(
                                    "notifyclientupdated",
                                    &params,
                                    &mut self.directory,
                                ) {
                                    changed = true;
                                    self.dispatch_event(Event::ClientUpdated(info));
                                }
                            }
                            for (sgid, add) in self.directory.take_pending_group_ops(id) {
                                let name = if add {
                                    "notifyservergroupclientadded"
                                } else {
                                    "notifyservergroupclientdeleted"
                                };
                                let op = HashMap::from([
                                    ("clid".to_string(), id.to_string()),
                                    ("sgid".to_string(), sgid),
                                ]);
                                if let Some(info) = server_directory::apply_client_notification(
                                    name,
                                    &op,
                                    &mut self.directory,
                                ) {
                                    changed = true;
                                    self.dispatch_event(Event::ClientUpdated(info));
                                }
                            }
                        }
                        if (changed || (!was_truncated && self.directory.truncated()))
                            && self.directory_ready
                        {
                            self.mark_directory_changed();
                        }
                    }
                    self.cmd_track.buffer(cmd.params.clone());
                }
                "channellistfinished" => {
                    self.directory.finish_welcome();
                    self.directory_ready = true;
                    for (_, waker) in self.directory_wakers.drain(..) {
                        let _ = waker.send(());
                    }
                    self.mark_directory_changed();
                }
                "error" => {
                    self.handle_error(&cmd.params);
                }
                _ => {
                    let params = if cmd.name.contains('=') {
                        let eq_idx = cmd.name.find('=').unwrap();
                        let k = crate::command::unescape(&cmd.name[..eq_idx]);
                        let v = crate::command::unescape(&cmd.name[eq_idx + 1..]);
                        let mut p = HashMap::new();
                        p.insert(k, v);
                        p.extend(cmd.params);
                        p
                    } else {
                        cmd.params
                    };
                    self.cmd_track.buffer(params);
                }
            }
        }
        if let Some((name, items)) = raw_notification {
            self.process_notification_result(
                NotificationResult::RawNotification { name, items },
                sender,
            );
        }
    }

    fn handle_error(&mut self, params: &HashMap<String, String>) {
        let (err, rc) = commands::parse_server_error(params);
        match rc {
            Some(rc) => self.cmd_track.resolve(rc, err),
            // Reply to a command sent without `return_code`: it contributed no
            // rows, so this must not drop a live command's rows.
            None => self.cmd_track.on_untracked_reply(),
        }

        let id = params.get("id").map(|s| s.as_str()).unwrap_or("0");
        if id == "3329" {
            // JS: `setImmediate(() => this.disconnect().catch(() => {}))`
            // Defer the disconnect to after process_packet returns and the
            // inner lock is released. The bg task checks this flag.
            self.pending_disconnect = true;
        }
    }

    /// Someone asked the server for this client's connection info, and it asks
    /// us for the figures: answer with what this connection has measured. A
    /// failure is only logged, as the request is not one a caller made.
    fn answer_connection_info_request(&self, sender: &PacketSender) {
        if self.status != ClientStatus::Connected {
            return;
        }
        let line = build_set_connection_info(
            &sender.traffic_snapshot(),
            sender.round_trip(),
            sender.packet_loss(),
        );
        let throttle = Arc::clone(&self.throttle);
        let final_cmd_handler = Arc::clone(&self.final_cmd_handler);
        let weak_inner = self.self_weak.clone();
        let logger = Arc::clone(&self.logger);
        tokio::spawn(async move {
            let send_result: Result<(), Error> = async {
                throttle.lock().await.wait(None).await?;
                let still_connected = weak_inner
                    .upgrade()
                    .is_some_and(|inner| inner.lock().unwrap().status == ClientStatus::Connected);
                if !still_connected {
                    return Err(Error::Teamspeak("not connected".into()));
                }
                let handler = final_cmd_handler.lock().unwrap().clone();
                handler(line).await
            }
            .await;
            if let Err(err) = send_result {
                logger.debug(
                    &format!("could not answer a connection info request: {err}"),
                    &[],
                );
            }
        });
    }

    fn process_notification_result(&mut self, result: NotificationResult, sender: &PacketSender) {
        match result {
            NotificationResult::ClientEnter { info } => {
                let is_self =
                    info.id != 0 && is_auto_nickname_match(&self.nickname, &info.nickname);
                if is_self {
                    self.clid = info.id;
                    sender.set_client_id(info.id);
                    self.cmd_track.signal_welcome_complete();
                }
                self.dispatch_event(Event::ClientEnter(info));
                self.mark_directory_changed();
            }
            NotificationResult::ClientLeave { event, is_self } => {
                self.dispatch_event(Event::ClientLeave(event.clone()));
                if is_self && (event.reason_id == 4 || event.reason_id == 5) {
                    for h in &self.event_handlers.kicked {
                        let h = Arc::clone(h);
                        let msg = event.reason_msg.clone();
                        tokio::spawn(async move {
                            h(Event::Kicked(msg));
                        });
                    }
                }
                self.mark_directory_changed();
            }
            NotificationResult::ClientMoved { event } => {
                self.dispatch_event(Event::ClientMoved(event));
                self.mark_directory_changed();
            }
            NotificationResult::TextMessage { message } => {
                self.dispatch_event(Event::TextMessage(message));
            }
            NotificationResult::Poked { event } => {
                self.dispatch_event(Event::Poked(event));
            }
            NotificationResult::StartUpload { info } => {
                self.ft_track
                    .notify(info.client_file_transfer_id, FtNotification::Upload(info));
            }
            NotificationResult::StartDownload { info } => {
                self.ft_track
                    .notify(info.client_file_transfer_id, FtNotification::Download(info));
            }
            NotificationResult::FileTransferStatus { info } => {
                self.ft_track
                    .notify(info.client_file_transfer_id, FtNotification::Status(info));
            }
            NotificationResult::PluginCommand { event } => {
                self.dispatch_event(Event::PluginCommand(event));
            }
            NotificationResult::ServerEdited { event } => {
                let updated = self.update_server_info(&event.changes, true);
                self.dispatch_event(Event::ServerEdited(event));
                if let Some(info) = updated {
                    self.dispatch_event(Event::ServerInfoUpdated(info));
                }
            }
            NotificationResult::ServerUpdated { params } => {
                self.merge_server_info(&params, false);
            }
            NotificationResult::RawNotification { name, items } => {
                self.dispatch_event(Event::RawNotification { name, items });
            }
        }
    }

    fn mark_directory_changed(&mut self) {
        if self.directory_ready {
            self.directory_publication.mark();
        }
    }

    fn flush_directory_if_due(&mut self) {
        if !self.directory_ready
            || !self
                .directory_publication
                .take_due(tokio::time::Instant::now())
        {
            return;
        }
        let snapshot = self.directory.snapshot();
        self.dispatch_event(Event::DirectorySnapshot(snapshot));
    }

    fn directory_deadline(&self) -> Option<tokio::time::Instant> {
        if self.directory_ready {
            self.directory_publication.deadline()
        } else {
            None
        }
    }

    fn ensure_lifecycle(&mut self) -> Option<&mut LifecycleDispatcher> {
        if self.lifecycle.is_none() && tokio::runtime::Handle::try_current().is_ok() {
            self.lifecycle = Some(LifecycleDispatcher::spawn());
        }
        self.lifecycle.as_mut()
    }

    fn emit_connected(&mut self) {
        let handlers = self.event_handlers.connected.clone();
        if handlers.is_empty() {
            return;
        }
        if let Some(lifecycle) = self.ensure_lifecycle() {
            lifecycle.dispatch(LifecycleJob::Connected(handlers));
        } else {
            for h in handlers {
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    h();
                }));
            }
        }
    }

    fn emit_disconnected(&mut self, err: Option<Error>) -> Option<watch::Receiver<bool>> {
        let handlers = self.event_handlers.disconnected.clone();
        if handlers.is_empty() {
            return None;
        }
        let (done_tx, done_rx) = watch::channel(false);
        if let Some(lifecycle) = self.ensure_lifecycle() {
            lifecycle.dispatch(LifecycleJob::Disconnected {
                handlers,
                err,
                done: done_tx,
            });
            self.pending_disconnect_done = Some(done_rx.clone());
            Some(done_rx)
        } else {
            for h in handlers {
                let e = err.clone();
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    h(Event::Disconnected(e));
                }));
            }
            None
        }
    }

    fn dispatch_event(&mut self, event: Event) {
        if self.defer_events {
            self.deferred_events.push(event);
            return;
        }
        let dispatched = Arc::new(Mutex::new(None::<Event>));
        let base: EventHandler = {
            let d = Arc::clone(&dispatched);
            Arc::new(move |ev: Event| {
                *d.lock().unwrap() = Some(ev);
            })
        };
        let chain = build_event_chain(&self.event_middlewares, base);
        chain(event);
        if let Some(evt) = dispatched.lock().unwrap().take() {
            self.deliver_event(evt);
        }
    }

    fn deliver_event(&mut self, evt: Event) {
        match evt {
            Event::TextMessage(msg) => {
                for h in &self.event_handlers.text_message {
                    let h = Arc::clone(h);
                    let m = msg.clone();
                    tokio::spawn(async move {
                        h(Event::TextMessage(m));
                    });
                }
            }
            Event::ClientEnter(info) => {
                for h in &self.event_handlers.client_enter {
                    let h = Arc::clone(h);
                    let i = info.clone();
                    tokio::spawn(async move {
                        h(Event::ClientEnter(i));
                    });
                }
            }
            Event::ClientUpdated(info) => {
                for handler in &mut self.event_handlers.client_updated {
                    handler.enqueue(Event::ClientUpdated(info.clone()));
                }
            }
            Event::ClientLeave(evt) => {
                for h in &self.event_handlers.client_leave {
                    let h = Arc::clone(h);
                    let e = evt.clone();
                    tokio::spawn(async move {
                        h(Event::ClientLeave(e));
                    });
                }
            }
            Event::ClientMoved(evt) => {
                for h in &self.event_handlers.client_moved {
                    let h = Arc::clone(h);
                    let e = evt.clone();
                    tokio::spawn(async move {
                        h(Event::ClientMoved(e));
                    });
                }
            }
            Event::DirectorySnapshot(snapshot) => {
                for handler in &mut self.event_handlers.directory_snapshot {
                    handler.enqueue(snapshot.clone());
                }
            }
            Event::ServerInfoUpdated(info) => {
                for handler in &mut self.event_handlers.server_info_updated {
                    handler.enqueue(Event::ServerInfoUpdated(info.clone()));
                }
            }
            Event::ServerEdited(evt) => {
                for h in &self.event_handlers.server_edited {
                    let h = Arc::clone(h);
                    let e = evt.clone();
                    tokio::spawn(async move {
                        h(Event::ServerEdited(e));
                    });
                }
            }
            Event::OwnPermissionsChanged => {
                for handler in &self.event_handlers.own_permissions_changed {
                    let handler = Arc::clone(handler);
                    tokio::spawn(async move {
                        handler(Event::OwnPermissionsChanged);
                    });
                }
            }
            Event::Poked(evt) => {
                for h in &self.event_handlers.poked {
                    let h = Arc::clone(h);
                    let e = evt.clone();
                    tokio::spawn(async move {
                        h(Event::Poked(e));
                    });
                }
            }
            Event::VoiceData(data) => {
                for handler in &mut self.event_handlers.voice_data {
                    handler.enqueue(Event::VoiceData(data.clone()));
                }
            }
            Event::Connected => {
                self.emit_connected();
            }
            Event::Disconnected(err) => {
                self.emit_disconnected(err);
            }
            Event::Kicked(msg) => {
                for h in &self.event_handlers.kicked {
                    let h = Arc::clone(h);
                    let m = msg.clone();
                    tokio::spawn(async move {
                        h(Event::Kicked(m));
                    });
                }
            }
            Event::PluginCommand(evt) => {
                for handler in &mut self.event_handlers.plugin_command {
                    handler.enqueue(Event::PluginCommand(evt.clone()));
                }
            }
            Event::RawNotification { name, items } => {
                let event = Event::RawNotification { name, items };
                for handler in &mut self.event_handlers.raw_notification {
                    handler.enqueue(event.clone());
                }
            }
            Event::NotificationOverflow { queue } => {
                let event = Event::NotificationOverflow { queue };
                for handler in &mut self.event_handlers.raw_notification {
                    handler.enqueue(event.clone());
                }
            }
        }
    }

    fn handle_connection_closed(&mut self, err: Option<Error>) {
        if self.status == ClientStatus::Disconnected {
            return;
        }
        self.status = ClientStatus::Disconnected;
        self.connection_epoch = self.connection_epoch.wrapping_add(1);
        self.reset_directory_availability();
        self.emit_disconnected(err);
    }

    pub(crate) fn mark_connected(&mut self) {
        self.status = ClientStatus::Connected;
        for (_, waker) in self.connected_wakers.drain(..) {
            let _ = waker.send(());
        }
        self.emit_connected();
    }

    fn reset_directory_availability(&mut self) {
        self.directory_ready = false;
        self.directory_wakers.clear();
        self.server_properties = None;
        if !self.own_permissions.is_empty() {
            self.own_permissions.clear();
            self.dispatch_event(Event::OwnPermissionsChanged);
        }
        self.directory_publication = DirectoryPublication::default();
    }

    pub(crate) fn server_info(&self) -> Option<ServerInfo> {
        self.server_properties
            .as_ref()
            .map(crate::server_info::build_server_info)
    }

    pub(crate) fn seed_server_info(&mut self, params: &HashMap<String, String>) {
        let props = crate::server_info::extract_server_properties(params, true);
        let info = crate::server_info::build_server_info(&props);
        self.server_properties = Some(props);
        self.dispatch_event(Event::ServerInfoUpdated(info));
    }

    fn update_server_info(
        &mut self,
        params: &HashMap<String, String>,
        include_counters: bool,
    ) -> Option<ServerInfo> {
        if self.server_properties.is_none() && self.status == ClientStatus::Disconnected {
            return None;
        }
        let target = self.server_properties.get_or_insert_with(HashMap::new);
        if crate::server_info::merge_server_properties(target, params, include_counters) {
            Some(crate::server_info::build_server_info(target))
        } else {
            None
        }
    }

    fn merge_server_info(&mut self, params: &HashMap<String, String>, include_counters: bool) {
        if let Some(info) = self.update_server_info(params, include_counters) {
            self.dispatch_event(Event::ServerInfoUpdated(info));
        }
    }

    /// Register a oneshot waker for `wait_connected` (`directory = false`) or
    /// `wait_directory_ready`. The id lets a cancelled wait take its entry
    /// back out; see [`WakerGuard`].
    fn register_waker(&mut self, directory: bool) -> (u64, oneshot::Receiver<()>) {
        let id = self.next_waker_id;
        self.next_waker_id += 1;
        let (tx, rx) = oneshot::channel();
        if directory {
            self.directory_wakers.push((id, tx));
        } else {
            self.connected_wakers.push((id, tx));
        }
        (id, rx)
    }
}

/// Cleans up a waiter registered by `wait_connected`/`wait_directory_ready`
/// when the wait ends without the awaited state arriving — aborted, or its
/// future dropped. Registered senders are only drained when the state arrives
/// or the connection ends, so a cancelled wait must remove its own entry or
/// the list grows by one per cancellation. After a real wakeup the id is
/// already gone from the (drained) list, making the retain a no-op.
struct WakerGuard {
    inner: Arc<Mutex<ClientInner>>,
    id: u64,
}

impl Drop for WakerGuard {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .connected_wakers
            .retain(|(waker_id, _)| *waker_id != self.id);
        inner
            .directory_wakers
            .retain(|(waker_id, _)| *waker_id != self.id);
    }
}

// ---- Client --------------------------------------------------------------------

pub struct Client {
    pub handler: Mutex<PacketHandler>,
    pub logger: Arc<dyn Logger>,
    pub nickname: String,
    resolver: Box<dyn AddrResolver>,
    throttle: Arc<tokio::sync::Mutex<CommandThrottle>>,
    final_cmd_handler: Arc<Mutex<CommandHandler>>,
    final_event_handler: Arc<Mutex<EventHandler>>,
    inner: Arc<Mutex<ClientInner>>,
    _bg_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Client {
    pub fn new(identity: Identity, addr: String, nickname: String, options: ClientOptions) -> Self {
        let client_init_options = ClientInitOptions::from(&options);
        let logger: Arc<dyn Logger> = options.logger;
        let resolver: Box<dyn AddrResolver> = options
            .resolver
            .unwrap_or_else(|| Box::new(Resolver::new()));
        let cmd_middlewares = options.command_middleware;
        let event_middlewares = options.event_middleware;

        let crypt = Crypt::new(identity.clone());
        let handler = PacketHandler::new(crypt.clone(), Arc::clone(&logger));
        let packet_sender = handler.create_packet_sender();

        let final_cmd_handler = Arc::new(Mutex::new(Self::build_cmd_handler_simple(
            &cmd_middlewares,
            &packet_sender,
        )));
        let throttle = Arc::new(tokio::sync::Mutex::new(CommandThrottle::new()));

        let (close_tx, _close_rx_dummy) = mpsc::unbounded_channel();
        let inner = Arc::new_cyclic(|weak| {
            Mutex::new(ClientInner::new(
                crypt,
                Arc::clone(&logger),
                nickname.clone(),
                identity,
                addr,
                client_init_options,
                cmd_middlewares,
                event_middlewares,
                close_tx,
                weak.clone(),
                Arc::clone(&throttle),
                Arc::clone(&final_cmd_handler),
            ))
        });

        let final_event_handler = Arc::new(Mutex::new(Self::build_event_handler_simple(
            &inner.lock().unwrap().event_middlewares,
            Arc::downgrade(&inner),
        )));

        Self {
            handler: Mutex::new(handler),
            logger,
            nickname,
            resolver,
            throttle,
            final_cmd_handler,
            final_event_handler,
            inner,
            _bg_task: Mutex::new(None),
        }
    }

    fn build_event_handler_simple(
        middlewares: &[Box<dyn EventMiddleware>],
        inner_weak: Weak<Mutex<ClientInner>>,
    ) -> EventHandler {
        let base: EventHandler = Arc::new(move |ev: Event| {
            if let Some(inner) = inner_weak.upgrade() {
                inner.lock().unwrap().deliver_event(ev);
            }
        });
        build_event_chain(middlewares, base)
    }

    fn flush_deferred_events(
        inner: &Arc<Mutex<ClientInner>>,
        final_event_handler: &Arc<Mutex<EventHandler>>,
    ) {
        loop {
            let events = {
                let mut i = inner.lock().unwrap();
                i.defer_events = false;
                if i.deferred_events.is_empty() {
                    break;
                }
                std::mem::take(&mut i.deferred_events)
            };
            let handler = final_event_handler.lock().unwrap().clone();
            for ev in events {
                handler(ev);
            }
        }
    }

    fn build_cmd_handler_simple(
        middlewares: &[Box<dyn CommandMiddleware>],
        sender: &PacketSender,
    ) -> CommandHandler {
        let sender = sender.create_command_sender();
        let base: CommandHandler = Arc::new(move |cmd: String| {
            let sender = Arc::clone(&sender);
            Box::pin(async move {
                sender(cmd.into_bytes());
                Ok(())
            }) as Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        });
        build_command_chain(middlewares, base)
    }

    // ---- Status ---------------------------------------------------------------

    pub fn status(&self) -> ClientStatus {
        self.inner.lock().unwrap().status
    }

    pub fn client_id(&self) -> i32 {
        self.inner.lock().unwrap().clid
    }

    pub fn channel_id(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner
            .directory
            .client(inner.clid)
            .map(|info| info.channel_id)
            .unwrap_or(0)
    }

    /// What this connection has carried so far, per direction and class.
    pub fn traffic_snapshot(&self) -> TrafficSnapshot {
        self.handler.lock().unwrap().traffic_snapshot()
    }

    /// The keepalive round-trip time, or `None` before the first pong.
    pub fn round_trip(&self) -> Option<RoundTrip> {
        self.handler.lock().unwrap().round_trip()
    }

    /// Estimated server-to-client packet loss fractions (`0.0..=1.0`) over the
    /// trailing 60-second window, or `None` until S2C sequence samples arrive.
    pub fn packet_loss(&self) -> Option<PacketLoss> {
        self.handler.lock().unwrap().packet_loss()
    }

    // ---- Connection -----------------------------------------------------------

    pub async fn connect(&mut self) -> Result<(), Error> {
        loop {
            let rx = self.inner.lock().unwrap().pending_disconnect_done.clone();
            let Some(mut rx) = rx else { break };
            if *rx.borrow() {
                break;
            }
            if rx.changed().await.is_err() {
                break;
            }
        }

        {
            let inner = self.inner.lock().unwrap();
            if inner.status != ClientStatus::Disconnected {
                return Err(Error::AlreadyConnected);
            }
        }

        self.reset_for_connect();

        {
            let mut inner = self.inner.lock().unwrap();
            inner.status = ClientStatus::Connecting;
        }

        let target_addr = {
            let inner = self.inner.lock().unwrap();
            resolve_addr_impl(&inner.addr, &*self.resolver).await
        };
        self.logger
            .info(&format!("connecting to server: {target_addr}"), &[]);
        {
            let handler = self.handler.lock().unwrap();
            handler.connect(&target_addr).await?;
        }

        // Spawn background packet processing task
        let inner = Arc::clone(&self.inner);
        let final_event_handler = Arc::clone(&self.final_event_handler);
        let sender = {
            let handler = self.handler.lock().unwrap();
            handler.create_packet_sender()
        };
        let handle = tokio::spawn(async move {
            let (mut packet_rx, mut close_rx) = {
                let mut i = inner.lock().unwrap();
                let (_, pr) = mpsc::unbounded_channel();
                let (_, cr) = mpsc::unbounded_channel();
                let packet_rx = std::mem::replace(&mut i.packet_rx, pr);
                let close_rx = std::mem::replace(&mut i.close_rx, cr);
                (packet_rx, close_rx)
            };

            loop {
                let directory_deadline = {
                    let mut i = inner.lock().unwrap();
                    i.defer_events = true;
                    i.flush_directory_if_due();
                    i.directory_deadline()
                };
                Self::flush_deferred_events(&inner, &final_event_handler);
                tokio::select! {
                    Some(packet) = packet_rx.recv() => {
                        let pending = {
                            let mut i = inner.lock().unwrap();
                            i.defer_events = true;
                            i.process_packet(&sender, packet);
                            if i.pending_disconnect {
                                i.pending_disconnect = false;
                                true
                            } else {
                                false
                            }
                        };
                        Self::flush_deferred_events(&inner, &final_event_handler);
                        if pending {
                            // Deferred disconnect — mirrors JS `disconnect()`:
                            // 1. Check if already disconnected → skip
                            // 2. Set status to Disconnected
                            // 3. If was Connected, send `clientdisconnect` (best-effort)
                            // 4. Close handler
                            // 5. Call disconnected handlers with None
                            let was_connected = {
                                let mut i = inner.lock().unwrap();
                                if i.status == ClientStatus::Disconnected {
                                    None
                                } else {
                                    let was_connected = i.status == ClientStatus::Connected;
                                    i.status = ClientStatus::Disconnected;
                                    i.connection_epoch = i.connection_epoch.wrapping_add(1);
                                    i.defer_events = true;
                                    i.reset_directory_availability();
                                    Some(was_connected)
                                }
                            };
                            Self::flush_deferred_events(&inner, &final_event_handler);
                            let Some(was_connected) = was_connected else {
                                break;
                            };
                            if was_connected {
                                let cmd_sender = sender.create_command_sender();
                                cmd_sender(b"clientdisconnect reasonmsg=Shutdown".to_vec());
                            }
                            sender.close();
                            inner.lock().unwrap().emit_disconnected(None);
                            break;
                        }
                    }
                    _ = wait_for_directory_deadline(directory_deadline) => {
                        {
                            let mut i = inner.lock().unwrap();
                            i.defer_events = true;
                            i.flush_directory_if_due();
                        }
                        Self::flush_deferred_events(&inner, &final_event_handler);
                    }
                    Some(err) = close_rx.recv() => {
                        {
                            let mut i = inner.lock().unwrap();
                            i.defer_events = true;
                            i.handle_connection_closed(err);
                        }
                        Self::flush_deferred_events(&inner, &final_event_handler);
                        break;
                    }
                    else => break,
                }
            }
        });
        *self._bg_task.lock().unwrap() = Some(handle);

        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), Error> {
        let was_connected = {
            let mut inner = self.inner.lock().unwrap();
            if inner.status == ClientStatus::Disconnected {
                return Ok(());
            }
            let was_connected = inner.status == ClientStatus::Connected;
            inner.status = ClientStatus::Disconnected;
            inner.connection_epoch = inner.connection_epoch.wrapping_add(1);
            inner.defer_events = true;
            inner.reset_directory_availability();
            was_connected
        };
        Self::flush_deferred_events(&self.inner, &self.final_event_handler);

        self.logger.info("disconnecting from server", &[]);

        if was_connected {
            let _ = self
                .exec_command("clientdisconnect reasonmsg=Shutdown", 1000)
                .await;
        }

        self.handler.lock().unwrap().close();
        let handler_handle = self.handler.lock().unwrap().take_handle();
        if let Some(h) = handler_handle {
            let _ = h.await;
        }

        if let Some(handle) = self._bg_task.lock().unwrap().take() {
            let _ = handle.await;
        }

        let done_rx = {
            let mut inner = self.inner.lock().unwrap();
            inner.emit_disconnected(None)
        };
        if let Some(mut rx) = done_rx {
            while !*rx.borrow() {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn wait_connected(&self, signal: Option<&AbortSignal>) -> Result<(), Error> {
        let (mut rx, _guard) = {
            let mut inner = self.inner.lock().unwrap();
            if inner.status == ClientStatus::Connected {
                return Ok(());
            }
            let (id, rx) = inner.register_waker(false);
            let guard = WakerGuard {
                inner: Arc::clone(&self.inner),
                id,
            };
            (rx, guard)
        };
        match signal {
            Some(sig) => {
                let canceled = Error::Teamspeak("connection cancelled".into());
                let aborted = Error::Teamspeak("aborted".into());
                tokio::select! {
                    r = &mut rx => r.map_err(|_| canceled)?,
                    _ = sig.wait_for_abort() => return Err(aborted),
                }
                Ok(())
            }
            None => {
                rx.await
                    .map_err(|_| Error::Teamspeak("connection cancelled".into()))?;
                Ok(())
            }
        }
    }

    /// Wait until the welcome stream has supplied the complete directory.
    pub async fn wait_directory_ready(&self, signal: Option<&AbortSignal>) -> Result<(), Error> {
        let (mut rx, _guard) = {
            let mut inner = self.inner.lock().unwrap();
            if inner.directory_ready {
                return Ok(());
            }
            if inner.status == ClientStatus::Disconnected {
                return Err(Error::Teamspeak("directory is not available".into()));
            }
            let (id, rx) = inner.register_waker(true);
            let guard = WakerGuard {
                inner: Arc::clone(&self.inner),
                id,
            };
            (rx, guard)
        };
        match signal {
            Some(sig) => {
                let canceled = Error::Teamspeak("directory wait cancelled".into());
                let aborted = Error::Teamspeak("aborted".into());
                tokio::select! {
                    r = &mut rx => r.map_err(|_| canceled)?,
                    _ = sig.wait_for_abort() => return Err(aborted),
                }
                Ok(())
            }
            None => {
                rx.await
                    .map_err(|_| Error::Teamspeak("directory wait cancelled".into()))?;
                Ok(())
            }
        }
    }

    // ---- Commands -------------------------------------------------------------

    pub async fn send_command_no_wait(&self, cmd: &str) -> Result<(), Error> {
        self.throttle.lock().await.wait(None).await?;
        let handler = self.final_cmd_handler.lock().unwrap().clone();
        handler(cmd.to_string()).await
    }

    pub async fn exec_command(&self, cmd: &str, timeout_ms: u64) -> Result<(), Error> {
        self.exec_command_with_response(cmd, timeout_ms).await?;
        Ok(())
    }

    pub async fn send_raw_command(&self, cmd: &str, params: &[(&str, &str)]) -> Result<(), Error> {
        crate::api::send_raw_command(self, cmd, params).await
    }

    pub async fn exec_command_with_response(
        &self,
        cmd: &str,
        timeout_ms: u64,
    ) -> Result<Vec<HashMap<String, String>>, Error> {
        exec_command_parts(
            &self.inner,
            &self.throttle,
            &self.final_cmd_handler,
            &*self.logger,
            cmd,
            timeout_ms,
            None,
            None,
        )
        .await
    }

    // ---- Events ---------------------------------------------------------------

    /// 服务器最近推送的本端有效权限；未报告或已撤销时为 None。
    pub fn permission_value(&self, permission_id: u32) -> Option<i32> {
        self.inner
            .lock()
            .unwrap()
            .own_permissions
            .get(&permission_id)
            .copied()
    }

    pub fn on_own_permissions_changed(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .own_permissions_changed
            .push(handler);
        self
    }

    pub fn on_text_message(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .text_message
            .push(handler);
        self
    }

    pub fn on_client_enter(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .client_enter
            .push(handler);
        self
    }

    pub fn on_client_updated(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .client_updated
            .push(SerialEventHandler::new(handler));
        self
    }

    pub fn on_client_leave(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .client_leave
            .push(handler);
        self
    }

    pub fn on_client_moved(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .client_moved
            .push(handler);
        self
    }

    /// Receive coalesced directory snapshots serially. A slow handler may miss
    /// older snapshots, but the newest pending state is retained.
    pub fn on_directory_snapshot(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .directory_snapshot
            .push(OrderedDirectoryHandler::new(handler));
        self
    }

    /// 串行订阅虚拟服务器信息快照更新（涵盖 `initserver`、`notifyserveredited`、`notifyserverupdated` 及 `serverinfo` 回写）。
    pub fn on_server_info_updated(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .server_info_updated
            .push(SerialEventHandler::new(handler));
        self
    }

    /// 订阅 `notifyserveredited` 编辑事件（含操作者信息与变更键值）。
    pub fn on_server_edited(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .server_edited
            .push(handler);
        self
    }

    pub fn on_poked(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .poked
            .push(handler);
        self
    }

    pub fn on_voice_data(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .voice_data
            .push(SerialEventHandler::new(handler));
        self
    }

    pub fn on_connected(&self, handler: Arc<dyn Fn() + Send + Sync>) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .connected
            .push(handler);
        self
    }

    pub fn on_disconnected(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .disconnected
            .push(handler);
        self
    }

    pub fn on_kicked(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .kicked
            .push(handler);
        self
    }

    pub fn on_plugin_command(&self, handler: EventHandler) -> &Self {
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .plugin_command
            .push(SerialEventHandler::with_capacity(
                handler,
                MAX_QUEUED_PLUGIN_COMMANDS,
            ));
        self
    }

    pub fn on_raw_notification(&self, handler: EventHandler) -> &Self {
        let logger = Arc::clone(&self.logger);
        self.inner
            .lock()
            .unwrap()
            .event_handlers
            .raw_notification
            .push(SerialRawNotificationHandler::new(handler, logger));
        self
    }

    pub fn directory_ready(&self) -> bool {
        self.inner.lock().unwrap().directory_ready
    }

    /// The bounded channel tree and clients after the welcome stream completed.
    /// Returns `None` while the directory is still loading or disconnected.
    pub fn directory_snapshot(&self) -> Option<DirectorySnapshot> {
        let inner = self.inner.lock().unwrap();
        inner.directory_ready.then(|| inner.directory.snapshot())
    }

    /// 读取当前本地缓存的虚拟服务器信息快照；未连接或尚未收到 `initserver` 时返回 `None`。
    pub fn server_info(&self) -> Option<ServerInfo> {
        self.inner.lock().unwrap().server_info()
    }

    pub fn use_command_middleware(&self, mw: Vec<Box<dyn CommandMiddleware>>) -> &Self {
        let sender = self.handler.lock().unwrap().create_packet_sender();
        let new_handler = {
            let mut inner = self.inner.lock().unwrap();
            inner.cmd_middlewares.extend(mw);
            Self::build_cmd_handler_simple(&inner.cmd_middlewares, &sender)
        };
        *self.final_cmd_handler.lock().unwrap() = new_handler;
        self
    }

    pub fn use_event_middleware(&self, mw: Vec<Box<dyn EventMiddleware>>) -> &Self {
        let new_handler = {
            let mut inner = self.inner.lock().unwrap();
            inner.event_middlewares.extend(mw);
            Self::build_event_handler_simple(&inner.event_middlewares, Arc::downgrade(&self.inner))
        };
        *self.final_event_handler.lock().unwrap() = new_handler;
        self
    }

    // ---- API shorthand --------------------------------------------------------

    pub fn send_voice(&self, data: Vec<u8>, codec: i32) {
        self.handler.lock().unwrap().send_voice_packet(data, codec);
    }

    /// Fetch detailed configuration information for a channel via `channelinfo`.
    pub async fn get_channel_info(&self, channel_id: u64) -> Result<ChannelInfo, Error> {
        crate::api::get_channel_info(self, channel_id).await
    }

    /// Fetch a channel's description via `channelgetdescription`.
    pub async fn get_channel_description(&self, channel_id: u64) -> Result<String, Error> {
        crate::api::get_channel_description(self, channel_id).await
    }

    /// 主动查询 `serverinfo`，同步刷新本地非计数器缓存并返回完整 [`ServerInfo`]。
    pub async fn get_server_info(&self) -> Result<ServerInfo, Error> {
        crate::api::get_server_info(self).await
    }

    /// 主动查询 `servergetvariables`，刷新本地非计数器缓存并返回合并后的 [`ServerInfo`]。
    pub async fn get_server_variables(&self) -> Result<ServerInfo, Error> {
        crate::api::get_server_variables(self).await
    }

    pub(crate) fn apply_server_info_response(&self, row: &HashMap<String, String>) {
        self.inner.lock().unwrap().merge_server_info(row, false);
    }

    pub(crate) fn apply_server_variables_response(
        &self,
        row: &HashMap<String, String>,
    ) -> ServerInfo {
        let mut inner = self.inner.lock().unwrap();
        inner.merge_server_info(row, false);
        inner
            .server_info()
            .unwrap_or_else(|| crate::server_info::build_server_info(row))
    }

    // ---- File Transfer --------------------------------------------------------

    pub async fn file_transfer_init_upload(
        &self,
        channel_id: u64,
        path: &str,
        password: &str,
        size: u64,
        overwrite: bool,
    ) -> Result<FileUploadInfo, Error> {
        let (cftid, mut ft_rx) = {
            let mut inner = self.inner.lock().unwrap();
            inner.ft_track.register()
        };
        let cmd =
            transfer::build_ft_init_upload(channel_id, path, password, size, cftid, overwrite);

        if let Err(err) = self.exec_command(&cmd, 10_000).await {
            let mut inner = self.inner.lock().unwrap();
            inner.ft_track.unregister(cftid);
            return Err(err);
        }

        let result = self
            .wait_file_transfer(cftid, &mut ft_rx, Duration::from_secs(10))
            .await?;

        match result {
            FtNotification::Upload(info) => Ok(info),
            FtNotification::Download(_) => {
                Err(Error::FileTransfer("unexpected download response".into()))
            }
            FtNotification::Status(st) => Err(Error::ServerError {
                id: st.status.to_string(),
                server_message: st.message,
                extra_message: None,
            }),
        }
    }

    pub async fn file_transfer_init_download(
        &self,
        channel_id: u64,
        path: &str,
        password: &str,
    ) -> Result<FileDownloadInfo, Error> {
        let (cftid, mut ft_rx) = {
            let mut inner = self.inner.lock().unwrap();
            inner.ft_track.register()
        };
        let cmd = transfer::build_ft_init_download(channel_id, path, password, cftid);

        if let Err(err) = self.exec_command(&cmd, 10_000).await {
            let mut inner = self.inner.lock().unwrap();
            inner.ft_track.unregister(cftid);
            return Err(err);
        }

        let result = self
            .wait_file_transfer(cftid, &mut ft_rx, Duration::from_secs(10))
            .await?;

        match result {
            FtNotification::Download(info) => Ok(info),
            FtNotification::Upload(_) => {
                Err(Error::FileTransfer("unexpected upload response".into()))
            }
            FtNotification::Status(st) => Err(Error::ServerError {
                id: st.status.to_string(),
                server_message: st.message,
                extra_message: None,
            }),
        }
    }

    async fn wait_file_transfer(
        &self,
        cftid: i32,
        receiver: &mut oneshot::Receiver<FtNotification>,
        timeout: Duration,
    ) -> Result<FtNotification, Error> {
        let result = tokio::time::timeout(timeout, receiver).await;
        self.inner.lock().unwrap().ft_track.unregister(cftid);
        result
            .map_err(|_| Error::FileTransferTimeout)?
            .map_err(|_| Error::Teamspeak("file transfer channel closed".into()))
    }

    pub async fn upload_file_data<R: tokio::io::AsyncRead + Unpin>(
        &self,
        host: &str,
        info: &FileUploadInfo,
        data: R,
    ) -> Result<(), Error> {
        transfer::upload_file_data(host, info, data).await
    }

    pub async fn download_file_data<W: tokio::io::AsyncWrite + Unpin>(
        &self,
        host: &str,
        info: &FileDownloadInfo,
        dest: W,
    ) -> Result<(), Error> {
        transfer::download_file_data(host, info, dest).await
    }

    /// 下载服务器文件区中的自定义图标，限制下载前的声明大小。
    pub async fn download_icon(
        &self,
        host: &str,
        icon_id: u32,
        max_bytes: usize,
    ) -> Result<Vec<u8>, Error> {
        crate::api::download_icon(self, host, icon_id, max_bytes).await
    }

    // ---- Internal (package-visible) -------------------------------------------

    /// Called by handshake when the server acknowledges our connection.
    pub fn mark_connected(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.mark_connected();
    }

    // ---- Private --------------------------------------------------------------

    fn reset_for_connect(&mut self) {
        self.handler.lock().unwrap().close();

        let new_crypt = {
            let inner = self.inner.lock().unwrap();
            Crypt::new(inner.identity.clone())
        };

        // Replace the PacketHandler entirely, matching JS resetForConnect
        *self.handler.lock().unwrap() =
            PacketHandler::new(new_crypt.clone(), Arc::clone(&self.logger));

        let (packet_tx, packet_rx) = mpsc::unbounded_channel();
        let (close_tx, close_rx) = mpsc::unbounded_channel();

        self.handler
            .lock()
            .unwrap()
            .set_on_packet(Box::new(move |p: Packet| {
                let _ = packet_tx.send(p);
            }));
        let close_tx_for_inner = close_tx.clone();
        self.handler
            .lock()
            .unwrap()
            .set_on_close(Box::new(move |err: Option<Error>| {
                let _ = close_tx.send(err);
            }));

        let sender = self.handler.lock().unwrap().create_packet_sender();
        {
            let mut inner = self.inner.lock().unwrap();
            inner.connection_epoch = inner.connection_epoch.wrapping_add(1);
            inner.pending_disconnect_done = None;
            inner.crypt = new_crypt;
            inner.cmd_track.reset();
            inner.ft_track.reset();
            inner.directory.clear();
            inner.reset_directory_availability();
            inner.clid = 0;
            inner.packet_rx = packet_rx;
            inner.close_rx = close_rx;
            inner.close_tx = close_tx_for_inner;
        }
        *self.final_cmd_handler.lock().unwrap() =
            Self::build_cmd_handler_simple(&self.inner.lock().unwrap().cmd_middlewares, &sender);
    }

    #[cfg(any(feature = "open-protocol", feature = "stream"))]
    pub(crate) fn weak_handle(&self) -> ClientWeak {
        ClientWeak {
            inner: Arc::downgrade(&self.inner),
            throttle: Arc::downgrade(&self.throttle),
            final_cmd_handler: Arc::downgrade(&self.final_cmd_handler),
            logger: Arc::downgrade(&self.logger),
        }
    }

    #[cfg(test)]
    pub(crate) fn process_packet_for_test(&self, sender: &PacketSender, p: Packet) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.defer_events = true;
            inner.process_packet(sender, p);
        }
        Self::flush_deferred_events(&self.inner, &self.final_event_handler);
    }

    #[cfg(all(test, feature = "open-protocol"))]
    pub(crate) fn install_open_protocol_test_transport(&self, seen_cmds: Arc<Mutex<Vec<String>>>) {
        let inner = Arc::downgrade(&self.inner);
        *self.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            seen_cmds.lock().unwrap().push(line.clone());
            let rc = line.split("return_code=").last().unwrap().parse().unwrap();
            if let Some(inner) = inner.upgrade() {
                inner.lock().unwrap().cmd_track.resolve(rc, None);
            }
            Box::pin(async { Ok(()) })
        });
    }

    #[cfg(all(test, feature = "open-protocol"))]
    pub(crate) fn inject_open_protocol_test_line(&self, clid: i32, line: &str) {
        let sender = self.handler.lock().unwrap().create_packet_sender();
        {
            let mut inner = self.inner.lock().unwrap();
            inner.clid = clid;
            inner.defer_events = true;
            inner.handle_command_str(line, &sender);
        }
        Self::flush_deferred_events(&self.inner, &self.final_event_handler);
    }

    #[cfg(all(test, feature = "stream"))]
    pub(crate) fn install_stream_test_handler<F>(&self, handler: F)
    where
        F: Fn(String) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        *self.final_cmd_handler.lock().unwrap() = Arc::new(handler);
    }

    #[cfg(all(test, feature = "stream"))]
    pub(crate) fn inject_stream_test_line(&self, clid: i32, line: &str) {
        let sender = self.handler.lock().unwrap().create_packet_sender();
        {
            let mut inner = self.inner.lock().unwrap();
            inner.clid = clid;
            inner.defer_events = true;
            inner.handle_command_str(line, &sender);
        }
        Self::flush_deferred_events(&self.inner, &self.final_event_handler);
    }

    #[cfg(all(test, feature = "stream"))]
    pub(crate) fn set_stream_test_connected(&self, clid: i32) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.clid = clid;
            inner.status = ClientStatus::Connected;
            inner.cmd_track.signal_welcome_complete();
            inner.defer_events = true;
            inner.mark_connected();
        }
        Self::flush_deferred_events(&self.inner, &self.final_event_handler);
    }

    #[cfg(all(test, feature = "stream"))]
    pub(crate) fn trigger_stream_test_disconnect(&self, err: Option<Error>) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.cmd_track.reset();
            inner.defer_events = true;
            inner.handle_connection_closed(err);
        }
        Self::flush_deferred_events(&self.inner, &self.final_event_handler);
    }

    #[cfg(all(test, feature = "stream"))]
    pub(crate) fn trigger_stream_test_overflow(&self) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.defer_events = true;
            inner.dispatch_event(Event::NotificationOverflow {
                queue: "raw_notification".to_string(),
            });
        }
        Self::flush_deferred_events(&self.inner, &self.final_event_handler);
    }

    #[cfg(all(test, feature = "stream"))]
    pub(crate) fn resolve_stream_test_rc(&self, rc: i32, err: Option<Error>) {
        self.inner.lock().unwrap().cmd_track.resolve(rc, err);
    }
}

#[cfg(any(feature = "open-protocol", feature = "stream"))]
#[derive(Clone)]
pub(crate) struct ClientWeak {
    inner: std::sync::Weak<Mutex<ClientInner>>,
    throttle: std::sync::Weak<tokio::sync::Mutex<CommandThrottle>>,
    final_cmd_handler: std::sync::Weak<Mutex<CommandHandler>>,
    logger: std::sync::Weak<dyn Logger>,
}

#[cfg(any(feature = "open-protocol", feature = "stream"))]
impl ClientWeak {
    pub(crate) fn upgrade(&self) -> Option<ClientHandle> {
        Some(ClientHandle {
            inner: self.inner.upgrade()?,
            throttle: self.throttle.upgrade()?,
            final_cmd_handler: self.final_cmd_handler.upgrade()?,
            logger: self.logger.upgrade()?,
        })
    }
}

#[cfg(any(feature = "open-protocol", feature = "stream"))]
pub(crate) struct ClientHandle {
    inner: Arc<Mutex<ClientInner>>,
    throttle: Arc<tokio::sync::Mutex<CommandThrottle>>,
    final_cmd_handler: Arc<Mutex<CommandHandler>>,
    logger: Arc<dyn Logger>,
}

#[cfg(any(feature = "open-protocol", feature = "stream"))]
impl ClientHandle {
    pub(crate) fn status(&self) -> ClientStatus {
        self.inner.lock().unwrap().status
    }

    #[cfg(feature = "open-protocol")]
    pub(crate) fn connection_epoch(&self) -> u64 {
        self.inner.lock().unwrap().connection_epoch
    }

    pub(crate) fn client_id(&self) -> i32 {
        self.inner.lock().unwrap().clid
    }

    #[cfg(feature = "open-protocol")]
    pub(crate) fn channel_id(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner
            .directory
            .client(inner.clid)
            .map(|info| info.channel_id)
            .unwrap_or(0)
    }

    pub(crate) fn directory_snapshot(&self) -> Option<DirectorySnapshot> {
        let inner = self.inner.lock().unwrap();
        inner.directory_ready.then(|| inner.directory.snapshot())
    }

    #[cfg(feature = "stream")]
    pub(crate) fn server_info(&self) -> Option<ServerInfo> {
        self.inner.lock().unwrap().server_info()
    }

    #[cfg(feature = "stream")]
    pub(crate) fn client_info(&self, clid: i32) -> Option<ClientInfo> {
        self.inner.lock().unwrap().directory.client(clid).cloned()
    }

    #[cfg(feature = "open-protocol")]
    pub(crate) async fn send_plugin_command_for_epoch(
        &self,
        name: &str,
        data: &str,
        target: PluginCommandTarget,
        target_id: Option<i32>,
        expected_epoch: u64,
    ) -> Result<(), Error> {
        let cmd = crate::api::build_plugin_command_line(name, data, target, target_id)?;
        exec_command_parts(
            &self.inner,
            &self.throttle,
            &self.final_cmd_handler,
            &*self.logger,
            &cmd,
            crate::api::CMD_TIMEOUT_MS,
            Some(expected_epoch),
            None,
        )
        .await?;
        Ok(())
    }

    #[cfg(feature = "stream")]
    pub(crate) async fn exec_command_for_stream(
        &self,
        cmd: &str,
        timeout_ms: u64,
        sent_flag: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<Vec<HashMap<String, String>>, Error> {
        exec_command_parts(
            &self.inner,
            &self.throttle,
            &self.final_cmd_handler,
            &*self.logger,
            cmd,
            timeout_ms,
            None,
            sent_flag,
        )
        .await
    }
}

async fn exec_command_parts(
    inner_arc: &Arc<Mutex<ClientInner>>,
    throttle_arc: &Arc<tokio::sync::Mutex<CommandThrottle>>,
    final_cmd_handler_arc: &Arc<Mutex<CommandHandler>>,
    logger: &dyn Logger,
    cmd: &str,
    timeout_ms: u64,
    expected_epoch: Option<u64>,
    sent_flag: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Vec<HashMap<String, String>>, Error> {
    if cmd
        .split_whitespace()
        .any(|part| part.starts_with("return_code="))
    {
        return Err(Error::Teamspeak(
            "return_code is managed by the client".into(),
        ));
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    for refused in 0..=3 {
        let mut registered = None;
        let mut sent = false;
        let attempt = async {
            let mut throttle = throttle_arc.lock().await;
            throttle.wait(None).await?;
            let (rc, rx) = {
                let mut inner = inner_arc.lock().unwrap();
                if expected_epoch.is_some_and(|exp| inner.connection_epoch != exp) {
                    return Err(Error::Teamspeak(
                        "connection epoch changed before command send".into(),
                    ));
                }
                inner.cmd_track.register_command(cmd)
            };
            registered = Some(rc);
            logger.debug(
                &format!(
                    "command {} awaiting response rc={rc}",
                    cmd.split_whitespace().next().unwrap_or("")
                ),
                &[],
            );
            let handler = final_cmd_handler_arc.lock().unwrap().clone();
            handler(commands::append_return_code(cmd, rc)).await?;
            sent = true;
            if let Some(flag) = sent_flag {
                flag.store(true, std::sync::atomic::Ordering::Release);
            }
            drop(throttle);
            rx.await
                .map_err(|_| Error::Teamspeak("command channel closed".into()))
        };
        let result = tokio::time::timeout_at(deadline, attempt).await;
        if let Some(rc) = registered {
            let mut inner = inner_arc.lock().unwrap();
            if sent {
                inner.cmd_track.unregister(rc);
            } else {
                inner.cmd_track.discard_unsent(rc);
            }
        }
        let result = result.map_err(|_| Error::CommandTimeout {
            command: cmd.split_whitespace().next().unwrap_or("").to_string(),
        })??;
        if let Some(error) = result.err {
            if let Some(delay) = error.flood_retry_delay() {
                // Match the reference client's recovery margin; only an explicit refusal is retried.
                let hold = delay.saturating_mul(2);
                logger.debug(
                    &format!("server requested command pause: {}ms", delay.as_millis()),
                    &[],
                );
                if refused < 3
                    && hold < deadline.saturating_duration_since(tokio::time::Instant::now())
                {
                    throttle_arc.lock().await.hold(hold);
                    continue;
                }
            }
            return Err(error);
        }
        return Ok(result.data);
    }
    unreachable!("bounded retries return on the final attempt")
}

// ---- Free functions -----------------------------------------------------------

async fn resolve_addr_impl(addr: &str, resolver: &dyn AddrResolver) -> String {
    let addr_with_port = if addr.contains(':') {
        addr.to_string()
    } else {
        format!("{}:9987", addr)
    };
    match resolver.resolve(addr, None).await {
        Ok(addrs) => addrs
            .first()
            .map(|a| a.addr.clone())
            .unwrap_or(addr_with_port),
        Err(_) => addr_with_port,
    }
}

async fn wait_for_directory_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    fn snapshot(channel_id: u64) -> DirectorySnapshot {
        DirectorySnapshot {
            channels: vec![ChannelInfo {
                id: channel_id,
                parent_id: 0,
                order: 0,
                name: channel_id.to_string(),
                description: String::new(),
                password_protected: None,
                max_clients: None,
                max_clients_limited: None,
                codec: None,
                icon_id: None,
            }],
            clients: Vec::new(),
            truncated: false,
        }
    }

    #[tokio::test]
    async fn timed_out_transfer_wait_unregisters_its_sender() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        );
        let (cftid, mut receiver) = client.inner.lock().unwrap().ft_track.register();
        assert!(matches!(
            client
                .wait_file_transfer(cftid, &mut receiver, Duration::ZERO)
                .await,
            Err(Error::FileTransferTimeout)
        ));
        assert_eq!(
            receiver.try_recv().unwrap_err(),
            oneshot::error::TryRecvError::Closed
        );

        let (next_id, mut next_receiver) = client.inner.lock().unwrap().ft_track.register();
        client.inner.lock().unwrap().ft_track.notify(
            cftid,
            FtNotification::Status(FileTransferStatusInfo {
                client_file_transfer_id: cftid,
                status: 1,
                message: String::new(),
            }),
        );
        assert_eq!(
            next_receiver.try_recv().unwrap_err(),
            oneshot::error::TryRecvError::Empty
        );
        client.inner.lock().unwrap().ft_track.notify(
            next_id,
            FtNotification::Status(FileTransferStatusInfo {
                client_file_transfer_id: next_id,
                status: 2,
                message: String::new(),
            }),
        );
        let notification = client
            .wait_file_transfer(next_id, &mut next_receiver, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(matches!(notification, FtNotification::Status(info) if info.status == 2));
    }

    #[test]
    fn directory_updates_are_coalesced_to_the_publish_interval() {
        let start = tokio::time::Instant::now();
        let mut publication = DirectoryPublication::default();
        publication.mark();
        assert!(publication.deadline().is_some());
        assert!(publication.take_due(start));

        publication.mark();
        assert!(!publication.take_due(start + Duration::from_millis(50)));
        assert!(publication.take_due(start + DIRECTORY_PUBLISH_INTERVAL));
    }

    #[test]
    fn late_client_rows_and_updates_mark_the_live_directory() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        );
        let sender = client.handler.lock().unwrap().create_packet_sender();
        let mut inner = client.inner.lock().unwrap();
        inner.directory_ready = true;

        inner.handle_command_str(
            "channelclientlist clid=7 cid=42 client_nickname=Alice \
             client_unique_identifier=uid client_type=0",
            &sender,
        );
        assert_eq!(inner.directory.client(7).expect("client").nickname, "Alice");
        assert!(inner.directory_publication.dirty);

        inner.directory_publication.dirty = false;
        inner.handle_command_str("notifyclientupdated clid=7 client_nickname=Alicia", &sender);
        assert_eq!(
            inner.directory.client(7).expect("client").nickname,
            "Alicia"
        );
        assert!(inner.directory_publication.dirty);

        assert!(inner.directory.insert_channel(ChannelInfo {
            id: 7,
            parent_id: 0,
            order: 0,
            name: "Live".to_string(),
            description: String::new(),
            password_protected: None,
            max_clients: None,
            max_clients_limited: None,
            codec: None,
            icon_id: None,
        }));
        inner.directory_publication.dirty = false;
        inner.handle_command_str(
            "channellist cid=7 pid=0 channel_order=0 channel_name=Stale",
            &sender,
        );
        assert_eq!(inner.directory.snapshot().channels[0].name, "Live");
        assert!(!inner.directory_publication.dirty);
    }

    #[tokio::test]
    async fn directory_snapshot_waits_for_the_welcome_to_finish() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        client.inner.lock().unwrap().status = ClientStatus::Connecting;

        assert!(!client.directory_ready());
        assert!(client.directory_snapshot().is_none());

        let waiter_client = Arc::clone(&client);
        let waiter = tokio::spawn(async move { waiter_client.wait_directory_ready(None).await });
        tokio::task::yield_now().await;

        client
            .inner
            .lock()
            .unwrap()
            .handle_command_str("channellistfinished", &sender);
        waiter.await.expect("waiter task").expect("directory ready");

        assert!(client.directory_ready());
        assert!(client.directory_snapshot().is_some());
    }

    #[tokio::test]
    async fn cancelled_waits_take_their_wakers_back() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        client.inner.lock().unwrap().status = ClientStatus::Connecting;
        let directory_wakers = || client.inner.lock().unwrap().directory_wakers.len();

        let signal = AbortSignal::new();
        let mut fut = Box::pin(client.wait_directory_ready(Some(&signal)));
        poll_once(fut.as_mut()).await;
        assert_eq!(directory_wakers(), 1);
        signal.abort();
        fut.await.expect_err("aborted");
        assert_eq!(directory_wakers(), 0);

        let mut fut = Box::pin(client.wait_directory_ready(None));
        poll_once(fut.as_mut()).await;
        assert_eq!(directory_wakers(), 1);
        drop(fut);
        assert_eq!(directory_wakers(), 0);

        let mut fut = Box::pin(client.wait_connected(None));
        poll_once(fut.as_mut()).await;
        assert_eq!(client.inner.lock().unwrap().connected_wakers.len(), 1);
        drop(fut);
        assert_eq!(client.inner.lock().unwrap().connected_wakers.len(), 0);
    }

    /// Poll `fut` exactly once, so a wait registers its waker without being
    /// driven to completion.
    async fn poll_once<F: Future>(mut fut: Pin<&mut F>) {
        std::future::poll_fn(|cx| {
            let _ = fut.as_mut().poll(cx);
            std::task::Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn welcome_updates_replay_when_the_client_row_arrives() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        client.inner.lock().unwrap().status = ClientStatus::Connecting;

        let updated_seen = Arc::new(Mutex::new(Vec::new()));
        let (ev_tx, ev_rx) = oneshot::channel();
        let ev_tx = Arc::new(Mutex::new(Some(ev_tx)));
        let handler_seen = Arc::clone(&updated_seen);
        client.on_client_updated(Arc::new(move |event| {
            if let Event::ClientUpdated(info) = event {
                handler_seen.lock().unwrap().push(info);
            }
            if let Some(done) = ev_tx.lock().unwrap().take() {
                let _ = done.send(());
            }
        }));

        // The update names a client the welcome has not introduced yet.
        client.inner.lock().unwrap().handle_command_str(
            "notifyclientupdated clid=7 client_input_muted=1 client_away=1 client_away_message=brb",
            &sender,
        );
        assert!(client.inner.lock().unwrap().directory.client(7).is_none());

        // The row is the batch snapshot from before the update was sent.
        client.inner.lock().unwrap().handle_command_str(
            "channelclientlist clid=7 cid=3 client_nickname=Alice client_input_muted=0",
            &sender,
        );
        let stored = client
            .inner
            .lock()
            .unwrap()
            .directory
            .client(7)
            .expect("client")
            .clone();
        assert_eq!(stored.nickname, "Alice");
        assert_eq!(stored.channel_id, 3);
        assert_eq!(stored.input_muted, Some(true));
        assert_eq!(stored.away, Some(true));
        assert_eq!(stored.away_message.as_deref(), Some("brb"));

        // The replayed patch dispatches the merged state as an event.
        tokio::time::timeout(Duration::from_secs(1), ev_rx)
            .await
            .expect("replayed client updated event")
            .ok();
        assert_eq!(updated_seen.lock().unwrap().len(), 1);
        assert_eq!(updated_seen.lock().unwrap()[0], stored);
    }

    #[tokio::test]
    async fn welcome_rows_yield_to_enterview_leftview_and_decode_escapes() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        client.inner.lock().unwrap().status = ClientStatus::Connecting;
        let mut inner = client.inner.lock().unwrap();

        // An enterview introduces the client with state newer than the row
        // that follows it; the row must not replace the entry.
        inner.handle_command_str(
            "notifycliententerview clid=7 ctid=3 client_nickname=Fresh client_input_muted=1",
            &sender,
        );
        inner.handle_command_str(
            "channelclientlist clid=7 cid=9 client_nickname=Stale client_input_muted=0",
            &sender,
        );
        let stored = inner.directory.client(7).expect("client").clone();
        assert_eq!(stored.nickname, "Fresh");
        assert_eq!(stored.channel_id, 3);
        assert_eq!(stored.input_muted, Some(true));

        // A client seen leaving must not be resurrected by its welcome row.
        inner.handle_command_str("notifyclientleftview clid=8 reasonid=1", &sender);
        inner.handle_command_str(
            "channelclientlist clid=8 cid=4 client_nickname=Ghost",
            &sender,
        );
        assert!(inner.directory.client(8).is_none());

        // Wire escapes are decoded by parse_command before rows reach the
        // cache; the away message needs no second unescape.
        inner.handle_command_str(
            "channelclientlist clid=9 cid=1 client_nickname=Esc client_away_message=brb\\saway",
            &sender,
        );
        assert_eq!(
            inner
                .directory
                .client(9)
                .expect("client")
                .away_message
                .as_deref(),
            Some("brb away")
        );
    }

    #[tokio::test]
    async fn client_updates_dispatch_events() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        client.inner.lock().unwrap().status = ClientStatus::Connecting;
        client.inner.lock().unwrap().handle_command_str(
            "channelclientlist clid=7 cid=3 client_nickname=Alice",
            &sender,
        );

        let seen = Arc::new(Mutex::new(Vec::new()));
        let (done_tx, done_rx) = oneshot::channel();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));
        let handler_seen = Arc::clone(&seen);
        client.on_client_updated(Arc::new(move |event| {
            if let Event::ClientUpdated(info) = event {
                handler_seen.lock().unwrap().push(info);
            }
            if let Some(done) = done_tx.lock().unwrap().take() {
                let _ = done.send(());
            }
        }));

        client
            .inner
            .lock()
            .unwrap()
            .handle_command_str("notifyclientupdated clid=7 client_input_muted=1", &sender);
        tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("client updated event")
            .ok();
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(seen[0].id, 7);
            assert_eq!(seen[0].nickname, "Alice");
            assert_eq!(seen[0].input_muted, Some(true));
        }

        // A server group change updates the record and dispatches too.
        client
            .inner
            .lock()
            .unwrap()
            .handle_command_str("notifyservergroupclientadded clid=7 sgid=8", &sender);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while seen.lock().unwrap().len() < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "server group event missing"
            );
            tokio::task::yield_now().await;
        }
        assert_eq!(seen.lock().unwrap()[1].server_groups, vec!["8"]);

        // Updates for unknown clients dispatch nothing.
        client
            .inner
            .lock()
            .unwrap()
            .handle_command_str("notifyclientupdated clid=99 client_input_muted=1", &sender);
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn welcome_group_ops_replay_when_the_client_row_arrives() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        client.inner.lock().unwrap().status = ClientStatus::Connecting;

        // A group change names a client the welcome has not introduced yet.
        client
            .inner
            .lock()
            .unwrap()
            .handle_command_str("notifyservergroupclientadded clid=7 sgid=8", &sender);
        assert!(client.inner.lock().unwrap().directory.client(7).is_none());

        // The row arrives without the group membership.
        client.inner.lock().unwrap().handle_command_str(
            "channelclientlist clid=7 cid=3 client_nickname=Alice client_servergroups=6",
            &sender,
        );
        assert_eq!(
            client
                .inner
                .lock()
                .unwrap()
                .directory
                .client(7)
                .expect("client")
                .server_groups,
            vec!["6", "8"]
        );
    }

    #[tokio::test]
    async fn multi_row_enter_view_carries_target_channel_to_all_clients() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        client.inner.lock().unwrap().status = ClientStatus::Connected;

        client.inner.lock().unwrap().handle_command_lines(
            "notifycliententerview cfid=0 ctid=20 reasonid=2 clid=110 client_nickname=Alice|clid=111 client_nickname=Bob|clid=112 client_nickname=Carol",
            &sender,
        );

        let inner = client.inner.lock().unwrap();
        assert_eq!(inner.directory.client(110).expect("110").channel_id, 20);
        assert_eq!(inner.directory.client(111).expect("111").channel_id, 20);
        assert_eq!(inner.directory.client(112).expect("112").channel_id, 20);
    }

    fn client_info(id: i32) -> ClientInfo {
        ClientInfo {
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
            is_streaming: false,
        }
    }

    #[tokio::test]
    async fn client_updated_handlers_run_in_dispatch_order() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (done_tx, done_rx) = oneshot::channel();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));

        let handler: EventHandler = {
            let seen = Arc::clone(&seen);
            let done_tx = Arc::clone(&done_tx);
            Arc::new(move |event| {
                let Event::ClientUpdated(info) = event else {
                    return;
                };
                std::thread::sleep(Duration::from_millis(2));
                seen.lock().unwrap().push(info.id);
                if seen.lock().unwrap().len() == 3 {
                    if let Some(done) = done_tx.lock().unwrap().take() {
                        let _ = done.send(());
                    }
                }
            })
        };

        let mut serial = SerialEventHandler::new(handler);
        for id in 1..=3 {
            serial.enqueue(Event::ClientUpdated(client_info(id)));
        }

        tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("client updated handlers did not finish")
            .ok();
        assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn slow_handlers_drop_the_oldest_queued_updates() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (done_tx, done_rx) = oneshot::channel();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));

        let handler: EventHandler = {
            let seen = Arc::clone(&seen);
            let done_tx = Arc::clone(&done_tx);
            Arc::new(move |event| {
                let Event::ClientUpdated(info) = event else {
                    return;
                };
                std::thread::sleep(Duration::from_millis(1));
                seen.lock().unwrap().push(info.id);
                if seen.lock().unwrap().len() == MAX_QUEUED_CLIENT_UPDATES {
                    if let Some(done) = done_tx.lock().unwrap().take() {
                        let _ = done.send(());
                    }
                }
            })
        };

        let mut serial = SerialEventHandler::new(handler);
        for id in 0..(MAX_QUEUED_CLIENT_UPDATES as i32 + 44) {
            serial.enqueue(Event::ClientUpdated(client_info(id)));
        }

        tokio::time::timeout(Duration::from_secs(10), done_rx)
            .await
            .expect("bounded queue did not drain")
            .ok();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), MAX_QUEUED_CLIENT_UPDATES);
        assert_eq!(seen[0], 44);
        assert_eq!(*seen.last().unwrap(), MAX_QUEUED_CLIENT_UPDATES as i32 + 43);
    }

    #[tokio::test]
    async fn directory_snapshot_handlers_run_serially_with_the_newest_state() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicBool::new(false));
        let overlapped = Arc::new(AtomicBool::new(false));
        let (done_tx, done_rx) = oneshot::channel();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));

        let handler: EventHandler = {
            let seen = Arc::clone(&seen);
            let active = Arc::clone(&active);
            let overlapped = Arc::clone(&overlapped);
            let done_tx = Arc::clone(&done_tx);
            Arc::new(move |event| {
                if active.swap(true, Ordering::SeqCst) {
                    overlapped.store(true, Ordering::SeqCst);
                }
                let Event::DirectorySnapshot(snapshot) = event else {
                    return;
                };
                std::thread::sleep(Duration::from_millis(2));
                seen.lock().unwrap().push(snapshot.channels[0].id);
                active.store(false, Ordering::SeqCst);
                if seen.lock().unwrap().len() == 1 {
                    if let Some(done) = done_tx.lock().unwrap().take() {
                        let _ = done.send(());
                    }
                }
            })
        };

        let mut handler = OrderedDirectoryHandler::new(handler);
        for channel_id in 1..=3 {
            handler.enqueue(snapshot(channel_id));
        }

        tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("directory handlers did not finish")
            .expect("directory handler");
        assert!(!overlapped.load(Ordering::SeqCst));
        assert_eq!(*seen.lock().unwrap(), vec![3]);
    }

    #[tokio::test]
    async fn directory_snapshot_handler_survives_a_panic() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (done_tx, done_rx) = oneshot::channel();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));
        let handler: EventHandler = {
            let calls = Arc::clone(&calls);
            let done_tx = Arc::clone(&done_tx);
            Arc::new(move |_| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("directory handler panic");
                }
                if let Some(done) = done_tx.lock().unwrap().take() {
                    let _ = done.send(());
                }
            })
        };

        let mut handler = OrderedDirectoryHandler::new(handler);
        handler.enqueue(snapshot(1));
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        handler.enqueue(snapshot(2));

        tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("directory handler stopped after panic")
            .expect("directory handler");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn effective_permissions_update_and_revoke_without_polluting_commands() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".into(),
            "test".into(),
            ClientOptions::default(),
        );
        let sender = client.handler.lock().unwrap().create_packet_sender();
        {
            let mut inner = client.inner.lock().unwrap();
            inner.status = ClientStatus::Connected;
            inner.cmd_track.signal_welcome_complete();
            let (rc, mut result) = inner.cmd_track.register_command("servergrouplist");
            inner.handle_command_str(
                "notifyclientneededpermissions permid=42 permvalue=75",
                &sender,
            );
            inner.handle_command_str("notifyservergrouplist sgid=6 name=Admin type=1", &sender);
            inner.cmd_track.resolve(rc, None);
            let rows = result.try_recv().unwrap().data;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0]["sgid"], "6");
        }
        assert_eq!(client.permission_value(42), Some(75));
        client
            .inner
            .lock()
            .unwrap()
            .handle_command_str("notifyclientneededpermissions permid=42", &sender);
        assert_eq!(client.permission_value(42), None);
    }
    #[tokio::test]
    async fn transfer_status_preserves_the_server_error_code() {
        for status in [781, 2568] {
            let client = Client::new(
                crate::crypto::generate_identity(0),
                "127.0.0.1:9987".into(),
                "test".into(),
                ClientOptions::default(),
            );
            let inner = Arc::clone(&client.inner);
            *client.final_cmd_handler.lock().unwrap() = Arc::new(move |_| {
                let inner = Arc::clone(&inner);
                Box::pin(async move {
                    let mut inner = inner.lock().unwrap();
                    inner.cmd_track.resolve(1, None);
                    inner.ft_track.notify(
                        1,
                        FtNotification::Status(FileTransferStatusInfo {
                            message: "fixture refusal".into(),
                            status,
                            client_file_transfer_id: 1,
                        }),
                    );
                    Ok(())
                })
            });
            let error = client
                .file_transfer_init_download(1, "/fixture", "")
                .await
                .unwrap_err();
            assert!(matches!(error,Error::ServerError{id,..} if id==status.to_string()));
        }
    }

    #[tokio::test]
    async fn explicit_flood_refusal_retries_with_a_new_return_code() {
        for (id, extra, expected) in [
            ("524", Some("retry in 1ms"), 2),
            ("1024", Some("retry in 1ms"), 1),
            ("524", None, 1),
            ("524", Some("please wait 600 seconds"), 1),
        ] {
            let client = Client::new(
                crate::crypto::generate_identity(0),
                "127.0.0.1:9987".into(),
                "test".into(),
                ClientOptions::default(),
            );
            let sent = Arc::new(Mutex::new(Vec::new()));
            let recorded = Arc::clone(&sent);
            let inner = Arc::clone(&client.inner);
            *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
                let rc = line
                    .split("return_code=")
                    .last()
                    .unwrap()
                    .parse::<i32>()
                    .unwrap();
                let mut sent = recorded.lock().unwrap();
                sent.push(rc);
                let error = (sent.len() == 1).then(|| Error::ServerError {
                    id: id.into(),
                    server_message: "refused".into(),
                    extra_message: extra.map(str::to_string),
                });
                inner.lock().unwrap().cmd_track.resolve(rc, error);
                Box::pin(async { Ok(()) })
            });
            let result = client
                .exec_command("setclientchannelgroup cgid=1 cid=2 cldbid=3", 1000)
                .await;
            assert_eq!(sent.lock().unwrap().len(), expected);
            assert_eq!(result.is_ok(), expected == 2);
            if expected == 2 {
                let sent = sent.lock().unwrap();
                assert_ne!(sent[0], sent[1]);
            }
        }
    }

    #[tokio::test]
    async fn command_deadline_includes_waiting_for_the_throttle() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".into(),
            "test".into(),
            ClientOptions::default(),
        );
        client.throttle.lock().await.hold(Duration::from_secs(1));
        *client.final_cmd_handler.lock().unwrap() =
            Arc::new(|_| panic!("timed out command must not be sent"));
        assert!(matches!(
            client.exec_command("clientinfo clid=1", 10).await,
            Err(Error::CommandTimeout { .. })
        ));
    }

    #[tokio::test]
    async fn failed_send_does_not_block_the_next_notification_query() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".into(),
            "test".into(),
            ClientOptions::default(),
        );
        client
            .inner
            .lock()
            .unwrap()
            .cmd_track
            .signal_welcome_complete();
        *client.final_cmd_handler.lock().unwrap() =
            Arc::new(|_| Box::pin(async { Err(Error::Teamspeak("send failed".into())) }));
        assert!(client.exec_command("servergrouplist", 1000).await.is_err());
        *client.final_cmd_handler.lock().unwrap() = Arc::new(|_| Box::pin(std::future::pending()));
        assert!(matches!(
            client.exec_command("servergrouplist", 10).await,
            Err(Error::CommandTimeout { .. })
        ));
        let inner = Arc::clone(&client.inner);
        *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            let rc = line.split("return_code=").last().unwrap().parse().unwrap();
            let mut inner = inner.lock().unwrap();
            inner.cmd_track.buffer_notification(
                "notifyfilelist",
                [("name".into(), "fixture".into())].into_iter().collect(),
            );
            inner.cmd_track.resolve(rc, None);
            Box::pin(async { Ok(()) })
        });
        assert_eq!(
            client
                .exec_command_with_response("ftgetfilelist cid=1 path=/", 1000)
                .await
                .unwrap()[0]["name"],
            "fixture"
        );
        assert!(
            client
                .exec_command("sendtextmessage msg=hi\\sreturn_code=1", 1000)
                .await
                .is_ok()
        );
        assert!(
            client
                .exec_command("servergrouplist return_code=123", 1000)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn disconnected_permissions_stay_empty_and_clearing_notifies_subscribers() {
        let mut client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".into(),
            "test".into(),
            ClientOptions::default(),
        );
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        client.on_own_permissions_changed(Arc::new(move |_| {
            let _ = tx.send(());
        }));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        {
            let mut inner = client.inner.lock().unwrap();
            inner.status = ClientStatus::Connected;
            inner.own_permissions.insert(42, 75);
            inner.status = ClientStatus::Disconnected;
            inner.reset_directory_availability();
            inner.handle_command_str(
                "notifyclientneededpermissions permid=42 permvalue=75",
                &sender,
            );
        }
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(client.permission_value(42), None);
        client.inner.lock().unwrap().own_permissions.insert(42, 75);
        client.reset_for_connect();
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(client.permission_value(42), None);
    }

    #[tokio::test]
    async fn get_channel_info_parses_nameless_row_and_fills_channel_id() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".into(),
            "test".into(),
            ClientOptions::default(),
        );
        let sender = client.handler.lock().unwrap().create_packet_sender();
        client
            .inner
            .lock()
            .unwrap()
            .cmd_track
            .signal_welcome_complete();
        let seen_cmd = Arc::new(Mutex::new(String::new()));
        let recorded = Arc::clone(&seen_cmd);
        let inner = Arc::clone(&client.inner);
        *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            *recorded.lock().unwrap() = line.clone();
            let rc = line.split("return_code=").last().unwrap().parse().unwrap();
            let mut inner = inner.lock().unwrap();
            inner.handle_command_str(
                "channel_name=Lobby\\s1 pid=2 channel_order=5 channel_topic=Topic \
                 channel_description=Rules\\sand\\sinfo channel_flag_password=1 \
                 channel_maxclients=16 channel_flag_maxclients_unlimited=0 \
                 channel_codec=4 channel_icon_id=-1",
                &sender,
            );
            inner.cmd_track.resolve(rc, None);
            Box::pin(async { Ok(()) })
        });

        let info = client.get_channel_info(42).await.unwrap();
        assert!(
            seen_cmd
                .lock()
                .unwrap()
                .starts_with("channelinfo cid=42 return_code=")
        );
        assert_eq!(info.id, 42);
        assert_eq!(info.parent_id, 2);
        assert_eq!(info.order, 5);
        assert_eq!(info.name, "Lobby 1");
        assert_eq!(info.description, "Rules and info");
        assert_eq!(info.password_protected, Some(true));
        assert_eq!(info.max_clients, Some(16));
        assert_eq!(info.max_clients_limited, Some(true));
        assert_eq!(info.codec, Some(4));
        assert_eq!(info.icon_id, Some(u32::MAX));
    }

    #[tokio::test]
    async fn get_channel_description_matches_target_channel_and_preserves_directory() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".into(),
            "test".into(),
            ClientOptions::default(),
        );
        let sender = client.handler.lock().unwrap().create_packet_sender();
        {
            let mut inner = client.inner.lock().unwrap();
            inner.status = ClientStatus::Connected;
            inner.directory_ready = true;
            inner.cmd_track.signal_welcome_complete();
            inner.directory.insert_channel(ChannelInfo {
                id: 7,
                parent_id: 0,
                order: 0,
                name: "Lobby".into(),
                description: "Cached Topic".into(),
                password_protected: None,
                max_clients: None,
                max_clients_limited: None,
                codec: None,
                icon_id: None,
            });
            inner.directory_publication.dirty = false;
        }

        let seen_cmd = Arc::new(Mutex::new(String::new()));
        let recorded = Arc::clone(&seen_cmd);
        let inner = Arc::clone(&client.inner);
        let sender_for_cmd = client.handler.lock().unwrap().create_packet_sender();
        *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            *recorded.lock().unwrap() = line.clone();
            let rc = line.split("return_code=").last().unwrap().parse().unwrap();
            let mut inner = inner.lock().unwrap();
            inner.handle_command_str(
                "notifychanneledited cid=7 reasonid=10 invokerid=2 channel_topic=Updated\\sTopic",
                &sender_for_cmd,
            );
            inner.handle_command_str(
                "notifychanneledited cid=8 reasonid=9 channel_description=Other\\schannel",
                &sender_for_cmd,
            );
            inner.handle_command_str(
                "notifychanneledited cid=7 reasonid=9 channel_description=Channel\\srules\\nhere",
                &sender_for_cmd,
            );
            inner.cmd_track.resolve(rc, None);
            Box::pin(async { Ok(()) })
        });

        let desc = client.get_channel_description(7).await.unwrap();
        assert!(
            seen_cmd
                .lock()
                .unwrap()
                .starts_with("channelgetdescription cid=7 return_code=")
        );
        assert_eq!(desc, "Channel rules\nhere");
        let snapshot = client.directory_snapshot().unwrap();
        assert_eq!(snapshot.channels[0].description, "Updated Topic");

        let inner = Arc::clone(&client.inner);
        *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            let rc = line.split("return_code=").last().unwrap().parse().unwrap();
            let mut inner = inner.lock().unwrap();
            inner.handle_command_str(
                "notifychanneledited cid=7 reasonid=10 invokerid=2 channel_topic=Only\\sTopic",
                &sender,
            );
            inner.cmd_track.resolve(rc, None);
            Box::pin(async { Ok(()) })
        });
        assert!(client.get_channel_description(7).await.is_err());
    }

    #[tokio::test]
    async fn plugin_command_dispatches_event() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        client.on_plugin_command(Arc::new(move |event| {
            if let Event::PluginCommand(ref ev) = event {
                let _ = tx.send(ev.clone());
            }
        }));

        client.inner.lock().unwrap().handle_command_str(
            "notifyplugincmd name=my_plugin data=hello\\sworld invokerid=42 invokername=Alice invokeruid=abc123=",
            &sender,
        );

        let received = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout waiting for plugin command event")
            .expect("event received");

        assert_eq!(received.name, "my_plugin");
        assert_eq!(received.data, "hello world");
        assert_eq!(received.invoker_id, 42);
        assert_eq!(received.invoker_name, "Alice");
        assert_eq!(received.invoker_uid, "abc123=");
    }

    #[tokio::test]
    async fn send_plugin_command_executes_correct_command() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let seen_cmd = Arc::new(Mutex::new(String::new()));
        let recorded = Arc::clone(&seen_cmd);
        let inner = Arc::clone(&client.inner);
        *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            *recorded.lock().unwrap() = line.clone();
            let rc = line.split("return_code=").last().unwrap().parse().unwrap();
            let mut inner = inner.lock().unwrap();
            inner.cmd_track.resolve(rc, None);
            Box::pin(async { Ok(()) })
        });

        // 1. Client target with id (targetmode=2 target=15)
        crate::api::send_plugin_command(
            &client,
            "test_cmd",
            "val 1",
            crate::types::PluginCommandTarget::Client,
            Some(15),
        )
        .await
        .expect("send_plugin_command succeeded");
        let executed = seen_cmd.lock().unwrap().clone();
        assert!(
            executed.starts_with("plugincmd name=test_cmd data=val\\s1 targetmode=2 target=15")
        );

        // 2. Channel target (targetmode=0)
        crate::api::send_plugin_command(
            &client,
            "test_cmd",
            "hello",
            crate::types::PluginCommandTarget::CurrentChannel,
            None,
        )
        .await
        .expect("send_plugin_command succeeded");
        let executed = seen_cmd.lock().unwrap().clone();
        assert!(executed.starts_with("plugincmd name=test_cmd data=hello targetmode=0"));

        // 3. Server target (targetmode=1)
        crate::api::send_plugin_command(
            &client,
            "test_cmd",
            "hello",
            crate::types::PluginCommandTarget::Server,
            None,
        )
        .await
        .expect("send_plugin_command succeeded");
        let executed = seen_cmd.lock().unwrap().clone();
        assert!(executed.starts_with("plugincmd name=test_cmd data=hello targetmode=1"));

        // 4. Escaping and empty payload
        crate::api::send_plugin_command(
            &client,
            "b2 plug",
            "a b|c/d",
            crate::types::PluginCommandTarget::CurrentChannel,
            None,
        )
        .await
        .expect("send_plugin_command succeeded");
        let executed = seen_cmd.lock().unwrap().clone();
        assert!(executed.starts_with("plugincmd name=b2\\splug data=a\\sb\\pc\\/d targetmode=0"));

        crate::api::send_plugin_command(
            &client,
            "plug",
            "",
            crate::types::PluginCommandTarget::CurrentChannel,
            None,
        )
        .await
        .expect("send_plugin_command succeeded");
        let executed = seen_cmd.lock().unwrap().clone();
        assert!(executed.starts_with("plugincmd name=plug data= targetmode=0"));

        // 5. Client target missing id -> error
        let err = crate::api::send_plugin_command(
            &client,
            "test_cmd",
            "val",
            crate::types::PluginCommandTarget::Client,
            None,
        )
        .await;
        assert!(err.is_err());

        // 6. Non-Client target with unexpected target_id -> error
        let err = crate::api::send_plugin_command(
            &client,
            "test_cmd",
            "val",
            crate::types::PluginCommandTarget::CurrentChannel,
            Some(10),
        )
        .await;
        assert!(err.is_err());

        let err = crate::api::send_plugin_command(
            &client,
            "test_cmd",
            "val",
            crate::types::PluginCommandTarget::Server,
            Some(10),
        )
        .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn stream_signaling_and_multi_item_notifications_dispatch_raw_notification() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        client.on_raw_notification(Arc::new(move |event| {
            if let Event::RawNotification { name, items } = event {
                let _ = tx.send((name, items));
            }
        }));

        let sdp = "v=0\r\no=- 98765 2 IN IP4 127.0.0.1\r\ns=-\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\n";
        let json_payload = format!(
            r#"{{"cmd":"joinResponse","args":{{"decision":1,"offer":"{}"}}}}"#,
            sdp
        );
        let wire_line = crate::command::build_command_ordered(
            "notifystreamsignaling",
            &[("id", "stream-42"), ("clid", "7"), ("json", &json_payload)],
        );

        client
            .inner
            .lock()
            .unwrap()
            .handle_command_lines(&wire_line, &sender);

        let (name, items) = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout waiting for raw notification")
            .expect("raw notification received");

        assert_eq!(name, "notifystreamsignaling");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].get("id").map(String::as_str), Some("stream-42"));
        assert_eq!(items[0].get("clid").map(String::as_str), Some("7"));
        assert_eq!(
            items[0].get("json").map(String::as_str),
            Some(json_payload.as_str())
        );

        // Multi-item notification separated by `|`
        client.inner.lock().unwrap().handle_command_lines(
            "notifystreaminfo id=s1 clid=10 name=Desktop\\sOne audio=1|id=s2 clid=11 name=Desktop\\sTwo audio=0",
            &sender,
        );

        let (name, items) = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("timeout waiting for multi-item raw notification")
            .expect("multi-item raw notification received");

        assert_eq!(name, "notifystreaminfo");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].get("id").map(String::as_str), Some("s1"));
        assert_eq!(items[0].get("clid").map(String::as_str), Some("10"));
        assert_eq!(
            items[0].get("name").map(String::as_str),
            Some("Desktop One")
        );
        assert_eq!(items[0].get("audio").map(String::as_str), Some("1"));
        assert_eq!(items[1].get("id").map(String::as_str), Some("s2"));
        assert_eq!(items[1].get("clid").map(String::as_str), Some("11"));
        assert_eq!(
            items[1].get("name").map(String::as_str),
            Some("Desktop Two")
        );
        assert_eq!(items[1].get("audio").map(String::as_str), Some("0"));
    }

    #[tokio::test]
    async fn send_raw_command_escapes_parameters_and_executes() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let seen_cmd = Arc::new(Mutex::new(String::new()));
        let recorded = Arc::clone(&seen_cmd);
        let inner = Arc::clone(&client.inner);
        *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            *recorded.lock().unwrap() = line.clone();
            let rc = line.split("return_code=").last().unwrap().parse().unwrap();
            let mut inner = inner.lock().unwrap();
            inner.cmd_track.resolve(rc, None);
            Box::pin(async { Ok(()) })
        });

        let json_payload = "{\"cmd\":\"answer\",\"args\":{\"answer\":\"v=0\r\na=mid:0|1\r\n\"}}";
        client
            .send_raw_command(
                "streamsignaling",
                &[("id", "stream-1"), ("clid", "7"), ("json", json_payload)],
            )
            .await
            .expect("send_raw_command succeeded");

        let executed = seen_cmd.lock().unwrap().clone();
        assert!(executed.starts_with(
            "streamsignaling id=stream-1 clid=7 json={\"cmd\":\"answer\",\"args\":{\"answer\":\"v=0\\r\\na=mid:0\\p1\\r\\n\"}} return_code="
        ));
    }

    #[tokio::test]
    async fn raw_notifications_maintain_strict_fifo_order() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (done_tx, done_rx) = oneshot::channel();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));

        let handler: EventHandler = {
            let seen = Arc::clone(&seen);
            let done_tx = Arc::clone(&done_tx);
            Arc::new(move |event| {
                if let Event::RawNotification { items, .. } = event {
                    let seq = items[0].get("seq").unwrap().parse::<i32>().unwrap();
                    seen.lock().unwrap().push(seq);
                    if seen.lock().unwrap().len() == 20 {
                        if let Some(done) = done_tx.lock().unwrap().take() {
                            let _ = done.send(());
                        }
                    }
                }
            })
        };

        let mut serial =
            SerialRawNotificationHandler::new(handler, Arc::new(crate::types::NoopLogger));
        for i in 1..=20 {
            serial.enqueue(Event::RawNotification {
                name: "notifystreamsignaling".to_string(),
                items: vec![HashMap::from([("seq".to_string(), i.to_string())])],
            });
        }

        tokio::time::timeout(Duration::from_secs(2), done_rx)
            .await
            .expect("raw notification FIFO did not finish")
            .ok();

        let expected: Vec<i32> = (1..=20).collect();
        assert_eq!(*seen.lock().unwrap(), expected);
    }

    #[tokio::test]
    async fn raw_notification_queue_overflow_resets_and_emits_notification_overflow() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (done_tx, done_rx) = oneshot::channel();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));

        let handler: EventHandler = {
            let events = Arc::clone(&events);
            let done_tx = Arc::clone(&done_tx);
            Arc::new(move |event| {
                // Introduce slight delay to allow backlog to form
                std::thread::sleep(Duration::from_millis(1));
                let is_overflow = matches!(event, Event::NotificationOverflow { .. });
                events.lock().unwrap().push(event);
                if is_overflow {
                    if let Some(done) = done_tx.lock().unwrap().take() {
                        let _ = done.send(());
                    }
                }
            })
        };

        let mut serial =
            SerialRawNotificationHandler::new(handler, Arc::new(crate::types::NoopLogger));
        for i in 0..(MAX_QUEUED_RAW_NOTIFICATIONS + 10) {
            serial.enqueue(Event::RawNotification {
                name: "notifystream".to_string(),
                items: vec![HashMap::from([("idx".to_string(), i.to_string())])],
            });
        }

        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("overflow event received")
            .ok();

        let received = events.lock().unwrap();
        assert!(received.iter().any(
            |ev| matches!(ev, Event::NotificationOverflow { queue } if queue == "raw_notification")
        ));
    }

    #[tokio::test]
    async fn send_raw_command_validates_cmd_key_and_nul() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));

        // 1. Invalid command name
        assert!(client.send_raw_command("", &[("k", "v")]).await.is_err());
        assert!(
            client
                .send_raw_command("cmd with space", &[("k", "v")])
                .await
                .is_err()
        );
        assert!(
            client
                .send_raw_command("cmd=1", &[("k", "v")])
                .await
                .is_err()
        );
        assert!(
            client
                .send_raw_command("cmd|pipe", &[("k", "v")])
                .await
                .is_err()
        );
        assert!(
            client
                .send_raw_command("cmd\0nul", &[("k", "v")])
                .await
                .is_err()
        );

        // 2. Invalid parameter key
        assert!(
            client
                .send_raw_command("testcmd", &[("", "v")])
                .await
                .is_err()
        );
        assert!(
            client
                .send_raw_command("testcmd", &[("key with space", "v")])
                .await
                .is_err()
        );
        assert!(
            client
                .send_raw_command("testcmd", &[("key=bad", "v")])
                .await
                .is_err()
        );
        assert!(
            client
                .send_raw_command("testcmd", &[("key|bad", "v")])
                .await
                .is_err()
        );
        assert!(
            client
                .send_raw_command("testcmd", &[("key\nbad", "v")])
                .await
                .is_err()
        );
        assert!(
            client
                .send_raw_command("testcmd", &[("key\0bad", "v")])
                .await
                .is_err()
        );

        // 3. NUL in parameter value
        assert!(
            client
                .send_raw_command("testcmd", &[("key", "val\0with_nul")])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn connection_info_request_sends_setconnectioninfo_and_emits_raw_notification() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            let _ = cmd_tx.send(line);
            Box::pin(async { Ok(()) })
        });
        let (raw_tx, mut raw_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        client.on_raw_notification(Arc::new(move |event| {
            if let Event::RawNotification { name, .. } = event {
                let _ = raw_tx.send(name);
            }
        }));

        let sender = client.handler.lock().unwrap().create_packet_sender();
        client.inner.lock().unwrap().status = ClientStatus::Connected;
        client
            .inner
            .lock()
            .unwrap()
            .handle_command_str("notifyconnectioninforequest", &sender);

        let sent = tokio::time::timeout(Duration::from_secs(1), cmd_rx.recv())
            .await
            .expect("timeout waiting for setconnectioninfo")
            .expect("setconnectioninfo sent");
        let parsed = parse_command(&sent).expect("valid setconnectioninfo command");
        assert_eq!(parsed.name, "setconnectioninfo");
        assert_eq!(parsed.params["connection_packets_sent_control"], "0");
        assert_eq!(parsed.params["connection_bytes_received_speech"], "0");
        assert!(!parsed.params.contains_key("connection_ping"));
        assert!(!sent.contains("packetloss"));

        let raw_name = tokio::time::timeout(Duration::from_secs(1), raw_rx.recv())
            .await
            .expect("timeout waiting for raw notification")
            .expect("raw notification received");
        assert_eq!(raw_name, "notifyconnectioninforequest");
    }

    #[tokio::test]
    async fn server_info_tracks_initserver_edits_queries_and_disconnect() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        assert!(client.server_info().is_none());

        let (info_tx, mut info_rx) = tokio::sync::mpsc::unbounded_channel();
        let (edited_tx, mut edited_rx) = tokio::sync::mpsc::unbounded_channel();
        let (raw_tx, mut raw_rx) = tokio::sync::mpsc::unbounded_channel();
        let (connected_seen_info_tx, mut connected_seen_info_rx) =
            tokio::sync::mpsc::unbounded_channel();

        let client_for_conn = Arc::clone(&client);
        client.on_connected(Arc::new(move || {
            let _ = connected_seen_info_tx.send(client_for_conn.server_info());
        }));
        client.on_server_info_updated(Arc::new(move |event| {
            if let Event::ServerInfoUpdated(info) = event {
                let _ = info_tx.send(info);
            }
        }));
        client.on_server_edited(Arc::new(move |event| {
            if let Event::ServerEdited(evt) = event {
                let _ = edited_tx.send(evt);
            }
        }));
        client.on_raw_notification(Arc::new(move |event| {
            if let Event::RawNotification { name, .. } = event {
                let _ = raw_tx.send(name);
            }
        }));

        let sender = client.handler.lock().unwrap().create_packet_sender();

        // 1. initserver 写入初始快照（不含 unique_identifier），且在 on_connected 之前可读
        client.inner.lock().unwrap().handle_command_str(
            "initserver aclid=7 client_nickname=bot virtualserver_name=Alpha\\sServer \
             virtualserver_welcomemessage=Hello\\nWorld virtualserver_icon_id=-1528016466 \
             virtualserver_maxclients=32 virtualserver_hostbanner_gfx_url=\\shttps:\\/\\/example.com\\/b.png\\s \
             virtualserver_platform=Linux virtualserver_version=3.13.8\\s[Build:\\s1]",
            &sender,
        );

        let connected_info =
            tokio::time::timeout(Duration::from_secs(1), connected_seen_info_rx.recv())
                .await
                .expect("connected handler timeout")
                .expect("connected handler payload")
                .expect("server_info available in on_connected");
        assert_eq!(connected_info.name, "Alpha Server");
        assert_eq!(connected_info.unique_identifier, "");
        assert_eq!(connected_info.welcome_message, "Hello\nWorld");
        assert_eq!(connected_info.icon_id, Some(2766950830));
        assert_eq!(connected_info.max_clients, Some(32));
        assert_eq!(
            connected_info.hostbanner_gfx_url.as_deref(),
            Some("https://example.com/b.png")
        );
        assert_eq!(connected_info.platform, "Linux");
        assert_eq!(connected_info.version, "3.13.8 [Build: 1]");
        assert_eq!(connected_info.get_raw("aclid"), None);
        assert_eq!(connected_info.get_raw("client_nickname"), None);
        assert_eq!(
            crate::icon_file_name(connected_info.icon_id.unwrap()),
            "/icon_2766950830"
        );

        let first_evt = tokio::time::timeout(Duration::from_secs(1), info_rx.recv())
            .await
            .expect("initserver ServerInfoUpdated timeout")
            .unwrap();
        assert_eq!(first_evt, connected_info);

        // 2. notifyserveredited 局部更新 + 空变更（TS6 修改欢迎语场景）
        client.inner.lock().unwrap().handle_command_str(
            "notifyserveredited invokerid=5 invokername=Admin\\sOne invokeruid=uid5= \
             virtualserver_name=Beta\\sServer virtualserver_icon_id=18446744072424443899",
            &sender,
        );
        let edited = tokio::time::timeout(Duration::from_secs(1), edited_rx.recv())
            .await
            .expect("ServerEdited timeout")
            .unwrap();
        assert_eq!(edited.invoker_id, 5);
        assert_eq!(edited.invoker_name, "Admin One");
        assert_eq!(edited.invoker_uid, "uid5=");
        assert_eq!(
            edited.changes.get("virtualserver_name").map(String::as_str),
            Some("Beta Server")
        );

        let updated = tokio::time::timeout(Duration::from_secs(1), info_rx.recv())
            .await
            .expect("edited ServerInfoUpdated timeout")
            .unwrap();
        assert_eq!(updated.name, "Beta Server");
        assert_eq!(updated.icon_id, Some(3009859579));
        assert_eq!(updated.welcome_message, "Hello\nWorld");

        // 重复内容不会再触发 ServerInfoUpdated，空 changes 仍触发 ServerEdited
        client.inner.lock().unwrap().handle_command_str(
            "notifyserveredited invokerid=5 invokername=Admin\\sOne invokeruid=uid5=",
            &sender,
        );
        let empty_edited = tokio::time::timeout(Duration::from_secs(1), edited_rx.recv())
            .await
            .expect("empty ServerEdited timeout")
            .unwrap();
        assert!(empty_edited.changes.is_empty());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(info_rx.try_recv().is_err());

        // 3. notifyserverupdated 合并非计数器字段并过滤计数器
        client.inner.lock().unwrap().handle_command_str(
            "notifyserverupdated virtualserver_welcomemessage=New\\sWelcome \
             virtualserver_maxclients=64 virtualserver_clientsonline=9 virtualserver_uptime=420",
            &sender,
        );
        let after_vars = tokio::time::timeout(Duration::from_secs(1), info_rx.recv())
            .await
            .expect("notifyserverupdated ServerInfoUpdated timeout")
            .unwrap();
        assert_eq!(after_vars.name, "Beta Server");
        assert_eq!(after_vars.welcome_message, "New Welcome");
        assert_eq!(after_vars.max_clients, Some(64));
        assert_eq!(after_vars.get_raw("virtualserver_clientsonline"), None);
        assert_eq!(after_vars.get_raw("virtualserver_uptime"), None);

        // 仅计数器变化时不派发 ServerInfoUpdated
        client.inner.lock().unwrap().handle_command_str(
            "notifyserverupdated virtualserver_clientsonline=10 virtualserver_uptime=425",
            &sender,
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(info_rx.try_recv().is_err());

        // 4. get_server_info (serverinfo) 补齐 unique_identifier，返回值保留计数器，缓存过滤计数器
        client
            .inner
            .lock()
            .unwrap()
            .cmd_track
            .signal_welcome_complete();
        let seen_cmd = Arc::new(Mutex::new(String::new()));
        let recorded = Arc::clone(&seen_cmd);
        let inner = Arc::clone(&client.inner);
        let sender_for_cmd = client.handler.lock().unwrap().create_packet_sender();
        *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            *recorded.lock().unwrap() = line.clone();
            let rc = line.split("return_code=").last().unwrap().parse().unwrap();
            let mut inner = inner.lock().unwrap();
            if line.starts_with("serverinfo ") {
                inner.handle_command_str(
                    "virtualserver_unique_identifier=srvUID123= virtualserver_name=Beta\\sServer \
                     virtualserver_welcomemessage=New\\sWelcome virtualserver_icon_id=0 \
                     virtualserver_maxclients=64 virtualserver_clientsonline=11 \
                     virtualserver_uptime=500",
                    &sender_for_cmd,
                );
            } else if line.starts_with("servergetvariables ") {
                inner.handle_command_str(
                    "notifyserverupdated virtualserver_welcomemessage=From\\sVars \
                     virtualserver_clientsonline=12",
                    &sender_for_cmd,
                );
            }
            inner.cmd_track.resolve(rc, None);
            Box::pin(async { Ok(()) })
        });

        let queried = client.get_server_info().await.unwrap();
        assert!(
            seen_cmd
                .lock()
                .unwrap()
                .starts_with("serverinfo return_code=")
        );
        assert_eq!(queried.unique_identifier, "srvUID123=");
        assert_eq!(queried.icon_id, Some(0));
        assert_eq!(queried.get_raw("virtualserver_uptime"), Some("500"));

        let after_query = tokio::time::timeout(Duration::from_secs(1), info_rx.recv())
            .await
            .expect("serverinfo writeback ServerInfoUpdated timeout")
            .unwrap();
        assert_eq!(after_query.unique_identifier, "srvUID123=");
        assert_eq!(after_query.icon_id, Some(0));
        assert_eq!(after_query.get_raw("virtualserver_uptime"), None);

        // 5. get_server_variables (servergetvariables) 保留已缓存的 name/unique_identifier
        let vars_info = client.get_server_variables().await.unwrap();
        assert!(
            seen_cmd
                .lock()
                .unwrap()
                .starts_with("servergetvariables return_code=")
        );
        assert_eq!(vars_info.name, "Beta Server");
        assert_eq!(vars_info.unique_identifier, "srvUID123=");
        assert_eq!(vars_info.welcome_message, "From Vars");
        let after_get_vars = tokio::time::timeout(Duration::from_secs(1), info_rx.recv())
            .await
            .expect("get_server_variables ServerInfoUpdated timeout")
            .unwrap();
        assert_eq!(after_get_vars, vars_info);

        // notifyserveredited 与 notifyserverupdated 均不落入 RawNotification
        assert!(raw_rx.try_recv().is_err());

        // 6. 断开后清空缓存，且迟到的 notifyserveredited 不会复活 server_info()
        client.inner.lock().unwrap().handle_connection_closed(None);
        assert!(client.server_info().is_none());
        client
            .inner
            .lock()
            .unwrap()
            .handle_command_str("notifyserveredited virtualserver_name=Late", &sender);
        assert!(client.server_info().is_none());
    }

    #[tokio::test]
    async fn connection_info_request_is_ignored_when_not_connected() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        *client.final_cmd_handler.lock().unwrap() = Arc::new(move |line: String| {
            let _ = cmd_tx.send(line);
            Box::pin(async { Ok(()) })
        });

        let sender = client.handler.lock().unwrap().create_packet_sender();
        client
            .inner
            .lock()
            .unwrap()
            .handle_command_str("notifyconnectioninforequest", &sender);

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(cmd_rx.try_recv().is_err());
    }

    fn make_voice_packet(
        p_type: PacketType,
        seq: u16,
        sender_clid: u16,
        codec: u8,
        audio: &[u8],
        received_at: std::time::Instant,
    ) -> Packet {
        let mut data = Vec::with_capacity(5 + audio.len());
        data.extend_from_slice(&seq.to_be_bytes());
        data.extend_from_slice(&sender_clid.to_be_bytes());
        data.push(codec);
        data.extend_from_slice(audio);
        Packet {
            type_flagged: (p_type as u8) | crate::transport::PacketFlags::Unencrypted,
            id: 0,
            client_id: 0,
            generation_id: 0,
            data,
            received_at,
        }
    }

    #[tokio::test]
    async fn voice_packet_parses_seq_whisper_received_at_and_empty_tail_frame() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        client.inner.lock().unwrap().clid = 99;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        client.on_voice_data(Arc::new(move |ev| {
            if let Event::VoiceData(vd) = ev {
                let _ = tx.send(vd);
            }
        }));

        let base = std::time::Instant::now();
        let t1 = base + Duration::from_millis(10);
        let t2 = base + Duration::from_millis(20);
        let t3 = base + Duration::from_millis(30);
        {
            let mut inner = client.inner.lock().unwrap();
            // 1. Voice packet with audio payload
            inner.process_packet(
                &sender,
                make_voice_packet(PacketType::Voice, 0x1234, 7, 4, &[0xaa, 0xbb], t1),
            );
            // 2. VoiceWhisper packet
            inner.process_packet(
                &sender,
                make_voice_packet(PacketType::VoiceWhisper, 0xfffe, 8, 5, &[0xcc], t2),
            );
            // 3. 5-byte end-of-speech tail packet (0-byte audio data)
            inner.process_packet(
                &sender,
                make_voice_packet(PacketType::Voice, 0x1235, 7, 4, &[], t3),
            );
            // 4. Malformed (< 5 bytes) and self-echo (clid == 99) must be ignored
            inner.process_packet(
                &sender,
                Packet {
                    type_flagged: (PacketType::Voice as u8)
                        | crate::transport::PacketFlags::Unencrypted,
                    id: 0,
                    client_id: 0,
                    generation_id: 0,
                    data: vec![0x12, 0x36, 0x00, 0x07],
                    received_at: t3,
                },
            );
            inner.process_packet(
                &sender,
                make_voice_packet(PacketType::Voice, 0x1237, 99, 4, &[0xdd], t3),
            );
        }

        let mut got = Vec::new();
        for _ in 0..3 {
            let vd = tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("voice event timeout")
                .expect("voice channel open");
            got.push(vd);
        }
        tokio::task::yield_now().await;
        assert!(rx.try_recv().is_err());

        assert_eq!(got[0].seq, 0x1234);
        assert_eq!(got[0].client_id, 7);
        assert_eq!(got[0].codec, 4);
        assert!(!got[0].whisper);
        assert_eq!(got[0].data.as_ref(), &[0xaa, 0xbb]);
        assert_eq!(got[0].received_at, t1);

        assert_eq!(got[1].seq, 0xfffe);
        assert_eq!(got[1].client_id, 8);
        assert_eq!(got[1].codec, 5);
        assert!(got[1].whisper);
        assert_eq!(got[1].data.as_ref(), &[0xcc]);
        assert_eq!(got[1].received_at, t2);

        assert_eq!(got[2].seq, 0x1235);
        assert_eq!(got[2].client_id, 7);
        assert_eq!(got[2].codec, 4);
        assert!(!got[2].whisper);
        assert!(got[2].data.is_empty());
        assert_eq!(got[2].received_at, t3);
    }

    #[tokio::test]
    async fn voice_data_passes_through_event_middleware() {
        struct VoiceFilterMiddleware {
            seen: Arc<Mutex<Vec<u16>>>,
            client_probe: Arc<Mutex<Option<ClientStatus>>>,
            client_ref: Arc<Mutex<Option<Arc<Client>>>>,
        }
        impl EventMiddleware for VoiceFilterMiddleware {
            fn wrap(&self, next: EventHandler) -> EventHandler {
                let seen = Arc::clone(&self.seen);
                let client_probe = Arc::clone(&self.client_probe);
                let client_ref = Arc::clone(&self.client_ref);
                Arc::new(move |ev| match ev {
                    Event::VoiceData(mut vd) => {
                        if let Some(client) = client_ref.lock().unwrap().as_ref() {
                            // Synchronous client APIs lock ClientInner; this must not deadlock.
                            let _ = client.client_id();
                            let _ = client.channel_id();
                            let _ = client.directory_snapshot();
                            *client_probe.lock().unwrap() = Some(client.status());
                        }
                        seen.lock().unwrap().push(vd.seq);
                        if vd.seq == 1 {
                            return;
                        }
                        vd.codec = 99;
                        next(Event::VoiceData(vd));
                    }
                    other => next(other),
                })
            }
        }

        let mw_seen = Arc::new(Mutex::new(Vec::new()));
        let client_probe = Arc::new(Mutex::new(None));
        let client_ref = Arc::new(Mutex::new(None));
        let mut opts = ClientOptions::default();
        opts.event_middleware.push(Box::new(VoiceFilterMiddleware {
            seen: Arc::clone(&mw_seen),
            client_probe: Arc::clone(&client_probe),
            client_ref: Arc::clone(&client_ref),
        }));
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            opts,
        ));
        *client_ref.lock().unwrap() = Some(Arc::clone(&client));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        let now = std::time::Instant::now();

        // Even before any on_voice_data listener is registered, middleware observes voiceData.
        client.process_packet_for_test(
            &sender,
            make_voice_packet(PacketType::Voice, 10, 7, 4, &[1], now),
        );
        assert_eq!(*mw_seen.lock().unwrap(), vec![10]);
        assert_eq!(
            *client_probe.lock().unwrap(),
            Some(ClientStatus::Disconnected)
        );

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        client.on_voice_data(Arc::new(move |ev| {
            if let Event::VoiceData(vd) = ev {
                let _ = tx.send(vd);
            }
        }));

        client.process_packet_for_test(
            &sender,
            make_voice_packet(PacketType::Voice, 1, 7, 4, &[1], now),
        );
        client.process_packet_for_test(
            &sender,
            make_voice_packet(PacketType::Voice, 2, 7, 4, &[2], now),
        );

        let delivered = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("middleware voice event timeout")
            .expect("channel open");
        tokio::task::yield_now().await;
        assert!(rx.try_recv().is_err());
        assert_eq!(delivered.seq, 2);
        assert_eq!(delivered.codec, 99);
        assert_eq!(*mw_seen.lock().unwrap(), vec![10, 1, 2]);
        *client_ref.lock().unwrap() = None;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn voice_data_handlers_run_serially_in_arrival_order_and_drop_oldest_above_256() {
        let client = Arc::new(Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        ));
        let sender = client.handler.lock().unwrap().create_packet_sender();
        let now = std::time::Instant::now();

        let seen = Arc::new(Mutex::new(Vec::<u16>::new()));
        let active = Arc::new(AtomicBool::new(false));
        let overlapped = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = oneshot::channel();
        let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let (done_tx, done_rx) = oneshot::channel();
        let done_tx = Arc::new(Mutex::new(Some(done_tx)));

        let seen_cb = Arc::clone(&seen);
        let active_cb = Arc::clone(&active);
        let overlapped_cb = Arc::clone(&overlapped);
        let entered_tx_cb = Arc::clone(&entered_tx);
        let gate_cb = Arc::clone(&gate);
        let done_tx_cb = Arc::clone(&done_tx);
        let total = MAX_QUEUED_CLIENT_UPDATES as u16 + 44;

        client.on_voice_data(Arc::new(move |ev| {
            if active_cb.swap(true, Ordering::SeqCst) {
                overlapped_cb.store(true, Ordering::SeqCst);
            }
            if let Event::VoiceData(vd) = ev {
                if vd.seq == 0 {
                    if let Some(tx) = entered_tx_cb.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    let (lock, cvar) = &*gate_cb;
                    let mut open = lock.lock().unwrap();
                    while !*open {
                        open = cvar.wait(open).unwrap();
                    }
                }
                let mut guard = seen_cb.lock().unwrap();
                guard.push(vd.seq);
                if guard.len() == 1 + MAX_QUEUED_CLIENT_UPDATES {
                    if let Some(tx) = done_tx_cb.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                }
            }
            active_cb.store(false, Ordering::SeqCst);
        }));

        client.process_packet_for_test(
            &sender,
            make_voice_packet(PacketType::Voice, 0, 7, 4, &[1], now),
        );
        tokio::time::timeout(Duration::from_secs(1), entered_rx)
            .await
            .expect("first voice callback did not start")
            .unwrap();

        {
            let mut inner = client.inner.lock().unwrap();
            for seq in 1..=total {
                inner.process_packet(
                    &sender,
                    make_voice_packet(PacketType::Voice, seq, 7, 4, &[1], now),
                );
            }
        }
        // Give another worker thread an opportunity to run concurrently if dispatch were not serial.
        tokio::time::sleep(Duration::from_millis(10)).await;
        {
            let (lock, cvar) = &*gate;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
        }

        tokio::time::timeout(Duration::from_secs(2), done_rx)
            .await
            .expect("voice serial queue did not drain")
            .ok();

        assert!(!overlapped.load(Ordering::SeqCst));
        let got = seen.lock().unwrap();
        assert_eq!(got.len(), 1 + MAX_QUEUED_CLIENT_UPDATES);
        assert_eq!(got[0], 0);
        assert_eq!(got[1], 45);
        assert_eq!(*got.last().unwrap(), total);
        assert!(got[1..].windows(2).all(|w| w[0] + 1 == w[1]));
    }

    #[tokio::test]
    async fn serial_event_handler_drops_captured_resources_when_client_drops() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        );
        let sender = client.handler.lock().unwrap().create_packet_sender();
        let resource = Arc::new(());
        let weak_resource = Arc::downgrade(&resource);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        client.on_voice_data(Arc::new(move |ev| {
            let _keep = &resource;
            if let Event::VoiceData(vd) = ev {
                let _ = tx.send(vd.seq);
            }
        }));

        client.process_packet_for_test(
            &sender,
            make_voice_packet(PacketType::Voice, 1, 7, 4, &[1], std::time::Instant::now()),
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rx.recv())
                .await
                .unwrap(),
            Some(1)
        );
        assert!(weak_resource.upgrade().is_some());

        drop(client);
        for _ in 0..20 {
            if weak_resource.upgrade().is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(weak_resource.upgrade().is_none());
    }

    #[tokio::test]
    async fn lifecycle_handlers_persist_across_reconnects_in_causal_order() {
        let client = Client::new(
            crate::crypto::generate_identity(0),
            "127.0.0.1:9987".to_string(),
            "bot".to_string(),
            ClientOptions::default(),
        );
        let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel();

        let log_conn = Arc::clone(&log);
        let tx_conn = done_tx.clone();
        client.on_connected(Arc::new(move || {
            log_conn.lock().unwrap().push("connected");
            let _ = tx_conn.send(());
        }));
        let log_disc = Arc::clone(&log);
        client.on_disconnected(Arc::new(move |_| {
            log_disc.lock().unwrap().push("disconnected");
            let _ = done_tx.send(());
        }));

        client.mark_connected();
        client.inner.lock().unwrap().handle_connection_closed(None);
        client.mark_connected();
        client.inner.lock().unwrap().handle_connection_closed(None);

        for _ in 0..4 {
            tokio::time::timeout(Duration::from_secs(1), done_rx.recv())
                .await
                .expect("lifecycle timeout")
                .expect("channel open");
        }
        assert_eq!(
            *log.lock().unwrap(),
            vec!["connected", "disconnected", "connected", "disconnected"]
        );
    }
}
