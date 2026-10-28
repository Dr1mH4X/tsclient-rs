//! Client lifecycle & connection management — mirrors `teamspeak-js/src/client.ts`

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::Error;
use crate::command::parse_command;
use crate::commands::{self, CommandTracker};
use crate::crypto::{Crypt, Identity};
use crate::directory as server_directory;
use crate::discovery::Resolver;
use crate::events::{build_command_chain, build_event_chain};
use crate::helpers::{is_auto_nickname_match, split_command_rows};
use crate::notifications::{NotificationResult, handle_notification};
use crate::throttle::CommandThrottle;
use crate::transfer::{self, FileTransferTracker, FtNotification};
use crate::transport::{Packet, PacketHandler, PacketSender, PacketType};
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

/// Delivers client updates to one handler in dispatch order: a drain task
/// pops a bounded FIFO, so an older record can never overtake a newer one
/// and a slow handler cannot accumulate unbounded work.
struct SerialEventHandler {
    handler: EventHandler,
    pending: Arc<Mutex<VecDeque<ClientInfo>>>,
    wake: Arc<Notify>,
    started: bool,
}

impl SerialEventHandler {
    fn new(handler: EventHandler) -> Self {
        Self {
            handler,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            wake: Arc::new(Notify::new()),
            started: false,
        }
    }

    fn enqueue(&mut self, info: ClientInfo) {
        if !self.started {
            self.started = true;
            self.start();
        }
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.len() >= MAX_QUEUED_CLIENT_UPDATES {
                pending.pop_front();
            }
            pending.push_back(info);
        }
        self.wake.notify_one();
    }

    fn start(&mut self) {
        let handler = Arc::clone(&self.handler);
        let pending = Arc::clone(&self.pending);
        let wake = Arc::clone(&self.wake);
        tokio::spawn(async move {
            loop {
                wake.notified().await;
                loop {
                    let next = pending.lock().unwrap().pop_front();
                    let Some(info) = next else { break };
                    let _ = catch_unwind(AssertUnwindSafe(|| {
                        handler(Event::ClientUpdated(info));
                    }));
                }
            }
        });
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
    poked: Vec<EventHandler>,
    own_permissions_changed: Vec<EventHandler>,
    voice_data: Vec<EventHandler>,
    connected: Vec<Arc<dyn Fn() + Send + Sync>>,
    disconnected: Vec<EventHandler>,
    kicked: Vec<EventHandler>,
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
            poked: Vec::new(),
            own_permissions_changed: Vec::new(),
            voice_data: Vec::new(),
            connected: Vec::new(),
            disconnected: Vec::new(),
            kicked: Vec::new(),
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
    pub(crate) cmd_track: CommandTracker,
    own_permissions: HashMap<u32, i32>,
    pub(crate) ft_track: FileTransferTracker,
    pub(crate) directory: server_directory::DirectoryCache,
    pub(crate) directory_ready: bool,
    directory_publication: DirectoryPublication,
    pub(crate) connected_wakers: Vec<(u64, oneshot::Sender<()>)>,
    directory_wakers: Vec<(u64, oneshot::Sender<()>)>,
    next_waker_id: u64,
    event_handlers: EventHandlers,
    pub(crate) cmd_middlewares: Vec<Box<dyn CommandMiddleware>>,
    pub(crate) event_middlewares: Vec<Box<dyn EventMiddleware>>,
    pub(crate) packet_rx: mpsc::UnboundedReceiver<Packet>,
    pub(crate) close_rx: mpsc::UnboundedReceiver<Option<Error>>,
    pub(crate) close_tx: mpsc::UnboundedSender<Option<Error>>,
    /// Set when error 3329 is received — signals the bg task to perform
    /// a deferred disconnect, matching JS `setImmediate(() => this.disconnect())`.
    pub(crate) pending_disconnect: bool,
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
            cmd_track: CommandTracker::new(),
            own_permissions: HashMap::new(),
            ft_track: FileTransferTracker::new(),
            directory: server_directory::DirectoryCache::default(),
            directory_ready: false,
            directory_publication: DirectoryPublication::default(),
            connected_wakers: Vec::new(),
            directory_wakers: Vec::new(),
            next_waker_id: 0,
            event_handlers: EventHandlers::new(),
            cmd_middlewares,
            event_middlewares,
            packet_rx,
            close_rx,
            close_tx,
            pending_disconnect: false,
        }
    }

    // ---- Packet processing -------------------------------------------------------

    fn process_packet(&mut self, sender: &PacketSender, p: Packet) {
        let p_type = p.type_flagged & 0x0f;
        if p_type == 8
        /* Init1 */
        {
            match crate::handshake::process_init1(&mut self.crypt, Some(&p.data)) {
                Ok(Some(response)) => {
                    sender.set_crypt(self.crypt.clone());
                    sender.send_packet(PacketType::Init1, response, 0);
                }
                _ => {}
            }
            return;
        }

        if (p_type == 0 /* Voice */ || p_type == 1/* VoiceWhisper */) && p.data.len() > 5 {
            self.handle_voice_packet(&p.data);
            return;
        }

        if (p_type == 2 /* Command */ || p_type == 3/* CommandLow */) && !p.data.is_empty() {
            if let Ok(s) = String::from_utf8(p.data) {
                self.handle_command_lines(&s, sender);
            }
        }
    }

    fn handle_voice_packet(&self, payload: &[u8]) {
        if self.event_handlers.voice_data.is_empty() {
            return;
        }

        if payload.len() < 5 {
            return;
        }

        let client_id = u16::from_be_bytes([payload[2], payload[3]]) as i32;
        if client_id == self.clid {
            return;
        }

        let codec = payload[4] as i32;
        let data = payload[5..].to_vec();

        let voice_data = VoiceData {
            client_id,
            codec,
            data: bytes::Bytes::from(data),
        };

        for h in &self.event_handlers.voice_data {
            let h = Arc::clone(h);
            let vd = voice_data.clone();
            tokio::spawn(async move {
                h(Event::VoiceData(vd));
            });
        }
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
            for row in split_command_rows(trimmed) {
                self.handle_command_str(&row, sender);
            }
        }
    }

    fn handle_command_str(&mut self, s: &str, sender: &PacketSender) {
        let cmd = match parse_command(s) {
            Some(c) => c,
            None => return,
        };

        if cmd.name.starts_with("notify") {
            self.cmd_track
                .buffer_notification(&cmd.name, cmd.params.clone());
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
            self.process_notification_result(result, &cmd.params, sender);
            let client_changed = updated_client.is_some();
            if let Some(info) = updated_client {
                self.dispatch_event(Event::ClientUpdated(info));
            }
            if channel_changed || client_changed || (!was_truncated && self.directory.truncated()) {
                self.mark_directory_changed();
            }
            return;
        }

        match cmd.name.as_str() {
            "clientinitiv" => {
                crate::handshake::handle_handshake_init_iv(self, sender, &cmd.params);
                sender.set_crypt(self.crypt.clone());
            }
            "initivexpand2" => {
                crate::handshake::handle_handshake_expand2(self, sender, &cmd.params);
                sender.set_crypt(self.crypt.clone());
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
                    if !self.directory.client_departed(id) && self.directory.client(id).is_none() {
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
                    let k = cmd.name[..eq_idx].to_string();
                    let v = cmd.name[eq_idx + 1..].to_string();
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

    fn process_notification_result(
        &mut self,
        result: NotificationResult,
        _params: &HashMap<String, String>,
        sender: &PacketSender,
    ) {
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
            NotificationResult::Unknown => {}
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

    fn dispatch_event(&mut self, event: Event) {
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
                        handler.enqueue(info.clone());
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
                    for h in &self.event_handlers.voice_data {
                        let h = Arc::clone(h);
                        let d = data.clone();
                        tokio::spawn(async move {
                            h(Event::VoiceData(d));
                        });
                    }
                }
                Event::Connected => {
                    for h in &self.event_handlers.connected {
                        let h = Arc::clone(h);
                        tokio::spawn(async move {
                            h();
                        });
                    }
                }
                Event::Disconnected(err) => {
                    for h in &self.event_handlers.disconnected {
                        let h = Arc::clone(h);
                        let e = err.clone();
                        tokio::spawn(async move {
                            h(Event::Disconnected(e));
                        });
                    }
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
            }
        }
    }

    fn handle_connection_closed(&mut self, err: Option<Error>) {
        if self.status == ClientStatus::Disconnected {
            return;
        }
        self.status = ClientStatus::Disconnected;
        self.reset_directory_availability();
        let handlers = std::mem::take(&mut self.event_handlers.disconnected);
        for h in handlers {
            let e = err.clone();
            tokio::spawn(async move {
                h(Event::Disconnected(e));
            });
        }
    }

    pub(crate) fn mark_connected(&mut self) {
        self.status = ClientStatus::Connected;
        for (_, waker) in self.connected_wakers.drain(..) {
            let _ = waker.send(());
        }
        let handlers = std::mem::take(&mut self.event_handlers.connected);
        for h in handlers {
            tokio::spawn(async move {
                h();
            });
        }
    }

    fn reset_directory_availability(&mut self) {
        self.directory_ready = false;
        self.directory_wakers.clear();
        if !self.own_permissions.is_empty() {
            self.own_permissions.clear();
            self.dispatch_event(Event::OwnPermissionsChanged);
        }
        self.directory_publication = DirectoryPublication::default();
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
    throttle: tokio::sync::Mutex<CommandThrottle>,
    final_cmd_handler: Mutex<CommandHandler>,
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

        let final_cmd_handler = Self::build_cmd_handler_simple(&cmd_middlewares, &packet_sender);

        let (close_tx, _close_rx_dummy) = mpsc::unbounded_channel();
        let inner = Arc::new(Mutex::new(ClientInner::new(
            crypt,
            Arc::clone(&logger),
            nickname.clone(),
            identity,
            addr,
            client_init_options,
            cmd_middlewares,
            event_middlewares,
            close_tx,
        )));

        Self {
            handler: Mutex::new(handler),
            logger,
            nickname,
            resolver,
            throttle: tokio::sync::Mutex::new(CommandThrottle::new()),
            final_cmd_handler: Mutex::new(final_cmd_handler),
            inner,
            _bg_task: Mutex::new(None),
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

    // ---- Connection -----------------------------------------------------------

    pub async fn connect(&mut self) -> Result<(), Error> {
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
                    i.flush_directory_if_due();
                    i.directory_deadline()
                };
                tokio::select! {
                    Some(packet) = packet_rx.recv() => {
                        let pending = {
                            let mut i = inner.lock().unwrap();
                            i.process_packet(&sender, packet);
                            if i.pending_disconnect {
                                i.pending_disconnect = false;
                                true
                            } else {
                                false
                            }
                        };
                        if pending {
                            // Deferred disconnect — mirrors JS `disconnect()`:
                            // 1. Check if already disconnected → skip
                            // 2. Set status to Disconnected
                            // 3. If was Connected, send `clientdisconnect` (best-effort)
                            // 4. Close handler
                            // 5. Call disconnected handlers with None
                            let (was_connected, handlers) = {
                                let mut i = inner.lock().unwrap();
                                if i.status == ClientStatus::Disconnected {
                                    break;
                                }
                                let was_connected = i.status == ClientStatus::Connected;
                                i.status = ClientStatus::Disconnected;
                                i.reset_directory_availability();
                                let handlers = std::mem::take(&mut i.event_handlers.disconnected);
                                (was_connected, handlers)
                            };
                            if was_connected {
                                let cmd_sender = sender.create_command_sender();
                                cmd_sender(b"clientdisconnect reasonmsg=Shutdown".to_vec());
                            }
                            sender.close();
                            for h in handlers {
                                tokio::spawn(async move {
                                    h(Event::Disconnected(None));
                                });
                            }
                            break;
                        }
                    }
                    _ = wait_for_directory_deadline(directory_deadline) => {
                        inner.lock().unwrap().flush_directory_if_due();
                    }
                    Some(err) = close_rx.recv() => {
                        let mut i = inner.lock().unwrap();
                        i.handle_connection_closed(err);
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
            inner.reset_directory_availability();
            was_connected
        };

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

        let handlers = {
            let mut inner = self.inner.lock().unwrap();
            std::mem::take(&mut inner.event_handlers.disconnected)
        };
        for h in handlers {
            h(Event::Disconnected(None));
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

    pub async fn exec_command_with_response(
        &self,
        cmd: &str,
        timeout_ms: u64,
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
                let mut throttle = self.throttle.lock().await;
                throttle.wait(None).await?;
                let (rc, rx) = self.inner.lock().unwrap().cmd_track.register_command(cmd);
                registered = Some(rc);
                self.logger.debug(
                    &format!(
                        "command {} awaiting response rc={rc}",
                        cmd.split_whitespace().next().unwrap_or("")
                    ),
                    &[],
                );
                let handler = self.final_cmd_handler.lock().unwrap().clone();
                handler(commands::append_return_code(cmd, rc)).await?;
                sent = true;
                drop(throttle);
                rx.await
                    .map_err(|_| Error::Teamspeak("command channel closed".into()))
            };
            let result = tokio::time::timeout_at(deadline, attempt).await;
            if let Some(rc) = registered {
                let mut inner = self.inner.lock().unwrap();
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
                    self.logger.debug(
                        &format!("server requested command pause: {}ms", delay.as_millis()),
                        &[],
                    );
                    if refused < 3
                        && hold < deadline.saturating_duration_since(tokio::time::Instant::now())
                    {
                        self.throttle.lock().await.hold(hold);
                        continue;
                    }
                }
                return Err(error);
            }
            return Ok(result.data);
        }
        unreachable!("bounded retries return on the final attempt")
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
            .push(handler);
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

    pub fn directory_ready(&self) -> bool {
        self.inner.lock().unwrap().directory_ready
    }

    /// The bounded channel tree and clients after the welcome stream completed.
    /// Returns `None` while the directory is still loading or disconnected.
    pub fn directory_snapshot(&self) -> Option<DirectorySnapshot> {
        let inner = self.inner.lock().unwrap();
        inner.directory_ready.then(|| inner.directory.snapshot())
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
        self.inner.lock().unwrap().event_middlewares.extend(mw);
        self
    }

    // ---- API shorthand --------------------------------------------------------

    pub fn send_voice(&self, data: Vec<u8>, codec: i32) {
        self.handler.lock().unwrap().send_voice_packet(data, codec);
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
        inner.status = ClientStatus::Connected;
        for (_, waker) in inner.connected_wakers.drain(..) {
            let _ = waker.send(());
        }
        let handlers = std::mem::take(&mut inner.event_handlers.connected);
        drop(inner);
        for h in handlers {
            tokio::spawn(async move {
                h();
            });
        }
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
            serial.enqueue(client_info(id));
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
            serial.enqueue(client_info(id));
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
}
