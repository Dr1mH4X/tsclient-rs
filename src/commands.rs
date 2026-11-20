//! Command tracking & return_code — mirrors `teamspeak-js/src/commands.ts`

use std::collections::{HashMap, HashSet, VecDeque};

use tokio::sync::oneshot;

use crate::Error;

#[derive(Debug, Clone)]
pub struct CommandResult {
    pub err: Option<Error>,
    pub data: Vec<HashMap<String, String>>,
}

/// Tracks in-flight commands by return_code.
///
/// The TS3/TS5 server sends a "welcome sequence" of unsolicited data immediately
/// after the connection handshake (channellist, channelclientlist, etc.). This
/// data arrives after we may have registered our first pending RC, which would
/// contaminate our command responses.
///
/// Solution: gate all row buffering on a `welcome_complete` flag. The flag is
/// set when `notifycliententerview` for our own clid arrives — the last event
/// the TS3/TS5 server sends in its welcome sequence. Any data arriving before
/// that is silently discarded.
///
/// Row storage differs from `teamspeak-js/src/commands.ts`, which keeps one
/// shared buffer: there a late or stale `error` line — one belonging to a
/// command the caller already gave up on, or to a command sent without
/// `return_code` — resets that buffer and silently destroys the rows of whatever
/// command is still in flight. Here every return_code owns its own bucket and
/// rows go to the oldest unresolved command, because the server answers
/// commands in the order it received them.
pub struct CommandTracker {
    pending: HashMap<i32, oneshot::Sender<CommandResult>>,
    /// return_codes in send order, including commands the caller gave up on.
    order: VecDeque<i32>,
    /// Rows per return_code; only the head of `order` receives new rows.
    rows: HashMap<i32, Vec<HashMap<String, String>>>,
    /// return_codes whose caller gave up (timeout or send failure).
    abandoned: HashSet<i32>,
    notifications: HashMap<i32, &'static str>,
    /// Correlated commands matched to notifications by a named parameter (`return_code`).
    correlated_specs: HashMap<i32, (&'static str, &'static str)>,
    /// Buffered correlated notification rows keyed by `return_code`.
    correlated_rows: HashMap<i32, Vec<HashMap<String, String>>>,
    /// Correlated commands that received `error id=0` before their notification arrived.
    settled_ok: HashSet<i32>,
    next_rc: i32,
    welcome_complete: bool,
}

impl CommandTracker {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
            order: VecDeque::new(),
            rows: HashMap::new(),
            abandoned: HashSet::new(),
            notifications: HashMap::new(),
            correlated_specs: HashMap::new(),
            correlated_rows: HashMap::new(),
            settled_ok: HashSet::new(),
            next_rc: 0,
            welcome_complete: false,
        }
    }

    fn allocate_pending(&mut self) -> (i32, oneshot::Receiver<CommandResult>) {
        self.next_rc += 1;
        let rc = self.next_rc;
        let (tx, rx) = oneshot::channel();
        self.pending.insert(rc, tx);
        (rc, rx)
    }

    pub fn register(&mut self) -> (i32, oneshot::Receiver<CommandResult>) {
        let (rc, rx) = self.allocate_pending();
        self.order.push_back(rc);
        self.rows.insert(rc, Vec::new());
        (rc, rx)
    }

    pub fn register_command(&mut self, command: &str) -> (i32, oneshot::Receiver<CommandResult>) {
        let cmd_name = command.split_whitespace().next().unwrap_or("");
        if cmd_name == "setupstream" {
            let (rc, response) = self.allocate_pending();
            self.correlated_specs
                .insert(rc, ("notifystreamstarted", "return_code"));
            return (rc, response);
        }
        let (rc, response) = self.register();
        let expected = match cmd_name {
            "apikeyadd" => Some("notifyapikeyadd"),
            "apikeylist" => Some("notifyapikeylist"),
            "banfind" => Some("notifybanfind"),
            "banlist" => Some("notifybanlist"),
            "channelclientpermlist" => Some("notifychannelclientpermlist"),
            "channelgetdescription" => Some("notifychanneledited"),
            "channelgroupadd" | "channelgroupcopy" | "channelgrouplist" => {
                Some("notifychannelgrouplist")
            }
            "channelgroupclientlist" => Some("notifychannelgroupclientlist"),
            "channelgrouppermlist" => Some("notifychannelgrouppermlist"),
            "channelpermlist" => Some("notifychannelpermlist"),
            "clientdbfind" => Some("notifyclientdbfind"),
            "clientdblist" => Some("notifyclientdblist"),
            "clientgetdbidfromuid" => Some("notifyclientdbidfromuid"),
            "clientgetids" => Some("notifyclientids"),
            "clientgetnamefromdbid" => Some("notifyclientnamefromdbid"),
            "clientgetnamefromuid" => Some("notifyclientnamefromuid"),
            "clientgetuidfromclid" => Some("notifyclientuidfromclid"),
            "clientsetserverquerylogin" => Some("notifyclientserverqueryloginpassword"),
            "clientpermlist" => Some("notifyclientpermlist"),
            "complainlist" => Some("notifycomplainlist"),
            "ftgetfileinfo" => Some("notifyfileinfo"),
            "ftgetfilelist" => Some("notifyfilelist"),
            "ftlist" => Some("notifyfiletransferlist"),
            "logview" => Some("notifyserverlog"),
            "messageget" => Some("notifymessage"),
            "messagelist" => Some("notifymessagelist"),
            "permfind" => Some("notifypermfind"),
            "permget" => Some("notifypermget"),
            "permissionlist" => Some("notifypermissionlist"),
            "permoverview" => Some("notifypermoverview"),
            "privilegekeyadd" | "tokenadd" => Some("notifytokenadd"),
            "privilegekeylist" | "tokenlist" => Some("notifytokenlist"),
            "servergetvariables" => Some("notifyserverupdated"),
            "servergroupadd" | "servergroupcopy" | "servergrouplist" => {
                Some("notifyservergrouplist")
            }
            "servergroupclientlist" => Some("notifyservergroupclientlist"),
            "servergrouppermlist" => Some("notifyservergrouppermlist"),
            "servergroupsbyclientid" => Some("notifyservergroupsbyclientid"),
            "serverrequestconnectioninfo" => Some("notifyserverconnectioninfo"),
            "servertemppasswordlist" => Some("notifyservertemppasswordlist"),
            _ => None,
        };
        if let Some(expected) = expected {
            self.notifications.insert(rc, expected);
        }
        (rc, response)
    }

    pub fn buffer_notification(&mut self, name: &str, params: HashMap<String, String>) {
        if self.offer_correlated(name, &params) {
            return;
        }
        let Some(rc) = self.order.front() else {
            return;
        };
        if self.notifications.get(rc).copied() == Some(name) {
            self.buffer(params);
        }
    }

    fn offer_correlated(&mut self, name: &str, params: &HashMap<String, String>) -> bool {
        if self.correlated_specs.is_empty() {
            return false;
        }
        let matched_rc = self
            .correlated_specs
            .iter()
            .find_map(|(&rc, &(expected_name, key))| {
                if expected_name != name {
                    return None;
                }
                let row_rc = params.get(key)?.trim().parse::<i32>().ok()?;
                (row_rc == rc).then_some(rc)
            });
        let Some(rc) = matched_rc else {
            return false;
        };
        self.correlated_rows
            .entry(rc)
            .or_default()
            .push(params.clone());
        if self.settled_ok.remove(&rc) {
            self.correlated_specs.remove(&rc);
            let rows = self.correlated_rows.remove(&rc).unwrap_or_default();
            if let Some(sender) = self.pending.remove(&rc) {
                let _ = sender.send(CommandResult {
                    err: None,
                    data: rows,
                });
            }
        }
        true
    }

    /// The caller stopped waiting for `rc`. Keep the slot in `order` until the
    /// server's `error` line for it arrives (or a later command resolves past
    /// it), so in-flight rows are not misattributed to the next command.
    pub fn unregister(&mut self, rc: i32) {
        if self.correlated_specs.remove(&rc).is_some() {
            self.pending.remove(&rc);
            self.correlated_rows.remove(&rc);
            self.settled_ok.remove(&rc);
            return;
        }
        if self.pending.remove(&rc).is_some() {
            self.abandoned.insert(rc);
        }
    }

    /// Remove a request which never reached the transport; no late reply is expected.
    pub fn discard_unsent(&mut self, rc: i32) {
        self.pending.remove(&rc);
        self.order.retain(|id| *id != rc);
        self.rows.remove(&rc);
        self.notifications.remove(&rc);
        self.abandoned.remove(&rc);
        self.correlated_specs.remove(&rc);
        self.correlated_rows.remove(&rc);
        self.settled_ok.remove(&rc);
    }

    pub fn signal_welcome_complete(&mut self) {
        self.welcome_complete = true;
        for rows in self.rows.values_mut() {
            rows.clear();
        }
    }

    pub fn buffer(&mut self, params: HashMap<String, String>) {
        if !self.welcome_complete {
            return;
        }
        if self.pending.is_empty() {
            return;
        }
        if let Some(rc) = self.order.front().copied() {
            self.rows.entry(rc).or_default().push(params);
        }
    }

    fn resolve_correlated(&mut self, rc: i32, err: Option<Error>) {
        if err.is_some() {
            self.correlated_specs.remove(&rc);
            self.correlated_rows.remove(&rc);
            self.settled_ok.remove(&rc);
            if let Some(sender) = self.pending.remove(&rc) {
                let _ = sender.send(CommandResult {
                    err,
                    data: Vec::new(),
                });
            }
            return;
        }
        if let Some(rows) = self.correlated_rows.remove(&rc) {
            self.correlated_specs.remove(&rc);
            self.settled_ok.remove(&rc);
            if let Some(sender) = self.pending.remove(&rc) {
                let _ = sender.send(CommandResult {
                    err: None,
                    data: rows,
                });
            }
            return;
        }
        if self.pending.contains_key(&rc) {
            // `error id=0` arrived before the correlated notification; hold resolution until it arrives.
            self.settled_ok.insert(rc);
        } else {
            self.correlated_specs.remove(&rc);
        }
    }

    pub fn resolve(&mut self, rc: i32, err: Option<Error>) {
        if self.correlated_specs.contains_key(&rc) {
            self.resolve_correlated(rc, err);
            return;
        }
        // The server answers in order, so an abandoned command ahead of `rc`
        // that is still unresolved never received a reply: the rows buffered for
        // it actually belong to `rc` and must be carried over. If instead its
        // `error` line had arrived first it would already have been dropped, and
        // its tail rows would not reach this command.
        let mut rows: Vec<HashMap<String, String>> = Vec::new();
        while let Some(front) = self.order.front().copied() {
            if front == rc || self.pending.contains_key(&front) {
                break;
            }
            self.order.pop_front();
            self.abandoned.remove(&front);
            self.notifications.remove(&front);
            if let Some(mut carried) = self.rows.remove(&front) {
                rows.append(&mut carried);
            }
        }

        self.order.retain(|queued| *queued != rc);
        self.abandoned.remove(&rc);
        self.notifications.remove(&rc);
        match self.rows.remove(&rc) {
            Some(mut own) => rows.append(&mut own),
            None => {}
        }

        match self.pending.remove(&rc) {
            Some(sender) => {
                let _ = sender.send(CommandResult { err, data: rows });
            }
            // Stale reply (timed-out or unknown return_code): drop it, and do
            // not touch the rows of commands that are still in flight.
            None => {}
        }
    }

    /// The server answered a command that carries no `return_code` (a command
    /// sent without one). Such a command never contributed rows, so there is
    /// nothing to discard — in particular this must not clear a live command's
    /// rows.
    pub fn on_untracked_reply(&mut self) {}

    pub fn reset(&mut self) {
        self.notifications.clear();
        self.pending.clear();
        self.order.clear();
        self.rows.clear();
        self.abandoned.clear();
        self.correlated_specs.clear();
        self.correlated_rows.clear();
        self.settled_ok.clear();
        self.welcome_complete = false;
        self.next_rc = 0;
    }
}

/// Parse and handle an `error` command from the server.
/// Returns the error (or None on success) and the resolved return_code.
pub fn parse_server_error(params: &HashMap<String, String>) -> (Option<Error>, Option<i32>) {
    let id = params.get("id").map(|s| s.as_str()).unwrap_or("0");
    let msg = params.get("msg").map(|s| s.as_str()).unwrap_or("");
    let rc_str = params.get("return_code");

    let err = if id != "0" {
        Some(Error::ServerError {
            id: id.to_string(),
            server_message: msg.to_string(),
            extra_message: params.get("extra_msg").filter(|s| !s.is_empty()).cloned(),
        })
    } else {
        None
    };

    let rc = rc_str.and_then(|s| {
        if s.is_empty() {
            None
        } else {
            s.parse::<i32>().ok()
        }
    });

    (err, rc)
}

/// Append a return_code parameter to a command string if not already present.
pub fn append_return_code(cmd: &str, rc: i32) -> String {
    if cmd
        .split_whitespace()
        .any(|part| part.starts_with("return_code="))
    {
        cmd.to_string()
    } else {
        format!("{} return_code={}", cmd, rc)
    }
}

#[cfg(test)]
mod tests {
    use super::CommandTracker;
    use std::collections::HashMap;

    fn row(key: &str) -> HashMap<String, String> {
        HashMap::from([("k".to_string(), key.to_string())])
    }

    fn keys(rows: &[HashMap<String, String>]) -> Vec<String> {
        rows.iter().map(|r| r["k"].clone()).collect()
    }

    fn tracker_past_welcome() -> CommandTracker {
        let mut tracker = CommandTracker::new();
        tracker.signal_welcome_complete();
        tracker
    }

    #[test]
    fn rows_are_resolved_to_their_own_command() {
        let mut tracker = tracker_past_welcome();
        let (rc_a, mut rx_a) = tracker.register();
        let (rc_b, mut rx_b) = tracker.register();

        tracker.buffer(row("a"));
        tracker.resolve(rc_a, None);
        tracker.buffer(row("b"));
        tracker.resolve(rc_b, None);

        let first = rx_a.try_recv().expect("first command resolved");
        assert_eq!(keys(&first.data), vec!["a"]);
        let second = rx_b.try_recv().expect("second command resolved");
        assert_eq!(keys(&second.data), vec!["b"]);
    }

    #[test]
    fn stale_error_does_not_wipe_live_rows() {
        let mut tracker = tracker_past_welcome();
        let (rc, mut rx) = tracker.register();
        tracker.buffer(row("a"));

        // A late `error return_code=<never registered>` line must not touch the
        // rows of the command that is still waiting.
        tracker.resolve(99, None);

        tracker.resolve(rc, None);
        let result = rx.try_recv().expect("command resolved");
        assert_eq!(keys(&result.data), vec!["a"]);
    }

    #[test]
    fn untracked_reply_does_not_wipe_live_rows() {
        let mut tracker = tracker_past_welcome();
        let (rc, mut rx) = tracker.register();
        tracker.buffer(row("a"));

        // `clientupdate` and friends are sent without a return_code, so their
        // reply cannot be attributed to any command.
        tracker.on_untracked_reply();

        tracker.resolve(rc, None);
        let result = rx.try_recv().expect("command resolved");
        assert_eq!(keys(&result.data), vec!["a"]);
    }

    #[test]
    fn skipped_command_hands_its_rows_to_the_next_one() {
        let mut tracker = tracker_past_welcome();
        let (rc_a, _rx_a) = tracker.register();
        let (rc_b, mut rx_b) = tracker.register();

        // The server skipped `a` and answered `b`; rows arrive before the caller
        // gives up on `a`.
        tracker.buffer(row("b"));
        tracker.unregister(rc_a);
        tracker.resolve(rc_b, None);

        let result = rx_b.try_recv().expect("command resolved");
        assert_eq!(keys(&result.data), vec!["b"]);
    }

    #[test]
    fn late_reply_of_timed_out_command_is_dropped() {
        let mut tracker = tracker_past_welcome();
        let (rc_a, _rx_a) = tracker.register();
        let (rc_b, mut rx_b) = tracker.register();
        tracker.unregister(rc_a);

        // Tail of the abandoned command's reply, then its own error line.
        tracker.buffer(row("a-tail"));
        tracker.resolve(rc_a, None);

        tracker.buffer(row("b"));
        tracker.resolve(rc_b, None);

        let result = rx_b.try_recv().expect("command resolved");
        assert_eq!(keys(&result.data), vec!["b"]);
    }
    #[test]
    fn notification_answers_only_enter_the_matching_query() {
        let mut tracker = tracker_past_welcome();
        let (rc, mut response) = tracker.register_command("ftgetfilelist cid=2 path=/");
        tracker.buffer_notification("notifyclientupdated", row("unrelated"));
        tracker.buffer_notification("notifyservergrouplist", row("wrong-query"));
        tracker.buffer_notification("notifyfilelist", row("file"));
        tracker.resolve(rc, None);
        assert_eq!(keys(&response.try_recv().unwrap().data), vec!["file"]);
        for (cmd, notify) in [
            ("banlist", "notifybanlist"),
            ("channelgetdescription cid=1", "notifychanneledited"),
            ("clientdblist", "notifyclientdblist"),
            (
                "privilegekeyadd tokentype=0 tokenid1=6 tokenid2=0",
                "notifytokenadd",
            ),
        ] {
            let (rc, mut response) = tracker.register_command(cmd);
            tracker.buffer_notification(notify, row(cmd));
            tracker.resolve(rc, None);
            assert_eq!(keys(&response.try_recv().unwrap().data), vec![cmd]);
        }
        let (rc, mut response) = tracker.register_command("clientinfo clid=1");
        tracker.buffer_notification("notifyfilelist", row("unsolicited"));
        tracker.resolve(rc, None);
        assert!(response.try_recv().unwrap().data.is_empty());
        assert_eq!(
            super::append_return_code("sendtextmessage msg=a\\sreturn_code=1", 7),
            "sendtextmessage msg=a\\sreturn_code=1 return_code=7"
        );
    }

    #[test]
    fn setupstream_resolves_when_notification_arrives_before_error() {
        let mut tracker = tracker_past_welcome();
        let (rc, mut response) = tracker.register_command("setupstream name=Demo");
        let mut row = HashMap::new();
        row.insert("id".to_string(), "stream-1".to_string());
        row.insert("return_code".to_string(), rc.to_string());
        tracker.buffer_notification("notifystreamstarted", row);
        assert!(response.try_recv().is_err());

        tracker.resolve(rc, None);
        let res = response.try_recv().expect("resolved");
        assert!(res.err.is_none());
        assert_eq!(
            res.data
                .first()
                .and_then(|r| r.get("id"))
                .map(String::as_str),
            Some("stream-1")
        );
    }

    #[test]
    fn setupstream_waits_for_correlated_notification_after_ok_error() {
        let mut tracker = tracker_past_welcome();
        let (rc, mut response) = tracker.register_command("setupstream name=Late");
        tracker.resolve(rc, None);
        assert!(response.try_recv().is_err());

        let mut other = HashMap::new();
        other.insert("id".to_string(), "other-stream".to_string());
        tracker.buffer_notification("notifystreamstarted", other);
        assert!(response.try_recv().is_err());

        let mut own = HashMap::new();
        own.insert("id".to_string(), "own-stream".to_string());
        own.insert("return_code".to_string(), rc.to_string());
        tracker.buffer_notification("notifystreamstarted", own);

        let res = response.try_recv().expect("resolved after notify");
        assert!(res.err.is_none());
        assert_eq!(
            res.data
                .first()
                .and_then(|r| r.get("id"))
                .map(String::as_str),
            Some("own-stream")
        );
    }

    #[test]
    fn setupstream_matches_concurrent_calls_and_does_not_block_inline_queries() {
        let mut tracker = tracker_past_welcome();
        let (rc1, mut rx1) = tracker.register_command("setupstream name=A");
        let (rc2, mut rx2) = tracker.register_command("setupstream name=B");
        let (rc_list, mut rx_list) = tracker.register_command("servergrouplist");

        tracker.resolve(rc1, None);
        tracker.resolve(rc2, None);
        tracker.buffer_notification("notifyservergrouplist", row("group-row"));
        tracker.resolve(rc_list, None);

        assert_eq!(keys(&rx_list.try_recv().unwrap().data), vec!["group-row"]);
        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_err());

        let mut r2 = HashMap::new();
        r2.insert("id".to_string(), "s2".to_string());
        r2.insert("return_code".to_string(), rc2.to_string());
        tracker.buffer_notification("notifystreamstarted", r2);

        let mut r1 = HashMap::new();
        r1.insert("id".to_string(), "s1".to_string());
        r1.insert("return_code".to_string(), rc1.to_string());
        tracker.buffer_notification("notifystreamstarted", r1);

        assert_eq!(
            rx2.try_recv().unwrap().data[0]
                .get("id")
                .map(String::as_str),
            Some("s2")
        );
        assert_eq!(
            rx1.try_recv().unwrap().data[0]
                .get("id")
                .map(String::as_str),
            Some("s1")
        );
    }

    #[test]
    fn setupstream_fails_immediately_on_server_error() {
        let mut tracker = tracker_past_welcome();
        let (rc, mut rx) = tracker.register_command("setupstream name=Denied");
        tracker.resolve(
            rc,
            Some(crate::errors::Error::ServerError {
                id: "2568".to_string(),
                server_message: "insufficient client permissions".to_string(),
                extra_message: None,
            }),
        );
        let res = rx.try_recv().expect("error resolved immediately");
        assert!(res.err.is_some());
    }
}
