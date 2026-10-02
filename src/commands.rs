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
            next_rc: 0,
            welcome_complete: false,
        }
    }

    pub fn register(&mut self) -> (i32, oneshot::Receiver<CommandResult>) {
        self.next_rc += 1;
        let rc = self.next_rc;
        let (tx, rx) = oneshot::channel();
        self.pending.insert(rc, tx);
        self.order.push_back(rc);
        self.rows.insert(rc, Vec::new());
        (rc, rx)
    }

    /// The caller stopped waiting for `rc`. Keep the slot in `order` until the
    /// server's `error` line for it arrives (or a later command resolves past
    /// it), so in-flight rows are not misattributed to the next command.
    pub fn unregister(&mut self, rc: i32) {
        if self.pending.remove(&rc).is_some() {
            self.abandoned.insert(rc);
        }
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

    pub fn resolve(&mut self, rc: i32, err: Option<Error>) {
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
            if let Some(mut carried) = self.rows.remove(&front) {
                rows.append(&mut carried);
            }
        }

        self.order.retain(|queued| *queued != rc);
        self.abandoned.remove(&rc);
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
        self.pending.clear();
        self.order.clear();
        self.rows.clear();
        self.abandoned.clear();
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
    if cmd.contains("return_code=") {
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
}
