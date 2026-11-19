use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use super::types::{
    ACK_GLOBAL_BURST, ACK_GLOBAL_RATE_PER_SEC, CooldownAction, PeerIdentity,
    SELF_ANNOUNCE_DEBOUNCE, TimerRequest, TimerToken,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CooldownKey {
    peer: String,
    action: CooldownAction,
}

/// 按 `(peer, action)` 隔离单播冷却窗口，并以全局令牌桶限制出站 ACK 突发。
pub struct AckRateLimiter {
    cooldowns: HashMap<CooldownKey, Instant>,
    tokens: f64,
    last_refill: Instant,
}

impl AckRateLimiter {
    pub fn new(now: Instant) -> Self {
        Self {
            cooldowns: HashMap::new(),
            tokens: ACK_GLOBAL_BURST,
            last_refill: now,
        }
    }

    pub fn try_acquire(&mut self, peer: &str, action: CooldownAction, now: Instant) -> bool {
        self.cooldowns
            .retain(|key, &mut last| now.saturating_duration_since(last) < key.action.cooldown());

        let key = CooldownKey {
            peer: peer.to_string(),
            action,
        };
        if let Some(&last) = self.cooldowns.get(&key)
            && now.saturating_duration_since(last) < action.cooldown()
        {
            return false;
        }

        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        self.tokens = (self.tokens + elapsed * ACK_GLOBAL_RATE_PER_SEC).min(ACK_GLOBAL_BURST);
        self.last_refill = self.last_refill.max(now);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;

        self.cooldowns.insert(key, now);
        true
    }

    pub fn rollback(&mut self, peer: &str, action: CooldownAction, acquired_at: Instant) {
        let key = CooldownKey {
            peer: peer.to_string(),
            action,
        };
        if self.cooldowns.get(&key) == Some(&acquired_at) {
            self.cooldowns.remove(&key);
        }
    }

    pub fn reset(&mut self, now: Instant) {
        self.cooldowns.clear();
        self.tokens = ACK_GLOBAL_BURST;
        self.last_refill = now;
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.cooldowns.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceDecision {
    SendNow {
        attempt_at: Instant,
        seq_at_start: u64,
    },
    ScheduleTrailing {
        wait: Duration,
    },
    AlreadyScheduled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceAttemptMode {
    Normal,
    Trailing,
}

/// 频道广播防抖状态机：合并短时间内连续切房，并在失败后允许单次快速重试。
#[derive(Default)]
pub struct AnnounceState {
    last_attempt: Option<Instant>,
    trailing_scheduled: bool,
    success_seq: u64,
}

impl AnnounceState {
    pub fn has_attempted(&self) -> bool {
        self.last_attempt.is_some()
    }

    pub fn try_begin(&mut self, mode: AnnounceAttemptMode, now: Instant) -> AnnounceDecision {
        if matches!(mode, AnnounceAttemptMode::Trailing) {
            self.trailing_scheduled = false;
        }
        if let Some(last) = self.last_attempt {
            let elapsed = now.saturating_duration_since(last);
            if elapsed < SELF_ANNOUNCE_DEBOUNCE {
                if !self.trailing_scheduled {
                    self.trailing_scheduled = true;
                    return AnnounceDecision::ScheduleTrailing {
                        wait: SELF_ANNOUNCE_DEBOUNCE - elapsed,
                    };
                }
                return AnnounceDecision::AlreadyScheduled;
            }
        }

        self.last_attempt = Some(now);
        self.trailing_scheduled = false;
        AnnounceDecision::SendNow {
            attempt_at: now,
            seq_at_start: self.success_seq,
        }
    }

    pub fn record_success(&mut self) {
        self.success_seq = self.success_seq.wrapping_add(1);
    }

    pub fn rollback_failure(&mut self, attempt_at: Instant, seq_at_start: u64) -> bool {
        if self.last_attempt == Some(attempt_at) {
            self.last_attempt = None;
        }
        self.success_seq == seq_at_start
    }

    pub fn should_run_failure_retry(&self, seq_at_start: u64) -> bool {
        self.success_seq == seq_at_start
    }

    pub fn reset(&mut self) {
        self.last_attempt = None;
        self.trailing_scheduled = false;
        self.success_seq = self.success_seq.wrapping_add(1);
    }
}

/// 发起侧 `query` 冷却记录，防止对普通客户端重复探测。
pub struct QueryRateLimiter {
    cooldown: Duration,
    last_sent: HashMap<String, Instant>,
}

impl QueryRateLimiter {
    pub fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            last_sent: HashMap::new(),
        }
    }

    pub fn try_acquire(&mut self, peer: &str, now: Instant) -> bool {
        if self.cooldown.is_zero() {
            return true;
        }
        self.last_sent
            .retain(|_, &mut last| now.saturating_duration_since(last) < self.cooldown);

        if let Some(&last) = self.last_sent.get(peer)
            && now.saturating_duration_since(last) < self.cooldown
        {
            return false;
        }
        self.last_sent.insert(peer.to_string(), now);
        true
    }

    pub fn rollback(&mut self, peer: &str, acquired_at: Instant) {
        if self.last_sent.get(peer) == Some(&acquired_at) {
            self.last_sent.remove(peer);
        }
    }

    pub fn reset(&mut self) {
        self.last_sent.clear();
    }
}

/// 在途 `list_req` 防重跟踪器，带 TTL 自动过期防止永久卡死。
pub struct InflightCommands {
    ttl: Duration,
    inflight: HashMap<String, Instant>,
}

impl InflightCommands {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inflight: HashMap::new(),
        }
    }

    pub fn try_begin(&mut self, peer_uid: &str, now: Instant) -> bool {
        self.inflight
            .retain(|_, &mut started| now.saturating_duration_since(started) < self.ttl);
        if self.inflight.contains_key(peer_uid) {
            return false;
        }
        self.inflight.insert(peer_uid.to_string(), now);
        true
    }

    pub fn complete(&mut self, peer_uid: &str) {
        self.inflight.remove(peer_uid);
    }

    pub fn rollback(&mut self, peer_uid: &str, started_at: Instant) {
        if self.inflight.get(peer_uid) == Some(&started_at) {
            self.inflight.remove(peer_uid);
        }
    }

    pub fn reset(&mut self) {
        self.inflight.clear();
    }
}

/// 低速跨频道背景探测队列：每连接每 UID 最多探测一次，遇防洪错误立即熔断清空。
pub struct ProbeQueue {
    interval: Duration,
    initial_delay: Duration,
    max_peers: usize,
    queue: VecDeque<(i32, String)>,
    queued_uids: HashSet<String>,
    probed_uids: HashSet<String>,
    last_directory_size: usize,
    next_probe_at: Option<Instant>,
    timer_armed: bool,
    halted_by_flood: bool,
}

impl ProbeQueue {
    pub fn new(interval: Duration, initial_delay: Duration, max_peers: usize) -> Self {
        Self {
            interval,
            initial_delay,
            max_peers,
            queue: VecDeque::new(),
            queued_uids: HashSet::new(),
            probed_uids: HashSet::new(),
            last_directory_size: 0,
            next_probe_at: None,
            timer_armed: false,
            halted_by_flood: false,
        }
    }

    pub fn is_halted(&self) -> bool {
        self.halted_by_flood
    }

    pub fn halt_for_flood(&mut self) {
        self.halted_by_flood = true;
        self.queue.clear();
        self.queued_uids.clear();
        self.timer_armed = false;
    }

    pub fn mark_discovered(&mut self, uid: &str) {
        if uid.is_empty() {
            return;
        }
        self.probed_uids.insert(uid.to_string());
        if self.queued_uids.remove(uid) {
            self.queue.retain(|(_, q_uid)| q_uid != uid);
        }
    }

    pub fn remove_clid(&mut self, clid: i32) {
        let queued_uids = &mut self.queued_uids;
        self.queue.retain(|(q_clid, q_uid)| {
            if *q_clid == clid {
                queued_uids.remove(q_uid);
                false
            } else {
                true
            }
        });
    }

    pub fn sync_peers(
        &mut self,
        self_clid: Option<i32>,
        self_channel_id: u64,
        peers: &[(i32, &str, u64)],
        known_identities: &HashMap<String, PeerIdentity>,
        now: Instant,
    ) -> Option<TimerRequest> {
        self.last_directory_size = peers.len();
        if self.halted_by_flood {
            return None;
        }
        if peers.len() > self.max_peers {
            self.queue.clear();
            self.queued_uids.clear();
            return None;
        }

        let mut live_by_uid: HashMap<&str, (i32, u64)> = HashMap::new();
        for &(clid, uid, channel_id) in peers {
            if clid > 0 && !uid.is_empty() && self_clid != Some(clid) {
                live_by_uid.insert(uid, (clid, channel_id));
            }
        }

        self.queued_uids.clear();
        let mut retained = VecDeque::with_capacity(self.queue.len());
        while let Some((_, uid)) = self.queue.pop_front() {
            if let Some(&(live_clid, _)) = live_by_uid.get(uid.as_str())
                && !known_identities.contains_key(&uid)
                && !self.probed_uids.contains(&uid)
                && self.queued_uids.insert(uid.clone())
            {
                retained.push_back((live_clid, uid));
            }
        }
        self.queue = retained;

        let mut same_channel = Vec::new();
        let mut other_channels = Vec::new();
        for &(clid, uid, channel_id) in peers {
            if clid <= 0
                || uid.is_empty()
                || self_clid == Some(clid)
                || known_identities.contains_key(uid)
                || self.probed_uids.contains(uid)
                || self.queued_uids.contains(uid)
            {
                continue;
            }
            if self_channel_id != 0 && channel_id == self_channel_id {
                same_channel.push((clid, uid.to_string()));
            } else {
                other_channels.push((clid, uid.to_string()));
            }
        }

        for (clid, uid) in same_channel.into_iter().chain(other_channels) {
            if self.queued_uids.insert(uid.clone()) {
                self.queue.push_back((clid, uid));
            }
        }

        self.arm_timer(now, self.initial_delay)
    }

    pub fn enqueue_joined(
        &mut self,
        clid: i32,
        uid: &str,
        self_clid: Option<i32>,
        known_identities: &HashMap<String, PeerIdentity>,
        now: Instant,
    ) -> Option<TimerRequest> {
        if self.halted_by_flood
            || self.last_directory_size >= self.max_peers
            || clid <= 0
            || uid.is_empty()
            || self_clid == Some(clid)
            || known_identities.contains_key(uid)
            || self.probed_uids.contains(uid)
            || !self.queued_uids.insert(uid.to_string())
        {
            return None;
        }
        self.queue.push_back((clid, uid.to_string()));
        self.arm_timer(now, self.initial_delay)
    }

    pub fn pop_next_ready(
        &mut self,
        known_identities: &HashMap<String, PeerIdentity>,
        now: Instant,
    ) -> (Option<(i32, String)>, Option<TimerRequest>) {
        self.timer_armed = false;
        if self.halted_by_flood {
            self.queue.clear();
            self.queued_uids.clear();
            return (None, None);
        }

        while let Some((clid, uid)) = self.queue.pop_front() {
            self.queued_uids.remove(&uid);
            if known_identities.contains_key(&uid) || self.probed_uids.contains(&uid) {
                continue;
            }
            self.probed_uids.insert(uid.clone());
            self.next_probe_at = Some(now + self.interval);
            let next_timer = self.arm_timer(now, self.interval);
            return (Some((clid, uid)), next_timer);
        }

        (None, None)
    }

    pub fn reset(&mut self) {
        self.queue.clear();
        self.queued_uids.clear();
        self.probed_uids.clear();
        self.last_directory_size = 0;
        self.next_probe_at = None;
        self.timer_armed = false;
        self.halted_by_flood = false;
    }

    fn arm_timer(&mut self, now: Instant, min_delay: Duration) -> Option<TimerRequest> {
        if self.queue.is_empty() || self.timer_armed || self.halted_by_flood {
            return None;
        }
        self.timer_armed = true;
        let paced = self.next_probe_at.map_or(min_delay, |next| {
            next.saturating_duration_since(now).max(min_delay)
        });
        Some(TimerRequest {
            delay: paced,
            token: TimerToken::ProbeNext,
        })
    }

    #[cfg(test)]
    pub(crate) fn queued_len(&self) -> usize {
        self.queue.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_protocol::types::ClientKind;

    #[test]
    fn ack_rate_limiter_isolates_actions_enforces_bucket_and_rolls_back() {
        let t0 = Instant::now();
        let mut limiter = AckRateLimiter::new(t0);

        assert!(limiter.try_acquire("uid_1", CooldownAction::IdentityAnnounce, t0));
        assert!(limiter.try_acquire("uid_1", CooldownAction::IdentityQuery, t0));
        assert!(limiter.try_acquire("uid_1", CooldownAction::CommandsList, t0));

        assert!(!limiter.try_acquire(
            "uid_1",
            CooldownAction::IdentityAnnounce,
            t0 + Duration::from_secs(9),
        ));
        assert!(limiter.try_acquire(
            "uid_1",
            CooldownAction::IdentityAnnounce,
            t0 + Duration::from_secs(10),
        ));

        let t_burst = t0 + Duration::from_secs(30);
        let mut burst = AckRateLimiter::new(t_burst);
        for i in 0..4 {
            assert!(burst.try_acquire(
                &format!("peer_{i}"),
                CooldownAction::IdentityAnnounce,
                t_burst,
            ));
        }
        assert!(!burst.try_acquire("peer_blocked", CooldownAction::IdentityAnnounce, t_burst));
        assert!(burst.try_acquire(
            "peer_blocked",
            CooldownAction::IdentityAnnounce,
            t_burst + Duration::from_millis(500),
        ));

        burst.rollback("peer_0", CooldownAction::IdentityAnnounce, t_burst);
        assert!(burst.try_acquire(
            "peer_0",
            CooldownAction::IdentityAnnounce,
            t_burst + Duration::from_secs(1),
        ));

        let t_prune = t_burst + Duration::from_secs(15);
        assert!(burst.try_acquire("peer_fresh", CooldownAction::CommandsList, t_prune));
        assert_eq!(burst.len(), 1);
    }

    #[test]
    fn announce_state_debounces_trails_and_rolls_back() {
        let mut state = AnnounceState::default();
        let t0 = Instant::now();

        assert_eq!(
            state.try_begin(AnnounceAttemptMode::Normal, t0),
            AnnounceDecision::SendNow {
                attempt_at: t0,
                seq_at_start: 0,
            }
        );
        assert_eq!(
            state.try_begin(AnnounceAttemptMode::Normal, t0 + Duration::from_millis(500)),
            AnnounceDecision::ScheduleTrailing {
                wait: Duration::from_millis(1500),
            }
        );
        assert_eq!(
            state.try_begin(AnnounceAttemptMode::Normal, t0 + Duration::from_secs(1)),
            AnnounceDecision::AlreadyScheduled
        );

        let t_trail = t0 + Duration::from_secs(2);
        assert_eq!(
            state.try_begin(AnnounceAttemptMode::Trailing, t_trail),
            AnnounceDecision::SendNow {
                attempt_at: t_trail,
                seq_at_start: 0,
            }
        );
        assert!(state.rollback_failure(t_trail, 0));
        assert!(state.should_run_failure_retry(0));
        state.record_success();
        assert!(!state.should_run_failure_retry(0));
    }

    #[test]
    fn inflight_commands_expire_after_ttl_and_query_limiter_cools_down() {
        let t0 = Instant::now();
        let mut inflight = InflightCommands::new(Duration::from_secs(15));
        assert!(inflight.try_begin("bot_1", t0));
        assert!(!inflight.try_begin("bot_1", t0 + Duration::from_secs(10)));
        assert!(inflight.try_begin("bot_1", t0 + Duration::from_secs(15)));

        let mut query = QueryRateLimiter::new(Duration::from_secs(30));
        assert!(query.try_acquire("peer_1", t0));
        assert!(!query.try_acquire("peer_1", t0 + Duration::from_secs(10)));
        query.rollback("peer_1", t0);
        assert!(query.try_acquire("peer_1", t0 + Duration::from_secs(1)));
        assert!(!query.try_acquire("peer_1", t0 + Duration::from_secs(5)));
        assert!(query.try_acquire("peer_1", t0 + Duration::from_secs(31)));
    }

    #[test]
    fn probe_queue_dedups_cancels_on_discovery_and_halts_on_flood() {
        let t0 = Instant::now();
        let mut queue =
            ProbeQueue::new(Duration::from_millis(900), Duration::from_millis(1500), 64);
        let known = HashMap::from([(
            "known-uid".to_string(),
            PeerIdentity::new(ClientKind::FlSpeak, "FlSpeak", "0.1.14"),
        )]);
        let peers = vec![
            (1, "self-uid", 5),
            (2, "known-uid", 5),
            (3, "other-a", 9),
            (4, "same-b", 5),
            (5, "other-c", 10),
        ];

        let timer = queue
            .sync_peers(Some(1), 5, &peers, &known, t0)
            .expect("timer armed");
        assert_eq!(timer.delay, Duration::from_millis(1500));
        assert_eq!(queue.queued_len(), 3);

        // 收到 announce 后反向剔除排队成员
        queue.mark_discovered("other-a");
        assert_eq!(queue.queued_len(), 2);

        // 同频道未识别成员优先出队
        let (first, next_timer) = queue.pop_next_ready(&known, t0 + Duration::from_millis(1500));
        assert_eq!(first, Some((4, "same-b".to_string())));
        assert_eq!(
            next_timer.map(|t| t.delay),
            Some(Duration::from_millis(900))
        );

        // 防洪熔断立即清空剩余队列并拒绝新入队
        queue.halt_for_flood();
        assert!(queue.is_halted());
        assert_eq!(queue.queued_len(), 0);
        assert!(
            queue
                .enqueue_joined(6, "late-uid", Some(1), &known, t0 + Duration::from_secs(2))
                .is_none()
        );
        let (popped_after_halt, _) = queue.pop_next_ready(&known, t0 + Duration::from_secs(3));
        assert_eq!(popped_after_halt, None);

        queue.reset();
        assert!(!queue.is_halted());
    }
}
