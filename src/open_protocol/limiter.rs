use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::types::{
    ACK_GLOBAL_BURST, ACK_GLOBAL_RATE_PER_SEC, CooldownAction, SELF_ANNOUNCE_DEBOUNCE,
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
