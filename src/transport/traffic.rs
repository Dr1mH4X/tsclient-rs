//! Transport traffic metering and keepalive RTT estimation — mirrors
//! `teamspeak-js/src/client/transport/traffic.ts`.

use std::collections::{BTreeMap, VecDeque};

use super::packet::PacketType;

const WINDOW_SECONDS: usize = 60;
const MAX_RTT_SAMPLES: usize = 10;
const UNRELIABLE_REORDER_WINDOW_MS: u64 = 1_000;
const UNRELIABLE_REORDER_PACKETS: usize = 64;
const CONTROL_REORDER_GRACE_MS: u64 = 50;
const KEEPALIVE_PONG_TIMEOUT_MS: u64 = 5_000;
const MAX_TRACKED_SEQS: u64 = 1_024;
const MAX_SEQ_JUMP: u64 = 4_096;
const MAX_EXPIRED_PINGS: usize = 32;

/// The three classes TeamSpeak counts traffic in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrafficClass {
    Speech,
    Keepalive,
    Control,
}

impl TrafficClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Speech => "speech",
            Self::Keepalive => "keepalive",
            Self::Control => "control",
        }
    }
}

/// Which class a packet type is counted in: voice is speech, ping and pong are
/// keepalive, the rest control.
pub fn traffic_class_of(p_type: PacketType) -> TrafficClass {
    match p_type {
        PacketType::Voice | PacketType::VoiceWhisper => TrafficClass::Speech,
        PacketType::Ping | PacketType::Pong => TrafficClass::Keepalive,
        _ => TrafficClass::Control,
    }
}

/// What one direction of one class has carried.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TrafficFigures {
    /// Datagrams, resent ones included.
    pub packets: u64,
    /// Bytes of those datagrams, headers included.
    pub bytes: u64,
    /// Bytes in the last second.
    pub bytes_last_second: u64,
    /// Bytes per second averaged over the last minute.
    pub bytes_per_second_last_minute: f64,
}

/// Per-class traffic figures for a single direction (sent or received).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TrafficCounters {
    pub speech: TrafficFigures,
    pub keepalive: TrafficFigures,
    pub control: TrafficFigures,
}

impl TrafficCounters {
    pub fn get(&self, class: TrafficClass) -> &TrafficFigures {
        match class {
            TrafficClass::Speech => &self.speech,
            TrafficClass::Keepalive => &self.keepalive,
            TrafficClass::Control => &self.control,
        }
    }
}

/// The traffic of a connection, per direction and class.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TrafficSnapshot {
    pub sent: TrafficCounters,
    pub received: TrafficCounters,
}

/// Mean and standard deviation of recent keepalive round-trip times in ms.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RoundTrip {
    pub mean_ms: f64,
    pub deviation_ms: f64,
}

/// Estimated server-to-client packet loss fractions (`0.0..=1.0`) over the
/// trailing 60-second window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PacketLoss {
    pub speech: f64,
    pub keepalive: f64,
    pub control: f64,
    pub total: f64,
}

fn advance_second_ring<T: Copy>(
    buckets: &mut [T; WINDOW_SECONDS],
    current_second: &mut u64,
    next_second: u64,
    zero: T,
) {
    if next_second <= *current_second {
        return;
    }
    let gap = (next_second - *current_second).min(WINDOW_SECONDS as u64) as usize;
    for i in 1..=gap {
        buckets[((*current_second as usize) + i) % WINDOW_SECONDS] = zero;
    }
    *current_second = next_second;
}

/// One direction of one class: totals plus a ring of one-second byte buckets.
struct Counter {
    packets: u64,
    bytes: u64,
    buckets: [u64; WINDOW_SECONDS],
    /// The second (whole) the newest bucket stands for.
    second: u64,
}

impl Counter {
    fn new() -> Self {
        Self {
            packets: 0,
            bytes: 0,
            buckets: [0; WINDOW_SECONDS],
            second: 0,
        }
    }

    fn advance(&mut self, second: u64) {
        advance_second_ring(&mut self.buckets, &mut self.second, second, 0);
    }

    fn add(&mut self, bytes: usize, now_ms: u64) {
        self.advance(now_ms / 1000);
        self.packets += 1;
        let bytes_u64 = bytes as u64;
        self.bytes += bytes_u64;
        self.buckets[(self.second as usize) % WINDOW_SECONDS] += bytes_u64;
    }

    fn figures(&mut self, now_ms: u64) -> TrafficFigures {
        self.advance(now_ms / 1000);
        // The second in progress is incomplete, so "the last second" is the
        // one before it, and the minute is the 59 before it plus that one.
        let sec = (self.second as usize) % WINDOW_SECONDS;
        let previous = (sec + WINDOW_SECONDS - 1) % WINDOW_SECONDS;
        let mut minute: u64 = 0;
        for i in 1..=WINDOW_SECONDS {
            minute += self.buckets[(sec + WINDOW_SECONDS * 2 - i) % WINDOW_SECONDS];
        }
        TrafficFigures {
            packets: self.packets,
            bytes: self.bytes,
            bytes_last_second: self.buckets[previous],
            bytes_per_second_last_minute: minute as f64 / WINDOW_SECONDS as f64,
        }
    }
}

struct DirectionCounters {
    speech: Counter,
    keepalive: Counter,
    control: Counter,
}

impl DirectionCounters {
    fn new() -> Self {
        Self {
            speech: Counter::new(),
            keepalive: Counter::new(),
            control: Counter::new(),
        }
    }

    fn add(&mut self, class: TrafficClass, bytes: usize, now_ms: u64) {
        match class {
            TrafficClass::Speech => self.speech.add(bytes, now_ms),
            TrafficClass::Keepalive => self.keepalive.add(bytes, now_ms),
            TrafficClass::Control => self.control.add(bytes, now_ms),
        }
    }

    fn snapshot(&mut self, now_ms: u64) -> TrafficCounters {
        TrafficCounters {
            speech: self.speech.figures(now_ms),
            keepalive: self.keepalive.figures(now_ms),
            control: self.control.figures(now_ms),
        }
    }
}

/// Counts the datagrams a connection sends and receives, by class, and the
/// bytes of the last second and the last minute. Time is passed in
/// (milliseconds, monotonic) so a test can drive it.
pub(crate) struct TrafficMeter {
    sent: DirectionCounters,
    received: DirectionCounters,
}

impl TrafficMeter {
    pub fn new() -> Self {
        Self {
            sent: DirectionCounters::new(),
            received: DirectionCounters::new(),
        }
    }

    pub fn record_sent(&mut self, p_type: PacketType, bytes: usize, now_ms: u64) {
        self.sent.add(traffic_class_of(p_type), bytes, now_ms);
    }

    pub fn record_received(&mut self, p_type: PacketType, bytes: usize, now_ms: u64) {
        self.received.add(traffic_class_of(p_type), bytes, now_ms);
    }

    pub fn snapshot(&mut self, now_ms: u64) -> TrafficSnapshot {
        TrafficSnapshot {
            sent: self.sent.snapshot(now_ms),
            received: self.received.snapshot(now_ms),
        }
    }
}

/// The round-trip time of the keepalive exchange: a mean and a deviation over
/// recent samples.
pub(crate) struct RoundTripEstimator {
    samples: VecDeque<f64>,
}

impl RoundTripEstimator {
    pub fn new() -> Self {
        Self {
            samples: VecDeque::with_capacity(MAX_RTT_SAMPLES),
        }
    }

    pub fn add(&mut self, ms: f64) {
        self.samples.push_back(ms);
        if self.samples.len() > MAX_RTT_SAMPLES {
            self.samples.pop_front();
        }
    }

    /// The mean and the standard deviation in milliseconds, or `None` before
    /// the first sample.
    pub fn get(&self) -> Option<RoundTrip> {
        let n = self.samples.len();
        if n == 0 {
            return None;
        }
        let mean = self.samples.iter().sum::<f64>() / n as f64;
        let variance = self
            .samples
            .iter()
            .map(|&b| (b - mean) * (b - mean))
            .sum::<f64>()
            / n as f64;
        Some(RoundTrip {
            mean_ms: mean,
            deviation_ms: variance.sqrt(),
        })
    }
}

struct LossCounter {
    buckets: [(u64, u64); WINDOW_SECONDS],
    second: u64,
}

impl LossCounter {
    fn new() -> Self {
        Self {
            buckets: [(0, 0); WINDOW_SECONDS],
            second: 0,
        }
    }

    fn record(&mut self, expected: u64, lost: u64, now_ms: u64) {
        if expected == 0 && lost == 0 {
            return;
        }
        advance_second_ring(&mut self.buckets, &mut self.second, now_ms / 1000, (0, 0));
        let slot = &mut self.buckets[(self.second as usize) % WINDOW_SECONDS];
        slot.0 += expected;
        slot.1 += lost;
    }

    fn totals(&mut self, now_ms: u64) -> (u64, u64) {
        advance_second_ring(&mut self.buckets, &mut self.second, now_ms / 1000, (0, 0));
        self.buckets
            .iter()
            .fold((0u64, 0u64), |(e_sum, l_sum), &(e, l)| {
                (e_sum + e, l_sum + l)
            })
    }
}

fn unwrap_u16_seq(next_expected: u64, id: u16) -> Option<u64> {
    let expected_u16 = (next_expected & 0xffff) as u16;
    let delta = id.wrapping_sub(expected_u16) as i16 as i64;
    u64::try_from(next_expected as i64 + delta).ok()
}

/// Sequence tracker for unreliable streams (`Voice`, `VoiceWhisper`, `S2C Ping`).
/// Delayed arrivals inside the 1s / 64-packet reorder window heal their gap.
struct UnreliableSeqTracker {
    initialized: bool,
    next_expected: u64,
    pending_gaps: VecDeque<(u64, u64)>,
}

impl UnreliableSeqTracker {
    fn new() -> Self {
        Self {
            initialized: false,
            next_expected: 0,
            pending_gaps: VecDeque::new(),
        }
    }

    fn expire(&mut self, now_ms: u64) -> u64 {
        let mut lost = 0;
        while let Some(&(_, detected_ms)) = self.pending_gaps.front() {
            if now_ms.saturating_sub(detected_ms) < UNRELIABLE_REORDER_WINDOW_MS {
                break;
            }
            self.pending_gaps.pop_front();
            lost += 1;
        }
        lost
    }

    fn on_packet(&mut self, id: u16, now_ms: u64) -> (u64, u64) {
        let mut lost = self.expire(now_ms);
        if !self.initialized {
            self.initialized = true;
            self.next_expected = id as u64 + 1;
            return (1, lost);
        }

        let Some(ext) = unwrap_u16_seq(self.next_expected, id) else {
            return (0, lost);
        };

        if ext < self.next_expected {
            if let Ok(idx) = self
                .pending_gaps
                .binary_search_by_key(&ext, |&(seq, _)| seq)
            {
                self.pending_gaps.remove(idx);
            }
            return (0, lost);
        }

        if ext == self.next_expected {
            self.next_expected += 1;
            return (1, lost);
        }

        let gap_len = ext - self.next_expected;
        if gap_len > MAX_SEQ_JUMP {
            self.pending_gaps.clear();
            self.next_expected = ext + 1;
            return (1, lost);
        }

        let keep_from = ext
            .saturating_sub(UNRELIABLE_REORDER_PACKETS as u64)
            .max(self.next_expected);
        lost += keep_from - self.next_expected;
        for seq in keep_from..ext {
            self.pending_gaps.push_back((seq, now_ms));
        }
        while let Some(&(oldest, _)) = self.pending_gaps.front() {
            if ext - oldest >= UNRELIABLE_REORDER_PACKETS as u64
                || self.pending_gaps.len() > UNRELIABLE_REORDER_PACKETS
            {
                self.pending_gaps.pop_front();
                lost += 1;
            } else {
                break;
            }
        }
        self.next_expected = ext + 1;
        (gap_len + 1, lost)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReliableSeqState {
    PendingGap { detected_ms: u64 },
    CountedLost,
    Received,
}

/// Sequence tracker for reliable streams (`Command`, `CommandLow`).
/// Only tolerates brief UDP reordering well below server RTO, and marks
/// expired gaps as `CountedLost` so delayed RTO retransmissions neither hide
/// earlier loss nor get double-counted.
struct ReliableSeqTracker {
    initialized: bool,
    next_expected: u64,
    states: BTreeMap<u64, ReliableSeqState>,
    pending_fifo: VecDeque<u64>,
}

impl ReliableSeqTracker {
    fn new() -> Self {
        Self {
            initialized: false,
            next_expected: 0,
            states: BTreeMap::new(),
            pending_fifo: VecDeque::new(),
        }
    }

    fn expire(&mut self, now_ms: u64) -> u64 {
        let mut lost = 0;
        while let Some(&seq) = self.pending_fifo.front() {
            let Some(state) = self.states.get_mut(&seq) else {
                self.pending_fifo.pop_front();
                continue;
            };
            match *state {
                ReliableSeqState::PendingGap { detected_ms } => {
                    if now_ms.saturating_sub(detected_ms) < CONTROL_REORDER_GRACE_MS {
                        break;
                    }
                    *state = ReliableSeqState::CountedLost;
                    self.pending_fifo.pop_front();
                    lost += 1;
                }
                ReliableSeqState::CountedLost | ReliableSeqState::Received => {
                    self.pending_fifo.pop_front();
                }
            }
        }
        lost
    }

    fn trim_window(&mut self) -> u64 {
        let mut lost = 0;
        let min_keep = self.next_expected.saturating_sub(MAX_TRACKED_SEQS);
        while let Some((&oldest_seq, _)) = self.states.first_key_value() {
            if oldest_seq >= min_keep {
                break;
            }
            if let Some(ReliableSeqState::PendingGap { .. }) = self.states.remove(&oldest_seq) {
                lost += 1;
            }
        }
        while self.pending_fifo.front().is_some_and(|&seq| seq < min_keep) {
            self.pending_fifo.pop_front();
        }
        lost
    }

    fn on_packet(&mut self, id: u16, now_ms: u64) -> (u64, u64) {
        let mut lost = self.expire(now_ms);
        if !self.initialized {
            self.initialized = true;
            self.next_expected = id as u64 + 1;
            self.states.insert(id as u64, ReliableSeqState::Received);
            return (1, lost);
        }

        let Some(ext) = unwrap_u16_seq(self.next_expected, id) else {
            return (0, lost);
        };
        let min_keep = self.next_expected.saturating_sub(MAX_TRACKED_SEQS);
        if ext < min_keep {
            return (0, lost);
        }

        if ext < self.next_expected {
            if let Some(state) = self.states.get_mut(&ext) {
                match *state {
                    ReliableSeqState::PendingGap { .. } | ReliableSeqState::CountedLost => {
                        *state = ReliableSeqState::Received;
                    }
                    ReliableSeqState::Received => {}
                }
            }
            return (0, lost);
        }

        if ext == self.next_expected {
            self.states.insert(ext, ReliableSeqState::Received);
            self.next_expected += 1;
            lost += self.trim_window();
            return (1, lost);
        }

        let gap_len = ext - self.next_expected;
        if gap_len > MAX_SEQ_JUMP {
            self.states.clear();
            self.pending_fifo.clear();
            self.states.insert(ext, ReliableSeqState::Received);
            self.next_expected = ext + 1;
            return (1, lost);
        }

        for seq in self.next_expected..ext {
            self.states.insert(
                seq,
                ReliableSeqState::PendingGap {
                    detected_ms: now_ms,
                },
            );
            self.pending_fifo.push_back(seq);
        }
        self.states.insert(ext, ReliableSeqState::Received);
        self.next_expected = ext + 1;
        lost += self.trim_window();
        (gap_len + 1, lost)
    }
}

struct KeepalivePongTracker {
    pending: VecDeque<(u16, u64)>,
    expired: VecDeque<u16>,
    max_pending: usize,
}

impl KeepalivePongTracker {
    fn new(max_pending: usize) -> Self {
        Self {
            pending: VecDeque::with_capacity(max_pending),
            expired: VecDeque::with_capacity(MAX_EXPIRED_PINGS),
            max_pending,
        }
    }

    fn push_expired(&mut self, id: u16) {
        if !self.expired.contains(&id) {
            self.expired.push_back(id);
            while self.expired.len() > MAX_EXPIRED_PINGS {
                self.expired.pop_front();
            }
        }
    }

    fn expire(&mut self, now_ms: u64) -> (u64, u64) {
        let mut expected = 0;
        let mut lost = 0;
        while let Some(&(id, sent_ms)) = self.pending.front() {
            if now_ms.saturating_sub(sent_ms) < KEEPALIVE_PONG_TIMEOUT_MS {
                break;
            }
            self.pending.pop_front();
            self.push_expired(id);
            expected += 1;
            lost += 1;
        }
        (expected, lost)
    }

    fn record_ping_sent(&mut self, id: u16, now_ms: u64) -> (u64, u64) {
        let (mut expected, mut lost) = self.expire(now_ms);
        self.expired.retain(|&old| old != id);
        if let Some(pos) = self.pending.iter().position(|&(old_id, _)| old_id == id) {
            self.pending.remove(pos);
            expected += 1;
            lost += 1;
        }
        self.pending.push_back((id, now_ms));
        while self.pending.len() > self.max_pending {
            if let Some((evicted_id, _)) = self.pending.pop_front() {
                self.push_expired(evicted_id);
                expected += 1;
                lost += 1;
            }
        }
        (expected, lost)
    }

    fn record_pong_received(
        &mut self,
        acked: u16,
        matched_pings_sent: bool,
        now_ms: u64,
    ) -> (u64, u64) {
        let (mut expected, lost) = self.expire(now_ms);
        if let Some(pos) = self.pending.iter().position(|&(id, _)| id == acked) {
            self.pending.remove(pos);
            expected += 1;
        } else if let Some(pos) = self.expired.iter().position(|&id| id == acked) {
            self.expired.remove(pos);
        } else if matched_pings_sent {
            expected += 1;
        }
        (expected, lost)
    }
}

fn loss_ratio(expected: u64, lost: u64) -> f64 {
    if expected == 0 {
        0.0
    } else {
        (lost as f64 / expected as f64).clamp(0.0, 1.0)
    }
}

/// Estimates server-to-client packet loss per `PacketType` sequence stream
/// while aggregating into `speech`, `keepalive`, and `control` 60s buckets.
pub(crate) struct PacketLossEstimator {
    last_now_ms: u64,
    voice: UnreliableSeqTracker,
    voice_whisper: UnreliableSeqTracker,
    command: ReliableSeqTracker,
    command_low: ReliableSeqTracker,
    s2c_ping: UnreliableSeqTracker,
    keepalive_pong: KeepalivePongTracker,
    speech: LossCounter,
    keepalive: LossCounter,
    control: LossCounter,
}

impl PacketLossEstimator {
    pub fn new(max_pending_pings: usize) -> Self {
        Self {
            last_now_ms: 0,
            voice: UnreliableSeqTracker::new(),
            voice_whisper: UnreliableSeqTracker::new(),
            command: ReliableSeqTracker::new(),
            command_low: ReliableSeqTracker::new(),
            s2c_ping: UnreliableSeqTracker::new(),
            keepalive_pong: KeepalivePongTracker::new(max_pending_pings),
            speech: LossCounter::new(),
            keepalive: LossCounter::new(),
            control: LossCounter::new(),
        }
    }

    fn monotonic_ms(&mut self, now_ms: u64) -> u64 {
        self.last_now_ms = self.last_now_ms.max(now_ms);
        self.last_now_ms
    }

    pub fn record_received(&mut self, p_type: PacketType, id: u16, now_ms: u64) {
        let now_ms = self.monotonic_ms(now_ms);
        match p_type {
            PacketType::Voice => {
                let (e, l) = self.voice.on_packet(id, now_ms);
                self.speech.record(e, l, now_ms);
            }
            PacketType::VoiceWhisper => {
                let (e, l) = self.voice_whisper.on_packet(id, now_ms);
                self.speech.record(e, l, now_ms);
            }
            PacketType::Command => {
                let (e, l) = self.command.on_packet(id, now_ms);
                self.control.record(e, l, now_ms);
            }
            PacketType::CommandLow => {
                let (e, l) = self.command_low.on_packet(id, now_ms);
                self.control.record(e, l, now_ms);
            }
            PacketType::Ping => {
                let (e, l) = self.s2c_ping.on_packet(id, now_ms);
                self.keepalive.record(e, l, now_ms);
            }
            PacketType::Pong | PacketType::Ack | PacketType::AckLow | PacketType::Init1 => {}
        }
    }

    pub fn record_ping_sent(&mut self, id: u16, now_ms: u64) {
        let now_ms = self.monotonic_ms(now_ms);
        let (e, l) = self.keepalive_pong.record_ping_sent(id, now_ms);
        self.keepalive.record(e, l, now_ms);
    }

    pub fn record_pong_received(&mut self, acked: u16, matched_pings_sent: bool, now_ms: u64) {
        let now_ms = self.monotonic_ms(now_ms);
        let (e, l) = self
            .keepalive_pong
            .record_pong_received(acked, matched_pings_sent, now_ms);
        self.keepalive.record(e, l, now_ms);
    }

    pub fn flush(&mut self, now_ms: u64) {
        let now_ms = self.monotonic_ms(now_ms);
        let speech_lost = self.voice.expire(now_ms) + self.voice_whisper.expire(now_ms);
        self.speech.record(0, speech_lost, now_ms);

        let control_lost = self.command.expire(now_ms) + self.command_low.expire(now_ms);
        self.control.record(0, control_lost, now_ms);

        let ping_lost = self.s2c_ping.expire(now_ms);
        self.keepalive.record(0, ping_lost, now_ms);

        let (e, l) = self.keepalive_pong.expire(now_ms);
        self.keepalive.record(e, l, now_ms);
    }

    pub fn snapshot(&mut self, now_ms: u64) -> Option<PacketLoss> {
        let now_ms = self.monotonic_ms(now_ms);
        self.flush(now_ms);
        let (speech_exp, speech_lost) = self.speech.totals(now_ms);
        let (keep_exp, keep_lost) = self.keepalive.totals(now_ms);
        let (ctrl_exp, ctrl_lost) = self.control.totals(now_ms);
        let total_exp = speech_exp + keep_exp + ctrl_exp;
        if total_exp == 0 {
            return None;
        }
        let total_lost = speech_lost + keep_lost + ctrl_lost;
        Some(PacketLoss {
            speech: loss_ratio(speech_exp, speech_lost),
            keepalive: loss_ratio(keep_exp, keep_lost),
            control: loss_ratio(ctrl_exp, ctrl_lost),
            total: loss_ratio(total_exp, total_lost),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_voice_as_speech_ping_and_pong_as_keepalive_rest_as_control() {
        assert_eq!(traffic_class_of(PacketType::Voice), TrafficClass::Speech);
        assert_eq!(
            traffic_class_of(PacketType::VoiceWhisper),
            TrafficClass::Speech
        );
        assert_eq!(traffic_class_of(PacketType::Ping), TrafficClass::Keepalive);
        assert_eq!(traffic_class_of(PacketType::Pong), TrafficClass::Keepalive);
        for t in [
            PacketType::Command,
            PacketType::CommandLow,
            PacketType::Ack,
            PacketType::AckLow,
            PacketType::Init1,
        ] {
            assert_eq!(traffic_class_of(t), TrafficClass::Control);
        }
    }

    #[test]
    fn counts_packets_and_bytes_per_direction_and_class() {
        let mut meter = TrafficMeter::new();
        meter.record_sent(PacketType::Voice, 100, 1_000);
        meter.record_sent(PacketType::VoiceWhisper, 50, 1_100);
        meter.record_sent(PacketType::Ping, 11, 1_200);
        meter.record_received(PacketType::Command, 70, 1_300);
        let snap = meter.snapshot(1_400);
        assert_eq!(snap.sent.speech.packets, 2);
        assert_eq!(snap.sent.speech.bytes, 150);
        assert_eq!(snap.sent.keepalive.packets, 1);
        assert_eq!(snap.sent.keepalive.bytes, 11);
        assert_eq!(snap.sent.control.packets, 0);
        assert_eq!(snap.sent.control.bytes, 0);
        assert_eq!(snap.received.control.packets, 1);
        assert_eq!(snap.received.control.bytes, 70);
    }

    #[test]
    fn reports_last_full_second_and_minute_average_per_second() {
        let mut meter = TrafficMeter::new();
        meter.record_sent(PacketType::Voice, 600, 10_100);
        meter.record_sent(PacketType::Voice, 60, 11_500);
        // At 12.2 s the last full second is 11 (60 bytes); the minute holds 660.
        let snap = meter.snapshot(12_200);
        assert_eq!(snap.sent.speech.bytes_last_second, 60);
        assert_eq!(snap.sent.speech.bytes_per_second_last_minute, 11.0);
        // A minute later the old traffic has left the window, the totals have not.
        let later = meter.snapshot(80_000);
        assert_eq!(later.sent.speech.bytes_last_second, 0);
        assert_eq!(later.sent.speech.bytes_per_second_last_minute, 0.0);
        assert_eq!(later.sent.speech.bytes, 660);
    }

    #[test]
    fn round_trip_has_nothing_before_first_sample() {
        assert_eq!(RoundTripEstimator::new().get(), None);
    }

    #[test]
    fn round_trip_reports_mean_and_standard_deviation() {
        let mut rtt = RoundTripEstimator::new();
        rtt.add(10.0);
        rtt.add(20.0);
        assert_eq!(
            rtt.get(),
            Some(RoundTrip {
                mean_ms: 15.0,
                deviation_ms: 5.0
            })
        );
    }

    #[test]
    fn round_trip_keeps_only_last_ten() {
        let mut rtt = RoundTripEstimator::new();
        for _ in 0..10 {
            rtt.add(1000.0);
        }
        for _ in 0..10 {
            rtt.add(10.0);
        }
        assert_eq!(
            rtt.get(),
            Some(RoundTrip {
                mean_ms: 10.0,
                deviation_ms: 0.0
            })
        );
    }

    #[test]
    fn packet_loss_is_none_until_observed_then_recovers_after_window() {
        let mut loss = PacketLossEstimator::new(16);
        assert_eq!(loss.snapshot(1_000), None);

        loss.record_received(PacketType::Voice, 10, 1_000);
        loss.record_received(PacketType::Voice, 12, 1_010);
        let snap = loss.snapshot(2_500).expect("speech sample");
        assert!((snap.speech - (1.0 / 3.0)).abs() < 1e-9);
        assert!((snap.total - (1.0 / 3.0)).abs() < 1e-9);

        // 60+ seconds later with clean voice traffic, old drop ages out.
        for i in 0..10u16 {
            loss.record_received(PacketType::Voice, 13 + i, 65_000 + i as u64 * 20);
        }
        let recovered = loss.snapshot(66_000).expect("fresh speech sample");
        assert_eq!(recovered.speech, 0.0);
        assert_eq!(recovered.total, 0.0);
    }

    #[test]
    fn packet_types_keep_independent_sequences_within_same_class() {
        let mut loss = PacketLossEstimator::new(16);
        // Interleave Voice (0..3) and VoiceWhisper (500..503) in Speech.
        loss.record_received(PacketType::Voice, 0, 1_000);
        loss.record_received(PacketType::VoiceWhisper, 500, 1_005);
        loss.record_received(PacketType::Voice, 1, 1_010);
        loss.record_received(PacketType::VoiceWhisper, 501, 1_015);
        loss.record_received(PacketType::Voice, 2, 1_020);
        loss.record_received(PacketType::VoiceWhisper, 502, 1_025);

        // Interleave Command (10..12) and CommandLow (900..902) in Control.
        loss.record_received(PacketType::Command, 10, 1_030);
        loss.record_received(PacketType::CommandLow, 900, 1_035);
        loss.record_received(PacketType::Command, 11, 1_040);
        loss.record_received(PacketType::CommandLow, 901, 1_045);

        // Interleave S2C Ping (42..44) and C2S Ping->Pong (0..1) in Keepalive.
        loss.record_received(PacketType::Ping, 42, 1_050);
        loss.record_ping_sent(0, 1_050);
        loss.record_received(PacketType::Ping, 43, 1_060);
        loss.record_pong_received(0, true, 1_070);

        let snap = loss.snapshot(3_000).unwrap();
        assert_eq!(snap.speech, 0.0);
        assert_eq!(snap.control, 0.0);
        assert_eq!(snap.keepalive, 0.0);
        assert_eq!(snap.total, 0.0);
    }

    #[test]
    fn unreliable_speech_heals_reorder_within_window_and_counts_expired_once() {
        let mut loss = PacketLossEstimator::new(16);
        loss.record_received(PacketType::Voice, 1, 1_000);
        loss.record_received(PacketType::Voice, 3, 1_010);
        // Arrives 500ms later (< 1000ms reorder window): healed, 0 loss.
        loss.record_received(PacketType::Voice, 2, 1_500);
        let healed = loss.snapshot(3_000).unwrap();
        assert_eq!(healed.speech, 0.0);

        // Now drop 5 across 1s expiry, then send a very late copy at t=6000ms.
        loss.record_received(PacketType::Voice, 4, 4_000);
        loss.record_received(PacketType::Voice, 6, 4_010);
        let expired = loss.snapshot(5_100).unwrap();
        // Total expected so far = 6 (seq 1..=6), lost = 1 (seq 5).
        assert!((expired.speech - (1.0 / 6.0)).abs() < 1e-9);

        loss.record_received(PacketType::Voice, 5, 6_000);
        let after_late = loss.snapshot(6_100).unwrap();
        assert!((after_late.speech - (1.0 / 6.0)).abs() < 1e-9);
    }

    #[test]
    fn control_rto_retransmit_is_not_swallowed_by_grace_or_double_counted() {
        let mut loss = PacketLossEstimator::new(16);
        loss.record_received(PacketType::Command, 0, 1_000);
        // Packet 1 dropped, packet 2 arrives at 1005ms.
        loss.record_received(PacketType::Command, 2, 1_005);

        // 500ms later (well past 50ms control reorder grace), server RTO retransmits id=1
        // EVEN without an intermediate flush/snapshot call.
        loss.record_received(PacketType::Command, 1, 1_505);
        let after_first_rto = loss.snapshot(1_510).unwrap();
        assert!((after_first_rto.control - (1.0 / 3.0)).abs() < 1e-9);

        // A second retransmit/duplicate of id=1 must NOT double-count.
        loss.record_received(PacketType::Command, 1, 2_005);
        let after_second_rto = loss.snapshot(2_010).unwrap();
        assert!((after_second_rto.control - (1.0 / 3.0)).abs() < 1e-9);

        // Verify separated expiry-before-retransmit path as well on CommandLow:
        loss.record_received(PacketType::CommandLow, 10, 3_000);
        loss.record_received(PacketType::CommandLow, 12, 3_005);
        // Snapshot expires gap 11 at 3100ms (>= 50ms).
        let mid = loss.snapshot(3_100).unwrap();
        // Control total: 3 (Command 0..2, 1 lost) + 3 (CommandLow 10..12, 1 lost) = 2/6.
        assert!((mid.control - (2.0 / 6.0)).abs() < 1e-9);
        // RTO retransmit of CommandLow id=11 arrives at 3505ms -> stays 2/6, not 3/6!
        loss.record_received(PacketType::CommandLow, 11, 3_505);
        let after_low_rto = loss.snapshot(3_510).unwrap();
        assert!((after_low_rto.control - (2.0 / 6.0)).abs() < 1e-9);
    }

    #[test]
    fn control_tolerates_tiny_udp_reorder_and_ignores_c2s_ack_drop_duplicates() {
        let mut loss = PacketLossEstimator::new(16);
        loss.record_received(PacketType::Command, 0, 1_000);
        loss.record_received(PacketType::Command, 2, 1_005);
        // 15ms UDP jitter reorder (< 50ms control grace) heals cleanly.
        loss.record_received(PacketType::Command, 1, 1_020);
        // Server resends id=0 later because client ACK was lost: must not add S2C loss.
        loss.record_received(PacketType::Command, 0, 1_600);

        let snap = loss.snapshot(2_000).unwrap();
        assert_eq!(snap.control, 0.0);
    }

    #[test]
    fn handles_u16_wraparound_for_both_unreliable_and_reliable_streams() {
        let mut loss = PacketLossEstimator::new(16);
        loss.record_received(PacketType::Voice, 65534, 1_000);
        loss.record_received(PacketType::Voice, 0, 1_010); // dropped 65535
        loss.record_received(PacketType::Voice, 1, 1_020);

        loss.record_received(PacketType::Command, 65534, 1_000);
        loss.record_received(PacketType::Command, 65535, 1_010);
        loss.record_received(PacketType::Command, 0, 1_020);
        loss.record_received(PacketType::Command, 1, 1_030);

        let snap = loss.snapshot(2_200).unwrap();
        assert!((snap.speech - (1.0 / 4.0)).abs() < 1e-9);
        assert_eq!(snap.control, 0.0);
    }

    #[test]
    fn keepalive_counts_ping_timeout_once_and_ignores_late_pong() {
        let mut loss = PacketLossEstimator::new(16);
        loss.record_ping_sent(0, 1_000);
        loss.record_pong_received(0, true, 1_040);
        loss.record_ping_sent(1, 6_000);
        // Ping 1 times out after 5s.
        let timed_out = loss.snapshot(11_500).unwrap();
        assert!((timed_out.keepalive - 0.5).abs() < 1e-9);
        // Late pong for already-timed-out Ping 1 must not change expected or lost.
        loss.record_pong_received(1, false, 12_000);
        let after_late = loss.snapshot(12_100).unwrap();
        assert!((after_late.keepalive - 0.5).abs() < 1e-9);
    }
}
