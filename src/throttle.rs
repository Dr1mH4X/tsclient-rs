//! Token-bucket rate limiter — mirrors `teamspeak-js/src/throttle.ts`

use std::time::Instant;

use crate::types::AbortSignal;

pub struct CommandThrottle {
    tokens: f64,
    last_update: Instant,
    hold_until: Instant,
}

impl CommandThrottle {
    const TOKEN_RATE: f64 = 4.0;
    const TOKEN_MAX: f64 = 8.0;

    pub fn new() -> Self {
        Self {
            tokens: 5.0,
            last_update: Instant::now(),
            hold_until: Instant::now(),
        }
    }

    pub fn hold(&mut self, duration: std::time::Duration) {
        self.hold_until = self.hold_until.max(Instant::now() + duration);
        self.last_update = self.hold_until;
        self.tokens = 0.0;
    }

    pub async fn wait(&mut self, signal: Option<&AbortSignal>) -> Result<(), crate::Error> {
        loop {
            if let Some(sig) = signal {
                if sig.is_aborted() {
                    return Err(crate::Error::Teamspeak("aborted".into()));
                }
            }

            let now = Instant::now();
            let elapsed = now
                .saturating_duration_since(self.last_update)
                .as_secs_f64();
            self.tokens = (self.tokens + elapsed * Self::TOKEN_RATE).min(Self::TOKEN_MAX);
            self.last_update = now.max(self.hold_until);

            if now >= self.hold_until && self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return Ok(());
            }

            let wait_ms = ((1.0 - self.tokens) / Self::TOKEN_RATE * 1000.0).ceil() as u64 + 10;
            let sleep = tokio::time::sleep(
                self.hold_until.saturating_duration_since(now)
                    + std::time::Duration::from_millis(wait_ms),
            );

            match signal {
                Some(sig) => tokio::select! {
                    _ = sleep => {}
                    _ = sig.wait_for_abort() => {
                        return Err(crate::Error::Teamspeak("aborted".into()));
                    }
                },
                None => sleep.await,
            }
        }
    }
}
