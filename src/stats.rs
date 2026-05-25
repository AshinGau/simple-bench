use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct Stats {
    pub sent: AtomicU64,
    pub confirmed: AtomicU64,
    pub failed: AtomicU64,
    pub start: Instant,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            sent: AtomicU64::new(0),
            confirmed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            start: Instant::now(),
        }
    }

    pub fn inc_sent(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_confirmed(&self) {
        self.confirmed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    pub fn tps(&self) -> f64 {
        let elapsed = self.elapsed_secs();
        if elapsed < 1.0 {
            return 0.0;
        }
        self.confirmed.load(Ordering::Relaxed) as f64 / elapsed
    }

    pub fn log_summary(&self, active_accounts: usize) {
        let sent = self.sent.load(Ordering::Relaxed);
        let confirmed = self.confirmed.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let tps = self.tps();
        log::info!(
            "[stats] sent={} confirmed={} failed={} active={} tps={:.1}",
            sent,
            confirmed,
            failed,
            active_accounts,
            tps
        );
    }

    pub fn log_final(&self) {
        let sent = self.sent.load(Ordering::Relaxed);
        let confirmed = self.confirmed.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let tps = self.tps();
        let elapsed = self.elapsed_secs();
        log::info!(
            "[stats] FINAL sent={} confirmed={} failed={} tps={:.1} elapsed={:.1}s",
            sent,
            confirmed,
            failed,
            tps,
            elapsed
        );
    }
}
