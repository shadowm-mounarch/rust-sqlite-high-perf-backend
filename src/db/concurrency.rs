use std::sync::atomic::{AtomicU64, Ordering};

pub struct TransactionManager {
    next_ts: AtomicU64,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_ts: AtomicU64::new(1),
        }
    }

    pub fn begin_read(&self) -> u64 {
        self.next_ts.load(Ordering::Relaxed)
    }

    pub fn begin_write(&self) -> u64 {
        self.next_ts.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
