use std::sync::atomic::{AtomicU64, Ordering};

/// 데이터베이스 메트릭
pub struct DBMetrics {
    pub postgres_timeouts: AtomicU64,
    pub redis_timeouts: AtomicU64,
    pub postgres_total_requests: AtomicU64,
    pub postgres_total_response_time_ms: AtomicU64,
    pub redis_total_requests: AtomicU64,
    pub redis_total_response_time_ms: AtomicU64,
}

impl Default for DBMetrics {
    fn default() -> Self {
        Self {
            postgres_timeouts: AtomicU64::new(0),
            redis_timeouts: AtomicU64::new(0),
            postgres_total_requests: AtomicU64::new(0),
            postgres_total_response_time_ms: AtomicU64::new(0),
            redis_total_requests: AtomicU64::new(0),
            redis_total_response_time_ms: AtomicU64::new(0),
        }
    }
}

impl DBMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment_postgres_timeout(&self) {
        self.postgres_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_redis_timeout(&self) {
        self.redis_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_postgres_query(&self, response_time_ms: u64) {
        self.postgres_total_requests.fetch_add(1, Ordering::Relaxed);
        self.postgres_total_response_time_ms
            .fetch_add(response_time_ms, Ordering::Relaxed);
    }

    pub fn record_redis_query(&self, response_time_ms: u64) {
        self.redis_total_requests.fetch_add(1, Ordering::Relaxed);
        self.redis_total_response_time_ms
            .fetch_add(response_time_ms, Ordering::Relaxed);
    }

    pub fn get_values(&self) -> (u64, u64, f64, f64) {
        let postgres_timeouts = self.postgres_timeouts.load(Ordering::Relaxed);
        let redis_timeouts = self.redis_timeouts.load(Ordering::Relaxed);

        let postgres_requests = self.postgres_total_requests.load(Ordering::Relaxed);
        let postgres_total_time = self.postgres_total_response_time_ms.load(Ordering::Relaxed);
        let postgres_avg_time = if postgres_requests > 0 {
            postgres_total_time as f64 / postgres_requests as f64
        } else {
            0.0
        };

        let redis_requests = self.redis_total_requests.load(Ordering::Relaxed);
        let redis_total_time = self.redis_total_response_time_ms.load(Ordering::Relaxed);
        let redis_avg_time = if redis_requests > 0 {
            redis_total_time as f64 / redis_requests as f64
        } else {
            0.0
        };

        (
            postgres_timeouts,
            redis_timeouts,
            postgres_avg_time,
            redis_avg_time,
        )
    }
}
