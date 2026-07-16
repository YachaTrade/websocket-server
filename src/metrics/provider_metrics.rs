use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// 개별 프로바이더 상태
#[derive(Debug)]
pub struct ProviderStatus {
    is_healthy: AtomicBool,
    last_updated: AtomicU64, // timestamp
}

impl ProviderStatus {
    fn new() -> Self {
        Self {
            is_healthy: AtomicBool::new(false), // 기본적으로 unhealthy로 시작
            last_updated: AtomicU64::new(0),
        }
    }

    fn set_healthy(&self, healthy: bool) {
        self.is_healthy.store(healthy, Ordering::Relaxed);
        self.last_updated.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }

    fn is_healthy(&self) -> bool {
        self.is_healthy.load(Ordering::Relaxed)
    }
}

/// 프로바이더 메트릭
pub struct ProviderMetrics {
    pub total_requests: AtomicU64,
    pub successful_requests: AtomicU64,
    pub rpc_timeouts: AtomicU64,
    pub total_response_time_ms: AtomicU64, // 총 응답시간 (밀리초)
    pub provider_health: DashMap<String, ProviderStatus>, // 개별 프로바이더 상태
}

impl Default for ProviderMetrics {
    fn default() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            rpc_timeouts: AtomicU64::new(0),
            total_response_time_ms: AtomicU64::new(0),
            provider_health: DashMap::new(),
        }
    }
}

impl ProviderMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_request(&self, success: bool) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        if success {
            self.successful_requests.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_request_with_time(&self, success: bool, response_time_ms: u64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_response_time_ms
            .fetch_add(response_time_ms, Ordering::Relaxed);
        if success {
            self.successful_requests.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_rpc_timeout(&self) {
        self.rpc_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    /// 개별 프로바이더 상태 설정
    pub fn set_provider_health(&self, provider_name: &str, is_healthy: bool) {
        let status = self
            .provider_health
            .entry(provider_name.to_string())
            .or_insert_with(ProviderStatus::new);
        status.set_healthy(is_healthy);
    }

    /// 개별 프로바이더 상태 조회  
    pub fn get_provider_health(&self, provider_name: &str) -> Option<bool> {
        self.provider_health
            .get(provider_name)
            .map(|status| status.is_healthy())
    }

    pub fn get_values(&self) -> (f64, f64, u64, f64) {
        let total_req = self.total_requests.load(Ordering::Relaxed);
        let successful = self.successful_requests.load(Ordering::Relaxed);
        let timeouts = self.rpc_timeouts.load(Ordering::Relaxed);
        let total_response_time = self.total_response_time_ms.load(Ordering::Relaxed);

        // 개별 프로바이더 상태에서 healthy 비율 계산
        let total_providers = self.provider_health.len() as u64;
        let healthy_providers = self
            .provider_health
            .iter()
            .filter(|entry| entry.value().is_healthy())
            .count() as u64;

        let success_rate = if total_req > 0 {
            (successful as f64 / total_req as f64) * 100.0
        } else {
            100.0
        };
        let health_rate = if total_providers > 0 {
            (healthy_providers as f64 / total_providers as f64) * 100.0
        } else {
            100.0
        };
        let avg_response_time = if total_req > 0 {
            total_response_time as f64 / total_req as f64
        } else {
            0.0
        };

        (success_rate, health_rate, timeouts, avg_response_time)
    }
}
