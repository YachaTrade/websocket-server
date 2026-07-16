use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// 스트림 상태 메트릭
#[derive(Debug)]
pub struct StreamMetrics {
    /// 마지막 curve 이벤트 수신 시간 (UNIX timestamp)
    last_curve_event_time: AtomicU64,
    /// 총 curve 이벤트 수
    curve_events_total: AtomicU64,
    /// 마지막 dex 이벤트 수신 시간 (UNIX timestamp)
    last_dex_event_time: AtomicU64,
    /// 총 dex 이벤트 수
    dex_events_total: AtomicU64,
    /// 스트림 시작 시간
    stream_start_time: AtomicU64,
}

impl StreamMetrics {
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            last_curve_event_time: AtomicU64::new(0),
            curve_events_total: AtomicU64::new(0),
            last_dex_event_time: AtomicU64::new(0),
            dex_events_total: AtomicU64::new(0),
            stream_start_time: AtomicU64::new(now),
        }
    }

    /// Curve 이벤트 수신 기록
    pub fn record_curve_event(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.last_curve_event_time.store(now, Ordering::Relaxed);
        self.curve_events_total.fetch_add(1, Ordering::Relaxed);
    }

    /// DEX 이벤트 수신 기록
    pub fn record_dex_event(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.last_dex_event_time.store(now, Ordering::Relaxed);
        self.dex_events_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Curve 스트림이 살아있는지 확인 (5분 이내 이벤트 있으면 healthy)
    pub fn is_curve_stream_healthy(&self) -> bool {
        let last_event = self.last_curve_event_time.load(Ordering::Relaxed);
        if last_event == 0 {
            // 아직 이벤트가 한 번도 없었다면 시작 후 5분은 기다려줌
            let start_time = self.stream_start_time.load(Ordering::Relaxed);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            return (now - start_time) < 300; // 시작 후 5분 이내
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        (now - last_event) < 300 // 5분 이내에 이벤트가 있었는지
    }

    /// DEX 스트림이 살아있는지 확인 (5분 이내 이벤트 있으면 healthy)
    pub fn is_dex_stream_healthy(&self) -> bool {
        let last_event = self.last_dex_event_time.load(Ordering::Relaxed);
        if last_event == 0 {
            // 아직 이벤트가 한 번도 없었다면 시작 후 5분은 기다려줌
            let start_time = self.stream_start_time.load(Ordering::Relaxed);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            return (now - start_time) < 300; // 시작 후 5분 이내
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        (now - last_event) < 300 // 5분 이내에 이벤트가 있었는지
    }

    /// 마지막 Curve 이벤트로부터 경과 시간 (초)
    pub fn seconds_since_last_curve_event(&self) -> u64 {
        let last_event = self.last_curve_event_time.load(Ordering::Relaxed);
        if last_event == 0 {
            return u64::MAX; // 아직 이벤트 없음
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(last_event)
    }

    /// 마지막 DEX 이벤트로부터 경과 시간 (초)
    pub fn seconds_since_last_dex_event(&self) -> u64 {
        let last_event = self.last_dex_event_time.load(Ordering::Relaxed);
        if last_event == 0 {
            return u64::MAX; // 아직 이벤트 없음
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(last_event)
    }

    /// Curve 스트림 메트릭 값들 반환: (is_healthy, total_events, seconds_since_last_event)
    pub fn get_curve_values(&self) -> (bool, u64, u64) {
        let is_healthy = self.is_curve_stream_healthy();
        let total_events = self.curve_events_total.load(Ordering::Relaxed);
        let seconds_since = self.seconds_since_last_curve_event();

        (is_healthy, total_events, seconds_since)
    }

    /// DEX 스트림 메트릭 값들 반환: (is_healthy, total_events, seconds_since_last_event)
    pub fn get_dex_values(&self) -> (bool, u64, u64) {
        let is_healthy = self.is_dex_stream_healthy();
        let total_events = self.dex_events_total.load(Ordering::Relaxed);
        let seconds_since = self.seconds_since_last_dex_event();

        (is_healthy, total_events, seconds_since)
    }

    /// 전체 스트림 메트릭 값들 반환 (기존 API 호환성): (curve_healthy, curve_events, curve_seconds)
    pub fn get_values(&self) -> (bool, u64, u64) {
        self.get_curve_values()
    }
}

impl Default for StreamMetrics {
    fn default() -> Self {
        Self::new()
    }
}
