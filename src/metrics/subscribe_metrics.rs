use std::sync::atomic::{AtomicU64, Ordering};

/// 구독 메트릭
pub struct SubscribeMetrics {
    pub token_subscriptions: AtomicU64,
    pub chart_subscriptions: AtomicU64,
    pub order_subscriptions: AtomicU64,
    pub newcontent_subscriptions: AtomicU64,
}

impl Default for SubscribeMetrics {
    fn default() -> Self {
        Self {
            token_subscriptions: AtomicU64::new(0),
            chart_subscriptions: AtomicU64::new(0),
            order_subscriptions: AtomicU64::new(0),
            newcontent_subscriptions: AtomicU64::new(0),
        }
    }
}

impl SubscribeMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    // 구독 증가 메서드들
    pub fn increment_token_subscription(&self) {
        self.token_subscriptions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_chart_subscription(&self) {
        self.chart_subscriptions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_order_subscription(&self) {
        self.order_subscriptions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_newcontent_subscription(&self) {
        self.newcontent_subscriptions
            .fetch_add(1, Ordering::Relaxed);
    }

    // 구독 해제 메서드들
    pub fn decrement_token_subscription(&self) {
        self.token_subscriptions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn decrement_chart_subscription(&self) {
        self.chart_subscriptions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn decrement_order_subscription(&self) {
        self.order_subscriptions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn decrement_newcontent_subscription(&self) {
        self.newcontent_subscriptions
            .fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get_values(&self) -> (u64, u64, u64, u64) {
        (
            self.token_subscriptions.load(Ordering::Relaxed),
            self.chart_subscriptions.load(Ordering::Relaxed),
            self.order_subscriptions.load(Ordering::Relaxed),
            self.newcontent_subscriptions.load(Ordering::Relaxed),
        )
    }
}
