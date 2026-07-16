use anyhow::Result;
use once_cell::sync::Lazy;
use tracing::warn;

pub mod db_metrics;
pub mod event_metrics;

pub mod monitor;
pub mod provider_metrics;
pub mod query;
pub mod server;
pub mod stream_metrics;
pub mod subscribe_metrics;

use db_metrics::DBMetrics;
use event_metrics::EventMetrics;
use provider_metrics::ProviderMetrics;
use stream_metrics::StreamMetrics;
use subscribe_metrics::SubscribeMetrics;

// Re-export event metrics types
pub use event_metrics::{
    monitored_broadcast_channel, monitored_channel, MonitoredBroadcastReceiver,
    MonitoredBroadcastSender, MonitoredReceiver, MonitoredSender,
};

/// 중앙 집중화된 메트릭 관리
pub struct Metrics {
    pub event: EventMetrics,
    pub provider: ProviderMetrics,
    pub db: DBMetrics,
    pub stream: StreamMetrics,
    pub subscribe: SubscribeMetrics,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            event: EventMetrics::new(),
            provider: ProviderMetrics::new(),
            db: DBMetrics::new(),
            stream: StreamMetrics::new(),
            subscribe: SubscribeMetrics::new(),
        }
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }
}

/// 전역 메트릭 인스턴스
pub static METRICS: Lazy<Metrics> = Lazy::new(Metrics::new);

pub use monitor::metrics_logging_task;
pub use server::metrics_handler;

// Re-export macros from query_metrics
pub use crate::{measure_postgres, measure_redis, measure_rpc};

pub async fn run_metrics_logging() -> Result<()> {
    match metrics_logging_task().await {
        Ok(()) => Ok(()),
        Err(err) => {
            warn!("[METRICS] logging task stopped: {err}");
            Err(err)
        }
    }
}
