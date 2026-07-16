use std::time::Duration;
use tokio::time::interval;
use tracing::info;

use super::METRICS;
use crate::config::METRICS_REPORT_INTERVAL;

pub async fn metrics_logging_task() -> anyhow::Result<()> {
    let interval_ms = *METRICS_REPORT_INTERVAL;
    info!("[METRICS] logging started with {}ms interval", interval_ms);

    let mut ticker = interval(Duration::from_millis(interval_ms));

    loop {
        ticker.tick().await;
        log_metrics_snapshot().await;
    }
}

async fn log_metrics_snapshot() {
    info!("[METRICS] === METRICS REPORT ===");

    // 1. 이벤트 채널 메트릭 (분리)
    let (
        event_total,
        event_healthy,
        event_sent_success,
        event_sent_failed_full,
        event_sent_failed_closed,
        event_received,
    ) = METRICS.event.get_event_values();
    info!(
        "[METRICS] 📡 Event Channels: Total {}, Healthy {}, Sent Success {}, Failed Full {}, Failed Closed {}, Received {}",
        event_total, event_healthy, event_sent_success, event_sent_failed_full, event_sent_failed_closed, event_received
    );

    // 1-1. 이벤트 채널 상세 정보 (-Curve, -DEX 등)
    let event_details = METRICS.event.get_event_channel_details();
    for (name, healthy, sent, received) in event_details {
        let status = if healthy { "✅" } else { "❌" };
        info!(
            "[METRICS] 📡   └─ {}: {} Sent:{} Received:{}",
            name, status, sent, received
        );
    }

    // 2. 브로드캐스트 채널 메트릭 (단순 카운터)
    let (broadcast_sent_success, broadcast_sent_failed, broadcast_received) =
        METRICS.event.get_broadcast_values();
    if broadcast_sent_success > 0 || broadcast_received > 0 {
        let status = if broadcast_sent_success >= broadcast_received {
            "⚠️"
        } else {
            "✅"
        };
        info!(
            "[METRICS] 📺 Broadcast Messages: {} Sent Success {}, Failed {}, Received {}",
            status, broadcast_sent_success, broadcast_sent_failed, broadcast_received
        );
    }

    // 3. 스트림 상태 메트릭
    let (curve_healthy, curve_events, curve_seconds) = METRICS.stream.get_curve_values();
    let (dex_healthy, dex_events, dex_seconds) = METRICS.stream.get_dex_values();

    // Curve Stream
    let curve_status = if curve_healthy { "✅" } else { "❌" };
    if curve_seconds == u64::MAX {
        info!(
            "[METRICS] 🌊 Curve Stream: {} No events yet, Total Events: {}",
            curve_status, curve_events
        );
    } else {
        info!(
            "[METRICS] 🌊 Curve Stream: {} Last event {}s ago, Total Events: {}",
            curve_status, curve_seconds, curve_events
        );
    }

    // DEX Stream
    let dex_status = if dex_healthy { "✅" } else { "❌" };
    if dex_seconds == u64::MAX {
        info!(
            "[METRICS] 🏢 DEX Stream: {} No events yet, Total Events: {}",
            dex_status, dex_events
        );
    } else {
        info!(
            "[METRICS] 🏢 DEX Stream: {} Last event {}s ago, Total Events: {}",
            dex_status, dex_seconds, dex_events
        );
    }

    // 4. 프로바이더 메트릭
    let (success_rate, health_rate, rpc_timeouts, avg_response_time) =
        METRICS.provider.get_values();
    info!("[METRICS] 🌐 Providers: Success {:.1}%, Health {:.1}%, RPC Timeouts {}, Avg Response {:.1}ms", 
          success_rate, health_rate, rpc_timeouts, avg_response_time);

    // 5. DB 메트릭
    let (pg_timeouts, redis_timeouts, pg_avg_time, redis_avg_time) = METRICS.db.get_values();
    info!(
        "[METRICS] 🗄️ Database: PostgreSQL Timeouts {}, Redis Timeouts {}",
        pg_timeouts, redis_timeouts
    );
    info!("[METRICS] 🗄️ Database: PostgreSQL Avg Response Time {:.1}ms, Redis Avg Response Time {:.1}ms", 
          pg_avg_time, redis_avg_time);

    // 6. 구독 메트릭
    let (token, chart, order, newcontent) = METRICS.subscribe.get_values();
    info!(
        "[METRICS] 👥 Subscriptions: Token {}, Chart {}, Order {}, NewContent {}",
        token, chart, order, newcontent
    );
}
