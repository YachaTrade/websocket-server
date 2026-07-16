use super::METRICS;
use axum::{http::StatusCode, response::Response};

/// Prometheus metrics handler
pub async fn metrics_handler() -> Result<Response<String>, StatusCode> {
    let mut output = String::new();

    // 1. 이벤트 채널 메트릭 (분리됨)
    let (
        event_total,
        event_healthy,
        event_sent_success,
        event_sent_failed_full,
        event_sent_failed_closed,
        event_received,
    ) = METRICS.event.get_event_values();
    output.push_str(&format!("event_channels_total {}\n", event_total));
    output.push_str(&format!("event_channels_healthy {}\n", event_healthy));
    output.push_str(&format!(
        "event_channels_sent_success_total {}\n",
        event_sent_success
    ));
    output.push_str(&format!(
        "event_channels_sent_failed_full_total {}\n",
        event_sent_failed_full
    ));
    output.push_str(&format!(
        "event_channels_sent_failed_closed_total {}\n",
        event_sent_failed_closed
    ));
    output.push_str(&format!(
        "event_channels_received_total {}\n",
        event_received
    ));

    // 1-2. 브로드캐스트 채널 메트릭 (단순 카운터)
    let (broadcast_sent_success, broadcast_sent_failed, broadcast_received) =
        METRICS.event.get_broadcast_values();
    output.push_str(&format!(
        "broadcast_messages_sent_success_total {}\n",
        broadcast_sent_success
    ));
    output.push_str(&format!(
        "broadcast_messages_sent_failed_total {}\n",
        broadcast_sent_failed
    ));
    output.push_str(&format!(
        "broadcast_messages_received_total {}\n",
        broadcast_received
    ));

    // 1-3. 스트림 상태 메트릭
    let (curve_healthy, curve_events, curve_seconds) = METRICS.stream.get_curve_values();
    let (dex_healthy, dex_events, dex_seconds) = METRICS.stream.get_dex_values();

    output.push_str(&format!(
        "stream_curve_alive {}\n",
        if curve_healthy { 1 } else { 0 }
    ));
    output.push_str(&format!("stream_curve_events_total {}\n", curve_events));
    output.push_str(&format!(
        "stream_curve_last_event_seconds_ago {}\n",
        if curve_seconds == u64::MAX {
            0
        } else {
            curve_seconds
        }
    ));

    output.push_str(&format!(
        "stream_dex_alive {}\n",
        if dex_healthy { 1 } else { 0 }
    ));
    output.push_str(&format!("stream_dex_events_total {}\n", dex_events));
    output.push_str(&format!(
        "stream_dex_last_event_seconds_ago {}\n",
        if dex_seconds == u64::MAX {
            0
        } else {
            dex_seconds
        }
    ));

    // 2. 프로바이더 메트릭
    let (success_rate, health_rate, rpc_timeouts, avg_response_time) =
        METRICS.provider.get_values();
    output.push_str(&format!(
        "provider_success_rate_percent {:.2}\n",
        success_rate
    ));
    output.push_str(&format!(
        "provider_health_rate_percent {:.2}\n",
        health_rate
    ));
    output.push_str(&format!("provider_rpc_timeouts_total {}\n", rpc_timeouts));
    output.push_str(&format!(
        "provider_avg_response_time_ms {:.2}\n",
        avg_response_time
    ));

    // 3. DB 메트릭
    let (pg_timeouts, redis_timeouts, pg_avg_time, redis_avg_time) = METRICS.db.get_values();
    output.push_str(&format!("db_postgres_timeouts_total {}\n", pg_timeouts));
    output.push_str(&format!("db_redis_timeouts_total {}\n", redis_timeouts));
    output.push_str(&format!(
        "db_postgres_avg_response_time_ms {:.2}\n",
        pg_avg_time
    ));
    output.push_str(&format!(
        "db_redis_avg_response_time_ms {:.2}\n",
        redis_avg_time
    ));

    // 4. 구독 메트릭
    let (token, chart, order, newcontent) = METRICS.subscribe.get_values();
    output.push_str(&format!("token_subscriptions {}\n", token));
    output.push_str(&format!("chart_subscriptions {}\n", chart));
    output.push_str(&format!("order_subscriptions {}\n", order));
    output.push_str(&format!("newcontent_subscriptions {}\n", newcontent));

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        .body(output)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}
