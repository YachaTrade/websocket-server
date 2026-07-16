use std::str::FromStr;

use crate::event::market::MARKET_EVENT_PRODUCER;
use crate::event::metrics::METRICS_EVENT_PRODUCER;
use crate::event::swap::SWAP_EVENT_PRODUCER;
use crate::types::order::TokenOrderType;
use crate::{error_log, metrics::METRICS};
use crate::{
    event::{
        chart::{ChartKey, CHART_EVENT_PRODUCER},
        order::ORDER_EVENT_PRODUCER,
    },
    types::chart::{ChartInterval, PriceType},
};
use anyhow::{Context, Result};
use axum::extract::ws::Message;
use serde_json::{json, Value};
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tracing::info;

use super::json_rpc::{send_success_response, JsonRpcRequest};

pub async fn handle_swap_subscribe(
    request: JsonRpcRequest,
    tx: Sender<Message>,
) -> Result<JoinHandle<()>> {
    let token_id = parse_token_id(request.params())
        .ok_or_else(|| anyhow::anyhow!("Invalid or missing token ID"))?;

    // token_id 존재 여부 체크
    let cache_manager = crate::db::cache::CacheManager::instance()?;
    if !cache_manager.check_white_list_token(&token_id).await? {
        return Err(anyhow::anyhow!("invalid token_id"));
    }

    // 토큰 구독 메트릭 기록
    METRICS.subscribe.increment_token_subscription();

    // TOKEN_EVENT_PRODUCER에서 이벤트 수신자 가져오기
    let mut receiver = SWAP_EVENT_PRODUCER
        .get()
        .expect("SWAP_EVENT_PRODUCER not initialized")
        .get_event_receiver(token_id.clone())
        .await
        .context("Failed to get token event receiver")?;

    let handle = tokio::spawn(async move {
        while let Ok(message) = receiver.recv().await {
            info!("New token message: {:?}", message);
            if let Err(e) = send_success_response(&tx, request.method(), json!(message)).await {
                error_log!("Failed to send token event: {}", e);
                break;
            }
        }
        // 토큰 구독 해제 메트릭 기록
        METRICS.subscribe.decrement_token_subscription();
        // 리시버 드롭
        drop(receiver);
    });

    Ok(handle)
}

pub async fn handle_order_subscribe(
    request: JsonRpcRequest,
    tx: Sender<Message>,
) -> Result<JoinHandle<()>> {
    let order_type =
        parse_order_type(request.params()).ok_or_else(|| anyhow::anyhow!("missing order type"))?;

    let order_type_enum = TokenOrderType::from_str(&order_type)
        .map_err(|err| anyhow::anyhow!("Invalid order type: {}", err))?;

    // 주문 구독 메트릭 기록
    METRICS.subscribe.increment_order_subscription();

    let mut receiver = ORDER_EVENT_PRODUCER
        .get()
        .expect("ORDER_EVENT_PRODUCER not initialized")
        .get_event_receiver(order_type_enum)
        .expect("Failed to get order receiver");

    let handle = tokio::spawn(async move {
        while let Ok(message) = receiver.recv().await {
            if let Err(e) = send_success_response(&tx, request.method(), json!(message)).await {
                error_log!("Failed to send order: {}", e);
                break;
            }
        }
        // 주문 구독 해제 메트릭 기록
        METRICS.subscribe.decrement_order_subscription();
        // Ensure receiver is dropped here
        drop(receiver);
    });
    Ok(handle)
}

pub async fn handle_chart_subscribe(
    request: JsonRpcRequest,
    tx: Sender<Message>,
) -> Result<JoinHandle<()>> {
    let token_id = parse_token_id(request.params())
        .ok_or_else(|| anyhow::anyhow!("Invalid or missing token ID"))?;

    // token_id 존재 여부 체크
    let cache_manager = crate::db::cache::CacheManager::instance()?;
    if !cache_manager.check_white_list_token(&token_id).await? {
        return Err(anyhow::anyhow!("invalid token_id"));
    }

    let interval_str = parse_chart(request.params())
        .ok_or_else(|| anyhow::anyhow!("Invalid or missing interval"))?;

    // interval 자동 변환
    let interval_str = match interval_str.as_str() {
        "60" => "1H".to_string(),
        "240" => "4H".to_string(),
        "1D" => "D".to_string(),
        "1W" => "W".to_string(),
        "1M" => "M".to_string(),
        other => other.to_string(),
    };

    // interval 타입 검증
    let _ = ChartInterval::try_from(interval_str.as_str()).map_err(|_| {
        anyhow::anyhow!(
            "Invalid interval type: {}. Valid types are: 1, 5, 15, 30, 60 (or 1H), 4H, D, W, M",
            interval_str
        )
    })?;

    // price_type 파싱 (기본값: Price)
    let price_type_str = parse_price_type(request.params()).unwrap_or_else(|| "price".to_string());
    let price_type = PriceType::try_from(price_type_str.as_str()).map_err(|_| {
        anyhow::anyhow!(
            "Invalid price type: {}. Valid types are: price, price_usd, market_cap, market_cap_usd",
            price_type_str
        )
    })?;

    // 차트 구독 메트릭 기록
    METRICS.subscribe.increment_chart_subscription();

    // ChartKey 생성
    let chart_key = ChartKey {
        token_id: token_id.clone(),
        interval: interval_str.clone(),
        price_type,
    };

    // CHART_EVENT_PRODUCER에서 이벤트 수신자 가져오기
    let mut receiver = CHART_EVENT_PRODUCER
        .get()
        .expect("CHART_EVENT_PRODUCER not initialized")
        .get_event_receiver(chart_key)
        .context("Failed to get chart event receiver")?;

    // receive-side 라우팅 검증용 expected 식별자 (페이로드 token_id/interval/k와 매칭)
    let expected_token_id = token_id.clone();
    let expected_interval = interval_str.clone();
    let expected_price_type = price_type.as_str();

    let handle = tokio::spawn(async move {
        while let Ok(message) = receiver.recv().await {
            // 라우팅 무결성 검증 — chart_key별로 broadcast 채널이 분리돼 있으므로
            // 정상 동작에선 항상 매칭된다. mismatch가 보이면 producer-side 라우팅
            // 버그(예: EventBufferKey cross-token 페어링)이므로 drop 후 경고.
            if message.token_id != expected_token_id
                || message.interval != expected_interval
                || message.k != expected_price_type
            {
                tracing::warn!(
                    "Chart routing mismatch — drop: expected ({}, {}, {}), got ({}, {}, {})",
                    expected_token_id,
                    expected_interval,
                    expected_price_type,
                    message.token_id,
                    message.interval,
                    message.k
                );
                continue;
            }

            if let Err(e) = send_success_response(&tx, request.method(), json!(message)).await {
                error_log!("Failed to send chart event: {}", e);
                break;
            }
        }
        // 차트 구독 해제 메트릭 기록
        METRICS.subscribe.decrement_chart_subscription();
        // 리시버 드롭
        drop(receiver);
    });

    Ok(handle)
}

fn parse_token_id(params: Option<&Value>) -> Option<String> {
    match params {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Object(obj)) => obj
            .get("token_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned()),
        _ => None,
    }
}

fn parse_chart(params: Option<&Value>) -> Option<String> {
    match params {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Object(obj)) => obj
            .get("resolution")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned()),
        _ => None,
    }
}

fn parse_order_type(params: Option<&Value>) -> Option<String> {
    match params {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Object(obj)) => obj
            .get("order_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned()),
        _ => None,
    }
}

fn parse_price_type(params: Option<&Value>) -> Option<String> {
    match params {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Object(obj)) => obj
            .get("price_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned()),
        _ => None,
    }
}

pub async fn handle_metrics_subscribe(
    request: JsonRpcRequest,
    tx: Sender<Message>,
) -> Result<JoinHandle<()>> {
    let token_id = parse_token_id(request.params())
        .ok_or_else(|| anyhow::anyhow!("Invalid or missing token ID"))?;

    // token_id 존재 여부 체크
    let cache_manager = crate::db::cache::CacheManager::instance()?;
    if !cache_manager.check_white_list_token(&token_id).await? {
        return Err(anyhow::anyhow!("invalid token_id"));
    }

    // Metrics 구독 메트릭 기록
    METRICS.subscribe.increment_token_subscription();

    // METRICS_EVENT_PRODUCER에서 이벤트 수신자 가져오기
    let mut receiver = METRICS_EVENT_PRODUCER
        .get()
        .expect("METRICS_EVENT_PRODUCER not initialized")
        .get_event_receiver(token_id.clone())
        .await
        .context("Failed to get metrics event receiver")?;

    let handle = tokio::spawn(async move {
        while let Ok(message) = receiver.recv().await {
            info!("New metrics message: {:?}", message);
            if let Err(e) = send_success_response(&tx, request.method(), json!(message)).await {
                error_log!("Failed to send metrics event: {}", e);
                break;
            }
        }
        // Metrics 구독 해제 메트릭 기록
        METRICS.subscribe.decrement_token_subscription();
        // 리시버 드롭
        drop(receiver);
    });

    Ok(handle)
}

pub async fn handle_market_subscribe(
    request: JsonRpcRequest,
    tx: Sender<Message>,
) -> Result<JoinHandle<()>> {
    let token_id = parse_token_id(request.params())
        .ok_or_else(|| anyhow::anyhow!("Invalid or missing token ID"))?;

    // token_id 존재 여부 체크
    let cache_manager = crate::db::cache::CacheManager::instance()?;
    if !cache_manager.check_white_list_token(&token_id).await? {
        return Err(anyhow::anyhow!("invalid token_id"));
    }

    // Market 구독 메트릭 기록
    METRICS.subscribe.increment_token_subscription();

    // MARKET_EVENT_PRODUCER에서 이벤트 수신자 가져오기
    let mut receiver = MARKET_EVENT_PRODUCER
        .get()
        .expect("MARKET_EVENT_PRODUCER not initialized")
        .get_event_receiver(token_id.clone())
        .await
        .context("Failed to get market event receiver")?;

    let handle = tokio::spawn(async move {
        while let Ok(message) = receiver.recv().await {
            info!("New market message: {:?}", message);
            if let Err(e) = send_success_response(&tx, request.method(), json!(message)).await {
                error_log!("Failed to send market event: {}", e);
                break;
            }
        }
        // Market 구독 해제 메트릭 기록
        METRICS.subscribe.decrement_token_subscription();
        // 리시버 드롭
        drop(receiver);
    });

    Ok(handle)
}
