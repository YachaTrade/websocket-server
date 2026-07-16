pub mod json_rpc;
pub mod subscribe;
use json_rpc::{
    send_error_response, send_success_response, JsonRpcErrorCode, JsonRpcMethod, JsonRpcRequest,
};
use std::{collections::HashMap, net::SocketAddr, time::Duration};
use subscribe::{
    handle_chart_subscribe, handle_market_subscribe, handle_metrics_subscribe,
    handle_order_subscribe, handle_swap_subscribe,
};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        ConnectInfo, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};

use crate::error_log;
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tracing::info;

use futures_util::{stream::StreamExt, SinkExt};
use std::sync::Arc;

/// Create WebSocket router
pub fn router() -> Router {
    Router::new().route("/wss", get(ws_handler))
}

/// Subscription key - unique identifier for each subscription type
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum SubscriptionKey {
    Chart {
        token_id: String,
        interval: String,
        price_type: String,
    },
    Token {
        token_id: String,
    },
    Metrics {
        token_id: String,
    },
    Market {
        token_id: String,
    },
    Order {
        order_type: String,
    },
}

/// Per-connection subscription state management
struct ConnectionState {
    /// Track active subscriptions (subscription key -> JoinHandle)
    subscriptions: Arc<Mutex<HashMap<SubscriptionKey, JoinHandle<()>>>>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add or overwrite subscription
    async fn add_subscription(&self, key: SubscriptionKey, handle: JoinHandle<()>) {
        let mut subs = self.subscriptions.lock().await;

        // Terminate existing subscription if present
        if let Some(old_handle) = subs.remove(&key) {
            info!("Terminating existing subscription: {:?}", key);
            old_handle.abort();
        }

        // Register new subscription
        info!("Registering new subscription: {:?}", key);
        subs.insert(key, handle);
    }

    /// Remove specific subscription
    async fn remove_subscription(&self, key: &SubscriptionKey) -> bool {
        let mut subs = self.subscriptions.lock().await;
        if let Some(handle) = subs.remove(key) {
            info!("Removing subscription: {:?}", key);
            handle.abort();
            true
        } else {
            info!("Subscription not found: {:?}", key);
            false
        }
    }

    /// Cleanup all subscriptions
    async fn cleanup_all(&self) {
        let mut subs = self.subscriptions.lock().await;
        for (key, handle) in subs.drain() {
            info!("Cleaning up subscription: {:?}", key);
            handle.abort();
        }
    }
}

/// WebSocket upgrade handler
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, addr))
}

/// WebSocket connection handler - simplified version
pub async fn handle_socket(socket: WebSocket, addr: SocketAddr) {
    info!("New WebSocket connection: {}", addr);

    let (mut sender, mut receiver) = socket.split();

    let (tx, mut rx) = mpsc::channel::<Message>(1000);

    // 연결별 구독 상태 관리자 생성
    let connection_state = Arc::new(ConnectionState::new());

    // 메시지 전송 태스크
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Keepalive: 30초마다 WebSocket 프로토콜 레벨 Ping 프레임 송신
    // - 중간 프록시(Cloudflare, HAProxy, nginx 등)의 idle timeout 회피
    //   (예: Cloudflare Free/Pro 플랜은 WebSocket idle 100초 후 강제 종료)
    // - 클라이언트가 비정상 종료된 경우 Ping 송신 실패로 즉시 감지
    // - tx 채널을 통해 보내므로 send_task가 sender로 forward
    let tx_ping = tx.clone();
    let ping_task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        // tick이 누적된 경우 한꺼번에 폭주하지 않도록 Skip 모드 사용
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // interval()의 첫 tick은 즉시 발화하므로 한 번 소비 (연결 직후 즉시 ping 방지)
        ticker.tick().await;
        loop {
            ticker.tick().await;
            // 빈 payload의 Ping 프레임 송신
            // tokio-tungstenite/axum이 자동으로 Pong을 응답받아 keepalive 완료
            if tx_ping
                .send(Message::Ping(Default::default()))
                .await
                .is_err()
            {
                // 채널이 닫혔으면 연결 종료 상태 → 태스크도 종료
                break;
            }
        }
    });

    // 메시지 수신 및 처리 — 메시지 타입별로 명시적 분기
    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            // 텍스트 메시지: ping 명령어 또는 JSON-RPC 요청
            Message::Text(text) => {
                if !handle_text_message(&text, &tx, &connection_state).await {
                    // 송신 채널이 끊긴 경우 (send_task 종료) → 루프 종료
                    break;
                }
            }
            // 클라이언트가 Close 프레임 송신 → 정상 종료
            Message::Close(frame) => {
                info!("Client closed connection: {} ({:?})", addr, frame);
                break;
            }
            // 프로토콜 레벨 Ping: tokio-tungstenite/axum이 자동으로 Pong 응답
            // Pong: 서버가 보낸 keepalive ping에 대한 클라이언트 응답 (무시)
            Message::Ping(_) | Message::Pong(_) => {}
            // 바이너리 메시지는 현재 미지원 — 무시
            Message::Binary(_) => {}
        }
    }

    // 연결 종료 시 정리
    info!("WebSocket 연결 종료: {}", addr);

    // 모든 구독 정리
    connection_state.cleanup_all().await;

    // Keepalive ping 태스크 종료
    ping_task.abort();

    // 전송 태스크 종료
    send_task.abort();
}

/// 텍스트 메시지 처리: ping 명령어 또는 JSON-RPC 요청 디스패치
///
/// 반환값:
/// - `true`: 정상 처리. 호출자는 다음 메시지 수신 계속
/// - `false`: 송신 채널이 끊김 → 호출자는 루프를 break해야 함
async fn handle_text_message(
    text: &str,
    tx: &mpsc::Sender<Message>,
    connection_state: &Arc<ConnectionState>,
) -> bool {
    info!("메시지 수신: {}", text);

    // ping 명령어 처리 (애플리케이션 레벨 텍스트 핑)
    let trimmed = text.trim();
    if trimmed == "\\ping" || trimmed == "ping" {
        // 사용자에게 보이도록 텍스트 메시지로 응답
        if tx
            .send(Message::Text("pong".to_string().into()))
            .await
            .is_err()
        {
            return false;
        }
        return true;
    }

    // JSON-RPC 요청 파싱
    let request: JsonRpcRequest = match serde_json::from_str(text) {
        Ok(req) => req,
        Err(e) => {
            error_log!("JSON 파싱 실패: {}", e);
            let _ = send_error_response(tx, JsonRpcErrorCode::ParseError, "Invalid JSON").await;
            return true;
        }
    };

    // 메서드별 디스패치
    match request.method() {
        JsonRpcMethod::OrderSubscribe => {
            // 먼저 파라미터 파싱
            let order_type = request
                .params()
                .and_then(|p| p.get("order_type"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            match handle_order_subscribe(request, tx.clone()).await {
                Ok(handle) => {
                    // 구독 타입이 있으면 등록
                    if let Some(order_type) = order_type {
                        let key = SubscriptionKey::Order { order_type };
                        connection_state.add_subscription(key, handle).await;
                    }
                }
                Err(e) => {
                    error_log!("주문 구독 실패: {}", e);
                    let _ = send_error_response(
                        tx,
                        JsonRpcErrorCode::InternalError,
                        &e.to_string(),
                    )
                    .await;
                }
            }
        }

        JsonRpcMethod::SwapSubscribe => {
            // 먼저 파라미터 파싱
            let token_id = request
                .params()
                .and_then(|p| p.get("token_id").or_else(|| p.as_str().map(|_| p)))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            match handle_swap_subscribe(request, tx.clone()).await {
                Ok(handle) => {
                    // 토큰 ID가 있으면 등록
                    if let Some(token_id) = token_id {
                        let key = SubscriptionKey::Token { token_id };
                        connection_state.add_subscription(key, handle).await;
                    }
                }
                Err(e) => {
                    error_log!("토큰 구독 실패: {}", e);
                    let error_code = if e.to_string().contains("invalid token_id") {
                        JsonRpcErrorCode::InvalidRequest
                    } else {
                        JsonRpcErrorCode::InternalError
                    };
                    let _ = send_error_response(tx, error_code, &e.to_string()).await;
                }
            }
        }

        JsonRpcMethod::ChartSubscribe => {
            // 먼저 파라미터 파싱
            let token_id = request
                .params()
                .and_then(|p| p.get("token_id").or_else(|| p.as_str().map(|_| p)))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let interval = request
                .params()
                .and_then(|p| p.get("resolution"))
                .and_then(|v| v.as_str())
                .map(|s| {
                    if s == "60" {
                        "1H".to_string()
                    } else {
                        s.to_string()
                    }
                });
            // price_type 파싱 (기본값: "price")
            let price_type = request
                .params()
                .and_then(|p| p.get("price_type"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "price".to_string());

            match handle_chart_subscribe(request, tx.clone()).await {
                Ok(handle) => {
                    // 토큰 ID와 interval이 모두 있으면 등록
                    if let (Some(token_id), Some(interval)) = (token_id, interval) {
                        let key = SubscriptionKey::Chart {
                            token_id,
                            interval,
                            price_type,
                        };
                        connection_state.add_subscription(key, handle).await;
                    }
                }
                Err(e) => {
                    error_log!("차트 구독 실패: {}", e);
                    let error_code = if e.to_string().contains("invalid token_id") {
                        JsonRpcErrorCode::InvalidRequest
                    } else {
                        JsonRpcErrorCode::InternalError
                    };
                    let _ = send_error_response(tx, error_code, &e.to_string()).await;
                }
            }
        }

        JsonRpcMethod::MetricsSubscribe => {
            // 먼저 파라미터 파싱
            let token_id = request
                .params()
                .and_then(|p| p.get("token_id").or_else(|| p.as_str().map(|_| p)))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            match handle_metrics_subscribe(request, tx.clone()).await {
                Ok(handle) => {
                    // 토큰 ID가 있으면 등록
                    if let Some(token_id) = token_id {
                        let key = SubscriptionKey::Metrics { token_id };
                        connection_state.add_subscription(key, handle).await;
                    }
                }
                Err(e) => {
                    error_log!("메트릭스 구독 실패: {}", e);
                    let error_code = if e.to_string().contains("invalid token_id") {
                        JsonRpcErrorCode::InvalidRequest
                    } else {
                        JsonRpcErrorCode::InternalError
                    };
                    let _ = send_error_response(tx, error_code, &e.to_string()).await;
                }
            }
        }

        JsonRpcMethod::MarketSubscribe => {
            let token_id = request
                .params()
                .and_then(|p| p.get("token_id").or_else(|| p.as_str().map(|_| p)))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            match handle_market_subscribe(request, tx.clone()).await {
                Ok(handle) => {
                    // 토큰 ID가 있으면 등록
                    if let Some(token_id) = token_id {
                        let key = SubscriptionKey::Market { token_id };
                        connection_state.add_subscription(key, handle).await;
                    }
                }
                Err(e) => {
                    error_log!("마켓 구독 실패: {}", e);
                    let error_code = if e.to_string().contains("invalid token_id") {
                        JsonRpcErrorCode::InvalidRequest
                    } else {
                        JsonRpcErrorCode::InternalError
                    };
                    let _ = send_error_response(tx, error_code, &e.to_string()).await;
                }
            }
        }

        // Unsubscribe 메서드 처리
        JsonRpcMethod::OrderUnsubscribe => {
            // 파라미터에서 order_type 파싱
            let order_type = request
                .params()
                .and_then(|p| p.get("order_type"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if let Some(order_type) = order_type {
                let key = SubscriptionKey::Order { order_type };
                let removed = connection_state.remove_subscription(&key).await;

                if removed {
                    let _ = send_success_response(
                        tx,
                        request.method(),
                        serde_json::json!({
                            "message": "success"
                        }),
                    )
                    .await;
                } else {
                    let _ =
                        send_error_response(tx, JsonRpcErrorCode::NotFound, "notfound").await;
                }
            } else {
                let _ = send_error_response(
                    tx,
                    JsonRpcErrorCode::InvalidParams,
                    "invalid params",
                )
                .await;
            }
        }

        JsonRpcMethod::SwapUnsubscribe => {
            // 파라미터에서 token_id 파싱
            let token_id = request
                .params()
                .and_then(|p| p.get("token_id").or_else(|| p.as_str().map(|_| p)))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if let Some(token_id) = token_id {
                let key = SubscriptionKey::Token { token_id };
                let removed = connection_state.remove_subscription(&key).await;

                if removed {
                    let _ = send_success_response(
                        tx,
                        request.method(),
                        serde_json::json!({
                            "message": "success"
                        }),
                    )
                    .await;
                } else {
                    let _ =
                        send_error_response(tx, JsonRpcErrorCode::NotFound, "notfound").await;
                }
            } else {
                let _ = send_error_response(
                    tx,
                    JsonRpcErrorCode::InvalidParams,
                    "invalid params",
                )
                .await;
            }
        }

        JsonRpcMethod::ChartUnsubscribe => {
            // 파라미터에서 token_id와 interval, price_type 파싱
            let token_id = request
                .params()
                .and_then(|p| p.get("token_id").or_else(|| p.as_str().map(|_| p)))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let interval = request
                .params()
                .and_then(|p| p.get("resolution"))
                .and_then(|v| v.as_str())
                .map(|s| {
                    if s == "60" {
                        "1H".to_string()
                    } else {
                        s.to_string()
                    }
                });
            // price_type 파싱 (기본값: "price")
            let price_type = request
                .params()
                .and_then(|p| p.get("price_type"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "price".to_string());

            if let (Some(token_id), Some(interval)) = (token_id, interval) {
                let key = SubscriptionKey::Chart {
                    token_id,
                    interval,
                    price_type,
                };
                let removed = connection_state.remove_subscription(&key).await;

                if removed {
                    let _ = send_success_response(
                        tx,
                        request.method(),
                        serde_json::json!({
                            "message": "success"
                        }),
                    )
                    .await;
                } else {
                    let _ =
                        send_error_response(tx, JsonRpcErrorCode::NotFound, "notfound").await;
                }
            } else {
                let _ = send_error_response(
                    tx,
                    JsonRpcErrorCode::InvalidParams,
                    "invalid params",
                )
                .await;
            }
        }

        JsonRpcMethod::MetricsUnsubscribe => {
            // 파라미터에서 token_id 파싱
            let token_id = request
                .params()
                .and_then(|p| p.get("token_id").or_else(|| p.as_str().map(|_| p)))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if let Some(token_id) = token_id {
                let key = SubscriptionKey::Metrics { token_id };
                let removed = connection_state.remove_subscription(&key).await;

                if removed {
                    let _ = send_success_response(
                        tx,
                        request.method(),
                        serde_json::json!({
                            "message": "success"
                        }),
                    )
                    .await;
                } else {
                    let _ =
                        send_error_response(tx, JsonRpcErrorCode::NotFound, "notfound").await;
                }
            } else {
                let _ = send_error_response(
                    tx,
                    JsonRpcErrorCode::InvalidParams,
                    "invalid params",
                )
                .await;
            }
        }

        JsonRpcMethod::MarketUnsubscribe => {
            // 파라미터에서 token_id 파싱
            let token_id = request
                .params()
                .and_then(|p| p.get("token_id").or_else(|| p.as_str().map(|_| p)))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if let Some(token_id) = token_id {
                let key = SubscriptionKey::Market { token_id };
                let removed = connection_state.remove_subscription(&key).await;

                if removed {
                    let _ = send_success_response(
                        tx,
                        request.method(),
                        serde_json::json!({
                            "message": "success"
                        }),
                    )
                    .await;
                } else {
                    let _ =
                        send_error_response(tx, JsonRpcErrorCode::NotFound, "notfound").await;
                }
            } else {
                let _ = send_error_response(
                    tx,
                    JsonRpcErrorCode::InvalidParams,
                    "invalid params",
                )
                .await;
            }
        }
    }

    true
}
