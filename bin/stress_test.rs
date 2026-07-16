/// WebSocket 서버 스트레스 테스트 도구
///
/// 이 도구는 WebSocket 서버에 대해 다음과 같은 스트레스 테스트를 수행합니다:
/// 1. 제공된 토큰 주소 목록 사용
/// 2. 각 메서드(swap, chart, order)에 대해 다수의 WebSocket 연결 생성
/// 3. 데이터 수신 메트릭 수집 및 출력
use anyhow::{Context, Result};
use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// 스트레스 테스트 설정
#[derive(Debug, Clone)]
struct Config {
    /// WebSocket 서버 URL
    server_url: String,
    /// 테스트할 토큰 주소 목록
    token_addresses: Vec<String>,
    /// 각 메서드당 WebSocket 연결 수
    stress_count: usize,
}

/// JSON-RPC 요청 구조체
#[derive(Debug, Serialize)]
struct JsonRpcRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    jsonrpc: Option<String>,
    method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<serde_json::Value>,
}

impl JsonRpcRequest {
    fn new(method: String, params: Option<serde_json::Value>) -> Self {
        Self {
            jsonrpc: Some("2.0".to_string()),
            method,
            params,
        }
    }
}

/// JSON-RPC 응답 구조체
#[derive(Debug, Deserialize)]
struct JsonRpcResponse {
    #[allow(dead_code)]
    jsonrpc: String,
    #[allow(dead_code)]
    id: Option<u64>,
    #[allow(dead_code)]
    result: Option<serde_json::Value>,
    #[allow(dead_code)]
    error: Option<serde_json::Value>,
}

/// 메트릭 수집 구조체
#[derive(Debug)]
struct Metrics {
    /// 각 메서드별 메시지 수신 횟수
    message_counts: Arc<RwLock<HashMap<String, AtomicU64>>>,
    /// 각 메서드별 연결 성공 횟수
    connection_success: Arc<RwLock<HashMap<String, AtomicU64>>>,
    /// 각 메서드별 연결 실패 횟수
    connection_failure: Arc<RwLock<HashMap<String, AtomicU64>>>,
    /// 테스트 시작 시간
    start_time: Instant,
}

impl Metrics {
    fn new() -> Self {
        Self {
            message_counts: Arc::new(RwLock::new(HashMap::new())),
            connection_success: Arc::new(RwLock::new(HashMap::new())),
            connection_failure: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
        }
    }

    /// 메시지 수신 카운트 증가
    async fn increment_message(&self, method: &str) {
        let counts = self.message_counts.read().await;
        if let Some(counter) = counts.get(method) {
            counter.fetch_add(1, Ordering::Relaxed);
        } else {
            drop(counts);
            let mut counts = self.message_counts.write().await;
            counts.insert(method.to_string(), AtomicU64::new(1));
        }
    }

    /// 연결 성공 카운트 증가
    async fn increment_connection_success(&self, method: &str) {
        let counts = self.connection_success.read().await;
        if let Some(counter) = counts.get(method) {
            counter.fetch_add(1, Ordering::Relaxed);
        } else {
            drop(counts);
            let mut counts = self.connection_success.write().await;
            counts.insert(method.to_string(), AtomicU64::new(1));
        }
    }

    /// 연결 실패 카운트 증가
    async fn increment_connection_failure(&self, method: &str) {
        let counts = self.connection_failure.read().await;
        if let Some(counter) = counts.get(method) {
            counter.fetch_add(1, Ordering::Relaxed);
        } else {
            drop(counts);
            let mut counts = self.connection_failure.write().await;
            counts.insert(method.to_string(), AtomicU64::new(1));
        }
    }

    /// 현재 메트릭 출력
    async fn print_stats(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();

        println!("\n==================== Stress Test Metrics ====================");
        println!("Elapsed Time: {:.2}s", elapsed);

        println!("\n--- Connection Stats ---");
        let success = self.connection_success.read().await;
        let failure = self.connection_failure.read().await;

        let mut methods: Vec<String> = success.keys().chain(failure.keys()).cloned().collect();
        methods.sort();
        methods.dedup();

        for method in &methods {
            let success_count = success
                .get(method)
                .map(|c| c.load(Ordering::Relaxed))
                .unwrap_or(0);
            let failure_count = failure
                .get(method)
                .map(|c| c.load(Ordering::Relaxed))
                .unwrap_or(0);
            println!(
                "  {}: Success={}, Failure={}",
                method, success_count, failure_count
            );
        }

        println!("\n--- Message Stats ---");
        let counts = self.message_counts.read().await;

        let mut total_messages = 0u64;
        for method in &methods {
            let count = counts
                .get(method)
                .map(|c| c.load(Ordering::Relaxed))
                .unwrap_or(0);
            total_messages += count;
            let rate = count as f64 / elapsed;
            println!("  {}: {} messages ({:.2} msg/s)", method, count, rate);
        }

        println!("\n--- Overall ---");
        println!("  Total Messages: {}", total_messages);
        println!(
            "  Overall Rate: {:.2} msg/s",
            total_messages as f64 / elapsed
        );
        println!("==============================================================\n");
    }
}

/// swap 메서드 구독
async fn subscribe_swap(
    server_url: &str,
    token_id: &str,
    metrics: Arc<Metrics>,
    instance_id: usize,
) -> Result<()> {
    let method = format!("swap_{}", instance_id);

    // WebSocket 연결
    let (ws_stream, _) = match connect_async(server_url).await {
        Ok(result) => {
            metrics.increment_connection_success(&method).await;
            result
        }
        Err(e) => {
            metrics.increment_connection_failure(&method).await;
            return Err(anyhow::anyhow!("Failed to connect: {}", e));
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // subscribe 요청 전송
    let request = JsonRpcRequest::new(
        "swap_subscribe".to_string(),
        Some(json!({ "token_id": token_id })),
    );

    let msg = Message::Text(serde_json::to_string(&request)?);
    write.send(msg).await?;

    // 첫 번째 응답 대기 (성공/실패 확인)
    if let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                if let Ok(response) = serde_json::from_str::<JsonRpcResponse>(&text) {
                    if response.error.is_some() {
                        eprintln!(
                            "swap_subscribe error response for token {}: {:?}",
                            token_id, response.error
                        );
                        return Err(anyhow::anyhow!(
                            "Server returned error: {:?}",
                            response.error
                        ));
                    }
                }
            }
            Ok(Message::Close(_)) => {
                eprintln!(
                    "swap_subscribe connection closed immediately for token {}",
                    token_id
                );
                return Err(anyhow::anyhow!("Connection closed"));
            }
            Err(e) => {
                eprintln!(
                    "swap_subscribe error reading response for token {}: {}",
                    token_id, e
                );
                return Err(anyhow::anyhow!("Error reading response: {}", e));
            }
            _ => {}
        }
    }

    // 메시지 수신 루프
    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            if let Ok(Message::Text(_text)) = msg {
                metrics.increment_message("swap").await;
            }
        }
    });

    Ok(())
}

/// chart 메서드 구독
async fn subscribe_chart(
    server_url: &str,
    token_id: &str,
    resolution: &str,
    metrics: Arc<Metrics>,
    instance_id: usize,
) -> Result<()> {
    let method = format!("chart_{}", instance_id);

    // WebSocket 연결
    let (ws_stream, _) = match connect_async(server_url).await {
        Ok(result) => {
            metrics.increment_connection_success(&method).await;
            result
        }
        Err(e) => {
            metrics.increment_connection_failure(&method).await;
            return Err(anyhow::anyhow!("Failed to connect: {}", e));
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // subscribe 요청 전송
    let request = JsonRpcRequest::new(
        "chart_subscribe".to_string(),
        Some(json!({
            "token_id": token_id,
            "resolution": resolution,
        })),
    );

    let msg = Message::Text(serde_json::to_string(&request)?);
    write.send(msg).await?;

    // 첫 번째 응답 대기 (성공/실패 확인)
    if let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                if let Ok(response) = serde_json::from_str::<JsonRpcResponse>(&text) {
                    if response.error.is_some() {
                        eprintln!(
                            "chart_subscribe error response for token {} ({}): {:?}",
                            token_id, resolution, response.error
                        );
                        return Err(anyhow::anyhow!(
                            "Server returned error: {:?}",
                            response.error
                        ));
                    }
                }
            }
            Ok(Message::Close(_)) => {
                eprintln!(
                    "chart_subscribe connection closed immediately for token {} ({})",
                    token_id, resolution
                );
                return Err(anyhow::anyhow!("Connection closed"));
            }
            Err(e) => {
                eprintln!(
                    "chart_subscribe error reading response for token {} ({}): {}",
                    token_id, resolution, e
                );
                return Err(anyhow::anyhow!("Error reading response: {}", e));
            }
            _ => {}
        }
    }

    // 메시지 수신 루프
    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            if let Ok(Message::Text(_text)) = msg {
                metrics.increment_message("chart").await;
            }
        }
    });

    Ok(())
}

/// order 메서드 구독
async fn subscribe_order(
    server_url: &str,
    order_type: &str,
    metrics: Arc<Metrics>,
    instance_id: usize,
) -> Result<()> {
    let method = format!("order_{}_{}", order_type, instance_id);
    let order_type_owned = order_type.to_string();

    // WebSocket 연결
    let (ws_stream, _) = match connect_async(server_url).await {
        Ok(result) => {
            metrics.increment_connection_success(&method).await;
            result
        }
        Err(e) => {
            metrics.increment_connection_failure(&method).await;
            return Err(anyhow::anyhow!("Failed to connect: {}", e));
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // subscribe 요청 전송
    let request = JsonRpcRequest::new(
        "order_subscribe".to_string(),
        Some(json!({ "order_type": order_type })),
    );

    let msg = Message::Text(serde_json::to_string(&request)?);
    write.send(msg).await?;

    // 메시지 수신 루프
    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            if let Ok(Message::Text(_text)) = msg {
                metrics
                    .increment_message(&format!("order_{}", order_type_owned))
                    .await;
            }
        }
    });

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // .env 로드
    dotenv().ok();

    // 설정 로드
    let server_url = std::env::var("STRESS_TEST_SERVER_URL")
        .unwrap_or_else(|_| "ws://localhost:8001/ws".to_string());

    let stress_count = std::env::var("STRESS_TEST_STRESS_COUNT")
        .unwrap_or_else(|_| "5".to_string())
        .parse()
        .context("Failed to parse STRESS_TEST_STRESS_COUNT")?;

    // 환경 변수에서 토큰 주소 목록 읽기 (쉼표로 구분)
    let token_addresses_str = std::env::var("STRESS_TEST_TOKEN_ADDRESSES")
        .context("STRESS_TEST_TOKEN_ADDRESSES must be set in .env file")?;

    let token_addresses: Vec<String> = token_addresses_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if token_addresses.is_empty() {
        println!("No token addresses provided in STRESS_TEST_TOKEN_ADDRESSES.");
        println!(
            "Please set STRESS_TEST_TOKEN_ADDRESSES in .env file with comma-separated addresses."
        );
        return Ok(());
    }

    println!("==================== Stress Test Configuration ====================");
    println!("Server URL: {}", server_url);
    println!("Token Count: {}", token_addresses.len());
    println!("Stress Count (per method): {}", stress_count);
    println!("====================================================================\n");

    // 토큰 목록 출력
    println!("Token addresses to test:");
    for (idx, token_addr) in token_addresses.iter().enumerate() {
        println!("  {}. {}", idx + 1, token_addr);
    }
    println!();

    // Config 생성
    let config = Config {
        server_url,
        token_addresses,
        stress_count,
    };

    // 메트릭 초기화
    let metrics = Arc::new(Metrics::new());

    // 연결 태스크 수집
    let mut handles = Vec::new();

    println!("Starting stress test...\n");

    // 1. swap 구독 (각 토큰별로 STRESS_COUNT개 연결) - 병렬 처리
    println!("Creating swap subscriptions...");
    for token_id in &config.token_addresses {
        for i in 0..config.stress_count {
            let server_url = config.server_url.clone();
            let token_id = token_id.clone();
            let metrics_clone = metrics.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = subscribe_swap(&server_url, &token_id, metrics_clone, i).await {
                    eprintln!(
                        "swap subscription error (token={}, instance={}): {}",
                        token_id, i, e
                    );
                }
            });
            handles.push(handle);
        }
    }

    // 2. chart 구독 (각 토큰별로 resolution "1"에 대해 STRESS_COUNT개 연결) - 병렬 처리
    println!("Creating chart subscriptions (resolution: 1)...");
    let resolution = "1";
    for token_id in &config.token_addresses {
        for i in 0..config.stress_count {
            let server_url = config.server_url.clone();
            let token_id = token_id.clone();
            let resolution = resolution.to_string();
            let metrics_clone = metrics.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) =
                    subscribe_chart(&server_url, &token_id, &resolution, metrics_clone, i).await
                {
                    eprintln!(
                        "chart subscription error (token={}, resolution={}, instance={}): {}",
                        token_id, resolution, i, e
                    );
                }
            });
            handles.push(handle);
        }
    }

    // 3. new_event 구독 제거됨

    // 4. order 구독 (각 order_type별로 STRESS_COUNT개 연결) - 병렬 처리
    println!("Creating order subscriptions...");
    let order_types = vec!["creation_time", "latest_trade"];
    for order_type in &order_types {
        for i in 0..config.stress_count {
            let server_url = config.server_url.clone();
            let order_type = order_type.to_string();
            let metrics_clone = metrics.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = subscribe_order(&server_url, &order_type, metrics_clone, i).await {
                    eprintln!(
                        "order subscription error (type={}, instance={}): {}",
                        order_type, i, e
                    );
                }
            });
            handles.push(handle);
        }
    }

    println!(
        "\nAll subscription tasks spawned! Total tasks: {}\n",
        handles.len()
    );
    println!("Waiting for connections to establish...\n");

    // 잠시 대기하여 모든 연결이 성공적으로 이루어지도록 함
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 주기적으로 메트릭 출력 (10초마다)
    let metrics_clone = metrics.clone();
    let stats_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            metrics_clone.print_stats().await;
        }
    });

    // Ctrl+C 핸들러
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("\n\nReceived Ctrl+C, shutting down...\n");
        }
    }

    // 최종 메트릭 출력
    metrics.print_stats().await;

    // 통계 출력 태스크 종료
    stats_handle.abort();

    println!("Stress test completed!");

    Ok(())
}
