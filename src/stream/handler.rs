use anyhow::Result;

use std::{future::Future, pin::Pin, time::Duration};

use crate::error_log;
use tracing::{info, warn};

use crate::types::stream::EventType;

/// JoinSet 태스크 결과 타입 (type_complexity 경고 해결용)
type TaskResult = Result<(usize, String), (usize, String, anyhow::Error)>;

pub trait EventHandler: Send + Sync + 'static {
    type Event: Send + 'static;

    fn stream_events(
        event_type: EventType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
}

// 재시도 설정 구조체
#[derive(Clone)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub initial_backoff_ms: u64,
    pub backoff_factor: f64,
    pub max_backoff_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff_ms: 1000, // 1초
            backoff_factor: 2.0,      // 지수 백오프
            max_backoff_ms: 60000,    // 최대 1분
        }
    }
}

pub async fn run_event_handler<H: EventHandler>(event_type: EventType) -> Result<()> {
    // 기본 재시도 설정 사용
    let retry_config = Some(RetryConfig::default());
    run_event_handler_with_retry::<H>(retry_config, event_type).await
}

pub async fn run_event_handler_with_retry<H: EventHandler>(
    retry_config: Option<RetryConfig>,
    event_type: EventType,
) -> Result<()> {
    let retry_config = retry_config.unwrap_or_default();
    let mut set = tokio::task::JoinSet::new();

    let handler_name = std::any::type_name::<H>()
        .split("::")
        .last()
        .unwrap_or("Unknown");
    info!(
        "🚀 Starting event handler '{}' with retry capability (max attempts: {})",
        handler_name, retry_config.max_attempts
    );

    // Stream task 시작 - 재시도 로직 포함
    spawn_task_with_retry::<H>(
        &mut set,
        event_type,
        retry_config.clone(),
        0,
        handler_name.to_string(),
    );

    // 모든 task가 종료될 때까지 기다림
    let mut first_error = None;
    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok((attempt, handler_name))) => {
                // 정상 종료 - 이 경우는 예상치 못한 상황
                warn!(
                    "⚠️ Task '{}' completed unexpectedly (attempt: {}) - this should not happen in normal operation",
                    handler_name, attempt
                );

                // 재시작 로직
                let next_attempt = attempt + 1;
                info!(
                    "🔄 Automatically restarting task '{}' (attempt: {}/{})",
                    handler_name, next_attempt, retry_config.max_attempts
                );

                spawn_task_with_retry::<H>(
                    &mut set,
                    event_type,
                    retry_config.clone(),
                    next_attempt,
                    handler_name,
                );
            }
            Ok(Err((attempt, handler_name, e))) => {
                error_log!(
                    "❌ Task '{}' failed (attempt: {}/{}): {}",
                    handler_name,
                    attempt + 1,
                    retry_config.max_attempts,
                    e
                );

                if let Some(source) = e.source() {
                    error_log!("   ↳ Error source: {}", source);
                    error_log!("   ↳ Error type: {}", std::any::type_name_of_val(&e));
                }

                let next_attempt = attempt + 1;
                if next_attempt <= retry_config.max_attempts {
                    // 백오프 시간 계산
                    let backoff_ms = calculate_backoff(attempt, &retry_config);
                    info!(
                        "🔄 Restarting task '{}' after {}ms delay (attempt: {}/{})",
                        handler_name, backoff_ms, next_attempt, retry_config.max_attempts
                    );

                    // 일정 시간 대기 후 재시작
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    info!(
                        "⏱️ Backoff period completed for task '{}', restarting now",
                        handler_name
                    );

                    spawn_task_with_retry::<H>(
                        &mut set,
                        event_type,
                        retry_config.clone(),
                        next_attempt,
                        handler_name,
                    );
                } else {
                    error_log!(
                        "❌❌ Task '{}' exceeded maximum retry attempts ({}) - giving up",
                        handler_name,
                        retry_config.max_attempts
                    );

                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
            Err(e) => {
                error_log!("💥 JoinSet error for handler '{}': {}", handler_name, e);
                if first_error.is_none() {
                    first_error = Some(anyhow::anyhow!(e));
                }
            }
        }
    }

    if let Some(error) = first_error {
        error_log!(
            "🛑 Handler '{}' stopped due to error: {}",
            handler_name,
            error
        );
        Err(error)
    } else {
        info!("✅ Handler '{}' completed successfully", handler_name);
        Ok(())
    }
}

fn calculate_backoff(attempt: usize, config: &RetryConfig) -> u64 {
    let backoff = config.initial_backoff_ms as f64 * config.backoff_factor.powi(attempt as i32);
    backoff.min(config.max_backoff_ms as f64) as u64
}

fn spawn_task_with_retry<H: EventHandler>(
    set: &mut tokio::task::JoinSet<TaskResult>,
    event_type: EventType,
    retry_config: RetryConfig,
    attempt: usize,
    handler_name: String,
) {
    set.spawn(async move {
        info!(
            "🏁 Task '{}' started (attempt: {}/{})",
            handler_name,
            attempt + 1,
            retry_config.max_attempts
        );

        match H::stream_events(event_type).await {
            Ok(()) => {
                info!("✅ Task '{}' completed normally", handler_name);
                Ok((attempt, handler_name))
            }
            Err(e) => {
                error_log!(
                    "❌ Error in task '{}' (attempt: {}/{}): {}",
                    handler_name,
                    attempt + 1,
                    retry_config.max_attempts,
                    e
                );
                Err((attempt, handler_name, e))
            }
        }
    });
}
