use std::future::Future;
use std::time::Duration;

/// fallible async 작업을 지수 백오프로 재시도한다.
///
/// `op` 가 `Ok` 를 반환하면 즉시 종료한다.
/// `Err` 일 때 `on_retry(attempt, &err, delay)` 콜백을 호출하고
/// `delay` 만큼 sleep 한 뒤 다시 시도한다. 백오프는 매 회 2배가 된다.
/// 마지막 시도까지 실패하면 마지막 `Err` 를 그대로 반환한다.
///
/// 용도: V2 Graduate 처리에서 indexer 가 아직 PG `market` 행을 commit 안 한 짧은
/// 레이스를 흡수하기 위해 `get_market_info` 를 짧게 재시도.
pub async fn retry_async<F, Fut, T, E, R>(
    mut op: F,
    max_attempts: u32,
    initial_delay_ms: u64,
    mut on_retry: R,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    R: FnMut(u32, &E, Duration),
{
    debug_assert!(max_attempts >= 1, "max_attempts must be >= 1");
    let mut attempt: u32 = 1;
    let mut delay_ms = initial_delay_ms;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt >= max_attempts {
                    return Err(e);
                }
                let delay = Duration::from_millis(delay_ms);
                on_retry(attempt, &e, delay);
                tokio::time::sleep(delay).await;
                attempt += 1;
                delay_ms = delay_ms.saturating_mul(2);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn succeeds_on_first_try_without_retry() {
        let calls = Arc::new(AtomicU32::new(0));
        let retries = Arc::new(AtomicU32::new(0));
        let result: Result<i32, &str> = {
            let calls = calls.clone();
            let retries = retries.clone();
            retry_async(
                move || {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, &str>(42)
                    }
                },
                5,
                1,
                move |_, _, _| {
                    retries.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await
        };
        assert_eq!(result, Ok(42));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(retries.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn succeeds_after_transient_failures() {
        let calls = Arc::new(AtomicU32::new(0));
        let retries = Arc::new(AtomicU32::new(0));
        let result: Result<i32, &str> = {
            let calls = calls.clone();
            let retries = retries.clone();
            retry_async(
                move || {
                    let calls = calls.clone();
                    async move {
                        let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                        if n < 3 {
                            Err("transient")
                        } else {
                            Ok(42)
                        }
                    }
                },
                5,
                1,
                move |_, _, _| {
                    retries.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await
        };
        assert_eq!(result, Ok(42));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        // 두 번 실패 → 두 번 on_retry 호출, 세 번째에 성공
        assert_eq!(retries.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn returns_last_error_after_max_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let result: Result<i32, &str> = {
            let calls = calls.clone();
            retry_async(
                move || {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Err::<i32, &str>("nope")
                    }
                },
                3,
                1,
                |_, _, _| {},
            )
            .await
        };
        assert_eq!(result, Err("nope"));
        // max_attempts 만큼 정확히 호출돼야 한다 (재시도 = max_attempts - 1).
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn backoff_doubles_each_retry() {
        let observed: Arc<std::sync::Mutex<Vec<Duration>>> = Arc::new(Default::default());
        let _: Result<(), &str> = {
            let observed = observed.clone();
            retry_async(
                || async { Err::<(), &str>("err") },
                4,
                10, // 10ms → 20ms → 40ms
                move |_, _, delay| observed.lock().unwrap().push(delay),
            )
            .await
        };
        let durations = observed.lock().unwrap().clone();
        assert_eq!(
            durations,
            vec![
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(40),
            ]
        );
    }
}
