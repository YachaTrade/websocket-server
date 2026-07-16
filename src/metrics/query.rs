/// PostgreSQL 전용 매크로 (기본 1000ms 타임아웃)
///
/// 이 매크로는 sqlx::Error를 anyhow::Error로 변환하면서도 원본 에러 타입을 보존합니다.
/// 호출하는 쪽에서 error.downcast_ref::<sqlx::Error>()를 사용하여
/// sqlx::Error::RowNotFound 등의 특정 에러를 패턴 매칭할 수 있습니다.
///
/// # 사용 예제
/// ```rust,ignore
/// let result = measure_postgres!("get_user",
///     sqlx::query("SELECT * FROM users WHERE id = $1")
///         .bind(user_id)
///         .fetch_one(&pool)
/// );
///
/// match result {
///     Ok(row) => { /* 성공 처리 */ },
///     Err(e) => {
///         // sqlx::Error::RowNotFound 체크
///         if let Some(sqlx_err) = e.downcast_ref::<sqlx::Error>() {
///             if matches!(sqlx_err, sqlx::Error::RowNotFound) {
///                 // 기본값 반환 또는 별도 처리
///                 return Ok(default_value);
///             }
///         }
///         // 다른 에러는 전파
///         return Err(e);
///     }
/// }
/// ```
#[macro_export]
macro_rules! measure_postgres {
    ($operation:expr, $query:expr) => {{
        let start_time = tokio::time::Instant::now();

        // 실제 timeout과 메트릭 수집을 함께 적용
        let result = tokio::time::timeout(std::time::Duration::from_millis(1000), $query).await;

        let elapsed = start_time.elapsed().as_millis() as u64;
        $crate::metrics::METRICS.db.record_postgres_query(elapsed);

        // tokio::timeout 결과를 anyhow로 매핑 (sqlx::Error는 원본 타입 보존)
        let query_result = result
            .map_err(|_| {
                // 실제 타임아웃 발생
                $crate::metrics::METRICS.db.increment_postgres_timeout();
                tracing::warn!(
                    "Database actual timeout - postgres {} (1000ms timeout)",
                    $operation
                );
                anyhow::anyhow!("Query timeout after 1000ms")
            })?
            .map_err(|e: sqlx::Error| {
                // sqlx::Error를 anyhow::Error로 변환
                // anyhow::Error::from()은 원본 에러를 보존하므로
                // 호출하는 쪽에서 downcast_ref()로 복구 가능
                anyhow::Error::from(e)
            })?;

        // 쿼리는 완료되었지만 임계값과 비교 - 임계치 이상일 때만 로깅
        if elapsed >= 1000 {
            tracing::warn!(
                "Database slow query - postgres {} ({}ms >= 1000ms threshold)",
                $operation,
                elapsed
            );
        }

        anyhow::Ok(query_result)
    }};
}

/// Redis 전용 매크로 (기본 300ms 타임아웃)
#[macro_export]
macro_rules! measure_redis {
    ($operation:expr, $query:expr) => {{
        let start_time = tokio::time::Instant::now();

        // 실제 timeout과 메트릭 수집을 함께 적용
        let result = tokio::time::timeout(std::time::Duration::from_millis(300), $query).await;

        let elapsed = start_time.elapsed().as_millis() as u64;
        $crate::metrics::METRICS.db.record_redis_query(elapsed);
        // 모든 쿼리의 실행 시간을 기록

        // tokio::timeout 결과를 anyhow로 매핑
        let query_result = result
            .map_err(|_| {
                // 실제 타임아웃 발생
                $crate::metrics::METRICS.db.increment_redis_timeout();
                tracing::warn!(
                    "Database actual timeout - redis {} (300ms timeout)",
                    $operation
                );
                anyhow::anyhow!("Query timeout after 300ms")
            })?
            .map_err(|e| anyhow::anyhow!("Database error: {}", e))?;

        // 쿼리는 완료되었지만 임계값과 비교 - 임계치 이상일 때만 로깅
        if elapsed >= 300 {
            tracing::warn!(
                "Database slow query - redis {} ({}ms >= 300ms threshold)",
                $operation,
                elapsed
            );
        }
        // 성공/실패 상관없이 응답시간 기록

        anyhow::Ok(query_result)
    }};
}

/// RPC 호출 성능 측정 매크로
#[macro_export]
macro_rules! measure_rpc {
    ($operation:expr, $rpc_call:expr) => {{
        let start_time = tokio::time::Instant::now();
        let result = tokio::time::timeout(std::time::Duration::from_millis(10000), $rpc_call).await;
        let elapsed = start_time.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;

        let rpc_result = result.map_err(|_| {
            // 타임아웃 - 실패와 타임아웃 모두 기록
            $crate::metrics::METRICS.provider.record_rpc_timeout();
            $crate::metrics::METRICS
                .provider
                .record_request_with_time(false, elapsed_ms);
            tracing::warn!("[METRICS] RPC timeout - {} ({}ms)", $operation, elapsed_ms);
            anyhow::anyhow!("RPC timeout after {}ms", elapsed_ms)
        })?;

        // 성공/실패 상관없이 응답시간 기록
        match &rpc_result {
            Ok(_) => {
                // 성공 - 응답시간과 함께 기록
                $crate::metrics::METRICS
                    .provider
                    .record_request_with_time(true, elapsed_ms);
                if elapsed_ms >= 2000 {
                    tracing::warn!("[METRICS] RPC slow - {} ({}ms)", $operation, elapsed_ms);
                }
            }
            Err(_) => {
                // 실패 - 응답시간과 함께 기록
                $crate::metrics::METRICS
                    .provider
                    .record_request_with_time(false, elapsed_ms);
            }
        }

        rpc_result
    }};
}
