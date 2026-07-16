use once_cell::sync::OnceCell;
use sqlx::postgres::PgPoolOptions;
use std::{env, str::FromStr, sync::Arc, time::Duration};

use crate::error_log;
use anyhow::Result;
use tokio::time::interval;
use tracing::{info, warn};

use crate::config::{
    PG_ACQUIRE_TIMEOUT, PG_IDLE_TIMEOUT, PG_MAX_CONNECTIONS, PG_MAX_LIFETIME, PG_MIN_CONNECTIONS,
};
static POSTGRES_DB: OnceCell<Arc<PostgresDatabase>> = OnceCell::new();

#[derive(Debug)]
pub struct PostgresDatabase {
    pub pool: sqlx::Pool<sqlx::Postgres>,
}
/*  sqlx::query: 구조체로 매핑할 필요 없이 쿼리를 실행할 때 사용
•	sqlx::query_as!: 쿼리 결과를 구조체로 매핑할 때 사용
•	sqlx::query!: 결과를 튜플로 가져오거나, 단순히 쿼리를 실행할 때 사용
*/
impl PostgresDatabase {
    // 글로벌 인스턴스 초기화
    pub async fn init() -> Result<(), sqlx::Error> {
        if POSTGRES_DB.get().is_some() {
            info!("PostgresDatabase already initialized");
            return Ok(());
        }

        let instance = Self::new().await;
        let arc_instance = Arc::new(instance);

        if POSTGRES_DB.set(arc_instance).is_err() {
            info!("PostgresDatabase was initialized by another task");
        } else {
            info!("PostgresDatabase global instance initialized successfully");
        }

        Ok(())
    }

    // 글로벌 인스턴스 가져오기
    pub fn instance() -> Result<Arc<PostgresDatabase>> {
        POSTGRES_DB.get().map(Arc::clone).ok_or_else(|| {
            anyhow::anyhow!("PostgresDatabase not initialized. Call PostgresDatabase::init() first")
        })
    }

    pub async fn new() -> Self {
        let pool = sqlx_connect().await;
        // Spawn background task to log detailed pool metrics periodically
        {
            let pool_clone = pool.clone();
            tokio::spawn(async move {
                let mut interval = interval(Duration::from_secs(60));
                loop {
                    interval.tick().await;

                    // 기본 풀 메트릭 수집
                    let size = pool_clone.size();
                    let idle = pool_clone.num_idle();
                    let acquired = size - idle as u32;

                    // pg_stat_activity 쿼리를 통한 상세 메트릭 수집
                    if let Ok(mut conn) = pool_clone.acquire().await {
                        let result = sqlx::query!(
                            r#"
                            SELECT 
                                count(*) as "total_connections!",
                                count(*) FILTER (WHERE state = 'active') as "active_connections!",
                                count(*) FILTER (WHERE state = 'idle') as "idle_connections!",
                                count(*) FILTER (WHERE state = 'idle in transaction') as "idle_in_transaction!"
                            FROM pg_stat_activity 
                            WHERE datname = current_database()
                            "#
                        )
                        .fetch_one(&mut *conn)
                        .await;

                        // 통합된 모니터링 결과 로깅
                        match result {
                            Ok(row) => {
                                warn!(
                                    "Postgres pool metrics: size={}, idle={}, acquired={}, db_total={}, db_active={}, db_idle={}, db_idle_in_transaction={}",
                                    size,
                                    idle,
                                    acquired,
                                    row.total_connections,
                                    row.active_connections,
                                    row.idle_connections,
                                    row.idle_in_transaction
                                );
                            }
                            Err(_) => {
                                // 쿼리 실패 시 기본 메트릭만 출력
                                warn!(
                                    "Postgres pool metrics: size={}, idle={}, acquired={}",
                                    size, idle, acquired
                                );
                            }
                        }
                    } else {
                        // 연결 획득 실패 시 기본 메트릭만 출력
                        warn!(
                            "Postgres pool metrics: size={}, idle={}, acquired={} (failed to acquire connection for detailed metrics)",
                            size, idle, acquired
                        );
                    }
                }
            });
        }
        Self { pool }
    }
}

async fn sqlx_connect() -> sqlx::Pool<sqlx::Postgres> {
    // REPLICA_DATABASE_URL이 있으면 사용 (pgbouncer 연결, prepared statement 비활성화)
    // 없으면 DATABASE_URL 사용
    let database_url = env::var("REPLICA_DATABASE_URL")
        .unwrap_or_else(|_| env::var("DATABASE_URL").expect("DATABASE_URL environment variable not set"));

    // pgbouncer 트랜잭션 풀링 모드에서는 prepared statement 사용 불가
    // statement_cache_capacity를 0으로 설정하여 비활성화
    let pool = PgPoolOptions::new()
        .max_connections(*PG_MAX_CONNECTIONS)
        .min_connections(*PG_MIN_CONNECTIONS)
        .max_lifetime(Duration::from_secs(*PG_MAX_LIFETIME))
        .acquire_timeout(Duration::from_secs(*PG_ACQUIRE_TIMEOUT))
        .idle_timeout(Duration::from_secs(*PG_IDLE_TIMEOUT))
        .connect_with(
            sqlx::postgres::PgConnectOptions::from_str(&database_url)
                .expect("Invalid database URL")
                .application_name(
                    &env::var("APP_NAME").expect("APP_NAME environment variable not set"),
                )
                // pgbouncer 환경에서 prepared statement 비활성화
                .statement_cache_capacity(0),
        )
        .await
        .map_err(|e| {
            error_log!("Failed to establish Postgres connection: {}", e);
            error_log!(
                "Database URL (masked): postgres://****@{}",
                database_url.split('@').nth(1).unwrap_or("unknown")
            );
            error_log!(
                "Connection settings: max_connections={}, min_connections={}, acquire_timeout={}s",
                *PG_MAX_CONNECTIONS, *PG_MIN_CONNECTIONS, *PG_ACQUIRE_TIMEOUT
            );
            e
        })
        .expect("Failed to establish Postgres connection");

    info!("PostgreSQL pool initialized");
    pool
}

impl PostgresDatabase {
    /// token의 holder_count와 total_supply 조회
    pub async fn get_token_stats(&self, token_id: &str) -> Result<(i64, String)> {
        let result = sqlx::query!(
            r#"
            SELECT
                token_holder_count,
                total_supply
            FROM token
            WHERE token_id = $1
            "#,
            token_id
        )
        .fetch_one(&self.pool)
        .await?;

        // total_supply는 NUMERIC 타입이므로 BigDecimal로 변환
        let total_supply_str = result.total_supply.normalized().to_plain_string();
        Ok((result.token_holder_count, total_supply_str))
    }
}
