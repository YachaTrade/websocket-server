use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    error_log,
    types::{
        chart::{Chart, ChartUpdateParams},
        metrics::{
            MakerCount, MetricItem, PriceSnapshot, SwapSnapshot, TimeFrame, TransactionCount,
            VolumeAmount,
        },
        stream::{Buy, CurveSync, DexSync, Sell},
        AccountInfo, MarketInfo, MarketType, TokenInfo,
    },
    utils::calculate_price_change_percent_precise,
};
use anyhow::{anyhow, Result};
use bigdecimal::BigDecimal;
use rand::Rng;
use tracing::{debug, error, info, warn};

use crate::{
    config::{V2_BURN_VAULT_ADDRESS, V2_GIFT_VAULT_ADDRESS, WETH_ADDRESS},
    db::{
        local_store::{LocalStore, TokenMarketData},
        postgres::PostgresDatabase,
        redis::RedisDatabase,
    },
    measure_postgres,
};
use dashmap::DashMap;
use once_cell::sync::OnceCell;
use sqlx::Row;

// 전역 CacheManager 인스턴스
static CACHE_MANAGER: OnceCell<Arc<CacheManager>> = OnceCell::new();

/// Token 정보 캐싱을 위한 관리자 구조체
/// token_info, account_info는 Redis에 캐싱
/// market_info는 LocalStore(로컬 메모리)에 캐싱
/// price_cache: quote별 block_number 기반 가격 DashMap 캐시 (observer와 동일 구조)
/// PostgreSQL은 캐시 미스 시 2차 저장소로 사용
#[derive(Clone)]
pub struct CacheManager {
    postgres: Arc<PostgresDatabase>,
    redis: Arc<RedisDatabase>,
    local_store: Arc<LocalStore>,
    /// quote_id → (block_number → price) 메모리 캐시
    price_cache: Arc<DashMap<String, DashMap<i64, BigDecimal>>>,
    /// quote_id → QuoteInfo 메모리 캐시 (quote_token 테이블은 거의 불변)
    quote_info_cache: Arc<DashMap<String, crate::types::QuoteInfo>>,
    /// token_id → (FeeInfo or None, 캐싱 시각) 메모리 캐시.
    /// fee_config 행이 거의 변하지 않으므로 Some 결과는 길게 캐싱.
    /// CreateCurve와 fee_config insert 사이의 race로 None이 잡힌 경우 영구 누락
    /// 방지를 위해 None 결과는 짧은 TTL로만 캐싱하여 곧 재조회되도록 한다.
    fee_info_cache: Arc<DashMap<String, (Option<crate::types::FeeInfo>, Instant)>>,
}

impl CacheManager {
    /// fee_info 캐시 TTL: Some(존재 확인됨)인 경우 — fee_config는 거의 불변
    const FEE_INFO_CACHE_TTL_HIT: Duration = Duration::from_secs(3600);
    /// fee_info 캐시 TTL: None(아직 없음)인 경우 — race로 누락된 경우 빠르게 회복
    const FEE_INFO_CACHE_TTL_MISS: Duration = Duration::from_secs(30);
}

impl CacheManager {
    /// 글로벌 인스턴스 초기화
    pub async fn init() -> Result<()> {
        if CACHE_MANAGER.get().is_some() {
            info!("CacheManager already initialized");
            return Ok(());
        }

        let instance = Self::new().await?;

        let arc_instance = Arc::new(instance);

        if CACHE_MANAGER.set(arc_instance).is_err() {
            info!("CacheManager was initialized by another task");
        } else {
            info!("CacheManager global instance initialized successfully");
        }

        Ok(())
    }

    /// 글로벌 인스턴스 가져오기
    pub fn instance() -> Result<Arc<CacheManager>> {
        CACHE_MANAGER
            .get()
            .map(Arc::clone)
            .ok_or_else(|| anyhow!("CacheManager not initialized. Call CacheManager::init() first"))
    }

    pub async fn new() -> Result<Self> {
        let postgres = PostgresDatabase::instance()?;
        let redis = RedisDatabase::instance()?;

        // LocalStore 초기화 (아직 안 됐으면)
        LocalStore::init();
        let local_store = LocalStore::instance();

        Ok(Self {
            postgres,
            redis,
            local_store,
            price_cache: Arc::new(DashMap::new()),
            quote_info_cache: Arc::new(DashMap::new()),
            fee_info_cache: Arc::new(DashMap::new()),
        })
    }

    //-------------------------------------------------------------------------
    // 화이트리스트 토큰 관련 메서드들
    //-------------------------------------------------------------------------

    /// 화이트리스트에 토큰 추가 (LocalStore)
    pub async fn insert_white_list_token(&self, token: &str, is_white: bool) -> Result<()> {
        // LocalStore에 추가
        self.local_store.insert_white_list_token(token, is_white);
        Ok(())
    }

    /// LocalStore에서만 토큰 화이트리스트 여부를 확인 (DB fallback 없음).
    ///
    /// Some(true)  → 캐시에 명시적으로 whitelist 등록됨
    /// Some(false) → 캐시에 등록 안 된 것으로 알려짐 (negative cache — 신뢰도 낮음)
    /// None        → 캐시에 정보 없음 (cold cache)
    ///
    /// V2 Curve Buy/Sell처럼 Create와 같은 tx에 동시 emit되는 race 케이스에서,
    /// Create handler가 LocalStore에 등록할 때까지 짧게 polling할 때 사용.
    /// 일반 whitelist 확인은 `check_white_list_token` 사용.
    pub fn check_white_list_token_local(&self, token: &str) -> Option<bool> {
        self.local_store.check_white_list_token(token)
    }

    /// 토큰이 화이트리스트에 있는지 확인 (LocalStore 캐시 우선, 없으면 PostgreSQL에서 조회)
    pub async fn check_white_list_token(&self, token: &str) -> Result<bool> {
        // LocalStore 캐시 확인
        if let Some(exists) = self.local_store.check_white_list_token(token) {
            return Ok(exists);
        }

        // PostgreSQL에서 토큰 존재 확인 - 재시도 로직 추가
        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500; // 기본 대기 시간 (밀리초)

        while retry_count < max_retries {
            // PostgreSQL 쿼리 실행
            let query = r#"SELECT EXISTS(SELECT 1 FROM token WHERE token_id = $1) as exists"#;
            match measure_postgres!(
                "check_white_list_token",
                sqlx::query(query)
                    .bind(token)
                    .fetch_one(&self.postgres.pool)
            ) {
                Ok(row) => {
                    let exists: bool = row.get("exists");
                    debug!(
                        "Token existence check in PostgreSQL: token={}, exists={}",
                        token, exists
                    );

                    // LocalStore에 캐싱
                    self.local_store.insert_white_list_token(token, exists);

                    return Ok(exists);
                }
                Err(e) => {
                    // 재시도 가능한 오류
                    retry_count += 1;

                    // 지수 백오프 계산 (1차: 500ms, 2차: 1000ms, 3차: 2000ms)
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "check_white_list_token - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        token, backoff_time, retry_count, max_retries, e
                    );

                    // 대기 후 재시도
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수를 초과한 경우
        error_log!(
            "check_white_list_token - PostgreSQL 연결 최대 재시도 횟수 초과 ({}), 기본값 반환",
            token
        );
        Ok(false)
    }

    //-------------------------------------------------------------------------
    // 토큰-POOL 관련 메서드들
    //-------------------------------------------------------------------------

    /// 토큰-POOL 관계 저장 (LocalStore)
    pub async fn insert_token_pool(&self, token: &str, pool: &str) -> Result<()> {
        // LocalStore에 저장
        self.local_store.insert_token_pool(token, pool);
        Ok(())
    }

    /// 토큰에 대한 POOL 정보 조회 (LocalStore 캐시 우선, 없으면 PostgreSQL에서 조회)
    pub async fn get_token_pool(&self, token: &str) -> Result<Option<String>> {
        // LocalStore 캐시 확인
        if let Some(pool) = self.local_store.get_token_pool(token) {
            debug!(
                "Token pool found in LocalStore: token={}, pool={}",
                token, pool
            );
            return Ok(Some(pool));
        }

        debug!("Token pool not found in LocalStore: token={}", token);

        // PostgreSQL에서 pool_id 조회 (POOL 유형의 마켓) - 재시도 로직 추가
        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500; // 기본 대기 시간 (밀리초)

        while retry_count < max_retries {
            // PostgreSQL 쿼리 실행
            let query = r#"SELECT pool_id FROM market WHERE token_id = $1 AND market_type = 'DEX'"#;
            match measure_postgres!(
                "get_token_pool",
                sqlx::query(query)
                    .bind(token)
                    .fetch_optional(&self.postgres.pool)
            ) {
                Ok(Some(row)) => {
                    let pool: String = row.get("pool_id");
                    debug!(
                        "Token pool found in PostgreSQL: token={}, pool={}",
                        token, pool
                    );

                    // 찾은 정보를 LocalStore에 캐싱
                    self.local_store.insert_token_pool(token, &pool);

                    return Ok(Some(pool));
                }
                Ok(None) => {
                    debug!("DEX market not found in PostgreSQL: token={}", token);
                    return Ok(None);
                }
                Err(e) => {
                    // 재시도 가능한 오류
                    retry_count += 1;

                    // 지수 백오프 계산
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "get_token_pool - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        token, backoff_time, retry_count, max_retries, e
                    );

                    // 대기 후 재시도
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수를 초과한 경우
        error_log!(
            "get_token_pool - PostgreSQL 연결 최대 재시도 횟수 초과 ({}), 기본값 반환",
            token
        );
        Ok(None)
    }

    //-------------------------------------------------------------------------
    // POOL 관련 메서드들
    //-------------------------------------------------------------------------

    /// 화이트리스트에 POOL 추가 (LocalStore)
    pub async fn insert_white_list_pool(&self, pool: &str, is_white: bool) -> Result<()> {
        // LocalStore에 저장
        self.local_store.insert_white_list_pool(pool, is_white);
        Ok(())
    }

    /// POOL가 화이트리스트에 있는지 확인 (LocalStore 캐시 우선, 없으면 PostgreSQL에서 조회)
    pub async fn check_white_list_pool(&self, pool: &str) -> Result<bool> {
        // LocalStore 캐시 확인
        if let Some(exists) = self.local_store.check_white_list_pool(pool) {
            return Ok(exists);
        }

        // PostgreSQL에서 POOL 존재 확인 - 재시도 로직 추가
        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500; // 기본 대기 시간 (밀리초)

        while retry_count < max_retries {
            // PostgreSQL 쿼리 실행
            info!("Checking POOL existence in PostgreSQL: {}", pool);
            let query = r#"SELECT EXISTS(SELECT 1 FROM market WHERE pool_id = $1 AND market_type IN ('DEX', 'V2_DEX')) as exists"#;
            match measure_postgres!(
                "check_white_list_pool",
                sqlx::query(query).bind(pool).fetch_one(&self.postgres.pool)
            ) {
                Ok(row) => {
                    let exists: bool = row.get("exists");

                    // LocalStore에 캐싱
                    self.local_store.insert_white_list_pool(pool, exists);

                    return Ok(exists);
                }
                Err(e) => {
                    // 재시도 가능한 오류
                    retry_count += 1;

                    // 지수 백오프 계산
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "check_white_list_pool - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        pool, backoff_time, retry_count, max_retries, e
                    );

                    // 대기 후 재시도
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수를 초과한 경우
        error_log!(
            "check_white_list_pool - PostgreSQL 연결 최대 재시도 횟수 초과 ({}), 기본값 반환",
            pool
        );
        Ok(false)
    }

    //-------------------------------------------------------------------------
    // POOL 페어 관련 메서드들
    //-------------------------------------------------------------------------

    /// POOL 페어 정보 저장 (LocalStore)
    pub async fn insert_pool_pair(&self, pool: &str, token0: &str, token1: &str) -> Result<()> {
        // LocalStore에 저장
        self.local_store.insert_pool_pair(pool, token0, token1);
        Ok(())
    }

    /// POOL 페어 정보 조회 (LocalStore 캐시 우선, 없으면 PostgreSQL에서 조회)
    pub async fn get_pool_pair(&self, pool: &str) -> Result<Option<(String, String)>> {
        // LocalStore 캐시 확인
        if let Some(pair) = self.local_store.get_pool_pair(pool) {
            debug!(
                "Pool pair found in LocalStore: pool={}, token0={}, token1={}",
                pool, pair.0, pair.1
            );
            return Ok(Some(pair));
        }

        debug!("Pool pair not found in LocalStore: pool={}", pool);

        // PostgreSQL에서 페어 정보 조회 - 재시도 로직 추가
        let max_retries = 5;
        let mut retry_count = 0;
        let backoff_base = 500; // 기본 대기 시간 (밀리초)

        while retry_count < max_retries {
            // PostgreSQL 쿼리 실행 — V2: quoteToken이 WETH이 아닐 수 있으므로
            // market 테이블의 quote_id를 함께 가져와서 (token_id, quote_id) 페어로 복원
            let query = r#"SELECT token_id, quote_id FROM market WHERE pool_id = $1"#;
            match measure_postgres!(
                "get_pool_pair",
                sqlx::query(query)
                    .bind(pool)
                    .fetch_optional(&self.postgres.pool)
            ) {
                Ok(Some(row)) => {
                    let token_id: String = row.get("token_id");
                    let quote_id: String = row.get("quote_id");
                    debug!(
                        "Pool pair found in PostgreSQL: pool={}, token_id={}, quote_id={}",
                        pool, token_id, quote_id
                    );

                    // 유니스왑 방식으로 token0, token1 정렬 (주소값 비교).
                    // V1처럼 WETH으로 강제 fallback하지 않음 — V2 non-WETH quote
                    // 토큰의 경우 (WETH, token_id)로 만들면 on-chain pool의 실제
                    // (token0, token1)과 어긋나서 reserve/amount 해석이 뒤집힘.
                    let (token0, token1) =
                        if quote_id.to_lowercase() < token_id.to_lowercase() {
                            (quote_id, token_id)
                        } else {
                            (token_id, quote_id)
                        };

                    debug!(
                        "Ordered pair (Uniswap style): token0={}, token1={}",
                        token0, token1
                    );

                    // 찾은 정보를 LocalStore에 캐싱
                    self.local_store.insert_pool_pair(pool, &token0, &token1);

                    return Ok(Some((token0, token1)));
                }
                Ok(None) => {
                    debug!("Pool pair not found in PostgreSQL: pool={}", pool);
                    return Ok(None);
                }
                Err(e) => {
                    // 재시도 가능한 오류
                    retry_count += 1;

                    // 지수 백오프 계산
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "get_pool_pair - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        pool, backoff_time, retry_count, max_retries, e
                    );

                    // 대기 후 재시도
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수를 초과한 경우
        error_log!(
            "get_pool_pair - PostgreSQL 연결 최대 재시도 횟수 초과 ({}), 기본값 반환",
            pool
        );
        Ok(None)
    }
}

impl CacheManager {
    pub async fn get_account_info(&self, account_id: &str) -> AccountInfo {
        // Redis 캐시 확인
        if let Ok(account_info) = self.redis.get_account_info(account_id).await {
            return account_info;
        }

        // PostgreSQL에서 계정 정보 조회
        let account_info = match (async {
            measure_postgres!(
                "get_account_info",
                sqlx::query_as::<_, AccountInfo>(
                    r#"
                    SELECT
                        a.account_id,
                        COALESCE(ax.x_handle, a.nickname) as nickname,
                        COALESCE(ax.x_image_uri, a.image_uri) as image_uri,
                        a.bio
                    FROM account a
                    LEFT JOIN account_x ax ON a.account_id = ax.account_id
                    WHERE a.account_id = $1
                    "#,
                )
                .bind(account_id)
                .fetch_one(&self.postgres.pool)
            )
        })
        .await
        {
            Ok(row) => row,
            Err(_e) => {
                // 모든 에러에서 기본 계정 정보 반환
                debug!(
                    "Account not found for {}, returning default account",
                    account_id
                );

                // 기본 이미지 선택: 1부터 5까지의 랜덤 숫자를 사용하여 이미지 URI 환경변수 가져오기
                let random_number = rand::rng().random_range(1..=5);
                let image_key = format!("DEFAULT_IMAGE_{}", random_number);
                let image_uri = env::var(image_key)
                    .unwrap_or_else(|_| "https://default-image.com/default.png".to_string());

                // 생성된 계정 정보 반환
                AccountInfo {
                    account_id: account_id.to_string(),
                    nickname: account_id.to_string(),
                    image_uri,
                    bio: "".to_string(),
                }
            }
        };

        // 계정 정보를 Redis에 캐싱
        if let Err(e) = self
            .redis
            .insert_account_info(account_id, &account_info)
            .await
        {
            warn!("Failed to cache account info in Redis: {}", e);
        }

        account_info
    }

    /// QuoteInfo 조회 (DashMap 캐시 → DB fallback)
    /// quote_token 테이블은 거의 변경되지 않으므로 한 번 조회 후 메모리에 캐싱.
    pub async fn get_quote_info(&self, quote_id: &str) -> crate::types::QuoteInfo {
        // 1. DashMap 캐시 확인
        if let Some(info) = self.quote_info_cache.get(quote_id) {
            return info.clone();
        }

        // 2. DB 조회
        let query = r#"
            SELECT quote_id, name, symbol, decimals, image_uri
            FROM quote_token
            WHERE quote_id = $1
        "#;

        #[derive(Debug, sqlx::FromRow)]
        struct QuoteRow {
            quote_id: String,
            name: String,
            symbol: String,
            decimals: i32,
            image_uri: String,
        }

        let info = match sqlx::query_as::<_, QuoteRow>(query)
            .bind(quote_id)
            .fetch_optional(&self.postgres.pool)
            .await
        {
            Ok(Some(row)) => crate::types::QuoteInfo {
                quote_id: row.quote_id,
                name: row.name,
                symbol: row.symbol,
                decimals: row.decimals,
                image_uri: row.image_uri,
            },
            _ => {
                warn!("quote_token not found for quote_id={}, using defaults", quote_id);
                crate::types::QuoteInfo {
                    quote_id: quote_id.to_string(),
                    ..Default::default()
                }
            }
        };

        // 3. 캐시에 저장
        self.quote_info_cache
            .insert(quote_id.to_string(), info.clone());

        info
    }

    pub async fn get_token_info(&self, token_id: &str) -> Result<TokenInfo> {
        #[derive(Debug, sqlx::FromRow)]
        struct TokenRow {
            token_id: String,
            name: String,
            symbol: String,
            description: Option<String>,
            twitter: Option<String>,
            telegram: Option<String>,
            website: Option<String>,
            image_uri: String,
            is_graduated: bool,
            is_nsfw: bool,
            is_cto: bool,
            version: crate::types::TokenVersion,
            created_at: i64,
            creator: String,
            creator_nickname: String,
            creator_image_uri: String,
            creator_bio: String,
        }

        // Redis 캐시 확인
        if let Ok(token_info) = self.redis.get_token_info(token_id).await {
            return Ok(token_info);
        }

        // PostgreSQL 쿼리 (V2: token.version 컬럼 포함)
        let query = r#"
             SELECT
                t.token_id,
                t.name,
                t.symbol,
                t.description,
                t.twitter,
                t.telegram,
                t.website,
                t.image_uri,
                t.is_graduated,
                t.created_at,
                t.creator,
                t.is_nsfw,
                t.is_cto,
                t.version,
                COALESCE(ax.x_handle, a.nickname) as creator_nickname,
                COALESCE(ax.x_image_uri, a.image_uri) as creator_image_uri,
                a.bio as creator_bio
            FROM token t
            JOIN account a ON t.creator = a.account_id
            LEFT JOIN account_x ax ON a.account_id = ax.account_id
            WHERE t.token_id = $1
        "#;

        let token_result = sqlx::query_as::<_, TokenRow>(query)
            .bind(token_id)
            .fetch_one(&self.postgres.pool)
            .await;

        match token_result {
            Ok(row) => {
                debug!(
                    "Token info found in PostgreSQL: token_id={}, symbol={}",
                    row.token_id, row.symbol
                );

                let token_info = TokenInfo {
                    token_id: row.token_id,
                    name: row.name,
                    symbol: row.symbol,
                    image_uri: row.image_uri,
                    description: row.description,
                    is_graduated: row.is_graduated,
                    is_nsfw: row.is_nsfw,
                    twitter: row.twitter,
                    telegram: row.telegram,
                    website: row.website,
                    created_at: row.created_at,
                    creator: AccountInfo {
                        account_id: row.creator,
                        nickname: row.creator_nickname,
                        bio: row.creator_bio,
                        image_uri: row.creator_image_uri,
                    },
                    is_cto: row.is_cto,
                    version: row.version,
                };
                // 찾은 정보를 Redis에 캐싱
                if let Err(e) = self.set_token_info(token_id, &token_info).await {
                    warn!(
                        "get_token_info - Failed to cache token info in Redis: {}",
                        e
                    );
                    // Redis 캐싱 실패는 치명적이지 않으므로 계속 진행
                }

                Ok(token_info)
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("no rows returned") || error_msg.contains("RowNotFound") {
                    debug!("Token not found in PostgreSQL: token_id={}", token_id);
                    Err(anyhow!("Token not found: {}", token_id))
                } else {
                    error_log!("get_token_info - Database error ({}): {}", token_id, e);
                    Err(anyhow!("Failed to get token info: {}", e))
                }
            }
        }
    }

    /// 토큰 정보를 Redis에 캐싱
    pub async fn set_token_info(&self, token_id: &str, token_info: &TokenInfo) -> Result<()> {
        // Redis에 토큰 정보 저장
        self.redis.set_token_info(token_id, token_info).await?;

        debug!(
            "Token info cached in Redis: token_id={}, symbol={}",
            token_info.token_id, token_info.symbol
        );

        Ok(())
    }

    /// 토큰의 total_supply, holder_count를 DB에서 조회하여 로컬 캐시 갱신
    async fn refresh_token_stats(&self, token_id: &str) -> Result<(BigDecimal, i64)> {
        #[derive(Debug, sqlx::FromRow)]
        struct StatsRow {
            total_supply: BigDecimal,
            holder_count: i64,
        }

        let query = r#"
            SELECT total_supply, token_holder_count as holder_count
            FROM token
            WHERE token_id = $1
        "#;

        let row = measure_postgres!(
            "refresh_token_stats",
            sqlx::query_as::<_, StatsRow>(query)
                .bind(token_id)
                .fetch_one(&self.postgres.pool)
        )?;

        let current_time = crate::utils::current_unix_timestamp();

        // 로컬 캐시 업데이트
        self.local_store.update_market(token_id, |data| {
            data.total_supply = row.total_supply.clone();
            data.holder_count = row.holder_count;
            data.last_stats_update = current_time;
        });

        debug!(
            "Token stats refreshed: token={}, total_supply={}, holder_count={}",
            token_id,
            row.total_supply.normalized().to_plain_string(),
            row.holder_count
        );

        Ok((row.total_supply, row.holder_count))
    }

    /// 마켓 정보 조회 (로컬 메모리 L1 캐시 + PostgreSQL L2)
    pub async fn get_market_info(&self, token_id: &str) -> Result<MarketInfo> {
        // 로컬 메모리 캐시 확인
        if let Some(local_data) = self.local_store.get_market(token_id) {
            let current_time = crate::utils::current_unix_timestamp();

            // 15초가 지났으면 total_supply, holder_count를 DB에서 갱신
            let (total_supply, holder_count) = if current_time - local_data.last_stats_update > 15 {
                match self.refresh_token_stats(token_id).await {
                    Ok((ts, hc)) => (ts, hc),
                    Err(_) => (local_data.total_supply.clone(), local_data.holder_count),
                }
            } else {
                (local_data.total_supply.clone(), local_data.holder_count)
            };

            // V2 토큰인데 fee_info가 None으로 캐시되어 있으면 재조회 시도.
            // CreateCurve 처리 시점에 indexer의 fee_config insert가 늦어 None으로
            // 캐시되면 영구히 누락되던 문제 회복용. fee_info_cache가 None TTL 30s
            // 후에 다시 DB 조회하므로 N초 이내에 자동 회복된다.
            let fee_info = if local_data.fee_info.is_none() {
                let refreshed = self.get_fee_info(token_id).await;
                if refreshed.is_some() {
                    let to_store = refreshed.clone();
                    self.local_store.update_market(token_id, |data| {
                        data.fee_info = to_store.clone();
                    });
                }
                refreshed
            } else {
                local_data.fee_info.clone()
            };

            // 로컬 데이터를 MarketInfo로 변환
            // BigDecimal → String 변환은 전부 .normalized().to_plain_string() 로 통일
            // quote_price: 해당 market의 quote asset USD 가격 (DashMap 캐시 or Pyth)
            let quote_price = crate::stream::price::get_quote_price(
                &local_data.quote_info.quote_id,
            )
            .await;
            let price_usd = (&local_data.price * &quote_price)
                .normalized()
                .to_plain_string();
            let quote_price = quote_price.normalized().to_plain_string();
            let price = local_data.price.normalized().to_plain_string();
            let reserve_quote = local_data.reserve_native.normalized().to_plain_string();
            let reserve_token = local_data.reserve_token.normalized().to_plain_string();
            let volume = local_data.volume_24h.normalized().to_plain_string();
            let ath_price = local_data.ath_price.normalized().to_plain_string();
            let ath_price_quote = local_data.ath_price_native.normalized().to_plain_string();
            let total_supply = total_supply.normalized().to_plain_string();

            let market_info = MarketInfo {
                // 저장된 market_type을 그대로 사용
                market_type: local_data.market_type.clone(),
                token_id: token_id.to_string(),
                quote_info: local_data.quote_info.clone(),
                market_id: local_data.market_id.clone(),
                token_price: price_usd.clone(), // token_price == price_usd (USD/Token)
                native_price: quote_price.clone(),
                quote_price,
                price: price.clone(),
                price_usd,
                price_native: price.clone(),
                price_quote: price,
                total_supply,
                reserve_native: reserve_quote.clone(),
                reserve_quote,
                reserve_token,
                volume,
                ath_price: ath_price.clone(),
                ath_price_usd: ath_price,
                ath_price_native: ath_price_quote.clone(),
                ath_price_quote,
                holder_count,
                last_stats_update: current_time,
                fee_info,
            };
            return Ok(market_info);
        }

        // PostgreSQL에서 마켓 정보 조회
        let max_retries = 2;
        let mut retry_count = 0;
        let backoff_base = 100;
        #[derive(Debug, sqlx::FromRow)]
        struct MarketRow {
            market_type: MarketType,
            token_id: String,
            market_id: String,
            token_price: BigDecimal,
            quote_price: BigDecimal,
            price: BigDecimal,
            total_supply: BigDecimal,
            reserve_quote: BigDecimal,
            reserve_token: BigDecimal,
            volume: BigDecimal,
            ath_price: BigDecimal,
            ath_price_quote: BigDecimal,
            holder_count: i64,
            // quote_token JOIN 결과
            quote_id: String,
            quote_name: String,
            quote_symbol: String,
            quote_decimals: i32,
            quote_image_uri: String,
            // fee_config LEFT JOIN 결과 (V2 전용)
            fee_creator_fee_rate: Option<i16>,
            fee_curve_protocol_fee_rate: Option<i16>,
            fee_dex_protocol_fee_rate: Option<i16>,
        }
        while retry_count < max_retries {
            // V2 스키마: DB 컬럼명은 전부 quote_* 기준
            // price 테이블은 (quote_id, block_number) 복합 PK
            // → 해당 market의 quote_id 기준으로 최신 가격 조회
            // quote_token LEFT JOIN으로 quote asset 메타데이터도 함께 가져옴
            let query = r#"
                SELECT
                    m.market_type,
                    m.token_id,
                    COALESCE(m.pool_id, '') as market_id,
                    (m.price * COALESCE(p.price, 0)) as token_price,
                    COALESCE(p.price, 0) as quote_price,
                    m.price as price,
                    t.total_supply as total_supply,
                    COALESCE(m.reserve_quote, 0) as reserve_quote,
                    COALESCE(m.reserve_token, 0) as reserve_token,
                    COALESCE(m.volume, 0) as volume,
                    COALESCE(m.ath_price, 0) as ath_price,
                    COALESCE(m.ath_price_quote, 0) as ath_price_quote,
                    t.token_holder_count as holder_count,
                    m.quote_id as quote_id,
                    COALESCE(qt.name, '') as quote_name,
                    COALESCE(qt.symbol, '') as quote_symbol,
                    COALESCE(qt.decimals, 18) as quote_decimals,
                    COALESCE(qt.image_uri, '') as quote_image_uri,
                    fc.creator_fee_rate as fee_creator_fee_rate,
                    fc.curve_protocol_fee_rate as fee_curve_protocol_fee_rate,
                    fc.dex_protocol_fee_rate as fee_dex_protocol_fee_rate
                FROM market m
                JOIN token t ON m.token_id = t.token_id
                LEFT JOIN quote_token qt ON m.quote_id = qt.quote_id
                LEFT JOIN fee_config fc ON m.token_id = fc.token_id
                LEFT JOIN LATERAL (
                    SELECT price
                    FROM price
                    WHERE quote_id = m.quote_id
                    ORDER BY block_number DESC
                    LIMIT 1
                ) p ON true
                WHERE m.token_id = $1
            "#;

            match measure_postgres!(
                "get_market_info",
                sqlx::query_as::<_, MarketRow>(query)
                    .bind(token_id)
                    .fetch_one(&self.postgres.pool)
            ) {
                Ok(row) => {
                    debug!(
                        "Market info found in PostgreSQL: token_id={}, market_type={:?}",
                        row.token_id, row.market_type
                    );

                    // market_id가 빈 문자열이면 BONDING_CURVE_ADDRESS 사용
                    let market_id = if row.market_id.is_empty() {
                        crate::config::V2_BONDING_CURVE_ADDRESS.clone()
                    } else {
                        row.market_id
                    };

                    // 먼저 is_graduated를 계산 (market_type 사용 전)
                    let is_graduated = matches!(row.market_type, MarketType::Dex);

                    // fee_info 구성 (V2 전용: fee_config 테이블에 데이터가 있을 때만)
                    let fee_info = match (
                        row.fee_creator_fee_rate,
                        row.fee_curve_protocol_fee_rate,
                        row.fee_dex_protocol_fee_rate,
                    ) {
                        (Some(creator), Some(curve), Some(dex)) => {
                            Some(crate::types::FeeInfo {
                                creator_fee_rate: creator,
                                curve_protocol_fee_rate: curve,
                                dex_protocol_fee_rate: dex,
                            })
                        }
                        _ => None,
                    };

                    // 찾은 정보를 로컬 메모리에 캐싱
                    let quote_info = crate::types::QuoteInfo {
                        quote_id: row.quote_id.clone(),
                        name: row.quote_name.clone(),
                        symbol: row.quote_symbol.clone(),
                        decimals: row.quote_decimals,
                        image_uri: row.quote_image_uri.clone(),
                    };
                    let local_data = TokenMarketData {
                        market_id: market_id.clone(),
                        quote_info: quote_info.clone(),
                        price: row.price.clone(),
                        reserve_native: row.reserve_quote.clone(),
                        reserve_token: row.reserve_token.clone(),
                        total_supply: row.total_supply.clone(),
                        fdv: &row.price * &row.total_supply,
                        volume_24h: row.volume.clone(),
                        ath_price: row.ath_price.clone(),
                        ath_price_native: row.ath_price_quote.clone(),
                        holder_count: row.holder_count,
                        is_graduated,
                        market_type: row.market_type.clone(),
                        last_stats_update: crate::utils::current_unix_timestamp(),
                        fee_info: fee_info.clone(),
                    };
                    self.local_store.set_market(token_id, local_data);

                    // price = MON/Token, price_usd = price * native_price = USD/Token
                    // BigDecimal → String 변환은 전부 .normalized().to_plain_string() 로 통일
                    let price_usd = (&row.price * &row.quote_price)
                        .normalized()
                        .to_plain_string();
                    let token_price = row.token_price.normalized().to_plain_string();
                    let quote_price = row.quote_price.normalized().to_plain_string();
                    let price = row.price.normalized().to_plain_string();
                    let reserve_quote = row.reserve_quote.normalized().to_plain_string();
                    let reserve_token = row.reserve_token.normalized().to_plain_string();
                    let total_supply = row.total_supply.normalized().to_plain_string();
                    let volume = row.volume.normalized().to_plain_string();
                    let ath_price = row.ath_price.normalized().to_plain_string();
                    let ath_price_quote = row.ath_price_quote.normalized().to_plain_string();

                    let market_info = MarketInfo {
                        market_type: row.market_type,
                        token_id: row.token_id,
                        quote_info,
                        market_id,
                        token_price,
                        native_price: quote_price.clone(),
                        quote_price,
                        price: price.clone(),
                        price_usd,
                        price_native: price.clone(),
                        price_quote: price,
                        total_supply,
                        reserve_native: reserve_quote.clone(),
                        reserve_quote,
                        reserve_token,
                        volume,
                        ath_price: ath_price.clone(),
                        ath_price_usd: ath_price,
                        ath_price_native: ath_price_quote.clone(),
                        ath_price_quote,
                        holder_count: row.holder_count,
                        last_stats_update: crate::utils::current_unix_timestamp(),
                        fee_info,
                    };

                    return Ok(market_info);
                }
                Err(e) => {
                    // RowNotFound 체크
                    let error_msg = e.to_string();
                    if error_msg.contains("no rows returned") || error_msg.contains("RowNotFound") {
                        debug!("Market not found in PostgreSQL: token_id={}", token_id);
                        return Err(anyhow!("Market not found: {}", token_id));
                    }

                    // 재시도 가능한 오류
                    retry_count += 1;
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "get_market_info - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        token_id, backoff_time, retry_count, max_retries, e
                    );

                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수 초과
        error_log!(
            "get_market_info - PostgreSQL 연결 최대 재시도 횟수 초과 ({})",
            token_id
        );
        Err(anyhow!(
            "Failed to get market info after {} retries",
            max_retries
        ))
    }

    /// Volume을 Wei에서 Ether 단위로 변환
    #[inline]
    fn convert_volume_to_ether(volume_wei_str: &str) -> String {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        match BigDecimal::from_str(volume_wei_str) {
            Ok(wei_value) => (&wei_value / &*crate::config::DECIMALS).to_plain_string(),
            Err(_) => "0".to_string(),
        }
    }

    /// 마켓 정보를 로컬 메모리에 캐싱
    pub async fn set_market_info(&self, token_id: &str, market_info: &MarketInfo) -> Result<()> {
        use std::str::FromStr;

        // volume을 Ether 단위로 변환
        let volume_str = Self::convert_volume_to_ether(&market_info.volume);

        // LocalStore에 market 데이터 저장
        let price = BigDecimal::from_str(&market_info.price).unwrap_or_default();
        let total_supply = BigDecimal::from_str(&market_info.total_supply).unwrap_or_default();

        let market_data = TokenMarketData {
            market_id: market_info.market_id.clone(),
            quote_info: market_info.quote_info.clone(),
            price: price.clone(),
            reserve_native: BigDecimal::from_str(&market_info.reserve_native).unwrap_or_default(),
            reserve_token: BigDecimal::from_str(&market_info.reserve_token).unwrap_or_default(),
            total_supply: total_supply.clone(),
            fdv: &total_supply * &price,
            volume_24h: BigDecimal::from_str(&volume_str).unwrap_or_default(),
            ath_price: BigDecimal::from_str(&market_info.ath_price).unwrap_or_default(),
            ath_price_native: BigDecimal::from_str(&market_info.ath_price_native)
                .unwrap_or_default(),
            holder_count: market_info.holder_count,
            is_graduated: matches!(market_info.market_type, crate::types::MarketType::Dex),
            market_type: market_info.market_type.clone(),
            last_stats_update: crate::utils::current_unix_timestamp(),
            fee_info: market_info.fee_info.clone(),
        };

        self.local_store.set_market(token_id, market_data);

        debug!(
            "Market info cached in LocalStore: token_id={}, market_type={:?}",
            market_info.token_id, market_info.market_type
        );

        Ok(())
    }

    /// 24시간 전 가격을 조회합니다.
    /// 24시간 전 시점의 가격이 없으면 토큰의 첫 가격(최초 생성 시 가격)을 반환합니다.
    /// Redis 캐시 사용 (TTL: 5분)
    pub async fn get_price_24h_ago(&self, token_id: &str) -> Result<Option<String>> {
        use crate::utils::current_unix_timestamp;

        #[derive(Debug, sqlx::FromRow)]
        struct PriceRow {
            price: BigDecimal,
        }

        // 현재 시각과 24시간 전 시각 계산
        let current_time = current_unix_timestamp();
        let time_24h_ago = current_time - 86400;

        // PostgreSQL에서 조회 (24시간 전 가격, 없으면 첫 가격)
        let query = r#"
            SELECT COALESCE(
                (SELECT ph.price
                 FROM price_history ph
                 WHERE ph.token_id = $1
                 AND ph.created_at <= $2
                 ORDER BY ph.created_at DESC, ph.block_number DESC, ph.tx_index DESC NULLS LAST, ph.log_index DESC
                 LIMIT 1),
                (SELECT ph.price
                 FROM price_history ph
                 WHERE ph.token_id = $1
                 ORDER BY ph.created_at ASC, ph.block_number ASC, ph.tx_index ASC NULLS LAST, ph.log_index ASC
                 LIMIT 1)
            ) as price
        "#;

        match measure_postgres!(
            "get_price_24h_ago",
            sqlx::query_as::<_, PriceRow>(query)
                .bind(token_id)
                .bind(time_24h_ago)
                .fetch_optional(&self.postgres.pool)
        ) {
            Ok(Some(row)) => {
                let price_str = row.price.normalized().to_plain_string();
                debug!(
                    "Price for percent calculation found for token_id={}: {}",
                    token_id, price_str
                );
                Ok(Some(price_str))
            }
            Ok(None) => {
                debug!("No price history found for token_id={}", token_id);
                Ok(None)
            }
            Err(e) => {
                warn!(
                    "get_price_24h_ago - 데이터베이스 쿼리 오류 ({}): {}",
                    token_id, e
                );
                Err(anyhow!("Failed to get price 24h ago: {}", e))
            }
        }
    }

    /// 로컬 캐시에서 토큰의 quote_id를 조회. 없으면 WETH fallback.
    pub fn get_market_quote_id(&self, token_id: &str) -> String {
        self.local_store
            .get_market(token_id)
            .map(|m| m.quote_info.quote_id.clone())
            .unwrap_or_else(|| WETH_ADDRESS.clone())
    }

    /// quote별 price 캐시 최대 엔트리 수. 초과 시 오래된 블록(하위 절반) 자동 정리.
    const PRICE_CACHE_MAX_PER_QUOTE: usize = 10;

    /// block_number 기준 quote asset USD 가격 조회
    ///
    /// 해당 블록의 가격이 DashMap 캐시에 있으면 사용,
    /// 없으면 가장 최신 Pyth 가격을 사용하고 캐시에 저장.
    /// DB 접근 없음.
    pub async fn get_quote_usd_price(
        &self,
        quote_id: &str,
        block_number: i64,
    ) -> BigDecimal {
        // 1. 캐시에서 해당 블록 가격 조회 (이미 historical로 결정된 값)
        if let Some(inner) = self.price_cache.get(quote_id) {
            if let Some(price) = inner.get(&block_number) {
                return price.clone();
            }
        }

        // 2. PostgreSQL의 price 테이블에서 historical 가격 조회.
        //    observer가 같은 quote_id로 block_number 기반으로 박는 가격과 동일.
        //    block_number와 정확히 일치하는 row가 없으면 그 이전의 가장 최근값을 사용
        //    (가격 update 간격으로 인해 모든 block에 대해 row가 있는 건 아님).
        //
        //    이걸 안 하면 cache miss 시점에 latest Pyth가 박혀 USD가 실시간/인덱싱과
        //    어긋난다. 어긋난 값을 (quote_id, block_number) 키로 캐시까지 해버려
        //    영구화되는 사고가 났음.
        let historical_query = r#"
            SELECT price
            FROM price
            WHERE quote_id = $1 AND block_number <= $2
            ORDER BY block_number DESC
            LIMIT 1
        "#;
        let historical: Option<BigDecimal> = match sqlx::query_scalar(historical_query)
            .bind(quote_id)
            .bind(block_number)
            .fetch_optional(&self.postgres.pool)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "get_quote_usd_price: historical lookup 실패 (quote={}, block={}): {} — Pyth latest로 fallback",
                    quote_id, block_number, e
                );
                None
            }
        };

        let price = if let Some(h) = historical {
            h
        } else {
            // 3. PG에도 없으면 (해당 block 이전의 가격이 아예 없음 — 매우 초기) 최신 Pyth로 fallback
            crate::stream::price::get_quote_price(quote_id).await
        };

        // 캐시에 저장 (동일 블록 재조회 시 즉시 반환)
        // RefMut(shard write lock)는 명시적으로 짧게 잡고 즉시 해제해야 한다.
        // evict_old_prices가 같은 quote_id로 .get()을 호출하므로, RefMut를
        // 들고 있는 채로 호출하면 같은 shard에 read 재진입 → parking_lot RwLock
        // self-deadlock으로 producer 태스크가 영구 정지된다.
        let needs_evict = {
            let inner = self
                .price_cache
                .entry(quote_id.to_string())
                .or_insert_with(|| DashMap::with_capacity(Self::PRICE_CACHE_MAX_PER_QUOTE));
            inner.insert(block_number, price.clone());
            inner.len() > Self::PRICE_CACHE_MAX_PER_QUOTE
        }; // ← 여기서 RefMut drop, shard write lock 해제

        if needs_evict {
            self.evict_old_prices(quote_id);
        }

        price
    }

    /// 오래된 price 캐시 엔트리 정리: block_number 기준 하위 절반 제거
    fn evict_old_prices(&self, quote_id: &str) {
        if let Some(inner) = self.price_cache.get(quote_id) {
            let mut blocks: Vec<i64> = inner.iter().map(|e| *e.key()).collect();
            blocks.sort_unstable();
            let cutoff = blocks.len() / 2;
            for &block in &blocks[..cutoff] {
                inner.remove(&block);
            }
            debug!(
                "Price cache evicted {} old entries for quote={} (remaining={})",
                cutoff,
                quote_id,
                inner.len()
            );
        }
    }

    /// 최신 가격 조회 (LocalStore L1 캐시 + PostgreSQL L2)
    pub async fn get_latest_price(&self) -> Result<String> {
        // LocalStore 캐시 확인
        if let Some(price) = self.local_store.get_latest_price() {
            return Ok(price);
        }

        // PostgreSQL에서 최신 가격 조회
        let max_retries = 2;
        let mut retry_count = 0;
        let backoff_base = 100;

        // V2 스키마: price PK가 (quote_id, block_number) 복합키로 바뀌었으므로
        // 이 서비스가 사용하는 기본 quote(WETH) 기준으로 최신가를 조회한다.
        let quote_id = crate::config::WETH_ADDRESS.as_str();

        while retry_count < max_retries {
            let query = r#"
                SELECT price
                FROM price
                WHERE quote_id = $1
                ORDER BY block_number DESC
                LIMIT 1
            "#;

            match measure_postgres!(
                "get_latest_price",
                sqlx::query_scalar::<_, BigDecimal>(query)
                    .bind(quote_id)
                    .fetch_one(&self.postgres.pool)
            ) {
                Ok(price_decimal) => {
                    let price = price_decimal.normalized().to_plain_string();
                    debug!("Latest price found in PostgreSQL: {}", price);

                    // 찾은 가격을 LocalStore에 캐싱
                    self.local_store.set_latest_price(&price);

                    return Ok(price);
                }
                Err(e) => {
                    // RowNotFound 체크
                    let error_msg = e.to_string();
                    if error_msg.contains("no rows returned") || error_msg.contains("RowNotFound") {
                        debug!("No price data found in PostgreSQL");
                        return Err(anyhow!("No price data found"));
                    }

                    // 재시도 가능한 오류
                    retry_count += 1;
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "get_latest_price - 데이터베이스 연결 오류, {}ms 후 재시도 {}/{}...: {}",
                        backoff_time, retry_count, max_retries, e
                    );

                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수 초과
        error_log!("get_latest_price - PostgreSQL 연결 최대 재시도 횟수 초과");
        Err(anyhow!(
            "Failed to get latest price after {} retries",
            max_retries
        ))
    }

    /// 토큰 total_supply 조회 (LocalStore L1 캐시 + PostgreSQL L2)
    pub async fn get_token_total_supply(&self, token_id: &str) -> Result<String> {
        // LocalStore 캐시 확인
        if let Some(total_supply) = self.local_store.get_token_total_supply(token_id) {
            return Ok(total_supply);
        }

        // PostgreSQL에서 토큰 total_supply 조회
        let max_retries = 2;
        let mut retry_count = 0;
        let backoff_base = 100;

        while retry_count < max_retries {
            let query = r#"
                SELECT total_supply
                FROM token
                WHERE token_id = $1
            "#;

            match measure_postgres!(
                "get_token_total_supply",
                sqlx::query_scalar::<_, BigDecimal>(query)
                    .bind(token_id)
                    .fetch_one(&self.postgres.pool)
            ) {
                Ok(total_supply_decimal) => {
                    let total_supply = total_supply_decimal.normalized().to_plain_string();
                    debug!(
                        "Token total_supply found in PostgreSQL: token_id={}, total_supply={}",
                        token_id, total_supply
                    );

                    // 찾은 total_supply를 LocalStore에 캐싱
                    self.local_store
                        .set_token_total_supply(token_id, &total_supply);

                    return Ok(total_supply);
                }
                Err(e) => {
                    // RowNotFound 체크
                    let error_msg = e.to_string();
                    if error_msg.contains("no rows returned") || error_msg.contains("RowNotFound") {
                        debug!("Token not found in PostgreSQL: token_id={}", token_id);
                        return Err(anyhow!("Token not found: {}", token_id));
                    }

                    // 재시도 가능한 오류
                    retry_count += 1;
                    let backoff_time = backoff_base * (1 << (retry_count - 1));

                    warn!(
                        "get_token_total_supply - 데이터베이스 연결 오류 ({}), {}ms 후 재시도 {}/{}...: {}",
                        token_id, backoff_time, retry_count, max_retries, e
                    );

                    tokio::time::sleep(std::time::Duration::from_millis(backoff_time)).await;
                    continue;
                }
            }
        }

        // 최대 재시도 횟수 초과
        error_log!(
            "get_token_total_supply - PostgreSQL 연결 최대 재시도 횟수 초과 ({})",
            token_id
        );
        Err(anyhow!(
            "Failed to get token total_supply after {} retries",
            max_retries
        ))
    }

    // 시간 제한 없이 가장 최근 차트의 close price 가져오기 (비율, 단위 없음)
    pub async fn get_last_chart_close_price_any(
        &self,
        token_id: &str,
        interval_type: &str,
    ) -> Result<BigDecimal> {
        use crate::types::chart::ChartInterval;

        // 현재 시간을 캔들 시작 시간으로 정규화
        let current_timestamp = crate::utils::current_unix_timestamp();
        let interval_obj = ChartInterval::try_from(interval_type)?;
        let candle_start = interval_obj.get_candle_start(current_timestamp);

        // PostgreSQL에서 현재 캔들 이전의 가장 최근 close price 조회 (fallback)
        let query = r#"
            SELECT close_price
            FROM chart
            WHERE token_id = $1 AND interval_type = $2 AND time_stamp < $3
            ORDER BY time_stamp DESC
            LIMIT 1
        "#;

        let max_retries = 3;
        let mut last_error = None;

        for attempt in 0..max_retries {
            match measure_postgres!(
                "get_last_chart_close_price_any",
                sqlx::query_scalar(query)
                    .bind(token_id)
                    .bind(interval_type)
                    .bind(candle_start)
                    .fetch_optional(&self.postgres.pool)
            ) {
                Ok(Some(price)) => {
                    info!("last close price: {}", price);
                    if attempt > 0 {
                        debug!(
                            "Successfully fetched last close price on retry attempt {}",
                            attempt + 1
                        );
                    }
                    // Price는 비율이므로 단위 변환 없이 그대로 반환
                    return Ok(price);
                }
                Ok(None) => {
                    // 데이터가 없는 경우 (새로운 토큰) - 0 반환
                    debug!(
                        "No previous chart data found for token {} interval {}, returning 0",
                        token_id, interval_type
                    );
                    return Ok(BigDecimal::from(0));
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < max_retries - 1 {
                        warn!(
                            "Failed to fetch last close price (attempt {}/{}): {:?}, retrying...",
                            attempt + 1,
                            max_retries,
                            last_error
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            100 * (attempt + 1) as u64,
                        ))
                        .await;
                    }
                }
            }
        }

        // 모든 재시도 실패 시 0 반환 (fallback)
        warn!(
            "Failed to get last chart close price after {} retries: {:?}, returning 0 as fallback",
            max_retries, last_error
        );
        Ok(BigDecimal::from(0))
    }

    /// Chart를 atomic하게 업데이트 (로컬 메모리 사용)
    ///
    /// # Arguments
    /// * `params` - 차트 업데이트 파라미터
    pub async fn update_chart_atomic(&self, params: &ChartUpdateParams<'_>) -> Result<Chart> {
        // 로컬 메모리에서 atomic 업데이트 시도
        if let Some(chart) = self.local_store.update_chart_atomic(params) {
            return Ok(chart);
        }

        // 로컬에 캔들이 없음 → PostgreSQL에서 조회하여 로컬에 로드
        info!(
            "Chart not found in local store for token={}, interval={}, timestamp={}, loading from PostgreSQL",
            params.token_id, params.interval, params.timestamp
        );

        match self
            .load_previous_candle_to_local(
                params.token_id,
                params.interval,
                params.timestamp,
                params.price,
                params.native_price,
                params.total_supply,
            )
            .await
        {
            Ok(_) => {
                info!("Successfully loaded previous candle from PostgreSQL, retrying chart update");
            }
            Err(e) => {
                error!("Failed to load previous candle from PostgreSQL: {}", e);
                return Err(e);
            }
        }

        // 재시도
        if let Some(chart) = self.local_store.update_chart_atomic(params) {
            Ok(chart)
        } else {
            Err(anyhow!(
                "Failed to update chart after loading from PostgreSQL"
            ))
        }
    }

    /// 이전 캔들을 PostgreSQL에서 조회하여 로컬 메모리에 로드
    ///
    /// 조회 순서:
    /// 1. chart 테이블에서 해당 interval의 가장 최근 캔들 조회 (USD 필드 포함)
    /// 2. 없으면 chart 테이블의 다른 interval에서라도 가장 최근 close_price + usd_close_price 조회
    /// 3. 그것도 없으면 price_history + price 테이블 JOIN으로 조회
    /// 4. 그것도 없으면 현재 가격을 close로 사용
    async fn load_previous_candle_to_local(
        &self,
        token_id: &str,
        interval: &str,
        current_timestamp: i64,
        current_price: &BigDecimal,
        native_price: &BigDecimal,
        total_supply: &BigDecimal,
    ) -> Result<()> {
        use crate::types::chart::ChartInterval;

        // 이전 타임스탬프 계산 (캔들 시작 시간으로 정규화)
        let interval_obj = ChartInterval::try_from(interval)?;
        // 현재 캔들의 시작 시간 계산
        let current_candle_start = interval_obj.get_candle_start(current_timestamp);

        // 이전 캔들의 시작 시간
        let prev_timestamp = interval_obj.previous_candle_start(current_candle_start);

        // 1. chart 테이블에서 현재 캔들 이전의 가장 최근 캔들 조회 (USD 필드 포함)
        let query = r#"
            SELECT
                'ok' as s,
                open_price as o,
                close_price as c,
                high_price as h,
                low_price as l,
                volume as v,
                time_stamp as t,
                usd_open_price as usd_o,
                usd_close_price as usd_c,
                usd_high_price as usd_h,
                usd_low_price as usd_l,
                usd_volume as usd_v,
                total_supply
            FROM chart
            WHERE token_id = $1 AND interval_type = $2 AND time_stamp < $3
            ORDER BY time_stamp DESC
            LIMIT 1
        "#;

        let chart: Option<Chart> = sqlx::query_as(query)
            .bind(token_id)
            .bind(interval)
            .bind(current_candle_start)
            .fetch_optional(&self.postgres.pool)
            .await?;

        if let Some(chart) = chart {
            // chart 테이블에서 찾음 → 로컬에 저장
            self.local_store
                .set_chart(token_id, interval, chart.clone());
            info!("Loaded previous candle from chart table: token={}, interval={}, timestamp={}, close={}, usd_close={}",
                token_id, interval, chart.t, chart.c, chart.usd_c);
            return Ok(());
        }

        // 2. chart 테이블에 해당 interval이 없으면 다른 interval에서라도 가장 최근 close price 조회
        warn!(
            "No previous candle for interval={}, checking any interval for token={}...",
            interval, token_id
        );

        let any_interval_query = r#"
            SELECT close_price, usd_close_price, total_supply
            FROM chart
            WHERE token_id = $1 AND time_stamp < $2
            ORDER BY time_stamp DESC
            LIMIT 1
        "#;

        let any_interval_result: Option<(BigDecimal, BigDecimal, BigDecimal)> =
            sqlx::query_as(any_interval_query)
                .bind(token_id)
                .bind(current_candle_start)
                .fetch_optional(&self.postgres.pool)
                .await?;

        let (close_price, usd_close_price, prev_total_supply) = if let Some((
            price,
            usd_price,
            ts,
        )) = any_interval_result
        {
            info!(
                "Found close_price from another interval: token={}, price={}, usd_price={}",
                token_id, price, usd_price
            );
            (price, usd_price, ts)
        } else {
            // 3. chart 테이블에 아예 없으면 price_history + price 테이블 JOIN으로 조회
            // (해당 블록 시점의 정확한 USD 가격을 매칭)
            warn!(
                "No chart data found, checking price_history with USD price for token={}...",
                token_id
            );

            // V2 스키마: price PK가 (quote_id, block_number)로 바뀌었으므로
            // 해당 token market의 quote_id 기준으로 historical quote/USD를 매칭한다.
            // WETH으로 고정하면 non-WETH quote 토큰의 USD open이 잘못 복원된다.
            let quote_id = match self.get_market_info(token_id).await {
                Ok(market) => market.quote_info.quote_id,
                Err(e) => {
                    warn!(
                        "Failed to resolve quote_id for chart history token={}, falling back to WETH: {}",
                        token_id, e
                    );
                    crate::config::WETH_ADDRESS.clone()
                }
            };
            let price_query = r#"
                SELECT ph.price, p.price as native_price
                FROM price_history ph
                LEFT JOIN price p ON p.quote_id = $3
                    AND p.block_number = (
                        SELECT MAX(block_number)
                        FROM price
                        WHERE quote_id = $3 AND block_number <= ph.block_number
                    )
                WHERE ph.token_id = $1 AND ph.created_at < $2
                ORDER BY ph.created_at DESC, ph.block_number DESC, ph.tx_index DESC, ph.log_index DESC
                LIMIT 1
            "#;

            let price_result: Option<(BigDecimal, Option<BigDecimal>)> =
                sqlx::query_as(price_query)
                    .bind(token_id)
                    .bind(current_candle_start)
                    .bind(&quote_id)
                    .fetch_optional(&self.postgres.pool)
                    .await?;

            if let Some((token_price, native_price_opt)) = price_result {
                let hist_native_price = native_price_opt.unwrap_or_else(|| BigDecimal::from(1));
                let usd_price = &token_price * &hist_native_price;
                info!(
                    "Found price from price_history: token={}, price={}, native_price={}, usd_price={}",
                    token_id, token_price, hist_native_price, usd_price
                );
                (token_price, usd_price, total_supply.clone())
            } else {
                // 4. price_history에도 없으면 현재 가격 사용
                warn!(
                    "No price_history found for token={}, using current price as close: {}",
                    token_id, current_price
                );
                let usd_price = current_price * native_price;
                (current_price.clone(), usd_price, total_supply.clone())
            }
        };

        // 더미 캔들 생성 (close만 의미 있는 값, 나머지는 0 또는 close와 동일)
        use crate::types::chart::CHART_STATUS_OK;

        let dummy_chart = Chart {
            s: CHART_STATUS_OK.to_string(),
            o: close_price.clone(),
            h: close_price.clone(),
            l: close_price.clone(),
            c: close_price.clone(),
            v: BigDecimal::from(0),
            t: prev_timestamp,
            usd_o: usd_close_price.clone(),
            usd_h: usd_close_price.clone(),
            usd_l: usd_close_price.clone(),
            usd_c: usd_close_price.clone(),
            usd_v: BigDecimal::from(0),
            total_supply: prev_total_supply,
        };

        // 로컬 메모리에 저장
        self.local_store
            .set_chart(token_id, interval, dummy_chart.clone());

        info!(
            "Created dummy candle with close={}, usd_close={}: token={}, interval={}, timestamp={}",
            dummy_chart.c, dummy_chart.usd_c, token_id, interval, prev_timestamp
        );

        Ok(())
    }

    /// CreateCurve 이벤트로부터 초기 Market 캐시 설정
    ///
    /// 새 토큰이 생성될 때 초기 market 정보를 로컬 메모리에 설정합니다.
    /// virtual_native / virtual_token으로 초기 가격을 계산합니다.
    ///
    /// # Arguments
    /// * `create_curve` - CreateCurve 이벤트 데이터
    ///
    /// # Returns
    /// * `Result<()>` - 성공 시 Ok(()), 실패 시 Error
    pub async fn init_market_cache_from_create_curve(
        &self,
        create_curve: &crate::types::stream::CreateCurve,
    ) -> Result<()> {
        use bigdecimal::RoundingMode;

        // 초기 가격 계산: virtual_native / virtual_token
        let price = (&create_curve.virtual_native / &create_curve.virtual_token)
            .with_scale_round(10, RoundingMode::Up);

        // ath_price 계산:
        // - ath_price_quote = price (Quote/Token)
        // - ath_price_usd = price * quote_usd_price (USD/Token)
        let block_number = create_curve.block_number as i64;
        let quote_price = self
            .get_quote_usd_price(&create_curve.quote_token, block_number)
            .await;
        let ath_price_usd = &price * &quote_price;
        let ath_price_native = price.clone();
        let total_supply = BigDecimal::from(1_000_000_000_000_000_000_000_000_000_i128);

        // 초기 market 데이터 설정
        let quote_info = self
            .get_quote_info(&create_curve.quote_token)
            .await;

        // V2 토큰인 경우 fee_config 조회
        // (이전엔 pair.is_some() 으로 implicit하게 V1/V2 판별했으나 명시 필드로 전환)
        let is_v2 = matches!(create_curve.version, crate::types::TokenVersion::V2);
        let fee_info = if is_v2 {
            self.get_fee_info(&create_curve.token).await
        } else {
            None
        };

        // CreateCurve 시점엔 항상 graduated 전이므로 Curve
        let market_type = crate::types::MarketType::Curve;

        let market_data = TokenMarketData {
            market_id: create_curve.token.clone(),
            quote_info,
            price: price.clone(),
            reserve_native: create_curve.virtual_native.clone(),
            reserve_token: create_curve.virtual_token.clone(),
            total_supply: total_supply.clone(), // DB에서 나중에 조회
            fdv: total_supply.clone() * price.clone(), // DB에서 나중에 조회
            volume_24h: BigDecimal::from(0),
            ath_price: ath_price_usd.clone(),
            ath_price_native: ath_price_native.clone(),
            holder_count: 0,
            is_graduated: false,
            market_type,
            last_stats_update: 0, // 다음 요청 시 DB 조회 트리거
            fee_info,
        };

        // LocalStore에 저장
        self.local_store
            .set_market(&create_curve.token, market_data);

        info!(
            "Market cache initialized from CreateCurve: token={}, price={}, ath_price_usd={}, reserve_native={}, reserve_token={}",
            create_curve.token,
            price.normalized().to_plain_string(),
            ath_price_usd.normalized().to_plain_string(),
            create_curve.virtual_native.normalized().to_plain_string(),
            create_curve.virtual_token.normalized().to_plain_string()
        );

        Ok(())
    }

    /// Curve Sync 이벤트로부터 Market 캐시 업데이트
    ///
    /// Curve Sync 이벤트가 발생하면 로컬 메모리의 market 정보를 업데이트합니다.
    /// 이를 통해 다음 market 조회 시 PostgreSQL 접근 없이 로컬 메모리에서 바로 반환 가능합니다.
    ///
    /// # Arguments
    /// * `sync` - Curve Sync 이벤트 데이터
    ///
    /// # Returns
    /// * `Result<()>` - 성공 시 Ok(()), 실패 시 Error
    pub async fn update_market_cache_from_curve_sync(
        &self,
        sync: &crate::types::stream::CurveSync,
    ) -> Result<()> {
        use bigdecimal::RoundingMode;

        // Curve Sync 이벤트로부터 업데이트 가능한 필드들:
        // 1. price: virtual_native_amount / virtual_token_amount
        // 2. reserve_native: virtual_native_amount
        // 3. reserve_token: virtual_token_amount
        let price = (&sync.virtual_native_amount / &sync.virtual_token_amount)
            .with_scale_round(10, RoundingMode::Up);

        // 로컬 메모리에서 기존 market 데이터 조회
        let existing_market = self.local_store.get_market(&sync.token);

        // market이 없으면 DB에서 먼저 로드
        if existing_market.is_none() {
            debug!(
                "Market not found in local store for token: {}. Loading from PostgreSQL first.",
                sync.token
            );
            // get_market_info 호출하면 DB에서 가져와서 로컬에 캐싱됨
            if let Err(e) = self.get_market_info(&sync.token).await {
                warn!(
                    "Failed to load market from PostgreSQL for token: {}. Error: {}",
                    sync.token, e
                );
                return Ok(()); // DB에도 없으면 skip
            }
        }

        // 다시 로컬에서 조회 (이제 있어야 함)
        let existing = match self.local_store.get_market(&sync.token) {
            Some(data) => data,
            None => {
                warn!(
                    "Market still not found after DB load for token: {}",
                    sync.token
                );
                return Ok(());
            }
        };

        // ath_price 계산: quote별 가격으로 USD 환산
        // existing.quote_info에서 quote_id를 가져와서 해당 quote의 USD 가격 조회
        let quote_price = self
            .get_quote_usd_price(&existing.quote_info.quote_id, sync.block_number as i64)
            .await;
        let price_usd = &sync.price * &quote_price;

        // USD ATH 업데이트
        let new_ath_usd = if price_usd > existing.ath_price {
            price_usd.clone()
        } else {
            existing.ath_price.clone()
        };

        // Quote ATH 업데이트 (Quote/Token 기준)
        let new_ath_native = if sync.price > existing.ath_price_native {
            sync.price.clone()
        } else {
            existing.ath_price_native.clone()
        };

        // LocalStore 업데이트
        self.local_store.update_market(&sync.token, |data| {
            data.price = price.clone();
            data.reserve_native = sync.virtual_native_amount.clone();
            data.reserve_token = sync.virtual_token_amount.clone();
            data.ath_price = new_ath_usd.clone();
            data.ath_price_native = new_ath_native.clone();
            data.is_graduated = false; // Curve sync이므로 graduated 아님
        });

        debug!(
            "Market cache updated from Curve Sync: token={}, price={}, ath_price_usd={}, ath_price_quote={}, reserve_native={}, reserve_token={}",
            sync.token,
            price.normalized().to_plain_string(),
            new_ath_usd.normalized().to_plain_string(),
            new_ath_native.normalized().to_plain_string(),
            sync.virtual_native_amount.normalized().to_plain_string(),
            sync.virtual_token_amount.normalized().to_plain_string()
        );

        Ok(())
    }

    /// DEX Sync 이벤트로부터 Market 캐시 업데이트
    ///
    /// DEX Sync 이벤트가 발생하면 로컬 메모리의 market 정보를 업데이트합니다.
    /// 이를 통해 다음 market 조회 시 PostgreSQL 접근 없이 로컬 메모리에서 바로 반환 가능합니다.
    ///
    /// # Arguments
    /// * `sync` - DEX Sync 이벤트 데이터
    ///
    /// # Returns
    /// * `Result<()>` - 성공 시 Ok(()), 실패 시 Error
    pub async fn update_market_cache_from_dex_sync(
        &self,
        sync: &crate::types::stream::DexSync,
    ) -> Result<()> {
        // 로컬 메모리에서 기존 market 데이터 조회
        let existing_market = self.local_store.get_market(&sync.token);

        // market이 없으면 DB에서 먼저 로드
        if existing_market.is_none() {
            debug!(
                "Market not found in local store for token: {}. Loading from PostgreSQL first.",
                sync.token
            );
            if let Err(e) = self.get_market_info(&sync.token).await {
                warn!(
                    "Failed to load market from PostgreSQL for token: {}. Error: {}",
                    sync.token, e
                );
                return Ok(());
            }
        }

        // 다시 로컬에서 조회 (이제 있어야 함)
        let existing = match self.local_store.get_market(&sync.token) {
            Some(data) => data,
            None => {
                warn!(
                    "Market still not found after DB load for token: {}",
                    sync.token
                );
                return Ok(());
            }
        };

        // ath_price 계산: quote별 가격으로 USD 환산
        let quote_price = self
            .get_quote_usd_price(&existing.quote_info.quote_id, sync.block_number as i64)
            .await;
        let price_usd = &sync.price * &quote_price;

        // USD ATH 업데이트
        let new_ath_usd = if price_usd > existing.ath_price {
            price_usd.clone()
        } else {
            existing.ath_price.clone()
        };

        // Native ATH 업데이트 (MON/Token 기준)
        let new_ath_native = if sync.price > existing.ath_price_native {
            sync.price.clone()
        } else {
            existing.ath_price_native.clone()
        };

        // LocalStore 업데이트
        self.local_store.update_market(&sync.token, |data| {
            data.price = sync.price.clone();
            data.reserve_native = sync.reserve_native.clone();
            data.reserve_token = sync.reserve_token.clone();
            data.ath_price = new_ath_usd.clone();
            data.ath_price_native = new_ath_native.clone();
            data.is_graduated = true; // DEX sync이므로 graduated
        });

        debug!(
            "DEX Market cache updated: token={}, price={}, ath_price_usd={}, ath_price_native={}, reserve_native={}, reserve_token={}",
            sync.token,
            sync.price.normalized().to_plain_string(),
            new_ath_usd.normalized().to_plain_string(),
            new_ath_native.normalized().to_plain_string(),
            sync.reserve_native.normalized().to_plain_string(),
            sync.reserve_token.normalized().to_plain_string()
        );

        Ok(())
    }

    /// fee_config 테이블에서 토큰의 수수료 설정 조회 (V2 전용)
    ///
    /// # 캐시 동작
    /// - `Some` 결과는 `FEE_INFO_CACHE_TTL_HIT` 동안 캐시 (fee_config는 거의 불변)
    /// - `None` 결과는 `FEE_INFO_CACHE_TTL_MISS` 동안만 캐시. CreateCurve가 indexer의
    ///   fee_config insert보다 빠르게 처리되어 None으로 잡힌 경우, 짧은 TTL 후 다음
    ///   호출에서 다시 DB 조회되어 회복된다.
    ///
    /// # Arguments
    /// * `token_id` - 토큰 ID
    ///
    /// # Returns
    /// * `Option<FeeInfo>` - 존재하면 Some(FeeInfo), 없으면 None
    pub async fn get_fee_info(&self, token_id: &str) -> Option<crate::types::FeeInfo> {
        // 1. 캐시 확인
        if let Some(entry) = self.fee_info_cache.get(token_id) {
            let (cached, cached_at) = entry.value();
            let ttl = if cached.is_some() {
                Self::FEE_INFO_CACHE_TTL_HIT
            } else {
                Self::FEE_INFO_CACHE_TTL_MISS
            };
            if cached_at.elapsed() < ttl {
                return cached.clone();
            }
            // TTL 만료 → fall through하여 재조회
        }

        // 2. DB 조회
        let query = r#"
            SELECT creator_fee_rate, curve_protocol_fee_rate, dex_protocol_fee_rate
            FROM fee_config
            WHERE token_id = $1
        "#;

        let result = match sqlx::query_as::<_, (i16, i16, i16)>(query)
            .bind(token_id)
            .fetch_optional(&self.postgres.pool)
            .await
        {
            Ok(Some((creator, curve, dex))) => Some(crate::types::FeeInfo {
                creator_fee_rate: creator,
                curve_protocol_fee_rate: curve,
                dex_protocol_fee_rate: dex,
            }),
            Ok(None) => None,
            Err(e) => {
                warn!(
                    "fee_config 조회 실패: token_id={}, error={}",
                    token_id, e
                );
                // 에러는 캐싱하지 않고 즉시 반환 (다음 호출이 곧바로 재시도하도록)
                return None;
            }
        };

        // 3. 캐시 갱신 (Some/None 둘 다 저장하되 TTL이 다름)
        self.fee_info_cache
            .insert(token_id.to_string(), (result.clone(), Instant::now()));

        result
    }

    /// Market volume을 증가시킵니다
    ///
    /// # Arguments
    /// * `token_id` - 토큰 ID
    /// * `increment` - 증가시킬 volume 값 (BigDecimal, wei 단위)
    pub async fn increment_market_volume(
        &self,
        token_id: &str,
        increment: &BigDecimal,
    ) -> Result<()> {
        // LocalStore에 market이 없으면 DB에서 먼저 로드
        if !self.local_store.exists_market(token_id) {
            debug!(
                "Market not found in LocalStore for volume increment, loading from DB: {}",
                token_id
            );
            // get_market_info 호출하면 DB에서 가져와서 로컬에 캐싱됨
            if let Err(e) = self.get_market_info(token_id).await {
                warn!(
                    "Failed to load market from DB for volume increment: {} - {}",
                    token_id, e
                );
                return Ok(()); // DB에도 없으면 skip
            }
        }

        // LocalStore에서 volume 증가 (Wei 단위 그대로)
        self.local_store.increment_volume(token_id, increment);

        Ok(())
    }

    /// graduated 이벤트로부터 TokenInfo와 MarketInfo 캐시 업데이트
    ///
    /// graduated 이벤트가 발생하면 (Curve → DEX 전환):
    /// 1. TokenInfo의 is_graduated을 true로 업데이트
    /// 2. MarketInfo의 is_graduated를 true로 업데이트
    ///
    /// # Arguments
    /// * `graduated` - graduated 이벤트 데이터
    ///
    /// # Returns
    /// * `Result<()>` - 성공 시 Ok(()), 실패 시 Error
    pub async fn update_cache_from_graduate(
        &self,
        graduate: &crate::types::stream::Graduate,
    ) -> Result<()> {
        // TokenInfo 조회
        let token_info_result = self.get_token_info(&graduate.token).await;

        match token_info_result {
            Ok(mut token_info) => {
                // TokenInfo 업데이트: is_graduated = true
                token_info.is_graduated = true;

                // TokenInfo 저장
                self.set_token_info(&graduate.token, &token_info).await?;

                debug!(
                    "TokenInfo updated from graduate: token={}, pool={}, is_graduated=true",
                    graduate.token, graduate.pool
                );
            }
            Err(e) => {
                debug!(
                    "TokenInfo not found for token: {}. Error: {}.",
                    graduate.token, e
                );
            }
        }

        // LocalStore에 market이 없으면 DB에서 먼저 로드
        if !self.local_store.exists_market(&graduate.token) {
            debug!(
                "Market not found in LocalStore for graduate, loading from DB: {}",
                graduate.token
            );
            if let Err(e) = self.get_market_info(&graduate.token).await {
                warn!(
                    "Failed to load market from DB for graduate: {} - {}",
                    graduate.token, e
                );
                return Ok(()); // DB에도 없으면 skip
            }
        }

        // graduate 시점에 fee_info 재조회 (V2 토큰의 경우 fee_config 행이 CreateCurve
        // 처리 시점엔 아직 인덱싱 안 됐을 수 있어 None으로 캐싱된 상태일 수 있음).
        // V1은 fee_info 자체가 없으므로 Curve 일 때만 시도.
        let current_market_type = self
            .local_store
            .get_market(&graduate.token)
            .map(|m| m.market_type);
        let refreshed_fee_info = if matches!(current_market_type, Some(MarketType::Curve)) {
            self.get_fee_info(&graduate.token).await
        } else {
            None
        };

        // LocalStore의 market 데이터 업데이트:
        // - is_graduated = true, market_id = pool
        // - market_type: Curve → Dex (graduated 후 socket 응답이 여전히
        //   Curve로 내려가던 버그 수정)
        // - fee_info: 재조회 결과로 갱신 (None이었던 캐시를 채워줌)
        self.local_store.update_market(&graduate.token, |data| {
            data.is_graduated = true;
            data.market_id = graduate.pool.clone();
            data.market_type = MarketType::Dex;
            if let Some(fee_info) = refreshed_fee_info.clone() {
                data.fee_info = Some(fee_info);
            }
        });

        debug!(
            "Market cache updated from graduate: token={}, market_id={}, market_type updated, is_graduated=true",
            graduate.token, graduate.pool
        );

        Ok(())
    }

    //-------------------------------------------------------------------------
    // Metrics 관련 메서드들 (Redis Sorted Set 사용)
    //-------------------------------------------------------------------------

    /// Buy 이벤트 처리 - Redis Sorted Set에 swap 데이터 저장
    pub async fn update_metrics_on_buy(&self, buy: &Buy) -> Result<()> {
        // market에서 quote_id 조회 (없으면 WETH fallback)
        let quote_id = self.get_market_quote_id(&buy.token);
        self.update_metrics_on_swap(
            &buy.token,
            buy.block_timestamp as i64,
            true,
            &buy.amount_in,
            &buy.account_id,
            &quote_id,
            buy.block_number as i64,
        )
        .await
    }

    /// Sell 이벤트 처리 - Redis Sorted Set에 swap 데이터 저장
    pub async fn update_metrics_on_sell(&self, sell: &Sell) -> Result<()> {
        let quote_id = self.get_market_quote_id(&sell.token);
        self.update_metrics_on_swap(
            &sell.token,
            sell.block_timestamp as i64,
            false,
            &sell.amount_out,
            &sell.account_id,
            &quote_id,
            sell.block_number as i64,
        )
        .await
    }

    /// CurveSync 이벤트 처리 - Redis Sorted Set에 price 데이터 저장
    pub async fn update_metrics_on_sync(&self, sync: &CurveSync) -> Result<()> {
        // sync.price는 이미 stream.rs에서 계산됨 (virtual_native_amount / virtual_token_amount)
        self.update_metrics_price(
            &sync.token,
            sync.block_timestamp as i64,
            sync.block_number as i64,
            &sync.price,
            sync.transaction_index as i32,
            sync.log_index as i64,
        )
        .await
    }

    /// CreateCurve 이벤트 처리 - 초기 price 데이터 저장
    /// 새 토큰 생성 시 virtual reserve 기준 초기 가격을 metrics에 저장합니다.
    /// 이후 같은 timestamp의 Sync price가 더 늦은 block/tx/log 순서로 들어오면
    /// LocalStore가 최신 chain order 기준으로 덮어씁니다.
    pub async fn init_metrics_from_create_curve(
        &self,
        create_curve: &crate::types::stream::CreateCurve,
    ) -> Result<()> {
        use bigdecimal::RoundingMode;

        // 초기 가격 계산: virtual_native / virtual_token
        let price = (&create_curve.virtual_native / &create_curve.virtual_token)
            .with_scale_round(10, RoundingMode::Up);

        // 초기 price 데이터 저장
        self.update_metrics_price(
            &create_curve.token,
            create_curve.block_timestamp as i64,
            create_curve.block_number as i64,
            &price,
            create_curve.transaction_index as i32,
            create_curve.log_index as i64,
        )
        .await?;

        info!(
            "Metrics initialized from CreateCurve: token={}, price={}, timestamp={}",
            create_curve.token,
            price.normalized().to_plain_string(),
            create_curve.block_timestamp
        );

        Ok(())
    }

    /// DexSync 이벤트 처리 - Redis Sorted Set에 price 데이터 저장
    pub async fn update_metrics_on_dex_sync(&self, sync: &DexSync) -> Result<()> {
        // DexSync의 price는 이미 올바르게 계산됨 (MON/TOKEN)
        self.update_metrics_price(
            &sync.token,
            sync.block_timestamp as i64,
            sync.block_number as i64,
            &sync.price,
            sync.transaction_index as i32,
            sync.log_index as i64,
        )
        .await
    }

    /// Swap 데이터를 로컬 메모리에 저장 (내부 메서드)
    /// value = (quote_amount / DECIMALS) * quote_usd_price로 계산하여 저장
    /// 새 데이터 추가 후 25시간+1분 이전 데이터 삭제
    async fn update_metrics_on_swap(
        &self,
        token_id: &str,
        timestamp: i64,
        is_buy: bool,
        native_amount: &BigDecimal,
        account_id: &str,
        quote_id: &str,
        block_number: i64,
    ) -> Result<()> {
        use crate::utils::current_unix_timestamp;

        // 1. Quote price 조회 (quote asset의 USD 가격, block 기준)
        let quote_price = self.get_quote_usd_price(quote_id, block_number).await;

        // 2. value = (quote_amount / DECIMALS) * quote_usd_price 계산
        let value = (native_amount / &*crate::config::DECIMALS) * &quote_price;

        // 3. Swap 데이터 생성
        let swap = SwapSnapshot {
            timestamp,
            is_buy,
            value: value.clone(),
            account_id: account_id.to_string(),
        };

        // 4. 로컬 메모리에 추가
        self.local_store.add_swap(token_id, swap);

        // 5. 25시간+1분 이전 데이터 삭제
        let cutoff = current_unix_timestamp() - (25 * 3600 + 60);
        self.local_store.remove_old_swaps(token_id, cutoff);

        debug!(
            "Metrics swap updated (local): token_id={}, is_buy={}, value={}, timestamp={}",
            token_id, is_buy, value, timestamp
        );

        Ok(())
    }

    /// Price 데이터를 로컬 메모리에 저장 (내부 메서드)
    /// 새 데이터 추가 후 25시간+1분 이전 데이터 삭제
    async fn update_metrics_price(
        &self,
        token_id: &str,
        timestamp: i64,
        block_number: i64,
        price: &BigDecimal,
        tx_index: i32,
        log_index: i64,
    ) -> Result<()> {
        use crate::utils::current_unix_timestamp;

        // 1. Price 데이터 생성 (normalized로 정밀도 정규화)
        let price_snapshot = PriceSnapshot {
            timestamp,
            block_number,
            price: price.normalized(),
            tx_index,
            log_index,
        };

        // 2. 로컬 메모리에 추가
        self.local_store.add_price(token_id, price_snapshot);

        // 3. 25시간+1분 이전 데이터 삭제
        let cutoff = current_unix_timestamp() - (25 * 3600 + 60);
        self.local_store.remove_old_prices(token_id, cutoff);

        debug!(
            "Metrics price updated (local): token_id={}, price={}, timestamp={}",
            token_id,
            price.to_string(),
            timestamp
        );

        Ok(())
    }

    /// 로컬 메모리에 metrics 데이터가 있는지 확인하고, 없으면 DB에서 로드
    /// 25시간 이내 데이터만 로드 (24H start_price는 get_price_change_with_fallback에서 필요시 DB 조회)
    pub async fn ensure_metrics_loaded(&self, token_id: &str) -> Result<()> {
        use crate::{measure_postgres, utils::current_unix_timestamp};

        // 1. DB에서 이미 로드했는지 확인 (exists_metrics가 아닌 db_loaded 플래그 체크)
        if self.local_store.is_metrics_db_loaded(token_id) {
            info!(
                "ensure_metrics_loaded: Metrics for {} already loaded from DB, skipping",
                token_id
            );
            return Ok(());
        }

        // 2. DB에서 25시간 이내 데이터 로드
        info!("Loading metrics for {} from DB", token_id);
        let current_time = current_unix_timestamp();
        let query_since = current_time - 25 * 3600;

        // 병렬로 swap, price_history 로드
        let (swaps_result, prices_result) = tokio::join!(
            // 25시간 이내 swap 데이터
            async {
                measure_postgres!(
                    "metrics_load_swaps",
                    sqlx::query!(
                        r#"
                        SELECT created_at, is_buy, value, account_id
                        FROM swap
                        WHERE token_id = $1 AND created_at >= $2
                        ORDER BY created_at ASC
                        "#,
                        token_id,
                        query_since
                    )
                    .fetch_all(&self.postgres.pool)
                )
            },
            // 25시간 이내 price 데이터
            async {
                measure_postgres!(
                    "metrics_load_prices",
                    sqlx::query!(
                        r#"
                        SELECT created_at, block_number, price, tx_index, log_index
                        FROM price_history
                        WHERE token_id = $1 AND created_at >= $2
                        ORDER BY created_at ASC, block_number ASC, tx_index ASC, log_index ASC
                        "#,
                        token_id,
                        query_since
                    )
                    .fetch_all(&self.postgres.pool)
                )
            }
        );

        let swaps = swaps_result?;
        let prices = prices_result?;

        info!(
            "ensure_metrics_loaded for {}: loaded {} swaps, {} prices from DB (query_since={})",
            token_id,
            swaps.len(),
            prices.len(),
            query_since
        );

        // 3. 로컬 메모리에 저장
        for row in swaps {
            let swap = SwapSnapshot {
                timestamp: row.created_at,
                is_buy: row.is_buy,
                value: row.value,
                account_id: row.account_id,
            };
            self.local_store.add_swap(token_id, swap);
        }

        for row in prices {
            let price_snapshot = PriceSnapshot {
                timestamp: row.created_at,
                block_number: row.block_number,
                price: row.price.normalized(),
                tx_index: row.tx_index,
                log_index: row.log_index as i64,
            };
            self.local_store.add_price(token_id, price_snapshot);
        }

        // 4. DB 로드 완료 플래그 설정
        self.local_store.set_metrics_db_loaded(token_id, true);

        info!("Loaded metrics for {} from DB to local memory", token_id);

        Ok(())
    }

    /// 여러 timeframe에 대한 metrics 계산
    pub async fn get_metrics(
        &self,
        token_id: &str,
        timeframes: Vec<TimeFrame>,
    ) -> Result<Vec<MetricItem>> {
        use crate::utils::current_unix_timestamp;

        // 1. 로컬 메모리에서 데이터 가져오기
        let current_time = current_unix_timestamp();
        let query_since = current_time - 25 * 3600;

        let swaps = self.local_store.get_swaps_since(token_id, query_since);
        let prices = self.local_store.get_prices_since(token_id, query_since);

        tracing::info!(
            "get_metrics (local) for token {}: swaps={}, prices={}, query_since={}",
            token_id,
            swaps.len(),
            prices.len(),
            query_since
        );

        // 2. 각 timeframe별로 계산
        let mut metrics = Vec::with_capacity(timeframes.len());
        for timeframe in timeframes {
            let metric = self
                .calculate_metrics_for_timeframe(token_id, &swaps, &prices, timeframe)
                .await;
            metrics.push(metric);
        }

        Ok(metrics)
    }

    /// 특정 timeframe에 대한 metrics 계산 (내부 메서드)
    async fn calculate_metrics_for_timeframe(
        &self,
        token_id: &str,
        swaps: &[SwapSnapshot],
        prices: &[PriceSnapshot],
        timeframe: TimeFrame,
    ) -> MetricItem {
        use crate::utils::current_unix_timestamp;
        use std::collections::HashSet;

        let period_seconds = timeframe.to_seconds();
        let current_time = current_unix_timestamp();
        let timeframe_ago = current_time - period_seconds;

        // 1. Swap 집계
        let mut buy_count = 0i64;
        let mut sell_count = 0i64;
        let mut buy_volume = BigDecimal::from(0);
        let mut sell_volume = BigDecimal::from(0);
        let mut buy_makers: HashSet<String> = HashSet::new();
        let mut sell_makers: HashSet<String> = HashSet::new();

        for swap in swaps.iter() {
            if swap.timestamp > timeframe_ago {
                if swap.is_buy {
                    buy_count += 1;
                    buy_volume += &swap.value;
                    buy_makers.insert(swap.account_id.clone());
                } else {
                    sell_count += 1;
                    sell_volume += &swap.value;
                    sell_makers.insert(swap.account_id.clone());
                }
            }
        }

        // 2. Price 변화율 계산
        // current_price: current_time 이하 중 가장 최근 것
        let filtered_for_current: Vec<&PriceSnapshot> = prices
            .iter()
            .filter(|p| p.timestamp <= current_time)
            .collect();

        // start_price: Redis에 있으면 사용, 없으면 DB 조회
        let (start_price, current_price) = self
            .get_price_change_with_fallback(
                token_id,
                &filtered_for_current,
                prices,
                timeframe_ago,
                current_time,
            )
            .await;

        tracing::debug!(
            "[METRICS_DEBUG] timeframe={:?}, prices.len={}, timeframe_ago={}, start_price={:?}, current_price={:?}",
            timeframe,
            prices.len(),
            timeframe_ago,
            &start_price,
            &current_price
        );

        let percent = match (&start_price, &current_price) {
            (Some(ref start), Some(ref current)) => {
                calculate_price_change_percent_precise(start, current).unwrap_or(0.0)
            }
            _ => {
                tracing::warn!(
                    "Price percent is 0.0 - timeframe: {:?}, start_price: {:?}, current_price: {:?}",
                    timeframe,
                    &start_price,
                    &current_price
                );
                0.0
            }
        };

        // 3. MetricItem 생성
        let total_volume = &buy_volume + &sell_volume;

        MetricItem {
            timeframe: timeframe.to_string().to_string(),
            percent,
            transactions: TransactionCount {
                buy: buy_count,
                sell: sell_count,
                total: buy_count + sell_count,
            },
            volume: VolumeAmount {
                buy: buy_volume.normalized().to_plain_string(),
                sell: sell_volume.normalized().to_plain_string(),
                total: total_volume.normalized().to_plain_string(),
            },
            makers: MakerCount {
                buy: buy_makers.len() as i64,
                sell: sell_makers.len() as i64,
                total: buy_makers.len() as i64 + sell_makers.len() as i64,
            },
        }
    }

    /// Price 변화율 계산
    /// 로컬 메모리에 25시간 데이터가 항상 유지되므로, 대부분의 경우 로컬 데이터만으로 계산 가능
    /// DB fallback은 예외 상황(서버 재시작 직후 등)을 위한 보험
    async fn get_price_change_with_fallback(
        &self,
        token_id: &str,
        prices_for_current: &[&PriceSnapshot],
        all_prices: &[PriceSnapshot],
        timeframe_ago: i64,
        _current_time: i64,
    ) -> (Option<BigDecimal>, Option<BigDecimal>) {
        // 1. Current price 계산 (항상 로컬 데이터 사용)
        let current_price = self.get_current_price(prices_for_current);

        // 2. Start price 계산 - 로컬 메모리에서 timeframe_ago 이전의 가장 최근 가격
        let start_price = self.get_start_price_from_local(all_prices, timeframe_ago);

        // 3. 로컬에 없는 경우에만 DB fallback (예외 상황)
        let start_price = if start_price.is_some() {
            start_price
        } else {
            tracing::info!(
                "No start_price in local memory for timeframe_ago={}, loading 25h data from DB for token {}",
                timeframe_ago,
                token_id
            );

            // 필요한 데이터:
            // 1. 25시간 이내 데이터 - 메트릭 계산용
            // 2. 25시간 이전의 가장 최근 1개 - 24H start_price용
            let query_since = crate::utils::current_unix_timestamp() - 25 * 3600;

            // 병렬로 두 쿼리 실행
            let (recent_result, oldest_result) = tokio::join!(
                // 1) 25시간 이내 데이터
                sqlx::query!(
                    r#"
                    SELECT price, created_at, block_number, tx_index, log_index
                    FROM price_history
                    WHERE token_id = $1 AND created_at >= $2
                    ORDER BY created_at ASC, block_number ASC, tx_index ASC, log_index ASC
                    "#,
                    token_id,
                    query_since
                )
                .fetch_all(&self.postgres.pool),
                // 2) 25시간 이전의 가장 최근 1개 (24H start_price용)
                sqlx::query!(
                    r#"
                    SELECT price, created_at, block_number, tx_index, log_index
                    FROM price_history
                    WHERE token_id = $1 AND created_at < $2
                    ORDER BY created_at DESC, block_number DESC, tx_index DESC, log_index DESC
                    LIMIT 1
                    "#,
                    token_id,
                    query_since
                )
                .fetch_optional(&self.postgres.pool)
            );

            match (recent_result, oldest_result) {
                (Ok(recent_rows), Ok(oldest_row)) => {
                    tracing::info!(
                        "Loaded {} recent + {} oldest price records from DB for token {}, caching to local memory",
                        recent_rows.len(),
                        if oldest_row.is_some() { 1 } else { 0 },
                        token_id
                    );

                    // 25시간 이내 데이터를 로컬 메모리에 캐싱
                    for row in &recent_rows {
                        let price_snapshot = PriceSnapshot {
                            timestamp: row.created_at,
                            block_number: row.block_number,
                            price: row.price.normalized(),
                            tx_index: row.tx_index,
                            log_index: row.log_index as i64,
                        };
                        self.local_store.add_price(token_id, price_snapshot);
                    }

                    // 25시간 이전 가장 최근 1개도 로컬 메모리에 캐싱 (있으면)
                    if let Some(ref oldest) = oldest_row {
                        let price_snapshot = PriceSnapshot {
                            timestamp: oldest.created_at,
                            block_number: oldest.block_number,
                            price: oldest.price.normalized(),
                            tx_index: oldest.tx_index,
                            log_index: oldest.log_index as i64,
                        };
                        self.local_store.add_price(token_id, price_snapshot);
                    }

                    // timeframe_ago 이전의 가장 최근 가격을 start_price로 반환
                    // 먼저 recent_rows에서 찾고, 없으면 oldest_row 사용
                    let start_from_recent = recent_rows
                        .iter()
                        .filter(|r| r.created_at <= timeframe_ago)
                        .max_by_key(|r| (r.created_at, r.tx_index, r.log_index))
                        .map(|r| r.price.clone());

                    start_from_recent.or_else(|| oldest_row.map(|r| r.price))
                }
                (Err(e), _) | (_, Err(e)) => {
                    tracing::warn!("DB fallback failed for token {}: {}", token_id, e);
                    None
                }
            }
        };

        (start_price, current_price)
    }

    /// PriceSnapshot 비교 함수: timestamp DESC, block_number DESC, tx_index DESC, log_index DESC
    #[inline]
    fn compare_price_snapshots(a: &PriceSnapshot, b: &PriceSnapshot) -> std::cmp::Ordering {
        match a.timestamp.cmp(&b.timestamp) {
            std::cmp::Ordering::Equal => match a.block_number.cmp(&b.block_number) {
                std::cmp::Ordering::Equal => match a.tx_index.cmp(&b.tx_index) {
                    std::cmp::Ordering::Equal => a.log_index.cmp(&b.log_index),
                    other => other,
                },
                other => other,
            },
            other => other,
        }
    }

    /// 로컬 데이터에서 start_price 계산 (내부 메서드)
    /// timeframe_ago 이전의 가장 최근 가격을 반환 (BigDecimal 그대로 반환하여 정밀도 유지)
    /// timeframe_ago 이전 데이터가 없으면 가장 오래된 (처음 생성된) 가격 사용
    fn get_start_price_from_local(
        &self,
        all_prices: &[PriceSnapshot],
        timeframe_ago: i64,
    ) -> Option<BigDecimal> {
        // 디버깅: prices 데이터 확인
        if let (Some(oldest), Some(newest)) = (
            all_prices.iter().min_by_key(|p| p.timestamp),
            all_prices.iter().max_by_key(|p| p.timestamp),
        ) {
            tracing::debug!(
                "[METRICS_DEBUG] get_start_price_from_local: all_prices.len={}, oldest_ts={}, newest_ts={}, timeframe_ago={}, oldest_price={}, newest_price={}",
                all_prices.len(),
                oldest.timestamp,
                newest.timestamp,
                timeframe_ago,
                oldest.price,
                newest.price
            );
        }

        // 1. timeframe_ago 이전의 가장 최근 가격 찾기
        let before_timeframe = all_prices
            .iter()
            .filter(|p| p.timestamp <= timeframe_ago)
            .max_by(|a, b| Self::compare_price_snapshots(a, b))
            .map(|p| p.price.clone());

        // 2. 없으면 가장 오래된 (첫 번째) 가격 사용
        if before_timeframe.is_some() {
            tracing::info!(
                "Using before_timeframe price for timeframe_ago={}",
                timeframe_ago
            );
            before_timeframe
        } else if !all_prices.is_empty() {
            tracing::info!(
                "Using oldest price (fallback) for timeframe_ago={}",
                timeframe_ago
            );
            all_prices
                .iter()
                .min_by(|a, b| Self::compare_price_snapshots(a, b))
                .map(|p| p.price.clone())
        } else {
            tracing::warn!("No prices available for timeframe_ago={}", timeframe_ago);
            None
        }
    }

    /// Current price 계산 (내부 메서드)
    fn get_current_price(&self, prices_for_current: &[&PriceSnapshot]) -> Option<BigDecimal> {
        prices_for_current
            .iter()
            .copied()
            .max_by(|a, b| Self::compare_price_snapshots(a, b))
            .map(|p| p.price.clone())
    }

    /// 서버 시작 시 모든 metrics 로컬 캐시를 초기화
    pub async fn clear_all_metrics() -> Result<()> {
        let cache_manager = Self::instance()?;

        // 로컬 메모리에서 모든 metrics 데이터 삭제
        info!("Clearing all metrics cache from local memory");

        cache_manager.local_store.metrics.clear();

        info!("All metrics cache cleared successfully");
        Ok(())
    }

    /// Check if address is EOA or EIP-7702 delegated EOA
    pub async fn check_is_eoa_or_delegated(&self, address: &str) -> Result<bool> {
        // 1. Check Redis cache (eoa_delegated: key)
        match self.redis.check_is_eoa_or_delegated(address).await {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => {}
            Err(e) => {
                error!("Error checking EOA/delegated: {}", e);
            }
        }

        // 2. RPC getCode
        let client = crate::client::RpcClient::instance()?;
        let addr = address
            .parse::<alloy::primitives::Address>()
            .map_err(|e| anyhow!("Invalid address: {}", e))?;
        let code = client.get_code(addr).await?;
        let is_eoa_or_delegated = code.is_empty()
            || (code.len() == 23 && code[0] == 0xef && code[1] == 0x01 && code[2] == 0x00);

        // 3. Cache in Redis
        if let Err(e) = self
            .redis
            .insert_is_eoa_or_delegated(address, is_eoa_or_delegated)
            .await
        {
            warn!("Failed to cache EOA/delegated: {}", e);
        }
        Ok(is_eoa_or_delegated)
    }

    /// 이벤트로부터 실제 actor를 해석한다.
    ///
    /// 게이트 우선순위 (observer와 동일):
    /// - [A] event_sender == V2_GIFT_VAULT  → GiftVault 즉시 반환
    /// - [B] event_sender == V2_BURN_VAULT  → BurnVault 즉시 반환
    /// - [C] swap_to == V2_BURN_VAULT       → BurnVault 즉시 반환
    /// - [D] swap_to == V2_GIFT_VAULT       → GiftVault 즉시 반환
    /// - [E] event_sender가 EOA/delegated   → event_sender 반환
    /// - [F] receipt 스캔: Transfer.to/from 후보 (0x0/0xdead 거부) → EOA면 반환
    /// - [G] fallback: tx.origin
    /// - [H] last resort: event_sender
    ///
    /// `swap_to`는 V2 NadFunPair `Swap.to`처럼 토큰 수신자 주소가 따로 있는 경우에만 사용.
    /// V2 Curve나 V1 경로처럼 동등 필드가 없는 호출부는 `None`을 넘긴다.
    pub async fn resolve_actor(
        &self,
        tx_hash: &str,
        event_sender: &str,
        token: &str,
        is_buy: bool,
        swap_to: Option<&str>,
    ) -> Result<String> {
        const ZERO: &str = "0x0000000000000000000000000000000000000000";
        const DEAD: &str = "0x000000000000000000000000000000000000dead";

        // 가드 [A]/[B]. Vault 컨트랙트 화이트리스트 (event_sender 기준)
        // GIFT/BURN vault가 buyback/gift 누적 목적으로 buy/sell할 때
        // contract여도 vault 주소 그대로 actor로 인정 (기명 actor)
        // (env가 빈 문자열이면 미매칭 처리)
        if !V2_GIFT_VAULT_ADDRESS.is_empty()
            && event_sender.eq_ignore_ascii_case(&V2_GIFT_VAULT_ADDRESS)
        {
            return Ok(event_sender.to_string());
        }
        if !V2_BURN_VAULT_ADDRESS.is_empty()
            && event_sender.eq_ignore_ascii_case(&V2_BURN_VAULT_ADDRESS)
        {
            return Ok(event_sender.to_string());
        }

        // 가드 [C]/[D]. Vault 컨트랙트 화이트리스트 (swap_to 기준)
        // V2 NadFunPair `Swap.to`처럼 vault가 token 수신자인 경우, msg.sender는
        // Router/Adapter여서 [A]/[B]에 안 걸린다. swap_to로 잡아 actor를 vault로 귀속.
        if let Some(to) = swap_to {
            if !V2_BURN_VAULT_ADDRESS.is_empty()
                && to.eq_ignore_ascii_case(&V2_BURN_VAULT_ADDRESS)
            {
                return Ok(V2_BURN_VAULT_ADDRESS.clone());
            }
            if !V2_GIFT_VAULT_ADDRESS.is_empty()
                && to.eq_ignore_ascii_case(&V2_GIFT_VAULT_ADDRESS)
            {
                return Ok(V2_GIFT_VAULT_ADDRESS.clone());
            }
        }

        // 가드 2. Zero address는 actor 자격 없음 → EOA 체크 skip하고 tx.origin으로 fall through
        let skip_eoa_check = event_sender.eq_ignore_ascii_case(ZERO);

        // 1. Check if event_sender is EOA/delegated (zero일 때는 skip)
        if !skip_eoa_check {
            match self.check_is_eoa_or_delegated(event_sender).await {
                Ok(true) => return Ok(event_sender.to_string()),
                Ok(false) => {}
                Err(e) => {
                    warn!("Failed EOA check for {}: {}", event_sender, e);
                    return Ok(event_sender.to_string());
                }
            }
        }

        // 2. Analyze tx receipt for ERC20 Transfer events
        let client = crate::client::RpcClient::instance()?;
        let hash = tx_hash
            .parse::<alloy::primitives::TxHash>()
            .map_err(|e| anyhow!("Invalid tx_hash: {}", e))?;
        let token_addr = token
            .parse::<alloy::primitives::Address>()
            .map_err(|e| anyhow!("Invalid token: {}", e))?;

        let transfer_sig: alloy::primitives::B256 =
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
                .parse()
                .unwrap();

        if let Ok(Some(receipt)) = client.get_transaction_receipt(hash).await {
            for log in receipt.inner.logs() {
                if log.address() != token_addr {
                    continue;
                }
                if log.topic0() != Some(&transfer_sig) {
                    continue;
                }
                if log.topics().len() < 3 {
                    continue;
                }

                let from_addr =
                    alloy::primitives::Address::from_slice(&log.topics()[1][12..]);
                let to_addr =
                    alloy::primitives::Address::from_slice(&log.topics()[2][12..]);
                let candidate = if is_buy {
                    to_addr.to_string()
                } else {
                    from_addr.to_string()
                };

                // 가드 2 보강: zero/dead address는 actor 후보에서 제외
                // (둘 다 코드 비어있어 EOA 체크가 true가 되지만 실제 사용자가 아님.
                //  vault buyback이 token을 0xdead로 burn할 때 후보가 0xdead로 새는 걸 차단)
                if candidate.eq_ignore_ascii_case(ZERO)
                    || candidate.eq_ignore_ascii_case(DEAD)
                {
                    continue;
                }

                if let Ok(true) = self.check_is_eoa_or_delegated(&candidate).await {
                    return Ok(candidate);
                }
            }
        }

        // 3. Fallback: tx.origin
        if let Ok(Some(tx)) = client.get_transaction_by_hash(hash).await {
            return Ok(tx.inner.signer().to_string());
        }
        Ok(event_sender.to_string())
    }
}
