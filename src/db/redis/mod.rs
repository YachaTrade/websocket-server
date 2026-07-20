use std::{env, sync::Arc};

use anyhow::{anyhow, Result};
use redis::{aio::ConnectionManager, AsyncCommands, Client, FromRedisValue};

use crate::metrics::measure_redis;
use crate::types::AccountInfo;
use crate::{error_log, types::TokenInfo};
use once_cell::sync::OnceCell;
use tracing::{debug, info};

use crate::config::{
    ACCOUNT_INFO_EXPIRATION, LATEST_PRICE_EXPIRATION, MARKET_INFO_EXPIRATION,
    REDIS_COMMAND_TIMEOUT_MS, TOKEN_INFO_EXPIRATION, WHITELIST_EXPIRATION,
};

// Redis에 저장할 데이터 유형별 키 접두사 (v5: 모든 키에 instance_id 추가 - ECS multi-region 지원)
const PREFIX_WHITE_LIST_TOKEN: &str = "white_list_token:";
const PREFIX_WHITE_LIST_POOL: &str = "white_list_pool:";

const PREFIX_TOKEN_CURVE: &str = "token_curve:";
const PREFIX_TOKEN_DEV: &str = "token_dev:";
const PREFIX_TOKEN_POOL: &str = "token_pool:";
const PREFIX_TOKEN_PAIR: &str = "token_pair:";

const PREFIX_ACCOUNT_INFO: &str = "account_info:";
const PREFIX_TOKEN_INFO: &str = "token_info:";

const PREFIX_TOKEN_TOTAL_SUPPLY: &str = "token_total_supply:";
const PREFIX_LATEST_PRICE: &str = "latest_price:";
const PREFIX_EOA_DELEGATED: &str = "eoa_delegated:";

/// 글로벌 Redis 키 prefix (env `REDIS_KEY_PREFIX`)를 키 앞에 prepend.
///
/// prefix 미설정이면 그대로 반환 (zero-cost). 같은 Redis 인스턴스에서
/// 여러 배포가 공존할 때만 사용됨.
fn with_prefix(key: String) -> String {
    let prefix = crate::config::redis_key_prefix();
    if prefix.is_empty() {
        key
    } else {
        format!("{}{}", prefix, key)
    }
}

static REDIS_DB: OnceCell<Arc<RedisDatabase>> = OnceCell::new();

/// Redis database wrapper for caching blockchain data
/// 블록체인 데이터 캐싱을 위한 Redis 데이터베이스 래퍼
#[derive(Clone)]
pub struct RedisDatabase {
    conn: Arc<ConnectionManager>,
}

impl RedisDatabase {
    pub async fn init() -> Result<()> {
        if REDIS_DB.get().is_some() {
            info!("RedisDatabase already initialized");
            return Ok(());
        }

        let instance = Self::new().await;
        let arc_instance = Arc::new(instance);

        if REDIS_DB.set(arc_instance).is_err() {
            info!("RedisDatabase was initialized by another task");
        } else {
            info!("RedisDatabase global instance initialized successfully");
        }

        Ok(())
    }

    /// 글로벌 인스턴스 가져오기
    pub fn instance() -> Result<Arc<RedisDatabase>> {
        REDIS_DB.get().map(Arc::clone).ok_or_else(|| {
            anyhow!("RedisDatabase not initialized. Call RedisDatabase::init() first")
        })
    }

    /// Creates a new Redis database connection
    /// Redis 데이터베이스 연결을 생성합니다
    pub async fn new() -> Self {
        let url = env::var("REDIS_URL")
            .unwrap_or_else(|_| panic!("REDIS_URL must be set in environment variables"));

        // Create Redis client - timeout will be handled at command level
        let client = Client::open(url.clone()).expect("Failed to create Redis client");

        info!(
            "Redis command timeout configured: {}ms",
            *REDIS_COMMAND_TIMEOUT_MS
        );

        // Create connection manager for automatic reconnection
        info!("Creating Redis connection manager...");
        let conn = ConnectionManager::new(client)
            .await
            .expect("Failed to create Redis connection manager");

        info!("Redis connection established with ElastiCache");

        RedisDatabase {
            conn: Arc::new(conn),
        }
    }

    /// Gets a connection manager reference
    /// 연결 매니저 참조를 가져옵니다
    fn get_conn(&self) -> ConnectionManager {
        (*self.conn).clone()
    }

    /// Redis 캐시 초기화.
    ///
    /// - `REDIS_KEY_PREFIX` 미설정: FLUSHALL (기존 동작) — DB 전체 비움.
    /// - `REDIS_KEY_PREFIX` 설정: SCAN + DEL로 본 인스턴스 prefix 매칭 키만 삭제.
    ///   같은 Redis를 다른 배포와 공유할 때 prod 데이터를 안 건드리도록.
    pub async fn flush_all(&self) -> Result<()> {
        let mut conn = self.get_conn();
        let prefix = crate::config::redis_key_prefix();

        if prefix.is_empty() {
            redis::cmd("FLUSHALL")
                .query_async::<()>(&mut conn)
                .await
                .map_err(|e| anyhow!("Failed to flush Redis: {}", e))?;
            info!("Redis FLUSHALL completed");
            return Ok(());
        }

        // prefix 매칭 키만 SCAN+DEL (cursor 기반 비차단 순회)
        let pattern = format!("{}*", prefix);
        info!("Redis prefix-scoped flush start: pattern={}", pattern);

        let mut cursor: u64 = 0;
        let mut total_deleted: usize = 0;
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut conn)
                .await
                .map_err(|e| anyhow!("Failed to SCAN Redis: {}", e))?;

            if !keys.is_empty() {
                let n = keys.len();
                redis::cmd("DEL")
                    .arg(&keys)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(|e| anyhow!("Failed to DEL during prefix flush: {}", e))?;
                total_deleted += n;
            }

            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        info!(
            "Redis prefix-scoped flush completed: pattern={}, deleted={}",
            pattern, total_deleted
        );
        Ok(())
    }

    /// TTL 갱신 헬퍼 메서드
    async fn refresh_ttl(&self, key: &str) -> Result<()> {
        let mut conn = self.get_conn();

        measure_redis!(
            "refresh_ttl",
            conn.expire::<_, ()>(key, *WHITELIST_EXPIRATION as i64)
        )
        .map_err(|e| {
            error_log!("Failed to refresh TTL for key {}: {}", key, e);
            anyhow!("Failed to refresh TTL: {}", e)
        })?;

        debug!("TTL refreshed for key: {}", key);
        Ok(())
    }

    /// Sets block timestamp with 10 second expiration
    /// 블록 타임스탬프를 10초 만료시간과 함께 설정합니다
    pub async fn set_block_timestamp(&self, block_number: u64, timestamp: u64) -> Result<()> {
        let mut conn = self.get_conn();

        measure_redis!(
            "set_block_timestamp",
            conn.set_ex::<String, u64, ()>(
                with_prefix(format!("block:{}:timestamp", block_number)),
                timestamp,
                10,
            )
        )
        .map_err(|e| {
            error_log!("Failed to set timestamp in Redis: {}", e);
            anyhow!("Failed to set timestamp in Redis: {}", e)
        })
    }

    /// Gets cached block timestamp if available
    /// 캐시된 블록 타임스탬프를 조회합니다
    pub async fn get_block_timestamp(&self, block_number: u64) -> Result<Option<u64>> {
        let mut conn = self.get_conn();

        measure_redis!(
            "redis_get",
            conn.get(with_prefix(format!("block:{}:timestamp", block_number)))
        )
        .map_err(|e| {
            error_log!("Failed to get timestamp from Redis: {}", e);
            anyhow!("Failed to get timestamp from Redis: {}", e)
        })
    }

    //-------------------------------------------------------------------------
    // 화이트리스트 토큰 관련 메서드들
    //-------------------------------------------------------------------------

    /// 화이트리스트에 토큰 추가
    pub async fn insert_white_list_token(&self, token: &str, is_white: bool) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_WHITE_LIST_TOKEN, instance_id, token));

        measure_redis!(
            "redis_set_ex",
            conn.set_ex::<String, bool, ()>(key, is_white, *WHITELIST_EXPIRATION,)
        )
        .map_err(|e| {
            error_log!("Failed to insert white list token: {}", e);
            anyhow!("Failed to insert white list token: {}", e)
        })?;

        debug!(
            "White list token inserted into Redis: {} = {}",
            token, is_white
        );
        Ok(())
    }

    /// 토큰이 화이트리스트에 있는지 확인
    pub async fn check_white_list_token(&self, token: &str) -> Result<Option<bool>> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_WHITE_LIST_TOKEN, instance_id, token));

        let exists: Option<bool> = measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
            error_log!("Failed to check white list token in Redis: {}", e);
            anyhow!("Failed to check white list token in Redis: {}", e)
        })?;

        // 토큰이 존재하고 true인 경우만 TTL 갱신
        if let Some(is_white) = exists {
            if is_white {
                self.refresh_ttl(&key).await?;
            }
        }

        Ok(exists)
    }

    //-------------------------------------------------------------------------
    // 토큰-개발자 관련 메서드들
    //-------------------------------------------------------------------------

    /// 토큰 개발자 정보 저장
    pub async fn insert_token_dev(&self, token: &str, account: &str) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_DEV, instance_id, token));

        measure_redis!(
            "redis_set_ex",
            conn.set_ex::<String, String, ()>(key, account.to_string(), *WHITELIST_EXPIRATION,)
        )
        .map_err(|e| {
            error_log!("Failed to insert token dev: {}", e);
            anyhow!("Failed to insert token dev: {}", e)
        })?;

        debug!(
            "Token dev mapping stored in Redis: token={}, account={}",
            token, account
        );
        Ok(())
    }

    /// 계정이 토큰의 개발자인지 확인
    pub async fn check_token_dev(&self, token: &str, account: &str) -> Result<bool> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_DEV, instance_id, token));

        let dev: Option<String> = measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
            error_log!("Failed to get token dev from Redis: {}", e);
            anyhow!("Failed to get token dev from Redis: {}", e)
        })?;

        // 개발자가 맞을 경우에만 TTL 갱신
        if let Some(ref stored_dev) = dev {
            if stored_dev == account {
                self.refresh_ttl(&key).await?;
            }
        }

        Ok(dev.is_some_and(|d| d == account))
    }

    /// 토큰 개발자 정보 조회
    pub async fn get_token_dev(&self, token: &str) -> Result<Option<String>> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_DEV, instance_id, token));

        let dev: Option<String> = measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
            error_log!("Failed to get token dev from Redis: {}", e);
            anyhow!("Failed to get token dev from Redis: {}", e)
        })?;

        // 데이터가 존재하면 TTL 갱신
        if dev.is_some() {
            self.refresh_ttl(&key).await?;
        }

        Ok(dev)
    }

    //-------------------------------------------------------------------------
    // 화이트리스트 POOL 관련 메서드들
    //-------------------------------------------------------------------------

    /// 화이트리스트에 POOL 추가
    pub async fn insert_white_list_pool(&self, pool: &str, is_white: bool) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_WHITE_LIST_POOL, instance_id, pool));

        measure_redis!(
            "redis_set_ex",
            conn.set_ex::<String, bool, ()>(key, is_white, *WHITELIST_EXPIRATION,)
        )
        .map_err(|e| {
            error_log!("Failed to insert white list pool: {}", e);
            anyhow!("Failed to insert white list pool: {}", e)
        })?;

        debug!(
            "White list pool inserted into Redis: {} = {}",
            pool, is_white
        );
        Ok(())
    }

    /// POOL가 화이트리스트에 있는지 확인
    pub async fn check_white_list_pool(&self, pool: &str) -> Result<Option<bool>> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_WHITE_LIST_POOL, instance_id, pool));

        let exists: Option<bool> = measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
            error_log!("Failed to check white list pool in Redis: {}", e);
            anyhow!("Failed to check white list pool in Redis: {}", e)
        })?;

        // 풀이 존재하고 true인 경우만 TTL 갱신
        if let Some(is_white) = exists {
            if is_white {
                self.refresh_ttl(&key).await?;
            }
        }

        Ok(exists)
    }

    //-------------------------------------------------------------------------
    // 토큰-POOL 관련 메서드들
    //-------------------------------------------------------------------------

    /// 토큰-POOL 관계 저장
    pub async fn insert_token_pool(&self, token: &str, pool: &str) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_POOL, instance_id, token));

        measure_redis!(
            "redis_set_ex",
            conn.set_ex::<String, String, ()>(key, pool.to_string(), *WHITELIST_EXPIRATION,)
        )
        .map_err(|e| {
            error_log!("Failed to insert token pool: {}", e);
            anyhow!("Failed to insert token pool: {}", e)
        })?;

        debug!(
            "Token pool mapping stored in Redis: token={}, pool={}",
            token, pool
        );
        Ok(())
    }

    /// 토큰에 대한 POOL 정보 조회
    pub async fn get_token_pool(&self, token: &str) -> Result<Option<String>> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_POOL, instance_id, token));

        let pool: Option<String> = measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
            error_log!("Failed to get token pool from Redis: {}", e);
            anyhow!("Failed to get token pool from Redis: {}", e)
        })?;

        // 데이터가 존재하면 TTL 갱신
        if pool.is_some() {
            self.refresh_ttl(&key).await?;
        }

        Ok(pool)
    }

    //-------------------------------------------------------------------------
    // POOL 페어 관련 메서드들
    //-------------------------------------------------------------------------

    /// POOL 페어 정보 저장 (token0, token1)
    pub async fn insert_pool_pair(&self, pool: &str, token0: &str, token1: &str) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_PAIR, instance_id, pool));
        let pair_data = format!("{}:{}", token0, token1);

        measure_redis!(
            "redis_set_ex",
            conn.set_ex::<String, String, ()>(key, pair_data, *WHITELIST_EXPIRATION,)
        )
        .map_err(|e| {
            error_log!("Failed to insert pool pair: {}", e);
            anyhow!("Failed to insert pool pair: {}", e)
        })?;

        debug!(
            "Pool pair stored in Redis: pool={}, token0={}, token1={}",
            pool, token0, token1
        );
        Ok(())
    }

    /// POOL 페어 정보 조회
    pub async fn get_pool_pair(&self, pool: &str) -> Result<Option<(String, String)>> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_PAIR, instance_id, pool));

        let pair_data: Option<String> =
            measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
                error_log!("Failed to get pool pair from Redis: {}", e);
                anyhow!("Failed to get pool pair from Redis: {}", e)
            })?;

        // 데이터가 존재하면 TTL 갱신
        if pair_data.is_some() {
            self.refresh_ttl(&key).await?;
        }

        match pair_data {
            Some(data) => {
                let parts: Vec<&str> = data.split(':').collect();
                if parts.len() == 2 {
                    Ok(Some((parts[0].to_string(), parts[1].to_string())))
                } else {
                    error_log!("Invalid pool pair format in Redis: {}", data);
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    //-------------------------------------------------------------------------
    // 유틸리티 메서드들
    //-------------------------------------------------------------------------

    /// Redis 연결 풀 상태 조회
    pub fn get_pool_status(&self) -> String {
        "Redis Connection Manager: Connected".to_string()
    }

    /// 특정 패턴의 키 개수 조회
    pub async fn count_keys(&self, pattern: &str) -> Result<usize> {
        let mut conn = self.get_conn();
        let keys: Vec<String> = measure_redis!("redis_keys", conn.keys(pattern))?;
        Ok(keys.len())
    }

    /// 캐시 통계 조회
    pub async fn get_cache_stats(&self) -> Result<CacheStats> {
        let instance_id = crate::config::get_instance_id();
        Ok(CacheStats {
            white_list_tokens: self
                .count_keys(&with_prefix(format!(
                    "{}{}:*",
                    PREFIX_WHITE_LIST_TOKEN, instance_id
                )))
                .await?,
            white_list_pools: self
                .count_keys(&with_prefix(format!(
                    "{}{}:*",
                    PREFIX_WHITE_LIST_POOL, instance_id
                )))
                .await?,
            token_curves: self
                .count_keys(&with_prefix(format!(
                    "{}{}:*",
                    PREFIX_TOKEN_CURVE, instance_id
                )))
                .await?,
            token_devs: self
                .count_keys(&with_prefix(format!(
                    "{}{}:*",
                    PREFIX_TOKEN_DEV, instance_id
                )))
                .await?,
            token_pools: self
                .count_keys(&with_prefix(format!(
                    "{}{}:*",
                    PREFIX_TOKEN_POOL, instance_id
                )))
                .await?,
            pool_pairs: self
                .count_keys(&with_prefix(format!(
                    "{}{}:*",
                    PREFIX_TOKEN_PAIR, instance_id
                )))
                .await?,
            pool_status: self.get_pool_status(),
        })
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub white_list_tokens: usize,
    pub white_list_pools: usize,
    pub token_curves: usize,
    pub token_devs: usize,
    pub token_pools: usize,
    pub pool_pairs: usize,
    pub pool_status: String,
}

impl RedisDatabase {
    pub async fn insert_account_info(
        &self,
        account_id: &str,
        account_info: &AccountInfo,
    ) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_ACCOUNT_INFO, instance_id, account_id));
        let json = serde_json::to_string(account_info)?;
        measure_redis!(
            "redis_pset_ex",
            conn.pset_ex::<_, _, ()>(key, json, *ACCOUNT_INFO_EXPIRATION)
        )
        .map_err(|e| {
            error_log!("Failed to insert account info in Redis: {}", e);
            anyhow!("Failed to insert account info in Redis: {}", e)
        })?;

        debug!(
            "Account info inserted into Redis: {}",
            account_info.account_id
        );
        Ok(())
    }

    pub async fn get_account_info(&self, account_id: &str) -> Result<AccountInfo> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_ACCOUNT_INFO, instance_id, account_id));

        // Redis에서 값 조회 (타임아웃 적용)
        let response: Option<String> =
            measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
                error_log!("Failed to get account info from Redis: {}", e);
                anyhow!("Failed to get account info from Redis: {}", e)
            })?;

        // 값이 없는 경우 오류 반환
        let json = match response {
            Some(json_str) => json_str,
            None => {
                return Err(anyhow!(
                    "Account info not found in Redis for account_id: {}",
                    account_id
                ));
            }
        };

        // JSON 문자열을 AccountInfo로 역직렬화
        let account_info = serde_json::from_str::<AccountInfo>(&json).map_err(|e| {
            error_log!("Failed to deserialize account info from JSON: {}", e);
            anyhow!("Failed to deserialize account info from JSON: {}", e)
        })?;

        Ok(account_info)
    }

    pub async fn get_token_info(&self, token_id: &str) -> Result<TokenInfo> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_INFO, instance_id, token_id));

        // Redis에서 값 조회 (타임아웃 적용)
        let response: Option<String> =
            measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
                error_log!("Failed to get token info from Redis: {}", e);
                anyhow!("Failed to get token info from Redis: {}", e)
            })?;

        // 값이 없는 경우 오류 반환
        let json = match response {
            Some(json_str) => json_str,
            None => {
                return Err(anyhow!(
                    "Token info not found in Redis for token_id: {}",
                    token_id
                ));
            }
        };

        // JSON 문자열을 TokenInfo로 역직렬화
        let token_info = serde_json::from_str::<TokenInfo>(&json).map_err(|e| {
            error_log!("Failed to deserialize token info from JSON: {}", e);
            anyhow!("Failed to deserialize token info from JSON: {}", e)
        })?;

        self.refresh_ttl(&key).await?;

        Ok(token_info)
    }

    pub async fn set_token_info(&self, token_id: &str, token_info: &TokenInfo) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_INFO, instance_id, token_id));
        let json = serde_json::to_string(token_info)?;

        measure_redis!(
            "redis_pset_ex",
            conn.pset_ex::<_, _, ()>(key, json, *TOKEN_INFO_EXPIRATION)
        )
        .map_err(|e| {
            error_log!("Failed to set token info in Redis: {}", e);
            anyhow!("Failed to set token info in Redis: {}", e)
        })?;

        debug!(
            "Token info inserted into Redis: token_id={}, symbol={}",
            token_info.token_id, token_info.symbol
        );
        Ok(())
    }

    /// Redis에서 최신 가격 조회
    pub async fn get_latest_price(&self) -> Result<String> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:", PREFIX_LATEST_PRICE, instance_id));

        // Redis에서 값 조회
        let response: Option<String> =
            measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
                error_log!("Failed to get latest price from Redis: {}", e);
                anyhow!("Failed to get latest price from Redis: {}", e)
            })?;

        // 값이 없는 경우 오류 반환
        match response {
            Some(price) => Ok(price),
            None => Err(anyhow!("Latest price not found in Redis")),
        }
    }

    /// Redis에 최신 가격 저장
    pub async fn set_latest_price(&self, price: &str) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:", PREFIX_LATEST_PRICE, instance_id));

        measure_redis!(
            "redis_pset_ex",
            conn.pset_ex::<_, _, ()>(key, price, *LATEST_PRICE_EXPIRATION)
        )
        .map_err(|e| {
            error_log!("Failed to set latest price in Redis: {}", e);
            anyhow!("Failed to set latest price in Redis: {}", e)
        })?;

        debug!("Latest price inserted into Redis: {}", price);
        Ok(())
    }

    /// Redis Hash에서 필드 값 조회 (HGET)
    pub async fn hget<T: FromRedisValue>(&self, key: &str, field: &str) -> Result<T> {
        let mut conn = self.get_conn();
        measure_redis!("redis_hget", conn.hget(key, field)).map_err(|e| {
            error_log!(
                "Failed to hget from Redis: key={}, field={}, error={}",
                key,
                field,
                e
            );
            anyhow!("Failed to hget from Redis: {}", e)
        })
    }

    /// Redis에서 토큰 total_supply 조회
    pub async fn get_token_total_supply(&self, token_id: &str) -> Result<String> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_TOTAL_SUPPLY, instance_id, token_id));

        // Redis에서 값 조회
        let response: Option<String> =
            measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
                error_log!("Failed to get token total_supply from Redis: {}", e);
                anyhow!("Failed to get token total_supply from Redis: {}", e)
            })?;

        // 값이 없는 경우 오류 반환
        match response {
            Some(total_supply) => {
                // TTL 갱신
                self.refresh_ttl(&key).await?;
                Ok(total_supply)
            }
            None => Err(anyhow!(
                "Token total_supply not found in Redis for token_id: {}",
                token_id
            )),
        }
    }

    /// Redis에 토큰 total_supply 저장
    pub async fn set_token_total_supply(&self, token_id: &str, total_supply: &str) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_TOKEN_TOTAL_SUPPLY, instance_id, token_id));

        measure_redis!(
            "redis_pset_ex",
            conn.pset_ex::<_, _, ()>(key, total_supply, *MARKET_INFO_EXPIRATION)
        )
        .map_err(|e| {
            error_log!("Failed to set token total_supply in Redis: {}", e);
            anyhow!("Failed to set token total_supply in Redis: {}", e)
        })?;

        debug!(
            "Token total_supply inserted into Redis: token_id={}, total_supply={}",
            token_id, total_supply
        );
        Ok(())
    }

    //-------------------------------------------------------------------------
    // Generic get/set 메서드들
    //-------------------------------------------------------------------------

    /// 일반 문자열 GET
    pub async fn get<T: FromRedisValue>(&self, key: &str) -> Result<Option<T>> {
        let mut conn = self.get_conn();

        let result: Option<T> = measure_redis!("redis_get", conn.get(key)).map_err(|e| {
            error_log!("Failed to get key {}: {}", key, e);
            anyhow!("Failed to get key {}: {}", key, e)
        })?;

        Ok(result)
    }

    /// 일반 문자열 SET with expiration (초 단위)
    pub async fn set_ex(&self, key: &str, value: &str, seconds: u64) -> Result<()> {
        let mut conn = self.get_conn();

        measure_redis!("redis_setex", conn.set_ex::<_, _, ()>(key, value, seconds)).map_err(
            |e| {
                error_log!("Failed to setex key {}: {}", key, e);
                anyhow!("Failed to setex key {}: {}", key, e)
            },
        )?;

        Ok(())
    }

    /// EOA or delegated EOA 캐시 저장
    pub async fn insert_is_eoa_or_delegated(&self, address: &str, result: bool) -> Result<()> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_EOA_DELEGATED, instance_id, address));

        measure_redis!(
            "redis_set_ex",
            conn.set_ex::<String, bool, ()>(key, result, *WHITELIST_EXPIRATION)
        )
        .map_err(|e| {
            error_log!("Failed to insert eoa_delegated: {}", e);
            anyhow!("Failed to insert eoa_delegated: {}", e)
        })?;

        debug!("EOA/delegated cached in Redis: {} = {}", address, result);
        Ok(())
    }

    /// EOA or delegated EOA 캐시 조회
    pub async fn check_is_eoa_or_delegated(&self, address: &str) -> Result<Option<bool>> {
        let mut conn = self.get_conn();
        let instance_id = crate::config::get_instance_id();
        let key = with_prefix(format!("{}{}:{}", PREFIX_EOA_DELEGATED, instance_id, address));

        let exists: Option<bool> = measure_redis!("redis_get", conn.get(&key)).map_err(|e| {
            error_log!("Failed to check eoa_delegated in Redis: {}", e);
            anyhow!("Failed to check eoa_delegated in Redis: {}", e)
        })?;

        Ok(exists)
    }
}
