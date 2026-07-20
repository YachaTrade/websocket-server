use bigdecimal::BigDecimal;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use std::{env, str::FromStr};
use uuid::Uuid;

/// 서버 인스턴스 고유 ID (서버 시작 시 생성되는 UUID)
/// Redis key에 포함되어 여러 ECS task 간 데이터 충돌 방지
static INSTANCE_ID: OnceCell<String> = OnceCell::new();

/// 서버 인스턴스 ID 초기화 (main에서 한 번만 호출)
pub fn init_instance_id() -> String {
    let id = Uuid::new_v4().to_string();
    INSTANCE_ID
        .set(id.clone())
        .expect("INSTANCE_ID already initialized");
    tracing::info!("🆔 Server Instance ID initialized: {}", id);
    id
}

/// 서버 인스턴스 ID 가져오기
pub fn get_instance_id() -> &'static str {
    INSTANCE_ID
        .get()
        .expect("INSTANCE_ID not initialized. Call init_instance_id() first")
}

/// 모든 Redis 키 앞에 붙는 글로벌 prefix.
///
/// 같은 Redis 인스턴스에 여러 배포가 공존할 때 키 충돌을 막기 위해 사용.
/// - env `REDIS_KEY_PREFIX` 미설정/빈 문자열: 빈 문자열 반환 (prefix 없음)
/// - env `REDIS_KEY_PREFIX="giwa"`: `"giwa:"` 반환 (모든 키 앞에 `giwa:` 부착)
///
/// trailing colon 자동 부착이라 호출자는 그대로 prepend만 하면 된다.
pub fn redis_key_prefix() -> &'static str {
    static FORMATTED: OnceCell<String> = OnceCell::new();
    FORMATTED.get_or_init(|| {
        let raw = env::var("REDIS_KEY_PREFIX").unwrap_or_default();
        let trimmed = raw.trim().trim_end_matches(':');
        if trimmed.is_empty() {
            String::new()
        } else {
            format!("{}:", trimmed)
        }
    })
}

lazy_static! {
    pub static ref WETH_ADDRESS: String = env::var("WETH").expect("WETH must be set");
}

// 컨트랙트 주소
lazy_static! {
    pub static ref BONDING_CURVE_ADDRESS: String =
        env::var("BONDING_CURVE").expect("BONDING_CURVE must be set");
    // Vault 주소 (없으면 빈 문자열 — actor 판별에서 미매칭 처리)
    // GIFT/BURN vault가 buyback/gift 누적 목적으로 buy/sell할 때 actor로 인정하기 위해 사용
    pub static ref GIFT_VAULT_ADDRESS: String =
        env::var("GIFT_VAULT").unwrap_or_default();
    pub static ref BURN_VAULT_ADDRESS: String =
        env::var("BURN_VAULT").unwrap_or_default();
}

// Native 토큰 Decimals (10^18)
// USD 가치 계산 시 사용: value = native_amount / DECIMALS * native_price
lazy_static! {
    pub static ref DECIMALS: BigDecimal =
        BigDecimal::from_str("1000000000000000000").unwrap(); // 10^18
}

// BigDecimal 상수: 2^96 (Uniswap V3 가격 계산용)
lazy_static! {
    pub static ref TWO_96: BigDecimal =
        BigDecimal::from(79_228_162_514_264_337_593_543_950_336u128);
}

// /// DB 최소 가격 제한
// lazy_static! {
//     pub static ref MIN_PRICE: BigDecimal = BigDecimal::from_str("0.0000838").unwrap();
// }

lazy_static! {
    pub static ref MIN_PRICE: BigDecimal = BigDecimal::from_str("0.0000000009").unwrap();
}

#[derive(Debug, Clone)]
pub struct ChartConfig {
    pub chart_type: Vec<String>,
}

impl Default for ChartConfig {
    fn default() -> Self {
        Self {
            chart_type: vec![
                "1m".to_string(),
                "5m".to_string(),
                "15m".to_string(),
                "30m".to_string(),
                "1h".to_string(),
                "4h".to_string(),
                "1d".to_string(),
                "1w".to_string(),
            ],
        }
    }
}

impl ChartConfig {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct RedisEnv {
    pub redis_url: String,
}

impl Default for RedisEnv {
    fn default() -> Self {
        RedisEnv {
            redis_url: env::var("REDIS_URL").expect("REDIS_URL must be set"),
        }
    }
}

impl RedisEnv {
    pub fn new() -> Self {
        Self::default()
    }
}

lazy_static! {
    pub static ref DEFAULT_DELAY: u64 = env::var("DEFAULT_DELAY")
        .expect("DEFAULT_DELAY must be set")
        .parse()
        .expect("DEFAULT_DELAY must be a number");
}

lazy_static! {
    /// Stream timeout in milliseconds (기본값: 1800000ms = 30분)
    pub static ref STREAM_TIMEOUT: u64 = env::var("STREAM_TIMEOUT")
        .unwrap_or_else(|_| "1800000".to_string())
        .parse()
        .expect("STREAM_TIMEOUT must be a number");
}

lazy_static! {
    pub static ref RPC_TIME_OUT: u64 = env::var("RPC_TIME_OUT")
        .expect("RPC_TIME_OUT must be set")
        .parse()
        .expect("RPC_TIME_OUT must be a number");
}

lazy_static! {
    pub static ref METRICS_REPORT_INTERVAL: u64 = env::var("METRICS_REPORT_INTERVAL")
        .expect("METRICS_REPORT_INTERVAL must be set")
        .parse()
        .expect("METRICS_REPORT_INTERVAL must be a number");
}

// 채널 버퍼 크기 설정
lazy_static! {
    pub static ref CHANNEL_SIZE: usize = env::var("CHANNEL_SIZE")
        .unwrap_or_else(|_| "1000".to_string())
        .parse()
        .expect("CHANNEL_SIZE must be a number");
}

// Redis 캐시 만료 시간 설정 (밀리초 단위)
lazy_static! {
    pub static ref WHITELIST_EXPIRATION: u64 = env::var("WHITELIST_EXPIRATION")
        .expect("WHITELIST_EXPIRATION must be set")
        .parse()
        .expect("WHITELIST_EXPIRATION must be a number");
    pub static ref ACCOUNT_INFO_EXPIRATION: u64 = env::var("ACCOUNT_INFO_EXPIRATION")
        .expect("ACCOUNT_INFO_EXPIRATION must be set")
        .parse()
        .expect("ACCOUNT_INFO_EXPIRATION must be a number");
    pub static ref TOKEN_INFO_EXPIRATION: u64 = env::var("TOKEN_INFO_EXPIRATION")
        .expect("TOKEN_INFO_EXPIRATION must be set")
        .parse()
        .expect("TOKEN_INFO_EXPIRATION must be a number");
    pub static ref MARKET_INFO_EXPIRATION: u64 = env::var("MARKET_INFO_EXPIRATION")
        .expect("MARKET_INFO_EXPIRATION must be set")
        .parse()
        .expect("MARKET_INFO_EXPIRATION must be a number");
    pub static ref LATEST_PRICE_EXPIRATION: u64 = env::var("LATEST_PRICE_EXPIRATION")
        .expect("LATEST_PRICE_EXPIRATION must be set")
        .parse()
        .expect("LATEST_PRICE_EXPIRATION must be a number");
}

// Redis Pool 설정
lazy_static! {
    pub static ref REDIS_POOL_MAX_SIZE: u32 = env::var("REDIS_POOL_MAX_SIZE")
        .expect("REDIS_POOL_MAX_SIZE must be set")
        .parse()
        .expect("REDIS_POOL_MAX_SIZE must be a number");
    pub static ref REDIS_POOL_WAIT_TIMEOUT_SECS: u64 = env::var("REDIS_POOL_WAIT_TIMEOUT_SECS")
        .expect("REDIS_POOL_WAIT_TIMEOUT_SECS must be set")
        .parse()
        .expect("REDIS_POOL_WAIT_TIMEOUT_SECS must be a number");
    pub static ref REDIS_POOL_CREATE_TIMEOUT_SECS: u64 = env::var("REDIS_POOL_CREATE_TIMEOUT_SECS")
        .expect("REDIS_POOL_CREATE_TIMEOUT_SECS must be set")
        .parse()
        .expect("REDIS_POOL_CREATE_TIMEOUT_SECS must be a number");
    pub static ref REDIS_POOL_RECYCLE_TIMEOUT_SECS: u64 =
        env::var("REDIS_POOL_RECYCLE_TIMEOUT_SECS")
            .expect("REDIS_POOL_RECYCLE_TIMEOUT_SECS must be set")
            .parse()
            .expect("REDIS_POOL_RECYCLE_TIMEOUT_SECS must be a number");
    pub static ref REDIS_COMMAND_TIMEOUT_MS: u64 = env::var("REDIS_COMMAND_TIMEOUT_MS")
        .unwrap_or_else(|_| "500".to_string())
        .parse()
        .expect("REDIS_COMMAND_TIMEOUT_MS must be a number");
    pub static ref SQL_COMMAND_TIMEOUT_MS: u64 = env::var("SQL_COMMAND_TIMEOUT_MS")
        .unwrap_or_else(|_| "500".to_string())
        .parse()
        .expect("SQL_COMMAND_TIMEOUT_MS must be a number");
}

// PostgreSQL Pool 설정
lazy_static! {
    pub static ref PG_MAX_CONNECTIONS: u32 = env::var("PG_MAX_CONNECTIONS")
        .expect("PG_MAX_CONNECTIONS must be set")
        .parse()
        .expect("PG_MAX_CONNECTIONS must be a number");
    pub static ref PG_MIN_CONNECTIONS: u32 = env::var("PG_MIN_CONNECTIONS")
        .expect("PG_MIN_CONNECTIONS must be set")
        .parse()
        .expect("PG_MIN_CONNECTIONS must be a number");
    pub static ref PG_MAX_LIFETIME: u64 = env::var("PG_MAX_LIFETIME")
        .expect("PG_MAX_LIFETIME must be set")
        .parse()
        .expect("PG_MAX_LIFETIME must be a number");
    pub static ref PG_ACQUIRE_TIMEOUT: u64 = env::var("PG_ACQUIRE_TIMEOUT")
        .expect("PG_ACQUIRE_TIMEOUT must be set")
        .parse()
        .expect("PG_ACQUIRE_TIMEOUT must be a number");
    pub static ref PG_IDLE_TIMEOUT: u64 = env::var("PG_IDLE_TIMEOUT")
        .expect("PG_IDLE_TIMEOUT must be set")
        .parse()
        .expect("PG_IDLE_TIMEOUT must be a number");
    pub static ref PG_STATEMENT_CACHE_CAPACITY: usize = env::var("PG_STATEMENT_CACHE_CAPACITY")
        .expect("PG_STATEMENT_CACHE_CAPACITY must be set")
        .parse()
        .expect("PG_STATEMENT_CACHE_CAPACITY must be a number");
    pub static ref PG_SSL_MODE: String = env::var("PG_SSL_MODE").expect("PG_SSL_MODE must be set");
}
