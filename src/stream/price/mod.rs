pub mod provider;

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::client::RpcClient;
use provider::{PriceProvider, build_provider, normalize_feed_id};

lazy_static! {
    /// 전역 Native(MON) 가격 싱글톤 인스턴스
    ///
    /// Thread-safe하게 읽기/쓰기 가능
    /// - 읽기: 여러 스레드에서 동시 접근 가능 (매우 빠름)
    /// - 쓰기: monitor 스레드에서만 1초마다 업데이트
    pub static ref NATIVE_PRICE: Arc<RwLock<BigDecimal>> = Arc::new(RwLock::new(
        BigDecimal::from_str("0.03").unwrap() // 초기값: $0.03
    ));

    /// Quote token 가격 매핑 (quote_address → USD 가격)
    /// WMON이 아닌 quote token의 가격을 저장
    /// register_quote_token()으로 등록하면 자동으로 주기적 업데이트됨
    pub static ref QUOTE_PRICES: Arc<DashMap<String, BigDecimal>> = Arc::new(DashMap::new());

    /// 등록된 quote token의 Pyth feed ID 매핑 (quote_address → pyth_feed_id)
    pub static ref QUOTE_FEED_IDS: Arc<DashMap<String, String>> = Arc::new(DashMap::new());
}

/// Native(MON) Pyth feed ID. Always fetched alongside any registered quote
/// feeds in each polling cycle.
const NATIVE_FEED_ID: &str =
    "0x31491744e2dbf6df7fcf4ac0820d18a609b49076d45066d3568424e62f686cd1";

/// Polling cadence — 10s, observer 의 NORMALIZE_WINDOW_SECS 와 동일.
///
/// 이전엔 1초 였는데, observer 와 같은 egress IP 를 공유하는 환경에서
/// 두 서비스가 합쳐 Pyth Hermes 의 30 req/10s 한도를 자주 넘겨 429 가
/// 발생함. observer 가 10s bucketing 으로 부하 절반 줄였으니
/// websocket-server 도 같은 cadence 로 맞춰서 합산 부하가 한도 안에
/// 들어오게 함.
///
/// 트레이드오프: live UI 의 USD 가격 freshness 가 1s → 10s 로 늘어남.
/// quote 토큰 가격은 분 단위로 크게 변하지 않으니 일반적인 거래 화면에는
/// 무시할 수 있는 수준. 더 빠른 freshness 가 필요해지면 observer 의
/// rate limiter 와 함께 다시 조정.
const POLL_INTERVAL: Duration = Duration::from_secs(10);
/// 에러 시 다음 retry까지 대기 시간 (provider 내부 backoff에 더해 최후 안전망).
const ERROR_BACKOFF: Duration = Duration::from_secs(2);

/// Native(MON) 가격 조회
///
/// 메모리에서 즉시 반환 (~1μs)
pub async fn get_native_price() -> BigDecimal {
    NATIVE_PRICE.read().await.clone()
}

/// Quote token 가격 조회
/// WMON이면 native_price 반환, 아니면 QUOTE_PRICES에서 조회
/// 등록되지 않은 quote token이면 native_price를 fallback으로 반환
pub async fn get_quote_price(quote_id: &str) -> BigDecimal {
    let wmon = crate::config::WMON_ADDRESS.as_str();
    if quote_id.eq_ignore_ascii_case(wmon) {
        return get_native_price().await;
    }

    if let Some(price) = QUOTE_PRICES.get(quote_id) {
        return price.clone();
    }

    let lower = quote_id.to_lowercase();
    if let Some(price) = QUOTE_PRICES.get(&lower) {
        return price.clone();
    }

    warn!(
        "Quote token {} 가격 미등록, native_price를 fallback으로 사용",
        quote_id
    );
    get_native_price().await
}

/// Quote token 등록
/// Pyth feed ID와 함께 등록하면 주기적으로 가격 업데이트됨
/// 초기 가격은 0으로 설정되며, 첫 번째 업데이트에서 실제 가격으로 갱신됨
pub fn register_quote_token(address: &str, pyth_feed_id: &str) {
    let addr_lower = address.to_lowercase();
    QUOTE_FEED_IDS.insert(addr_lower.clone(), pyth_feed_id.to_string());
    QUOTE_PRICES.insert(addr_lower.clone(), BigDecimal::from(0));
    info!(
        "📝 Quote token 등록: address={}, feed_id={}",
        addr_lower, pyth_feed_id
    );
}

async fn set_native_price(price: BigDecimal) {
    let mut native_price = NATIVE_PRICE.write().await;
    *native_price = price;
}

/// Pyth 질의 timestamp lag (초).
///
/// Pyth `/v2/updates/price/{ts}`는 현재 초(또는 ~1초 전)를 질의하면 404를
/// 반환하는 경우가 있다 — publisher가 아직 해당 초를 커밋하지 못한 상태.
/// 3초 뒤로 물러나면 publish window가 안정된 구간에 들어간다.
const PYTH_TS_LAG_SECS: u64 = 3;

/// Timestamp 버킷 (초) — observer giwa 브랜치의 값과 반드시 동일해야 한다.
/// 두 서비스가 같은 논리 윈도우에 대해 같은 timestamp로 Pyth를 질의하도록
/// (ts - LAG)를 이 배수로 내림 정렬한다.
const PYTH_TS_BUCKET_SECS: u64 = 10;

/// (ts - LAG)를 BUCKET 배수로 내림 정렬.
///
/// 기존 블록번호 modulo 방식(Monad 0.4s 고정 블록 가정)을 대체 — giwa
/// (Arbitrum 계열)는 블록 생성이 수요 기반이라 블록번호가 시간과 비례하지 않음.
fn align_pyth_ts(ts: u64) -> u64 {
    let target = ts.saturating_sub(PYTH_TS_LAG_SECS);
    target - (target % PYTH_TS_BUCKET_SECS)
}

/// 체인 최신 블록의 timestamp를 [`align_pyth_ts`]로 정렬해 Pyth 질의에 사용.
async fn pyth_query_ts(client: &RpcClient) -> Result<u64> {
    let latest = client.get_latest_block_number().await?;
    let ts = client.get_block_timestamp(latest).await?;
    Ok(align_pyth_ts(ts))
}

/// Native + Quote Price 업데이트 시작.
///
/// 1초마다 native(MON) feed와 등록된 모든 quote feed를 **단일 batch 요청**으로
/// 한 번에 가져와 인메모리 캐시(NATIVE_PRICE, QUOTE_PRICES)에 반영.
///
/// 가격 fetch는 [`PriceProvider`] trait를 거치며, 이 abstraction은 observer
/// 측과 동일한 형태(`provider/{mod,pyth,mock}.rs`)로 정렬되어 있어 두
/// 프로젝트의 메커니즘을 한 곳만 보면 이해 가능.
pub async fn start_update_price() -> Result<()> {
    let provider: Arc<dyn PriceProvider> =
        build_provider().context("Failed to build PriceProvider")?;
    let client = RpcClient::instance().context("RpcClient not initialized")?;
    let mode = std::env::var("MODE").unwrap_or_else(|_| "mainnet".to_string());
    let testnet = mode.to_lowercase() == "testnet";

    tokio::spawn(async move {
        info!(
            "🚀 Price monitor started (mode={}, batch fetch via PriceProvider)",
            mode
        );

        loop {
            // 모든 등록된 feed_id 수집 (native + quote tokens).
            let mut feed_ids: Vec<String> = vec![NATIVE_FEED_ID.to_string()];
            for entry in QUOTE_FEED_IDS.iter() {
                feed_ids.push(entry.value().clone());
            }
            let feed_id_refs: Vec<&str> = feed_ids.iter().map(|s| s.as_str()).collect();

            // 체인의 최신 블록 timestamp를 그대로 Pyth에 사용.
            // 블록은 항상 과거에 commit됐으므로 Pyth `/price/{ts}` 가 404 안 남.
            // observer가 block_timestamp를 넘기는 것과 동일한 의미.
            let ts = match pyth_query_ts(client).await {
                Ok(ts) => ts,
                Err(e) => {
                    error!("❌ Failed to resolve chain timestamp for Pyth: {}", e);
                    tokio::time::sleep(ERROR_BACKOFF).await;
                    continue;
                }
            };

            match provider.fetch_batch(&feed_id_refs, ts).await {
                Ok(prices) => {
                    // Native price 업데이트
                    let native_key = normalize_feed_id(NATIVE_FEED_ID);
                    if let Some(price) = prices.get(&native_key) {
                        set_native_price(price.clone()).await;
                        info!("💰 Native price updated: ${}", price);
                    } else if !testnet {
                        warn!("⚠️  Pyth response missing native feed");
                    }

                    // Quote token 가격 일괄 업데이트
                    for entry in QUOTE_FEED_IDS.iter() {
                        let address = entry.key();
                        let feed_id = entry.value();
                        let key = normalize_feed_id(feed_id);
                        if let Some(price) = prices.get(&key) {
                            QUOTE_PRICES.insert(address.clone(), price.clone());
                            info!(
                                "💰 Quote token price updated: {}=${}",
                                address, price
                            );
                        } else {
                            warn!(
                                "⚠️  Pyth response missing feed for {}: feed_id={}",
                                address, feed_id
                            );
                        }
                    }

                    tokio::time::sleep(POLL_INTERVAL).await;
                }
                Err(e) => {
                    error!("❌ Batch price fetch failed: {}", e);
                    tokio::time::sleep(ERROR_BACKOFF).await;
                }
            }
        }
    });

    info!("✅ Price monitor initialized");
    Ok(())
}

#[cfg(test)]
mod pyth_ts_tests {
    use super::align_pyth_ts;

    // giwa(Arbitrum 계열)는 블록 생성이 수요 기반이라 블록번호 modulo 정렬이
    // 벽시계 시간과 비례하지 않는다. timestamp를 직접 버킷팅해야 observer와
    // 같은 Pyth 질의 timestamp를 공유한다 (LAG 3s, BUCKET 10s — observer와 동일).
    #[test]
    fn aligns_to_10s_bucket_after_3s_lag() {
        assert_eq!(align_pyth_ts(1_000), 990); // 997 → 990
        assert_eq!(align_pyth_ts(1_013), 1_010); // 1010 → 1010 (경계)
        assert_eq!(align_pyth_ts(1_012), 1_000); // 1009 → 1000
        assert_eq!(align_pyth_ts(2), 0); // underflow는 saturating
    }
}
