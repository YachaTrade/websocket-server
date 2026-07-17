use std::{sync::Arc, time::Duration};

use alloy::{
    rpc::types::{Filter, Log},
    sol,
    sol_types::SolEvent,
};
use anyhow::Result;

use bigdecimal::{BigDecimal, RoundingMode};
use dashmap::DashMap;
use tokio::time::Instant;
use tokio_stream::StreamExt;

use crate::{
    error_log,
    types::{
        stream::{DexBurn, DexMint},
        MarketType,
    },
};
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};

use crate::{
    client::RpcClient,
    config::WETH_ADDRESS,
    db::cache::CacheManager,
    types::stream::{Buy, DexChartUpdate, DexEventType, DexSync, EventType, Sell},
    utils::to_big_decimal,
};

use super::receive::receive_dex_event;

/// 이벤트 버퍼링용 키 (transaction_hash + log_index)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct EventBufferKey {
    transaction_hash: String,
    log_index: u64,
}

/// 트랜잭션당 이벤트 버퍼
struct TransactionEventBuffer {
    /// 거래 이벤트 (Buy 또는 Sell) - enum으로 성능 최적화
    trade: Option<DexEventType>,
    /// Sync 이벤트
    sync: Option<DexSync>,
    /// 블록 번호 (미래 확장용으로 유지)
    #[allow(dead_code)]
    block_number: u64,
    /// 버퍼링 시작 시각
    timestamp: tokio::time::Instant,
}

/// 이벤트 버퍼 (transaction_hash + log_index -> events)
static EVENT_BUFFER: once_cell::sync::Lazy<Arc<DashMap<EventBufferKey, TransactionEventBuffer>>> =
    once_cell::sync::Lazy::new(|| Arc::new(DashMap::new()));

sol! {
    #[allow(missing_docs)]
    #[sol(rpc)]
    IUniswapV3Pool,
    "abi/v1/IUniswapV3Pool.json"
}

#[instrument()]
pub async fn stream_dex_events(event_type: EventType) -> Result<()> {
    let client = loop {
        match RpcClient::instance() {
            Ok(client) => break client,
            Err(e) => {
                error_log!(
                    "Failed to get RpcClient instance: {}, retrying in 5 seconds...",
                    e
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        }
    };

    let filter = Filter::new().events(vec![
        IUniswapV3Pool::Swap::SIGNATURE,
        IUniswapV3Pool::Mint::SIGNATURE,
        IUniswapV3Pool::Burn::SIGNATURE,
    ]);

    let stream_timeout = Duration::from_millis(*crate::config::STREAM_TIMEOUT);
    let reconnect_delay = Duration::from_secs(1);

    // 트랜잭션 sender 로컬 캐시 - (sender, 저장 시각)
    // 같은 블록 내 여러 로그에서 동일 트랜잭션을 빠르게 재사용
    let tx_sender_cache: Arc<DashMap<String, (String, Instant)>> = Arc::new(DashMap::new());

    // 블록 타임스탬프 로컬 캐시 - (timestamp, 저장 시각)
    // Redis 대신 로컬 메모리 사용으로 성능 향상
    let block_timestamp_cache: Arc<DashMap<u64, (u64, Instant)>> = Arc::new(DashMap::new());

    // 백그라운드 태스크: 오래된 캐시 항목 자동 정리
    // - tx_sender: 2초 이상 지난 항목 삭제 (1초마다 실행)
    // - block_timestamp: 10초 이상 지난 항목 삭제 (5초마다 실행)
    // - event_buffer: 5초 이상 매칭되지 않은 이벤트 정리 (3초마다 실행)
    {
        let tx_cache = tx_sender_cache.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                let now = Instant::now();
                tx_cache.retain(|_, (_, timestamp)| {
                    now.duration_since(*timestamp) < Duration::from_secs(2)
                });
            }
        });

        let block_cache = block_timestamp_cache.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;
                let now = Instant::now();
                block_cache.retain(|_, (_, timestamp)| {
                    now.duration_since(*timestamp) < Duration::from_secs(10)
                });
            }
        });

        // 이벤트 버퍼 정리 태스크
        let event_buf = EVENT_BUFFER.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(3)).await;
                let now = tokio::time::Instant::now();
                event_buf.retain(|key, buf| {
                    let elapsed = now.duration_since(buf.timestamp);
                    if elapsed >= Duration::from_secs(5) {
                        warn!(
                            "⏰ DEX 이벤트 버퍼 타임아웃: tx={}, log_idx={}, trade={}, sync={}, elapsed={:?}",
                            key.transaction_hash,
                            key.log_index,
                            buf.trade.is_some(),
                            buf.sync.is_some(),
                            elapsed
                        );
                        false // 삭제
                    } else {
                        true // 유지
                    }
                });
            }
        });
    }

    // CacheManager 인스턴스를 외부에서 한 번만 획득하여 재사용
    let cache_manager = match CacheManager::instance() {
        Ok(cm) => cm,
        Err(e) => {
            panic!("Failed to get CacheManager instance: {}", e);
        }
    };

    // 무한 루프로 지속적인 스트림 유지
    loop {
        let current_provider_index = client.get_current_provider_index().await;

        // 새로운 스트림 생성 시도 (표준 eth_subscribe("logs"))
        let mut stream = match client.get_stream(&filter).await {
            Ok(stream) => stream,
            Err(e) => {
                error_log!("Failed to create dex stream, retrying in 1s: {}", e);
                client.update_best_provider().await;
                tokio::time::sleep(reconnect_delay).await;
                continue;
            }
        };

        info!("DEX log stream started successfully");

        // 스트림 이벤트 처리 루프 - provider 변경 감지
        loop {
            // Provider 변경 감지 (타임아웃 시에만 체크하도록 최적화)
            let new_provider_index = client.get_current_provider_index().await;
            if new_provider_index != current_provider_index {
                info!(
                    "Provider changed from {} to {}, reconnecting DEX stream",
                    current_provider_index, new_provider_index
                );
                // 명시적으로 stream을 drop하여 subscription 정리
                drop(stream);
                info!("DEX stream subscription closed due to provider change");
                break; // 외부 루프로 돌아가서 새 스트림 생성
            }

            match tokio::time::timeout(stream_timeout, stream.next()).await {
                Ok(Some(log)) => {
                    // Reorg로 되돌려진 로그(removed=true)는 무시 — 표준 logs 구독은
                    // reorg 시 같은 로그를 removed 플래그와 함께 재전달한다.
                    if log.removed {
                        continue;
                    }

                    // 스트림에서 로그를 받았으므로 DEX 스트림이 살아있음을 기록
                    crate::metrics::METRICS.stream.record_dex_event();

                    let sender_cache = tx_sender_cache.clone();
                    let block_cache = block_timestamp_cache.clone();
                    let cache_mgr = cache_manager.clone();
                    tokio::spawn(async move {
                        let events =
                            parse_log(log, client, cache_mgr, sender_cache, block_cache).await;
                        match events {
                            Ok(events) => {
                                for event in events {
                                    info!("Dex Stream Event: {event:?}");
                                    if let Err(e) = receive_dex_event(event).await {
                                        error_log!("Failed to handle dex event: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                if !e.to_string().contains("Not a white list dex address") {
                                    error_log!("Failed to parse dex log - error: {}", e);
                                }
                            }
                        }
                    });
                }
                Ok(None) => {
                    warn!("DEX stream ended, reconnecting...");
                    // 명시적으로 stream을 drop하여 subscription 정리
                    drop(stream);
                    info!("DEX stream subscription closed due to stream end");
                    break;
                }
                Err(_) => {
                    warn!("DEX stream timed out after {}s, reconnecting provider to cleanup subscriptions...", stream_timeout.as_secs());
                    // 명시적으로 stream을 drop하여 subscription 정리
                    drop(stream);
                    info!("DEX stream subscription closed due to timeout");

                    // Provider를 재연결하여 WebSocket connection과 모든 subscription 정리
                    if let Err(e) = client.reconnect_current_provider().await {
                        warn!(
                            "Failed to reconnect provider: {}, will retry on next loop",
                            e
                        );
                    }

                    break;
                }
            }
        }

        // Stream이 종료된 후 재연결 전 짧은 대기
        // reconnect_current_provider()가 이미 200ms 대기하므로 추가 대기는 최소화
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// DEX pool whitelist 체크 및 token pair 조회 공통 함수
/// Swap, Mint, Burn 모두 동일한 로직을 사용하므로 중복 제거
async fn check_pool_and_get_pair(
    pool: &str,
    cache_manager: &CacheManager,
) -> Result<(String, String)> {
    // 1. Whitelist 체크
    let is_whitelist_dex = match cache_manager.check_white_list_pool(pool).await {
        Ok(result) => result,
        Err(e) => {
            error_log!(
                "Error checking whitelist for pool {}: {} - allowing through as fallback",
                pool,
                e
            );
            true
        }
    };
    if !is_whitelist_dex {
        return Err(anyhow::anyhow!("Not a white list dex address"));
    }

    // 2. Pool pair 조회
    let pool_pair = match cache_manager.get_pool_pair(pool).await {
        Ok(result) => result,
        Err(e) => {
            error_log!(
                "Error getting pool pair for pool {}: {} - filtering out",
                pool,
                e
            );
            return Err(anyhow::anyhow!("DEX pair not found"));
        }
    };

    pool_pair.ok_or_else(|| anyhow::anyhow!("DEX pair not found"))
}

async fn parse_log(
    log: Log,
    client: &RpcClient,
    cache_manager: Arc<CacheManager>,
    _sender_cache: Arc<DashMap<String, (String, Instant)>>,
    block_timestamp_cache: Arc<DashMap<u64, (u64, Instant)>>,
) -> Result<Vec<DexEventType>> {
    let transaction_hash = match log.transaction_hash {
        Some(hash) => hash.to_string(),
        None => {
            error_log!("No transaction hash found in log: {:?}", log);
            return Err(anyhow::anyhow!("No transaction hash"));
        }
    };

    let block_number = match log.block_number {
        Some(number) => number,
        None => match client.get_latest_block_number().await {
            Ok(block_number) => block_number,
            Err(e) => {
                error_log!("Failed to get latest block number: {}", e);
                return Err(anyhow::anyhow!(
                    "Failed to get Dex block number {e:?}\\nLog{log:?}"
                ));
            }
        },
    };

    let block_timestamp = match log.block_timestamp {
        Some(timestamp) => timestamp,
        None => {
            match get_cached_block_timestamp(client, block_number, &block_timestamp_cache).await {
                Ok(timestamp) => timestamp,
                Err(e) => {
                    error_log!(
                        "Failed to get block timestamp for block {}: {} Log: {:?}",
                        block_number,
                        e,
                        log
                    );
                    return Err(anyhow::anyhow!(
                    "Failed to get block timestamp for block {block_number}: {e:?}\\nLog: {log:?}"
                ));
                }
            }
        }
    };
    let log_index = log.log_index.unwrap_or(u64::MAX); // 또는 unwrap_or(0)
    let transaction_index = log.transaction_index.unwrap_or(u64::MAX);
    match log.topic0() {
        Some(&IUniswapV3Pool::Swap::SIGNATURE_HASH) => {
            let pool = log.address().to_string();
            // 공통 함수로 whitelist 체크 및 pool pair 조회
            let (token0, token1) = check_pool_and_get_pair(&pool, &cache_manager).await?;

            let IUniswapV3Pool::Swap {
                sender: event_sender,
                amount0,
                amount1,
                sqrtPriceX96,
                liquidity,
                ..
            } = log.log_decode()?.inner.data;
            let token0_is_weth = token0.eq_ignore_ascii_case(&WETH_ADDRESS);
            // token은 참조로 사용하고 필요한 곳에서만 clone
            let token = if token0_is_weth { &token1 } else { &token0 };

            // Determine is_buy before resolve_actor (needed for actor resolution)
            let is_buy_for_resolve = match (token0_is_weth, amount0.is_positive()) {
                (true, true) => true,   // native in, token out => Buy
                (true, false) => false,  // native out, token in => Sell
                (false, true) => false,  // token in, native out => Sell
                (false, false) => true,  // token out, native in => Buy
            };

            // V1 DEX는 swap_to 동등 필드가 없어 None 전달.
            let account_id = cache_manager
                .resolve_actor(&transaction_hash, &event_sender.to_string(), token, is_buy_for_resolve, None)
                .await
                .unwrap_or_else(|e| {
                    error!("[DEX] Failed to resolve actor for Swap: {}", e);
                    event_sender.to_string()
                });
            let (amount_in, amount_out, is_buy) = match (token0_is_weth, amount0.is_positive()) {
                (true, true) => {
                    // token0 is native, native in (+), ERC20 out (-) => Buy
                    (
                        to_big_decimal(amount0.abs()),
                        to_big_decimal(amount1.abs()),
                        true,
                    )
                }
                (true, false) => {
                    // token0 is native, native out (-), ERC20 in (+) => Sell
                    (
                        to_big_decimal(amount1.abs()),
                        to_big_decimal(amount0.abs()),
                        false,
                    )
                }
                (false, true) => {
                    // token1 is native, ERC20 in (+), native out (-) => Sell
                    (
                        to_big_decimal(amount0.abs()),
                        to_big_decimal(amount1.abs()),
                        false,
                    )
                }
                (false, false) => {
                    // token1 is native, ERC20 out (-), native in (+) => Buy
                    (
                        to_big_decimal(amount1.abs()),
                        to_big_decimal(amount0.abs()),
                        true,
                    )
                }
            };

            let price = calculate_mon_token_price(to_big_decimal(sqrtPriceX96), token0_is_weth)
                .with_scale_round(10, RoundingMode::Up);

            let sqrt_price_x96_decimal = to_big_decimal(sqrtPriceX96);
            let two_pow_96 = BigDecimal::from(2u128.pow(96));
            let sqrt_price = &sqrt_price_x96_decimal / &two_pow_96;
            let liquidity_decimal = BigDecimal::from(liquidity);

            // Virtual reserves calculation:
            // reserve0 = L / sqrtPrice
            // reserve1 = L * sqrtPrice
            let (reserve_native, reserve_token) = if token0_is_weth {
                // token0 is native (WETH)
                let reserve0 =
                    (&liquidity_decimal / &sqrt_price).with_scale_round(0, RoundingMode::Down); // native reserve (정수)
                let reserve1 =
                    (&liquidity_decimal * &sqrt_price).with_scale_round(0, RoundingMode::Down); // token reserve (정수)
                (reserve0, reserve1)
            } else {
                // token1 is native (WETH)
                let reserve0 =
                    (&liquidity_decimal / &sqrt_price).with_scale_round(0, RoundingMode::Down); // token reserve (정수)
                let reserve1 =
                    (&liquidity_decimal * &sqrt_price).with_scale_round(0, RoundingMode::Down); // native reserve (정수)
                (reserve1, reserve0)
            };

            let price = price.with_scale_round(10, RoundingMode::Up);

            // String 변환을 한 번만 수행 (최적화)
            let token_str = token.to_string();

            // Reserve 로그 출력 (디버깅용)
            error_log!(
                "🔍 DEX Reserve: token={}, token0_is_weth={}, reserve_native={}, reserve_token={}, price={}, sqrtPriceX96={}, liquidity={}",
                token_str,
                token0_is_weth,
                reserve_native,
                reserve_token,
                price,
                sqrtPriceX96,
                liquidity
            );

            let dex_sync = DexSync {
                token: token_str.clone(), // 재사용을 위해 clone 유지
                pool: pool.clone(),       // pool은 여러 곳에서 사용되므로 clone 유지
                price,                    // clone 제거 (move)
                reserve_native,
                reserve_token,
                transaction_hash: transaction_hash.clone(), // tx hash도 여러 곳에서 사용
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            };

            // Swap 이벤트 생성 (Buy 또는 Sell) - enum으로 성능 최적화
            let swap_event: DexEventType = if is_buy {
                info!(
                    "🔵 DEX BUY 이벤트 생성: token={}, amount_in={}, amount_out={}",
                    token_str, amount_in, amount_out
                );
                let buy = Buy {
                    account_id: account_id.clone(), // to에도 사용되므로 clone 유지
                    to: Some(account_id.clone()),
                    amount_in,            // clone 제거 (move)
                    amount_out,           // clone 제거 (move)
                    token: token_str,     // clone 제거 (move, String 재사용)
                    market: pool.clone(), // pool은 아래 Sell에서도 사용
                    market_type: MarketType::Dex,
                    transaction_hash: transaction_hash.clone(), // 아래에서도 사용
                    block_number,
                    block_timestamp,
                    log_index,
                    transaction_index,
                };

                // NOTE: metrics/market 업데이트는 event handler에서 순차 처리됨
                // (race condition 방지를 위해 stream에서 제거)

                DexEventType::Buy(buy)
            } else {
                // String 변환을 한 번만 수행 (최적화)
                let token_str = token.to_string();

                info!(
                    "🔴 DEX SELL 이벤트 생성: token={}, amount_in={}, amount_out={}",
                    token_str, amount_in, amount_out
                );
                let sell = Sell {
                    account_id: account_id.clone(), // to에도 사용되므로 clone 유지
                    to: Some(account_id.clone()),
                    amount_in,        // clone 제거 (move)
                    amount_out,       // clone 제거 (move)
                    token: token_str, // clone 제거 (move, String 재사용)
                    market: pool.clone(),
                    market_type: MarketType::Dex,
                    transaction_hash: transaction_hash.clone(),
                    block_number,
                    block_timestamp,
                    log_index,
                    transaction_index,
                };

                // NOTE: metrics/market 업데이트는 event handler에서 순차 처리됨
                // (race condition 방지를 위해 stream에서 제거)

                DexEventType::Sell(sell)
            };

            // 버퍼링 방식으로 변경: Trade와 Sync를 매칭하여 DexChartUpdate 생성
            let buffer = EVENT_BUFFER.clone();

            // Trade 이벤트 (Buy 또는 Sell) 버퍼에 추가
            let trade_key = EventBufferKey {
                transaction_hash: transaction_hash.clone(),
                log_index,
            };
            info!(
                "📥 DEX {} event: tx={}, log_idx={}",
                if is_buy { "Buy" } else { "Sell" },
                transaction_hash,
                log_index
            );
            process_trade_event(swap_event.clone(), trade_key, buffer.clone()).await?;

            // Sync 이벤트 버퍼에 추가 (같은 log_index)
            let sync_key = EventBufferKey {
                transaction_hash: transaction_hash.clone(),
                log_index, // 같은 log_index
            };
            info!(
                "📥 DEX Sync event: tx={}, log_idx={}",
                transaction_hash, log_index
            );
            let sync_event = DexEventType::DexSync(dex_sync.clone());
            process_sync_event(sync_event.clone(), &dex_sync, sync_key, buffer).await?;

            // 개별 이벤트도 receive_dex_event로 전송 (Mint/Burn, Metrics 등 다른 핸들러용)
            let events: Vec<DexEventType> = vec![sync_event, swap_event];
            Ok(events)
        }
        Some(&IUniswapV3Pool::Mint::SIGNATURE_HASH) => {
            let pool = log.address().to_string();
            // 공통 함수로 whitelist 체크 및 pool pair 조회
            let (token0, token1) = check_pool_and_get_pair(&pool, &cache_manager).await?;

            let token0_is_weth = token0.eq_ignore_ascii_case(&WETH_ADDRESS);
            let token = if token0_is_weth { &token1 } else { &token0 };
            let IUniswapV3Pool::Mint { amount, .. } = match log.log_decode() {
                Ok(decoded) => decoded.inner.data,
                Err(e) => {
                    error_log!("Failed to decode Mint log: {}", e);
                    return Err(anyhow::anyhow!("Failed to decode Mint log: {}", e));
                }
            };

            let amount = to_big_decimal(amount);
            let mint_event = DexEventType::DexMint(DexMint {
                token: token.to_string(),
                pool,
                amount,
                transaction_hash,
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            });
            let events: Vec<DexEventType> = vec![mint_event];
            Ok(events)
        }
        Some(&IUniswapV3Pool::Burn::SIGNATURE_HASH) => {
            let pool = log.address().to_string();
            // 공통 함수로 whitelist 체크 및 pool pair 조회
            let (token0, token1) = check_pool_and_get_pair(&pool, &cache_manager).await?;

            let token0_is_weth = token0.eq_ignore_ascii_case(&WETH_ADDRESS);
            let token = if token0_is_weth { &token1 } else { &token0 };
            let IUniswapV3Pool::Burn { amount, .. } = match log.log_decode() {
                Ok(decoded) => decoded.inner.data,
                Err(e) => {
                    error_log!("Failed to decode Burn log: {}", e);
                    return Err(anyhow::anyhow!("Failed to decode Burn log: {}", e));
                }
            };
            let amount = to_big_decimal(amount);
            let burn_event = DexEventType::DexBurn(DexBurn {
                token: token.to_string(),
                pool,
                amount,
                transaction_hash,
                block_number,
                block_timestamp,
                log_index,
                transaction_index,
            });
            let events: Vec<DexEventType> = vec![burn_event];
            Ok(events)
        }
        _ => Err(anyhow::anyhow!("Unknown event type")),
    }
}

/**
 * token0_is_weth 매개변수 설명:
 *
 * 이 함수는 항상 mon/token 형태의 가격 비율을 반환합니다.
 * token0_is_weth 매개변수는 token0이 mon(기준 토큰)인지 여부를 나타냅니다.
 *
 * ETH = MON, USDC = TOKEN이라고 정의할 때:
 *
 * 예시 1: MON/TOKEN 풀
 * - token0가 MON이고 token0_is_weth = true일 때:
 *   => 최종 반환값: 0.0000005 (1 MON당 TOKEN 가격)
 *
 * 예시 2: TOKEN/MON 풀
 * - token0가 TOKEN이고 token0_is_weth = false일 때:
 *   => 최종 반환값: 0.0000005 (1 MON당 TOKEN 가격)
 *
 * 중요: 이 함수는 풀의 구성이나 토큰 순서와 관계없이 항상 mon/token 형태의 가격을 반환합니다.
 * token0_is_weth 매개변수를 통해 어떤 토큰이 mon인지 지정하면, 그에 맞게 가격 비율이 계산됩니다.
 *
 * 계산 예시:
 *
 * 입력:
 * - sqrt_price_x96 = 1771845812128583464494622 (Uniswap V3 풀의 실제 값)
 * - token0_is_weth = true (MON이 token0이고 기준 토큰인 경우)
 *
 * 계산 과정:
 * 1. sqrt_price_x96 / 2^96 = 0.0000000223638...
 * 2. (sqrt_price_x96 / 2^96)^2 = 0.0000000000005002...
 * 3. 최종 가격 = 약 0.0000005 (1 MON당 TOKEN 가격)
 */
fn calculate_mon_token_price(sqrt_price_x96: BigDecimal, token0_is_weth: bool) -> BigDecimal {
    // 전역 상수 TWO_96 사용 (매번 생성하지 않음)
    if token0_is_weth {
        // price = (2^96 / sqrtP)^2
        let ratio = crate::config::TWO_96.clone() / &sqrt_price_x96;
        let price_ratio = &ratio * &ratio;
        price_ratio.with_scale(60) // 극단적으로 큰 sqrtP 대비 높은 정밀도 유지
    } else {
        // price = (sqrtP / 2^96)^2
        let ratio = &sqrt_price_x96 / crate::config::TWO_96.clone();
        let price_ratio = &ratio * &ratio;
        price_ratio.with_scale(60) // 극단적으로 작은 sqrtP 대비 높은 정밀도 유지
    }
}
/// 블록 타임스탬프 로컬 캐시 조회
/// Redis 대신 로컬 메모리 사용으로 성능 향상 (10초 TTL)
async fn get_cached_block_timestamp(
    client: &RpcClient,
    block_number: u64,
    cache: &DashMap<u64, (u64, Instant)>,
) -> Result<u64> {
    // 1. 로컬 캐시 확인
    if let Some(entry) = cache.get(&block_number) {
        return Ok(entry.0);
    }

    // 2. RPC에서 조회
    let block_timestamp = match client.get_block_timestamp(block_number).await {
        Ok(timestamp) => timestamp,
        Err(e) => {
            error_log!(
                "Failed to get block timestamp for block {}: {}",
                block_number,
                e
            );
            return Err(anyhow::anyhow!(
                "Failed to get block timestamp for block {}: {}",
                block_number,
                e
            ));
        }
    };

    // 3. 캐시에 저장 (10초 TTL)
    cache.insert(block_number, (block_timestamp, Instant::now()));

    Ok(block_timestamp)
}

/// Trade 이벤트(Buy/Sell) 처리 - 버퍼에 추가하고 매칭 확인
async fn process_trade_event(
    event: DexEventType,
    key: EventBufferKey,
    buffer: Arc<DashMap<EventBufferKey, TransactionEventBuffer>>,
) -> Result<()> {
    let block_number = event.get_block_number();

    // 버퍼에 Trade 추가
    buffer
        .entry(key.clone())
        .or_insert(TransactionEventBuffer {
            trade: None,
            sync: None,
            block_number,
            timestamp: tokio::time::Instant::now(),
        })
        .trade = Some(event);

    // 두 개 모였는지 확인
    check_and_send_chart_update(key, buffer).await
}

/// Sync 이벤트 처리 - 버퍼에 추가하고 매칭 확인
async fn process_sync_event(
    event: DexEventType,
    sync: &DexSync,
    key: EventBufferKey,
    buffer: Arc<DashMap<EventBufferKey, TransactionEventBuffer>>,
) -> Result<()> {
    let block_number = event.get_block_number();

    // 버퍼에 Sync 추가
    buffer
        .entry(key.clone())
        .or_insert(TransactionEventBuffer {
            trade: None,
            sync: None,
            block_number,
            timestamp: tokio::time::Instant::now(),
        })
        .sync = Some(sync.clone());

    // 두 개 모였는지 확인
    check_and_send_chart_update(key, buffer).await
}

/// 버퍼에서 두 개 모였는지 확인하고 DexChartUpdate 생성 및 전송
async fn check_and_send_chart_update(
    key: EventBufferKey,
    buffer: Arc<DashMap<EventBufferKey, TransactionEventBuffer>>,
) -> Result<()> {
    // 두 개 다 있는지 확인
    if let Some(entry) = buffer.get(&key) {
        let has_trade = entry.trade.is_some();
        let has_sync = entry.sync.is_some();
        let has_both = has_trade && has_sync;
        drop(entry);

        info!(
            "🔍 DEX Buffer check: tx={}, log_idx={}, has_trade={}, has_sync={}, ready={}",
            key.transaction_hash, key.log_index, has_trade, has_sync, has_both
        );

        if has_both {
            // 두 개 모임! 버퍼에서 제거하고 ChartUpdate 생성
            if let Some((_, buf)) = buffer.remove(&key) {
                if let (Some(trade), Some(sync)) = (buf.trade, buf.sync) {
                    let chart_update = DexChartUpdate {
                        trade: Some(Box::new(trade)),
                        sync: sync.clone(),
                        block_number: sync.block_number,
                        transaction_hash: sync.transaction_hash.clone(),
                    };

                    info!(
                        "🎯 DexChartUpdate created: tx={}, token={}",
                        chart_update.transaction_hash, chart_update.sync.token
                    );

                    // receive_dex_event 통해 chart_producer에 전송
                    receive_dex_event(DexEventType::DexChartUpdate(chart_update)).await?;
                }
            }
        }
    } else {
        info!(
            "⚠️  DEX Buffer miss: tx={}, log_idx={} not found in buffer",
            key.transaction_hash, key.log_index
        );
    }

    Ok(())
}
