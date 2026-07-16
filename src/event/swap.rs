use crate::{
    error_log,
    types::{
        swap::{TokenSwap, TokenSwapMessage},
        SwapInfo, SwapType,
    },
};
use anyhow::Result;
use dashmap::DashMap;
use tracing::info;

use std::{fmt, sync::Arc};
use tokio::sync::{Mutex, OnceCell};

use crate::{
    config::{CHANNEL_SIZE, DECIMALS, WMON_ADDRESS},
    db::cache::CacheManager,
    metrics::{
        monitored_broadcast_channel, monitored_channel, MonitoredBroadcastReceiver,
        MonitoredBroadcastSender, MonitoredReceiver, MonitoredSender,
    },
    types::stream::{CurveEventType, DexEventType},
};

pub static SWAP_EVENT_PRODUCER: OnceCell<Arc<SwapEventProducer>> = OnceCell::const_new();
pub struct SwapEventProducer {
    /// Sender channel for DEX events (Arc로 zero-copy 공유)
    pub dex_event_sender: MonitoredSender<Arc<DexEventType>>,

    /// Receiver channel for DEX events
    pub dex_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<DexEventType>>>>,

    /// Sender channel for Curve events (Arc로 zero-copy 공유)
    pub curve_event_sender: MonitoredSender<Arc<CurveEventType>>,

    /// Receiver channel for Curve events
    pub curve_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<CurveEventType>>>>,

    pub event_sender: Arc<DashMap<String, MonitoredBroadcastSender<TokenSwapMessage>>>,
}

impl fmt::Debug for SwapEventProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwapEventProducer").finish()
    }
}

impl SwapEventProducer {
    pub fn init() -> Result<()> {
        let (dex_event_sender, dex_event_receiver) = monitored_channel("Swap-DEX", *CHANNEL_SIZE);
        let (curve_event_sender, curve_event_receiver) =
            monitored_channel("Swap-Curve", *CHANNEL_SIZE);
        let event_sender = Arc::new(DashMap::new());

        let producer = SwapEventProducer {
            dex_event_sender,
            dex_event_receiver: Arc::new(Mutex::new(dex_event_receiver)),
            curve_event_sender,
            curve_event_receiver: Arc::new(Mutex::new(curve_event_receiver)),
            event_sender,
        };

        SWAP_EVENT_PRODUCER
            .set(Arc::new(producer))
            .expect("Failed to set SWAP_EVENT_PRODUCER");

        // 주기적 정리 태스크 시작
        Self::start_cleanup_task()?;

        Ok(())
    }

    /// SwapEventProducer의 메인 함수
    /// receive_curve_event와 receive_dex_event를 실행하는 태스크를 생성
    pub async fn main() -> Result<()> {
        let producer = SWAP_EVENT_PRODUCER
            .get()
            .ok_or_else(|| anyhow::anyhow!("SwapEventProducer not initialized"))?
            .clone();

        // receive_curve_event 태스크 실행

        let curve_producer = producer.clone();
        tokio::spawn(async move {
            if let Err(e) = curve_producer.receive_curve_event().await {
                error_log!("Error in receive_curve_event: {}", e);
            }
        });

        // receive_dex_event 태스크 실행
        let dex_producer = producer.clone();
        tokio::spawn(async move {
            if let Err(e) = dex_producer.receive_dex_event().await {
                error_log!("Error in receive_dex_event: {}", e);
            }
        });

        tracing::info!(
            "SwapEventProducer main started: receive_curve_event and receive_dex_event tasks spawned"
        );

        // 이 함수는 즉시 반환되고, 생성된 태스크들은 백그라운드에서 계속 실행됨
        Ok(())
    }

    pub async fn receive_curve_event(&self) -> Result<()> {
        //여기서 해야할 일은 event curve_event 를 받아서 해당 token_id 의 event_sender에 전달하는 것이다.
        let cache_manager = CacheManager::instance()?;

        let mut receiver = self.curve_event_receiver.lock().await;
        while let Some(event) = receiver.recv().await {
            info!("SwapEventProducer receive curve event: {:?}", event);

            // 여기서 event에서 token_id를 추출하고 SwapMessage로 변환하여 전달
            // TODO: 변환 로직 구현
            //이제 event 를 받으면 ,  buy,sell,graduated 으로 변환하고 그거에 맞게끔 swap message 구성해야함.

            //buy 는 SwapMessage 만들면되고,  결국 accountInfo 만 구하면됨. 이건 caching 으로 먼저 가지고 오고, 그 뒤에 db 에 쿼리
            //Sell 도 SwapMessage 만들면됨. 결국 accountInfo 만 구하면됨. 이건 caching 으로 먼저 가지고 오고, 그 뒤에 db 에 쿼리

            //graduated 은 업데이트 마켓

            let message: TokenSwapMessage = if let Some(buy) = event.is_buy() {
                let account_info = cache_manager.get_account_info(&buy.account_id).await;

                // block_number 기준 가격 조회 (observer 인덱싱 데이터와 일치)
                let block_number = buy.block_number as i64;
                let quote_id = match cache_manager.get_market_info(&buy.token).await {
                    Ok(market) => market.quote_info.quote_id,
                    Err(_) => WMON_ADDRESS.clone(),
                };
                let quote_price = cache_manager
                    .get_quote_usd_price(&quote_id, block_number)
                    .await;
                let native_price = if quote_id.eq_ignore_ascii_case(&WMON_ADDRESS) {
                    quote_price.clone()
                } else {
                    cache_manager
                        .get_quote_usd_price(&WMON_ADDRESS, block_number)
                        .await
                };

                // USD 가치 계산: value = quote_amount / 10^18 * quote_price
                let value = ((&buy.amount_in / &*DECIMALS) * &quote_price)
                    .normalized()
                    .to_plain_string();
                let native_price = native_price.normalized().to_plain_string();
                let quote_price = quote_price.normalized().to_plain_string();
                let quote_amount = buy.amount_in.normalized().to_plain_string();
                let token_amount = buy.amount_out.normalized().to_plain_string();

                TokenSwapMessage {
                    token_id: buy.token.clone(),
                    swaps: vec![TokenSwap {
                        swap_info: SwapInfo {
                            event_type: SwapType::Buy,
                            native_amount: quote_amount.clone(),
                            quote_amount,
                            token_amount,
                            native_price,
                            quote_price,
                            value,
                            transaction_hash: buy.transaction_hash.clone(),
                            created_at: buy.block_timestamp as i64,
                        },
                        account_info,
                    }],
                    total_count: 1,
                }
            } else if let Some(sell) = event.is_sell() {
                let account_info = cache_manager.get_account_info(&sell.account_id).await;

                // block_number 기준 가격 조회 (observer 인덱싱 데이터와 일치)
                let block_number = sell.block_number as i64;
                let quote_id = match cache_manager.get_market_info(&sell.token).await {
                    Ok(market) => market.quote_info.quote_id,
                    Err(_) => WMON_ADDRESS.clone(),
                };
                let quote_price = cache_manager
                    .get_quote_usd_price(&quote_id, block_number)
                    .await;
                let native_price = if quote_id.eq_ignore_ascii_case(&WMON_ADDRESS) {
                    quote_price.clone()
                } else {
                    cache_manager
                        .get_quote_usd_price(&WMON_ADDRESS, block_number)
                        .await
                };

                // USD 가치 계산: value = quote_amount / 10^18 * quote_price
                let value = ((&sell.amount_out / &*DECIMALS) * &quote_price)
                    .normalized()
                    .to_plain_string();
                let native_price = native_price.normalized().to_plain_string();
                let quote_price = quote_price.normalized().to_plain_string();
                let quote_amount = sell.amount_out.normalized().to_plain_string();
                let token_amount = sell.amount_in.normalized().to_plain_string();

                TokenSwapMessage {
                    token_id: sell.token.clone(),
                    swaps: vec![TokenSwap {
                        swap_info: SwapInfo {
                            event_type: SwapType::Sell,
                            native_amount: quote_amount.clone(),
                            quote_amount,
                            token_amount,
                            native_price,
                            quote_price,
                            value,
                            transaction_hash: sell.transaction_hash.clone(),
                            created_at: sell.block_timestamp as i64,
                        },
                        account_info,
                    }],
                    total_count: 1,
                }
            } else {
                tracing::warn!("SwapEventProducer unknown curve event type");
                continue;
            };

            tracing::debug!("Processed event for token_id: {}", message.token_id);
            let _ = self.send_message(message).await;
        }

        Ok(())
    }

    pub async fn receive_dex_event(&self) -> Result<()> {
        //여기서 해야할 일은 event 가 왔을경우 해당 token_id 의 event_sender에 전닌하는 것이다.

        let cache_manager = CacheManager::instance()?;

        let mut receiver = self.dex_event_receiver.lock().await;
        while let Some(event) = receiver.recv().await {
            info!("SwapEventProducer receive dex event: {:?}", event);

            let message: TokenSwapMessage = if let Some(buy) = event.is_buy() {
                let account_info = cache_manager.get_account_info(&buy.account_id).await;

                // block_number 기준 가격 조회 (observer 인덱싱 데이터와 일치)
                let block_number = buy.block_number as i64;
                let quote_id = match cache_manager.get_market_info(&buy.token).await {
                    Ok(market) => market.quote_info.quote_id,
                    Err(_) => WMON_ADDRESS.clone(),
                };
                let quote_price = cache_manager
                    .get_quote_usd_price(&quote_id, block_number)
                    .await;
                let native_price = if quote_id.eq_ignore_ascii_case(&WMON_ADDRESS) {
                    quote_price.clone()
                } else {
                    cache_manager
                        .get_quote_usd_price(&WMON_ADDRESS, block_number)
                        .await
                };

                // USD 가치 계산: value = quote_amount / 10^18 * quote_price
                let value = ((&buy.amount_in / &*DECIMALS) * &quote_price)
                    .normalized()
                    .to_plain_string();
                let native_price = native_price.normalized().to_plain_string();
                let quote_price = quote_price.normalized().to_plain_string();
                let quote_amount = buy.amount_in.normalized().to_plain_string();
                let token_amount = buy.amount_out.normalized().to_plain_string();

                TokenSwapMessage {
                    token_id: buy.token.clone(),
                    swaps: vec![TokenSwap {
                        swap_info: SwapInfo {
                            event_type: SwapType::Buy,
                            native_amount: quote_amount.clone(),
                            quote_amount,
                            token_amount,
                            native_price,
                            quote_price,
                            value,
                            transaction_hash: buy.transaction_hash.clone(),
                            created_at: buy.block_timestamp as i64,
                        },
                        account_info,
                    }],
                    total_count: 1,
                }
            } else if let Some(sell) = event.is_sell() {
                tracing::debug!(
                    "SwapEventProducer receive DEX Sell event for swap: {}",
                    sell.token
                );
                let account_info = cache_manager.get_account_info(&sell.account_id).await;

                // block_number 기준 가격 조회 (observer 인덱싱 데이터와 일치)
                let block_number = sell.block_number as i64;
                let quote_id = match cache_manager.get_market_info(&sell.token).await {
                    Ok(market) => market.quote_info.quote_id,
                    Err(_) => WMON_ADDRESS.clone(),
                };
                let quote_price = cache_manager
                    .get_quote_usd_price(&quote_id, block_number)
                    .await;
                let native_price = if quote_id.eq_ignore_ascii_case(&WMON_ADDRESS) {
                    quote_price.clone()
                } else {
                    cache_manager
                        .get_quote_usd_price(&WMON_ADDRESS, block_number)
                        .await
                };

                // USD 가치 계산: value = quote_amount / 10^18 * quote_price
                let value = ((&sell.amount_out / &*DECIMALS) * &quote_price)
                    .normalized()
                    .to_plain_string();
                let native_price = native_price.normalized().to_plain_string();
                let quote_price = quote_price.normalized().to_plain_string();
                let quote_amount = sell.amount_out.normalized().to_plain_string();
                let token_amount = sell.amount_in.normalized().to_plain_string();

                TokenSwapMessage {
                    token_id: sell.token.clone(),
                    swaps: vec![TokenSwap {
                        swap_info: SwapInfo {
                            event_type: SwapType::Sell,
                            native_amount: quote_amount.clone(),
                            quote_amount,
                            token_amount,
                            native_price,
                            quote_price,
                            value,
                            transaction_hash: sell.transaction_hash.clone(),
                            created_at: sell.block_timestamp as i64,
                        },
                        account_info,
                    }],
                    total_count: 1,
                }
            } else {
                tracing::warn!("Received unknown dex event type");
                continue;
            };
            // TODO: 이벤트 타입에 따라 SwapMessage로 변환
            tracing::debug!("Processed event for token_id: {}", message.token_id);
            let _ = self.send_message(message).await;
        }

        Ok(())
    }

    async fn send_message(&self, message: TokenSwapMessage) -> Result<()> {
        // clone 제거: 먼저 참조로 체크한 후 move
        if let Some(sender) = self.event_sender.get(&message.token_id) {
            if sender.receiver_count() > 0 {
                if let Err(e) = sender.send(message) {
                    error_log!("Failed to send swap message: {}", e);
                }
            } else {
                tracing::debug!(
                    "No receivers for SwapMessage with token_id: {}, skipping message",
                    message.token_id
                );
            }
        } else {
            tracing::debug!(
                "No sender registered for token_id: {}, skipping message",
                message.token_id
            );
        }

        Ok(())
    }

    pub async fn get_event_receiver(
        &self,
        token_id: String,
    ) -> Result<MonitoredBroadcastReceiver<TokenSwapMessage>> {
        let sender = self
            .event_sender
            .entry(token_id.clone())
            .or_insert_with(|| {
                let channel_name = format!("Swap-{}", token_id);
                let (sender, _receiver) =
                    monitored_broadcast_channel::<TokenSwapMessage>(&channel_name, *CHANNEL_SIZE);
                sender
            })
            .clone();

        Ok(sender.subscribe())
    }

    fn start_cleanup_task() -> Result<()> {
        let producer = SWAP_EVENT_PRODUCER
            .get()
            .ok_or_else(|| anyhow::anyhow!("SWAP_EVENT_PRODUCER not initialized"))?;

        let event_sender = Arc::clone(&producer.event_sender);

        tokio::spawn(async move {
            let mut cleanup_interval = tokio::time::interval(tokio::time::Duration::from_secs(300));

            loop {
                cleanup_interval.tick().await;

                let mut removed_count = 0;

                event_sender.retain(|token_id, sender| {
                    if sender.receiver_count() == 0 {
                        tracing::info!("정리 중: 사용되지 않는 토큰 {}", token_id);
                        removed_count += 1;
                        false
                    } else {
                        true
                    }
                });

                if removed_count > 0 {
                    tracing::info!(
                        "SwapEventProducer cleanup: {}개 토큰 제거, 남은 토큰: {}개",
                        removed_count,
                        event_sender.len()
                    );
                }
            }
        });

        Ok(())
    }
}
