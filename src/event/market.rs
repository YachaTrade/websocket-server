use crate::{error_log, types::market::TokenMarketMessage};
use anyhow::Result;
use dashmap::DashMap;
use std::{fmt, sync::Arc};
use tokio::sync::{Mutex, OnceCell};
use tracing::info;

use crate::{
    config::CHANNEL_SIZE,
    db::cache::CacheManager,
    metrics::{
        monitored_broadcast_channel, monitored_channel, MonitoredBroadcastReceiver,
        MonitoredBroadcastSender, MonitoredReceiver, MonitoredSender,
    },
    types::stream::{CurveEventType, DexEventType},
};

/// 전역 MarketEventProducer 인스턴스
pub static MARKET_EVENT_PRODUCER: OnceCell<Arc<MarketEventProducer>> = OnceCell::const_new();

/// Market 이벤트를 받아서 WebSocket 구독자들에게 전송하는 프로듀서
pub struct MarketEventProducer {
    /// Sender channel for DEX events (Arc로 zero-copy 공유)
    pub dex_event_sender: MonitoredSender<Arc<DexEventType>>,

    /// Receiver channel for DEX events
    pub dex_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<DexEventType>>>>,

    /// Sender channel for Curve events (Arc로 zero-copy 공유)
    pub curve_event_sender: MonitoredSender<Arc<CurveEventType>>,

    /// Receiver channel for Curve events
    pub curve_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<CurveEventType>>>>,

    /// token_id별 WebSocket broadcast sender
    pub event_sender: Arc<DashMap<String, MonitoredBroadcastSender<TokenMarketMessage>>>,
}

impl fmt::Debug for MarketEventProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MarketEventProducer").finish()
    }
}

impl MarketEventProducer {
    /// MarketEventProducer 초기화
    pub fn init() -> Result<()> {
        let (dex_event_sender, dex_event_receiver) = monitored_channel("Market-DEX", *CHANNEL_SIZE);
        let (curve_event_sender, curve_event_receiver) =
            monitored_channel("Market-Curve", *CHANNEL_SIZE);
        let event_sender = Arc::new(DashMap::new());

        let producer = MarketEventProducer {
            dex_event_sender,
            dex_event_receiver: Arc::new(Mutex::new(dex_event_receiver)),
            curve_event_sender,
            curve_event_receiver: Arc::new(Mutex::new(curve_event_receiver)),
            event_sender,
        };

        MARKET_EVENT_PRODUCER
            .set(Arc::new(producer))
            .expect("Failed to set MARKET_EVENT_PRODUCER");

        // 주기적 정리 태스크 시작
        Self::start_cleanup_task()?;

        Ok(())
    }

    /// MarketEventProducer의 메인 함수
    pub async fn main() -> Result<()> {
        let producer = MARKET_EVENT_PRODUCER
            .get()
            .ok_or_else(|| anyhow::anyhow!("MarketEventProducer not initialized"))?
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
            "MarketEventProducer main started: receive_curve_event and receive_dex_event tasks spawned"
        );

        Ok(())
    }

    /// Curve 이벤트를 받아서 market info를 구독자들에게 전송
    /// 먼저 Redis 업데이트 후 market info 전송 (순차 처리로 race condition 방지)
    pub async fn receive_curve_event(&self) -> Result<()> {
        let cache_manager = CacheManager::instance()?;

        let mut receiver = self.curve_event_receiver.lock().await;
        while let Some(event) = receiver.recv().await {
            info!("MarketEventProducer receive curve event: {:?}", event);

            // 이벤트 타입에 따라 Redis 업데이트 후 token_id 추출
            let token_id = if let Some(create_curve) = event.is_create_curve() {
                let token = create_curve.token.clone();

                // CreateCurve: 초기 market cache 설정
                if let Err(e) = cache_manager
                    .init_market_cache_from_create_curve(create_curve)
                    .await
                {
                    tracing::warn!(
                        "Failed to init market cache from create curve for {}: {}",
                        token,
                        e
                    );
                }

                token
            } else if let Some(sync) = event.is_curve_sync() {
                let token = sync.token.clone();

                // 1. Redis에 market cache 업데이트
                if let Err(e) = cache_manager
                    .update_market_cache_from_curve_sync(sync)
                    .await
                {
                    tracing::warn!(
                        "Failed to update market cache from curve sync for {}: {}",
                        token,
                        e
                    );
                }

                token
            } else if let Some(buy) = event.is_buy() {
                // Buy 이벤트: volume 업데이트만
                let token = buy.token.clone();
                if let Err(e) = cache_manager
                    .increment_market_volume(&token, &buy.amount_in)
                    .await
                {
                    tracing::warn!("Failed to increment market volume for buy {}: {}", token, e);
                }
                // Buy 이벤트는 market info broadcast 필요 없음 (volume만 업데이트)
                continue;
            } else if let Some(sell) = event.is_sell() {
                // Sell 이벤트: volume 업데이트만
                let token = sell.token.clone();
                if let Err(e) = cache_manager
                    .increment_market_volume(&token, &sell.amount_out)
                    .await
                {
                    tracing::warn!(
                        "Failed to increment market volume for sell {}: {}",
                        token,
                        e
                    );
                }
                // Sell 이벤트는 market info broadcast 필요 없음 (volume만 업데이트)
                continue;
            } else if let Some(graduate) = event.is_graduated() {
                graduate.token.clone() // Graduate는 이미 stream에서 처리됨
            } else {
                tracing::debug!("Unknown curve event type, skipping market broadcast");
                continue;
            };

            // Redis/DB에서 market info 조회
            match cache_manager.get_market_info(&token_id).await {
                Ok(market_info) => {
                    let message = TokenMarketMessage {
                        token_id, // clone 제거 (move)
                        market_info,
                    };

                    if let Err(e) = self.send_message(message).await {
                        error_log!("Failed to send market message: {}", e);
                    }
                }
                Err(e) => {
                    error_log!("Failed to get market info for token {}: {}", token_id, e);
                }
            }
        }

        Ok(())
    }

    /// DEX 이벤트를 받아서 market info를 구독자들에게 전송
    /// 먼저 Redis 업데이트 후 market info 전송 (순차 처리로 race condition 방지)
    pub async fn receive_dex_event(&self) -> Result<()> {
        let cache_manager = CacheManager::instance()?;

        let mut receiver = self.dex_event_receiver.lock().await;
        while let Some(event) = receiver.recv().await {
            info!("MarketEventProducer receive dex event: {:?}", event);

            // 이벤트 타입에 따라 Redis 업데이트 후 token_id 추출
            let token_id = if let Some(sync) = event.is_dex_sync() {
                let token = sync.token.clone();

                // 1. Redis에 market cache 업데이트
                if let Err(e) = cache_manager.update_market_cache_from_dex_sync(sync).await {
                    tracing::warn!(
                        "Failed to update market cache from dex sync for {}: {}",
                        token,
                        e
                    );
                }

                token
            } else if let Some(buy) = event.is_buy() {
                // Buy 이벤트: volume 업데이트만
                let token = buy.token.clone();
                if let Err(e) = cache_manager
                    .increment_market_volume(&token, &buy.amount_in)
                    .await
                {
                    tracing::warn!(
                        "Failed to increment market volume for dex buy {}: {}",
                        token,
                        e
                    );
                }
                // Buy 이벤트는 market info broadcast 필요 없음 (volume만 업데이트)
                continue;
            } else if let Some(sell) = event.is_sell() {
                // Sell 이벤트: volume 업데이트만
                let token = sell.token.clone();
                if let Err(e) = cache_manager
                    .increment_market_volume(&token, &sell.amount_out)
                    .await
                {
                    tracing::warn!(
                        "Failed to increment market volume for dex sell {}: {}",
                        token,
                        e
                    );
                }
                // Sell 이벤트는 market info broadcast 필요 없음 (volume만 업데이트)
                continue;
            } else {
                tracing::warn!("Received unknown dex event type");
                continue;
            };

            // Redis/DB에서 market info 조회
            match cache_manager.get_market_info(&token_id).await {
                Ok(market_info) => {
                    error_log!(
                        "📤 DEX Market info retrieved: token={}, price={}, reserve_native={}, reserve_token={}",
                        token_id,
                        market_info.price,
                        market_info.reserve_native,
                        market_info.reserve_token
                    );

                    let message = TokenMarketMessage {
                        token_id, // clone 제거 (move)
                        market_info,
                    };

                    if let Err(e) = self.send_message(message).await {
                        error_log!("Failed to send market message: {}", e);
                    }
                }
                Err(e) => {
                    error_log!("Failed to get market info for token {}: {}", token_id, e);
                }
            }
        }

        Ok(())
    }

    /// token_id별 구독자들에게 market 메시지 전송
    async fn send_message(&self, message: TokenMarketMessage) -> Result<()> {
        // clone 제거: 먼저 참조로 체크한 후 move
        if let Some(sender) = self.event_sender.get(&message.token_id) {
            if sender.receiver_count() > 0 {
                if let Err(e) = sender.send(message) {
                    error_log!("Failed to send market message: {}", e);
                }
            } else {
                tracing::debug!(
                    "No receivers for MarketMessage with token_id: {}, skipping message",
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

    /// 특정 token_id에 대한 market 구독 receiver 생성
    pub async fn get_event_receiver(
        &self,
        token_id: String,
    ) -> Result<MonitoredBroadcastReceiver<TokenMarketMessage>> {
        let sender = self
            .event_sender
            .entry(token_id.clone())
            .or_insert_with(|| {
                let channel_name = format!("Market-{}", token_id);
                let (sender, _receiver) =
                    monitored_broadcast_channel::<TokenMarketMessage>(&channel_name, *CHANNEL_SIZE);
                sender
            })
            .clone();

        Ok(sender.subscribe())
    }

    /// 사용되지 않는 토큰 채널 정리 태스크 시작 (5분마다 실행)
    fn start_cleanup_task() -> Result<()> {
        let producer = MARKET_EVENT_PRODUCER
            .get()
            .ok_or_else(|| anyhow::anyhow!("MARKET_EVENT_PRODUCER not initialized"))?;

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
                        "MarketEventProducer cleanup: {}개 토큰 제거, 남은 토큰: {}개",
                        removed_count,
                        event_sender.len()
                    );
                }
            }
        });

        Ok(())
    }
}
