use crate::{
    error_log,
    types::metrics::{TimeFrame, TokenMetricsMessage},
};
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

/// 전역 MetricsEventProducer 인스턴스
pub static METRICS_EVENT_PRODUCER: OnceCell<Arc<MetricsEventProducer>> = OnceCell::const_new();

/// Metrics 이벤트를 받아서 WebSocket 구독자들에게 전송하는 프로듀서
pub struct MetricsEventProducer {
    /// Sender channel for DEX events (Arc로 zero-copy 공유)
    pub dex_event_sender: MonitoredSender<Arc<DexEventType>>,

    /// Receiver channel for DEX events
    pub dex_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<DexEventType>>>>,

    /// Sender channel for Curve events (Arc로 zero-copy 공유)
    pub curve_event_sender: MonitoredSender<Arc<CurveEventType>>,

    /// Receiver channel for Curve events
    pub curve_event_receiver: Arc<Mutex<MonitoredReceiver<Arc<CurveEventType>>>>,

    /// token_id별 WebSocket broadcast sender
    pub event_sender: Arc<DashMap<String, MonitoredBroadcastSender<TokenMetricsMessage>>>,
}

impl fmt::Debug for MetricsEventProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsEventProducer").finish()
    }
}

impl MetricsEventProducer {
    /// MetricsEventProducer 초기화
    pub fn init() -> Result<()> {
        let (dex_event_sender, dex_event_receiver) =
            monitored_channel("Metrics-DEX", *CHANNEL_SIZE);
        let (curve_event_sender, curve_event_receiver) =
            monitored_channel("Metrics-Curve", *CHANNEL_SIZE);
        let event_sender = Arc::new(DashMap::new());

        let producer = MetricsEventProducer {
            dex_event_sender,
            dex_event_receiver: Arc::new(Mutex::new(dex_event_receiver)),
            curve_event_sender,
            curve_event_receiver: Arc::new(Mutex::new(curve_event_receiver)),
            event_sender,
        };

        METRICS_EVENT_PRODUCER
            .set(Arc::new(producer))
            .expect("Failed to set METRICS_EVENT_PRODUCER");

        // 주기적 정리 태스크 시작
        Self::start_cleanup_task()?;

        Ok(())
    }

    /// MetricsEventProducer의 메인 함수
    /// receive_curve_event와 receive_dex_event를 실행하는 태스크를 생성
    pub async fn main() -> Result<()> {
        let producer = METRICS_EVENT_PRODUCER
            .get()
            .ok_or_else(|| anyhow::anyhow!("MetricsEventProducer not initialized"))?
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
            "MetricsEventProducer main started: receive_curve_event and receive_dex_event tasks spawned"
        );

        Ok(())
    }

    /// Curve 이벤트를 받아서 metrics를 계산하고 구독자들에게 전송
    /// 모든 이벤트에서 먼저 Redis 업데이트 후 metrics 전송 (순차 처리로 race condition 방지)
    pub async fn receive_curve_event(&self) -> Result<()> {
        let cache_manager = CacheManager::instance()?;

        let mut receiver = self.curve_event_receiver.lock().await;
        while let Some(event) = receiver.recv().await {
            info!("MetricsEventProducer receive curve event: {:?}", event);

            // 이벤트 타입에 따라 Redis 업데이트 후 token_id 추출
            // CreateCurve: 초기 metrics price 설정 (WebSocket 전송 없음)
            // Buy/Sell: swap만 업데이트하고 Sync price 반영 후 전송
            // Sync: price 업데이트 후 WebSocket 전송
            let token_id = if let Some(create_curve) = event.is_create_curve() {
                // CreateCurve 이벤트: 초기 가상 가격을 먼저 저장하고 broadcast는 하지 않음
                if let Err(e) = cache_manager
                    .init_metrics_from_create_curve(create_curve)
                    .await
                {
                    tracing::warn!(
                        "Failed to init metrics from create curve for {}: {}",
                        create_curve.token,
                        e
                    );
                }
                tracing::debug!("CreateCurve event: metrics initialized, skipping broadcast");
                continue;
            } else if let Some(buy) = event.is_buy() {
                let token = buy.token.clone();

                // 1. ensure_metrics_loaded로 DB에서 Redis로 데이터 로드 (없을 경우)
                if let Err(e) = cache_manager.ensure_metrics_loaded(&token).await {
                    tracing::warn!("Failed to ensure metrics loaded for {}: {}", token, e);
                }

                // 2. Redis에 swap 데이터 업데이트
                if let Err(e) = cache_manager.update_metrics_on_buy(buy).await {
                    tracing::warn!("Failed to update metrics on buy for {}: {}", token, e);
                }

                tracing::debug!("Curve Buy event: swap updated, waiting for Sync price broadcast");
                continue;
            } else if let Some(sell) = event.is_sell() {
                let token = sell.token.clone();

                // 1. ensure_metrics_loaded로 DB에서 Redis로 데이터 로드 (없을 경우)
                if let Err(e) = cache_manager.ensure_metrics_loaded(&token).await {
                    tracing::warn!("Failed to ensure metrics loaded for {}: {}", token, e);
                }

                // 2. Redis에 swap 데이터 업데이트
                if let Err(e) = cache_manager.update_metrics_on_sell(sell).await {
                    tracing::warn!("Failed to update metrics on sell for {}: {}", token, e);
                }

                tracing::debug!("Curve Sell event: swap updated, waiting for Sync price broadcast");
                continue;
            } else if let Some(sync) = event.is_curve_sync() {
                // Sync 이벤트: price 업데이트 후 metrics broadcast
                if let Err(e) = cache_manager.update_metrics_on_sync(sync).await {
                    tracing::warn!("Failed to update metrics on sync for {}: {}", sync.token, e);
                }
                sync.token.clone()
            } else {
                tracing::warn!("MetricsEventProducer unknown curve event type");
                continue;
            };

            // Redis에서 metrics 계산 (모든 timeframe)
            let timeframes = vec![
                TimeFrame::ThirtyMinutes,
                TimeFrame::OneHour,
                TimeFrame::FourHours,
                TimeFrame::OneDay,
            ];

            match cache_manager.get_metrics(&token_id, timeframes).await {
                Ok(metrics) => {
                    let message = TokenMetricsMessage {
                        token_id, // clone 제거 (move)
                        metrics,
                    };

                    if let Err(e) = self.send_message(message).await {
                        error_log!("Failed to send metrics message: {}", e);
                    }
                }
                Err(e) => {
                    error_log!("Failed to get metrics for token {}: {}", token_id, e);
                }
            }
        }

        Ok(())
    }

    /// DEX 이벤트를 받아서 metrics를 계산하고 구독자들에게 전송
    /// 모든 이벤트에서 먼저 Redis 업데이트 후 metrics 전송 (순차 처리로 race condition 방지)
    pub async fn receive_dex_event(&self) -> Result<()> {
        let cache_manager = CacheManager::instance()?;

        let mut receiver = self.dex_event_receiver.lock().await;
        while let Some(event) = receiver.recv().await {
            info!("MetricsEventProducer receive dex event: {:?}", event);

            // 이벤트 타입에 따라 Redis 업데이트 후 token_id 추출
            // Buy/Sell: metrics 업데이트 + WebSocket 전송
            // DexSync: metrics price 업데이트 + WebSocket 전송
            let token_id = if let Some(buy) = event.is_buy() {
                let token = buy.token.clone();

                // 1. ensure_metrics_loaded로 DB에서 Redis로 데이터 로드 (없을 경우)
                if let Err(e) = cache_manager.ensure_metrics_loaded(&token).await {
                    tracing::warn!("Failed to ensure metrics loaded for {}: {}", token, e);
                }

                // 2. Redis에 swap 데이터 업데이트
                if let Err(e) = cache_manager.update_metrics_on_buy(buy).await {
                    tracing::warn!("Failed to update metrics on buy for {}: {}", token, e);
                }

                token
            } else if let Some(sell) = event.is_sell() {
                let token = sell.token.clone();

                // 1. ensure_metrics_loaded로 DB에서 Redis로 데이터 로드 (없을 경우)
                if let Err(e) = cache_manager.ensure_metrics_loaded(&token).await {
                    tracing::warn!("Failed to ensure metrics loaded for {}: {}", token, e);
                }

                // 2. Redis에 swap 데이터 업데이트
                if let Err(e) = cache_manager.update_metrics_on_sell(sell).await {
                    tracing::warn!("Failed to update metrics on sell for {}: {}", token, e);
                }

                token
            } else if let Some(sync) = event.is_dex_sync() {
                // DexSync 이벤트: price 업데이트 후 metrics broadcast
                if let Err(e) = cache_manager.update_metrics_on_dex_sync(sync).await {
                    tracing::warn!(
                        "Failed to update metrics on dex sync for {}: {}",
                        sync.token,
                        e
                    );
                }
                sync.token.clone()
            } else {
                tracing::warn!("Received unknown dex event type");
                continue;
            };

            // Redis에서 metrics 계산 (모든 timeframe)
            let timeframes = vec![
                TimeFrame::ThirtyMinutes,
                TimeFrame::OneHour,
                TimeFrame::FourHours,
                TimeFrame::OneDay,
            ];

            match cache_manager.get_metrics(&token_id, timeframes).await {
                Ok(metrics) => {
                    let message = TokenMetricsMessage {
                        token_id, // clone 제거 (move)
                        metrics,
                    };

                    if let Err(e) = self.send_message(message).await {
                        error_log!("Failed to send metrics message: {}", e);
                    }
                }
                Err(e) => {
                    error_log!("Failed to get metrics for token {}: {}", token_id, e);
                }
            }
        }

        Ok(())
    }

    /// token_id별 구독자들에게 metrics 메시지 전송
    async fn send_message(&self, message: TokenMetricsMessage) -> Result<()> {
        // clone 제거: 먼저 참조로 체크한 후 move
        if let Some(sender) = self.event_sender.get(&message.token_id) {
            if sender.receiver_count() > 0 {
                if let Err(e) = sender.send(message) {
                    error_log!("Failed to send metrics message: {}", e);
                }
            } else {
                tracing::debug!(
                    "No receivers for MetricsMessage with token_id: {}, skipping message",
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

    /// 특정 token_id에 대한 metrics 구독 receiver 생성
    pub async fn get_event_receiver(
        &self,
        token_id: String,
    ) -> Result<MonitoredBroadcastReceiver<TokenMetricsMessage>> {
        let sender = self
            .event_sender
            .entry(token_id.clone())
            .or_insert_with(|| {
                let channel_name = format!("Metrics-{}", token_id);
                let (sender, _receiver) = monitored_broadcast_channel::<TokenMetricsMessage>(
                    &channel_name,
                    *CHANNEL_SIZE,
                );
                sender
            })
            .clone();

        Ok(sender.subscribe())
    }

    /// 사용되지 않는 토큰 채널 정리 태스크 시작 (5분마다 실행)
    fn start_cleanup_task() -> Result<()> {
        let producer = METRICS_EVENT_PRODUCER
            .get()
            .ok_or_else(|| anyhow::anyhow!("METRICS_EVENT_PRODUCER not initialized"))?;

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
                        "MetricsEventProducer cleanup: {}개 토큰 제거, 남은 토큰: {}개",
                        removed_count,
                        event_sender.len()
                    );
                }
            }
        });

        Ok(())
    }
}
