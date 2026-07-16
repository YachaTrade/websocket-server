use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{broadcast, mpsc};

/// 개별 채널 상태
#[derive(Debug)]
struct ChannelStatus {
    is_healthy: AtomicBool,
    sent_success: AtomicU64,       // 성공한 전송
    sent_failed_full: AtomicU64,   // Channel Full 에러
    sent_failed_closed: AtomicU64, // Channel Closed 에러
    received_count: AtomicU64,
}

impl ChannelStatus {
    fn new() -> Self {
        Self {
            is_healthy: AtomicBool::new(true), // 기본적으로 healthy로 시작
            sent_success: AtomicU64::new(0),
            sent_failed_full: AtomicU64::new(0),
            sent_failed_closed: AtomicU64::new(0),
            received_count: AtomicU64::new(0),
        }
    }
}

/// 브로드캐스트 채널 총합 상태 (단순 카운터)
#[derive(Debug)]
struct BroadcastTotals {
    sent_success: AtomicU64, // 성공한 전송
    sent_failed: AtomicU64,  // 실패한 전송 (broadcast는 Full 에러 없음)
    received_count: AtomicU64,
}

impl BroadcastTotals {
    fn new() -> Self {
        Self {
            sent_success: AtomicU64::new(0),
            sent_failed: AtomicU64::new(0),
            received_count: AtomicU64::new(0),
        }
    }
}

/// 이벤트 채널 메트릭 관리
#[derive(Debug)]
pub struct EventMetrics {
    // 이벤트 처리 채널들 (-Curve, -DEX 등) - 개별 추적
    event_channels: DashMap<String, ChannelStatus>,
    // 브로드캐스트 채널들 (-Broadcast 등) - 총합만 관리
    broadcast_totals: BroadcastTotals,
}

impl EventMetrics {
    pub fn new() -> Self {
        Self {
            event_channels: DashMap::new(),
            broadcast_totals: BroadcastTotals::new(),
        }
    }

    /// 채널 등록 (타입에 따라 적절한 저장소에 등록)
    pub fn register_channel(&self, name: &str) {
        if !name.contains("-Broadcast") {
            // 이벤트 채널은 개별 추적 (브로드캐스트 채널은 등록만)
            self.event_channels
                .insert(name.to_string(), ChannelStatus::new());
        }
    }

    /// 채널을 건강한 상태로 표시
    pub fn mark_channel_healthy(&self, name: &str) {
        if name.contains("-Broadcast") {
            // 브로드캐스트 채널은 무시
            return;
        }

        if let Some(status) = self.event_channels.get(name) {
            status.is_healthy.store(true, Ordering::Relaxed);
        }
    }

    /// 채널을 죽은 상태로 표시
    pub fn mark_channel_dead(&self, name: &str) {
        if name.contains("-Broadcast") {
            // 브로드캐스트 채널은 무시
            return;
        }

        if let Some(status) = self.event_channels.get(name) {
            status.is_healthy.store(false, Ordering::Relaxed);
        }
    }

    /// 보낸 메시지 수 증가 (deprecated - 특정 채널용 함수를 사용하세요)
    pub fn increment_sent(&self) {
        // 기존 API 호환성을 위해 첫 번째 event 채널에 기록
        if let Some(entry) = self.event_channels.iter().next() {
            entry.sent_success.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 받은 메시지 수 증가 (deprecated - 특정 채널용 함수를 사용하세요)
    pub fn increment_received(&self) {
        // 기존 API 호환성을 위해 첫 번째 event 채널에 기록
        if let Some(entry) = self.event_channels.iter().next() {
            entry.received_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 특정 채널의 성공한 전송 수 증가
    pub fn increment_sent_success_for_channel(&self, name: &str) {
        if name.contains("-Broadcast") {
            self.broadcast_totals
                .sent_success
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        if let Some(status) = self.event_channels.get(name) {
            status.sent_success.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 특정 채널의 Full 에러 수 증가
    pub fn increment_sent_failed_full_for_channel(&self, name: &str) {
        if name.contains("-Broadcast") {
            // Broadcast 채널은 Full 에러가 없음
            return;
        }

        if let Some(status) = self.event_channels.get(name) {
            status.sent_failed_full.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 특정 채널의 Closed 에러 수 증가
    pub fn increment_sent_failed_closed_for_channel(&self, name: &str) {
        if name.contains("-Broadcast") {
            self.broadcast_totals
                .sent_failed
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        if let Some(status) = self.event_channels.get(name) {
            status.sent_failed_closed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 특정 채널의 받은 메시지 수 증가
    pub fn increment_received_for_channel(&self, name: &str) {
        if name.contains("-Broadcast") {
            self.broadcast_totals
                .received_count
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        if let Some(status) = self.event_channels.get(name) {
            status.received_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 채널 통계 업데이트 (기존 API 호환성)
    pub fn update_channel_stats(&self, _total: usize, _healthy: usize) {
        // 기존 API 호환을 위한 더미 함수
        // 실제로는 register_channel과 mark_channel_* 함수들을 사용
    }

    /// 이벤트 채널 통계 반환: (전체 채널 수, 건강한 채널 수, 총 성공, 총 Full 실패, 총 Closed 실패, 총 수신)
    pub fn get_event_values(&self) -> (usize, usize, u64, u64, u64, u64) {
        self.get_channel_group_values(&self.event_channels)
    }

    /// 브로드캐스트 채널 통계 반환: (총 성공, 총 실패, 총 수신)
    pub fn get_broadcast_values(&self) -> (u64, u64, u64) {
        (
            self.broadcast_totals.sent_success.load(Ordering::Relaxed),
            self.broadcast_totals.sent_failed.load(Ordering::Relaxed),
            self.broadcast_totals.received_count.load(Ordering::Relaxed),
        )
    }

    /// 채널 그룹의 통계를 계산하는 헬퍼 함수
    fn get_channel_group_values(
        &self,
        channel_map: &DashMap<String, ChannelStatus>,
    ) -> (usize, usize, u64, u64, u64, u64) {
        let total_channels = channel_map.len();
        let mut healthy_channels = 0;
        let mut total_sent_success = 0u64;
        let mut total_sent_failed_full = 0u64;
        let mut total_sent_failed_closed = 0u64;
        let mut total_received = 0u64;

        for entry in channel_map.iter() {
            let status = entry.value();

            if status.is_healthy.load(Ordering::Relaxed) {
                healthy_channels += 1;
            }

            total_sent_success += status.sent_success.load(Ordering::Relaxed);
            total_sent_failed_full += status.sent_failed_full.load(Ordering::Relaxed);
            total_sent_failed_closed += status.sent_failed_closed.load(Ordering::Relaxed);
            total_received += status.received_count.load(Ordering::Relaxed);
        }

        (
            total_channels,
            healthy_channels,
            total_sent_success,
            total_sent_failed_full,
            total_sent_failed_closed,
            total_received,
        )
    }

    /// 전체 통계 반환 (이벤트 채널만, 브로드캐스트 제외): (전체 채널 수, 건강한 채널 수, 총 성공, 총 Full 실패, 총 Closed 실패, 총 수신)
    pub fn get_values(&self) -> (usize, usize, u64, u64, u64, u64) {
        // 브로드캐스트는 제외하고 이벤트 채널만 반환
        self.get_event_values()
    }

    /// 이벤트 채널별 상세 정보 반환
    pub fn get_event_channel_details(&self) -> Vec<(String, bool, u64, u64)> {
        self.get_channel_group_details(&self.event_channels)
    }

    /// 브로드캐스트 채널별 상세 정보 반환 (단순 카운터)
    pub fn get_broadcast_channel_details(&self) -> Vec<(String, bool, u64, u64)> {
        let (sent_success, sent_failed, received) = self.get_broadcast_values();
        let total_sent = sent_success + sent_failed;
        if total_sent > 0 || received > 0 {
            vec![(
                "Broadcast-Total".to_string(),
                total_sent >= received,
                total_sent,
                received,
            )]
        } else {
            vec![]
        }
    }

    /// 채널 그룹의 상세 정보를 반환하는 헬퍼 함수
    fn get_channel_group_details(
        &self,
        channel_map: &DashMap<String, ChannelStatus>,
    ) -> Vec<(String, bool, u64, u64)> {
        let mut details = Vec::new();

        for entry in channel_map.iter() {
            let channel_name = entry.key().clone();
            let status = entry.value();

            let is_healthy = status.is_healthy.load(Ordering::Relaxed);
            let sent_success = status.sent_success.load(Ordering::Relaxed);
            let sent_failed_full = status.sent_failed_full.load(Ordering::Relaxed);
            let sent_failed_closed = status.sent_failed_closed.load(Ordering::Relaxed);
            let total_sent = sent_success + sent_failed_full + sent_failed_closed;
            let received = status.received_count.load(Ordering::Relaxed);

            details.push((channel_name, is_healthy, total_sent, received));
        }

        // 이름 순으로 정렬
        details.sort_by(|a, b| a.0.cmp(&b.0));
        details
    }

    /// 개별 채널별 상세 정보 반환 (기존 API 호환성)
    pub fn get_channel_details(&self) -> Vec<(String, bool, u64, u64)> {
        let mut details = self.get_event_channel_details();
        details.extend(self.get_broadcast_channel_details());

        // 이름 순으로 정렬
        details.sort_by(|a, b| a.0.cmp(&b.0));
        details
    }
}

impl Default for EventMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Monitored mpsc Sender 래퍼
#[derive(Debug)]
pub struct MonitoredSender<T> {
    sender: mpsc::Sender<T>,
    channel_name: String,
    metrics: &'static EventMetrics,
}

impl<T> MonitoredSender<T> {
    pub fn new(
        sender: mpsc::Sender<T>,
        channel_name: String,
        metrics: &'static EventMetrics,
    ) -> Self {
        metrics.register_channel(&channel_name);
        Self {
            sender,
            channel_name,
            metrics,
        }
    }

    pub async fn send(&self, value: T) -> Result<(), mpsc::error::SendError<T>> {
        match self.sender.send(value).await {
            Ok(()) => {
                self.metrics
                    .increment_sent_success_for_channel(&self.channel_name);
                Ok(())
            }
            Err(e) => {
                self.metrics
                    .increment_sent_failed_closed_for_channel(&self.channel_name);
                self.metrics.mark_channel_dead(&self.channel_name);
                Err(e)
            }
        }
    }

    pub fn try_send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        match self.sender.try_send(value) {
            Ok(()) => {
                self.metrics
                    .increment_sent_success_for_channel(&self.channel_name);
                Ok(())
            }
            Err(e) => {
                match &e {
                    mpsc::error::TrySendError::Full(_) => {
                        self.metrics
                            .increment_sent_failed_full_for_channel(&self.channel_name);
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        self.metrics
                            .increment_sent_failed_closed_for_channel(&self.channel_name);
                        self.metrics.mark_channel_dead(&self.channel_name);
                    }
                }
                Err(e)
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl<T> Clone for MonitoredSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            channel_name: self.channel_name.clone(),
            metrics: self.metrics,
        }
    }
}

/// Monitored mpsc Receiver 래퍼
#[derive(Debug)]
pub struct MonitoredReceiver<T> {
    receiver: mpsc::Receiver<T>,
    channel_name: String,
    metrics: &'static EventMetrics,
}

impl<T> MonitoredReceiver<T> {
    pub fn new(
        receiver: mpsc::Receiver<T>,
        channel_name: String,
        metrics: &'static EventMetrics,
    ) -> Self {
        Self {
            receiver,
            channel_name,
            metrics,
        }
    }

    pub async fn recv(&mut self) -> Option<T> {
        match self.receiver.recv().await {
            Some(value) => {
                self.metrics
                    .increment_received_for_channel(&self.channel_name);
                Some(value)
            }
            None => {
                self.metrics.mark_channel_dead(&self.channel_name);
                None
            }
        }
    }

    pub fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        match self.receiver.try_recv() {
            Ok(value) => {
                self.metrics
                    .increment_received_for_channel(&self.channel_name);
                Ok(value)
            }
            Err(e) => {
                if matches!(e, mpsc::error::TryRecvError::Disconnected) {
                    self.metrics.mark_channel_dead(&self.channel_name);
                }
                Err(e)
            }
        }
    }
}

/// Monitored broadcast Sender 래퍼
#[derive(Debug)]
pub struct MonitoredBroadcastSender<T> {
    sender: broadcast::Sender<T>,
    channel_name: String,
    metrics: &'static EventMetrics,
}

impl<T: Clone> MonitoredBroadcastSender<T> {
    pub fn new(
        sender: broadcast::Sender<T>,
        channel_name: String,
        metrics: &'static EventMetrics,
    ) -> Self {
        metrics.register_channel(&channel_name);
        Self {
            sender,
            channel_name,
            metrics,
        }
    }

    pub fn send(&self, value: T) -> Result<usize, broadcast::error::SendError<T>> {
        match self.sender.send(value) {
            Ok(receiver_count) => {
                self.metrics
                    .increment_sent_success_for_channel(&self.channel_name);
                Ok(receiver_count)
            }
            Err(e) => {
                self.metrics
                    .increment_sent_failed_closed_for_channel(&self.channel_name);
                Err(e)
            }
        }
    }

    pub fn subscribe(&self) -> MonitoredBroadcastReceiver<T> {
        let receiver = self.sender.subscribe();
        MonitoredBroadcastReceiver::new(receiver, self.channel_name.clone(), self.metrics)
    }

    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl<T> Clone for MonitoredBroadcastSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            channel_name: self.channel_name.clone(),
            metrics: self.metrics,
        }
    }
}

/// Monitored broadcast Receiver 래퍼
#[derive(Debug)]
pub struct MonitoredBroadcastReceiver<T> {
    receiver: broadcast::Receiver<T>,
    channel_name: String,
    metrics: &'static EventMetrics,
}

impl<T: Clone> MonitoredBroadcastReceiver<T> {
    pub fn new(
        receiver: broadcast::Receiver<T>,
        channel_name: String,
        metrics: &'static EventMetrics,
    ) -> Self {
        Self {
            receiver,
            channel_name,
            metrics,
        }
    }

    pub async fn recv(&mut self) -> Result<T, broadcast::error::RecvError> {
        match self.receiver.recv().await {
            Ok(value) => {
                self.metrics
                    .increment_received_for_channel(&self.channel_name);
                Ok(value)
            }
            Err(e) => {
                if matches!(e, broadcast::error::RecvError::Closed) {
                    self.metrics.mark_channel_dead(&self.channel_name);
                }
                Err(e)
            }
        }
    }

    pub fn try_recv(&mut self) -> Result<T, broadcast::error::TryRecvError> {
        match self.receiver.try_recv() {
            Ok(value) => {
                self.metrics
                    .increment_received_for_channel(&self.channel_name);
                Ok(value)
            }
            Err(e) => {
                if matches!(e, broadcast::error::TryRecvError::Closed) {
                    self.metrics.mark_channel_dead(&self.channel_name);
                }
                Err(e)
            }
        }
    }
}

/// 모니터링되는 mpsc 채널 생성
pub fn monitored_channel<T>(
    name: &str,
    buffer: usize,
) -> (MonitoredSender<T>, MonitoredReceiver<T>) {
    let (sender, receiver) = mpsc::channel(buffer);
    // METRICS는 Lazy<Metrics> static이므로 이미 'static lifetime을 가짐
    let metrics: &'static EventMetrics = &crate::metrics::METRICS.event;

    (
        MonitoredSender::new(sender, name.to_string(), metrics),
        MonitoredReceiver::new(receiver, name.to_string(), metrics),
    )
}

/// 모니터링되는 broadcast 채널 생성
pub fn monitored_broadcast_channel<T: Clone>(
    name: &str,
    buffer: usize,
) -> (MonitoredBroadcastSender<T>, MonitoredBroadcastReceiver<T>) {
    let (sender, receiver) = broadcast::channel(buffer);
    // METRICS는 Lazy<Metrics> static이므로 이미 'static lifetime을 가짐
    let metrics: &'static EventMetrics = &crate::metrics::METRICS.event;

    (
        MonitoredBroadcastSender::new(sender, name.to_string(), metrics),
        MonitoredBroadcastReceiver::new(receiver, name.to_string(), metrics),
    )
}
