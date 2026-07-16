use bigdecimal::BigDecimal;
use serde::Serialize;

use std::{any::Any, fmt::Debug};

use crate::types::MarketType;

// 이벤트 타입 열거형
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    Curve,
    Dex,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Curve => "curve",
            EventType::Dex => "dex",
        }
    }

    pub fn all() -> [EventType; 2] {
        [EventType::Curve, EventType::Dex]
    }
}

//======================== Curve ========================
/// Curve 이벤트를 나타내는 Enum (성능 최적화: Box<dyn Trait> 대체)
#[derive(Debug, Clone)]
pub enum CurveEventType {
    CreateCurve(CreateCurve),
    Buy(Buy),
    Sell(Sell),
    CurveSync(CurveSync),
    Graduate(Graduate),
    CurveChartUpdate(CurveChartUpdate),
}

impl CurveEventType {
    /// 토큰 주소 반환
    pub fn get_token(&self) -> &str {
        match self {
            CurveEventType::CreateCurve(e) => &e.token,
            CurveEventType::Buy(e) => &e.token,
            CurveEventType::Sell(e) => &e.token,
            CurveEventType::CurveSync(e) => &e.token,
            CurveEventType::Graduate(e) => &e.token,
            CurveEventType::CurveChartUpdate(e) => &e.sync.token,
        }
    }

    /// 블록 번호 반환
    pub fn get_block_number(&self) -> u64 {
        match self {
            CurveEventType::CreateCurve(e) => e.block_number,
            CurveEventType::Buy(e) => e.block_number,
            CurveEventType::Sell(e) => e.block_number,
            CurveEventType::CurveSync(e) => e.block_number,
            CurveEventType::Graduate(e) => e.block_number,
            CurveEventType::CurveChartUpdate(e) => e.block_number,
        }
    }

    /// 로그 인덱스 반환
    pub fn get_log_index(&self) -> u64 {
        match self {
            CurveEventType::CreateCurve(e) => e.log_index,
            CurveEventType::Buy(e) => e.log_index,
            CurveEventType::Sell(e) => e.log_index,
            CurveEventType::CurveSync(e) => e.log_index,
            CurveEventType::Graduate(e) => e.log_index,
            CurveEventType::CurveChartUpdate(e) => e.sync.log_index,
        }
    }

    /// 트랜잭션 인덱스 반환
    pub fn get_transaction_index(&self) -> u64 {
        match self {
            CurveEventType::CreateCurve(e) => e.transaction_index,
            CurveEventType::Buy(e) => e.transaction_index,
            CurveEventType::Sell(e) => e.transaction_index,
            CurveEventType::CurveSync(e) => e.transaction_index,
            CurveEventType::Graduate(e) => e.transaction_index,
            CurveEventType::CurveChartUpdate(e) => e.sync.transaction_index,
        }
    }

    /// Buy 또는 Sell 이벤트인지 확인 (trade 이벤트)
    pub fn is_trade(&self) -> bool {
        matches!(self, CurveEventType::Buy(_) | CurveEventType::Sell(_))
    }

    /// Buy 이벤트 확인 및 반환
    pub fn is_buy(&self) -> Option<&Buy> {
        match self {
            CurveEventType::Buy(buy) => Some(buy),
            _ => None,
        }
    }

    /// Sell 이벤트 확인 및 반환
    pub fn is_sell(&self) -> Option<&Sell> {
        match self {
            CurveEventType::Sell(sell) => Some(sell),
            _ => None,
        }
    }

    /// CreateCurve 이벤트 확인 및 반환
    pub fn is_create_curve(&self) -> Option<&CreateCurve> {
        match self {
            CurveEventType::CreateCurve(create) => Some(create),
            _ => None,
        }
    }

    /// CurveSync 이벤트 확인 및 반환
    pub fn is_curve_sync(&self) -> Option<&CurveSync> {
        match self {
            CurveEventType::CurveSync(sync) => Some(sync),
            _ => None,
        }
    }

    /// Graduate 이벤트 확인 및 반환
    pub fn is_graduated(&self) -> Option<&Graduate> {
        match self {
            CurveEventType::Graduate(grad) => Some(grad),
            _ => None,
        }
    }

    /// CurveChartUpdate 이벤트 확인 및 반환
    pub fn is_chart_update(&self) -> Option<&CurveChartUpdate> {
        match self {
            CurveEventType::CurveChartUpdate(update) => Some(update),
            _ => None,
        }
    }
}

// 하위 호환성을 위한 trait 유지 (기존 코드가 사용 중일 수 있음)
pub trait CurveEvent: Any + Send + Sync + Debug {
    fn clone_event(&self) -> Box<dyn CurveEvent>;
    fn as_any(&self) -> &dyn Any;

    // 타입을 확인하는 메서드 추가
    fn is_create_curve(&self) -> Option<&CreateCurve> {
        None
    }
    fn is_buy(&self) -> Option<&Buy> {
        None
    }
    fn is_sell(&self) -> Option<&Sell> {
        None
    }
    fn is_curve_sync(&self) -> Option<&CurveSync> {
        None
    }
    fn is_graduated(&self) -> Option<&Graduate> {
        None
    }
    fn is_chart_update(&self) -> Option<&CurveChartUpdate> {
        None
    }
    // 정렬에 사용될 블록 번호를 반환합니다.
    fn get_block_number(&self) -> u64 {
        0
    }

    // 정렬에 사용될 로그 인덱스를 반환합니다.
    fn get_log_index(&self) -> u64 {
        0
    }
    fn get_transaction_index(&self) -> u64 {
        0
    }
    fn get_token(&self) -> String {
        "".to_string()
    }
}

impl Clone for Box<dyn CurveEvent> {
    fn clone(&self) -> Box<dyn CurveEvent> {
        self.clone_event()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TokenMetadata {
    pub image_uri: String,
    pub description: Option<String>,
    pub website: Option<String>,
    pub twitter: Option<String>,
    pub telegram: Option<String>,
    pub is_nsfw: bool,
}

#[derive(Debug, Clone)]
pub struct CreateCurve {
    pub creator: String,
    pub token: String,
    pub virtual_token: BigDecimal,
    pub virtual_native: BigDecimal,
    pub token_metadata: TokenMetadata,
    pub name: String,
    pub symbol: String,
    pub transaction_hash: String,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
    /// quote token 주소 (V1: WMON, V2: quoteToken)
    pub quote_token: String,
    /// pair 주소 (V1: None, V2: Create 시 pair 생성)
    pub pair: Option<String>,
    /// 토큰 버전 — V1/V2 분기에 사용
    /// (이전엔 pair.is_some()으로 V1/V2 구분했으나 implicit해서 명시 필드로 전환)
    pub version: crate::types::TokenVersion,
}

#[derive(Debug, Clone, Serialize)]
pub struct Buy {
    pub account_id: String,
    pub to: Option<String>,
    pub amount_in: BigDecimal,
    pub amount_out: BigDecimal,
    pub token: String,
    pub market: String,
    #[serde(skip_serializing)]
    pub market_type: MarketType,
    pub transaction_hash: String,
    #[serde(skip_serializing)]
    pub block_number: u64,
    pub block_timestamp: u64,
    #[serde(skip_serializing)]
    pub log_index: u64,

    #[serde(skip_serializing)]
    pub transaction_index: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sell {
    pub account_id: String,
    pub to: Option<String>,
    pub amount_in: BigDecimal,
    pub amount_out: BigDecimal,
    pub token: String,
    pub market: String,
    #[serde(skip_serializing)]
    pub market_type: MarketType,
    pub transaction_hash: String,
    #[serde(skip_serializing)]
    pub block_number: u64,
    pub block_timestamp: u64,

    #[serde(skip_serializing)]
    pub log_index: u64,
    #[serde(skip_serializing)]
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct CurveSync {
    pub token: String,
    pub reserve_native_amount: BigDecimal,
    pub reserve_token_amount: BigDecimal,
    pub virtual_native_amount: BigDecimal,
    pub virtual_token_amount: BigDecimal,
    pub price: BigDecimal,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: String,
    pub log_index: u64,
    pub transaction_index: u64,
}

#[derive(Debug, Clone)]
pub struct Graduate {
    pub token: String,
    pub pool: String,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_hash: String,
    pub log_index: u64,
    pub transaction_index: u64,
}

// CreateCurve 이벤트에 대한 구현
impl CurveEvent for CreateCurve {
    fn clone_event(&self) -> Box<dyn CurveEvent> {
        Box::new(self.clone())
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_create_curve(&self) -> Option<&CreateCurve> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
}

// Buy 이벤트에 대한 구현
impl CurveEvent for Buy {
    fn clone_event(&self) -> Box<dyn CurveEvent> {
        Box::new(self.clone())
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_buy(&self) -> Option<&Buy> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
}

// Sell 이벤트에 대한 구현
impl CurveEvent for Sell {
    fn clone_event(&self) -> Box<dyn CurveEvent> {
        Box::new(self.clone())
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_sell(&self) -> Option<&Sell> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
}

// CurveSync에 대한 구현
impl CurveEvent for CurveSync {
    fn clone_event(&self) -> Box<dyn CurveEvent> {
        Box::new(self.clone())
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_curve_sync(&self) -> Option<&CurveSync> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
}

// Graduate 대한 구현
impl CurveEvent for Graduate {
    fn clone_event(&self) -> Box<dyn CurveEvent> {
        Box::new(self.clone())
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_graduated(&self) -> Option<&Graduate> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
}

//======================== Dex ========================
/// Dex 이벤트를 나타내는 Enum (성능 최적화: Box<dyn Trait> 대체)
#[derive(Debug, Clone)]
pub enum DexEventType {
    Buy(Buy),
    Sell(Sell),
    DexSync(DexSync),
    DexMint(DexMint),
    DexBurn(DexBurn),
    DexChartUpdate(DexChartUpdate),
}

impl DexEventType {
    /// 토큰 주소 반환
    pub fn get_token(&self) -> &str {
        match self {
            DexEventType::Buy(e) => &e.token,
            DexEventType::Sell(e) => &e.token,
            DexEventType::DexSync(e) => &e.token,
            DexEventType::DexMint(e) => &e.token,
            DexEventType::DexBurn(e) => &e.token,
            DexEventType::DexChartUpdate(e) => &e.sync.token,
        }
    }

    /// 블록 번호 반환
    pub fn get_block_number(&self) -> u64 {
        match self {
            DexEventType::Buy(e) => e.block_number,
            DexEventType::Sell(e) => e.block_number,
            DexEventType::DexSync(e) => e.block_number,
            DexEventType::DexMint(e) => e.block_number,
            DexEventType::DexBurn(e) => e.block_number,
            DexEventType::DexChartUpdate(e) => e.block_number,
        }
    }

    /// 로그 인덱스 반환
    pub fn get_log_index(&self) -> u64 {
        match self {
            DexEventType::Buy(e) => e.log_index,
            DexEventType::Sell(e) => e.log_index,
            DexEventType::DexSync(e) => e.log_index,
            DexEventType::DexMint(e) => e.log_index,
            DexEventType::DexBurn(e) => e.log_index,
            DexEventType::DexChartUpdate(e) => e.sync.log_index,
        }
    }

    /// 트랜잭션 인덱스 반환
    pub fn get_transaction_index(&self) -> u64 {
        match self {
            DexEventType::Buy(e) => e.transaction_index,
            DexEventType::Sell(e) => e.transaction_index,
            DexEventType::DexSync(e) => e.transaction_index,
            DexEventType::DexMint(e) => e.transaction_index,
            DexEventType::DexBurn(e) => e.transaction_index,
            DexEventType::DexChartUpdate(e) => e.sync.transaction_index,
        }
    }

    /// Buy 또는 Sell 이벤트인지 확인 (trade 이벤트)
    pub fn is_trade(&self) -> bool {
        matches!(self, DexEventType::Buy(_) | DexEventType::Sell(_))
    }

    /// Buy 이벤트 확인 및 반환
    pub fn is_buy(&self) -> Option<&Buy> {
        match self {
            DexEventType::Buy(buy) => Some(buy),
            _ => None,
        }
    }

    /// Sell 이벤트 확인 및 반환
    pub fn is_sell(&self) -> Option<&Sell> {
        match self {
            DexEventType::Sell(sell) => Some(sell),
            _ => None,
        }
    }

    /// DexSync 이벤트 확인 및 반환
    pub fn is_dex_sync(&self) -> Option<&DexSync> {
        match self {
            DexEventType::DexSync(sync) => Some(sync),
            _ => None,
        }
    }

    /// DexChartUpdate 이벤트 확인 및 반환
    pub fn is_dex_chart_update(&self) -> Option<&DexChartUpdate> {
        match self {
            DexEventType::DexChartUpdate(update) => Some(update),
            _ => None,
        }
    }
}

// 하위 호환성을 위한 trait 유지 (기존 코드가 사용 중일 수 있음)
pub trait DexEvent: Any + Send + Sync + Debug {
    fn clone_event(&self) -> Box<dyn DexEvent>;
    fn as_any(&self) -> &dyn Any;

    // 타입을 확인하는 메서드 추가
    fn is_buy(&self) -> Option<&Buy> {
        None
    }
    fn is_sell(&self) -> Option<&Sell> {
        None
    }
    fn is_dex_sync(&self) -> Option<&DexSync> {
        None
    }
    fn is_dex_mint(&self) -> Option<&DexMint> {
        None
    }
    fn is_dex_burn(&self) -> Option<&DexBurn> {
        None
    }
    fn is_dex_chart_update(&self) -> Option<&DexChartUpdate> {
        None
    }
    // 정렬에 사용될 블록 번호를 반환합니다.
    fn get_block_number(&self) -> u64 {
        0
    }

    // 정렬에 사용될 로그 인덱스를 반환합니다.
    fn get_log_index(&self) -> u64 {
        0
    }
    fn get_transaction_index(&self) -> u64 {
        0
    }
    fn get_token(&self) -> String {
        "".to_string()
    }
}

impl Clone for Box<dyn DexEvent> {
    fn clone(&self) -> Box<dyn DexEvent> {
        self.clone_event()
    }
}

#[derive(Debug, Clone)]
pub struct DexSync {
    pub token: String,
    pub pool: String,
    pub price: BigDecimal,
    pub reserve_native: BigDecimal,
    pub reserve_token: BigDecimal,
    pub transaction_hash: String,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}
// Buy와 Sell에 대한 DexEvent 구현 (CurveEvent와 동일한 로직)
impl DexEvent for Buy {
    fn clone_event(&self) -> Box<dyn DexEvent> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_buy(&self) -> Option<&Buy> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
}

impl DexEvent for Sell {
    fn clone_event(&self) -> Box<dyn DexEvent> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_sell(&self) -> Option<&Sell> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
}

// DexSync에 대한 구현
impl DexEvent for DexSync {
    fn clone_event(&self) -> Box<dyn DexEvent> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_dex_sync(&self) -> Option<&DexSync> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
}
#[derive(Debug, Clone)]
pub struct DexMint {
    pub token: String,
    pub pool: String,
    pub amount: BigDecimal,
    pub transaction_hash: String,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}
impl DexEvent for DexMint {
    fn clone_event(&self) -> Box<dyn DexEvent> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_dex_mint(&self) -> Option<&DexMint> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
}

#[derive(Debug, Clone)]
pub struct DexBurn {
    pub token: String,
    pub pool: String,
    pub amount: BigDecimal,
    pub transaction_hash: String,
    pub block_timestamp: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub transaction_index: u64,
}

impl DexEvent for DexBurn {
    fn clone_event(&self) -> Box<dyn DexEvent> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_dex_burn(&self) -> Option<&DexBurn> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.transaction_index
    }
    fn get_token(&self) -> String {
        self.token.clone()
    }
}

//======================== Chart Update ========================
/// 차트 업데이트를 위한 이벤트 그룹
/// Sync 이벤트와 함께 Buy/Sell 이벤트를 묶어서 순서를 보장합니다.
///
/// 처리 순서:
/// 1. Sync 먼저 처리 (가격 업데이트 및 캔들 생성)
/// 2. Trade (Buy/Sell) 처리 (거래량 업데이트)
#[derive(Debug, Clone)]
pub struct CurveChartUpdate {
    /// 거래 이벤트 (Buy 또는 Sell) - Box로 감싸서 재귀 타입 해결
    pub trade: Option<Box<CurveEventType>>,
    /// 가격 동기화 이벤트 (항상 포함)
    pub sync: CurveSync,
    /// 블록 번호
    pub block_number: u64,
    /// 트랜잭션 해시
    pub transaction_hash: String,
}

#[derive(Debug, Clone)]
pub struct DexChartUpdate {
    /// 거래 이벤트 (Buy 또는 Sell) - Box로 감싸서 재귀 타입 해결
    pub trade: Option<Box<DexEventType>>,
    /// 가격 동기화 이벤트 (항상 포함)
    pub sync: DexSync,
    /// 블록 번호
    pub block_number: u64,
    /// 트랜잭션 해시
    pub transaction_hash: String,
}

// CurveChartUpdate에 CurveEvent trait 구현
impl CurveEvent for CurveChartUpdate {
    fn clone_event(&self) -> Box<dyn CurveEvent> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_chart_update(&self) -> Option<&CurveChartUpdate> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.sync.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.sync.transaction_index
    }
    fn get_token(&self) -> String {
        self.sync.token.clone()
    }
}

// DexChartUpdate에 DexEvent trait 구현
impl DexEvent for DexChartUpdate {
    fn clone_event(&self) -> Box<dyn DexEvent> {
        Box::new(self.clone())
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn is_dex_chart_update(&self) -> Option<&DexChartUpdate> {
        Some(self)
    }
    fn get_block_number(&self) -> u64 {
        self.block_number
    }
    fn get_log_index(&self) -> u64 {
        self.sync.log_index
    }
    fn get_transaction_index(&self) -> u64 {
        self.sync.transaction_index
    }
    fn get_token(&self) -> String {
        self.sync.token.clone()
    }
}
