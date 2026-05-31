"""Shared constants and small data models."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Literal, Optional, cast

DEFAULT_BASE_URL = "https://fapi.binance.com"
KLINE_PATH = "/fapi/v1/klines"
SERVER_TIME_PATH = "/fapi/v1/time"
EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"

INTERVAL_1M = "1m"
ONE_MINUTE_MS = 60_000
MAX_KLINE_LIMIT = 1500
DEFAULT_REQUEST_WEIGHT_LIMIT_PER_MINUTE = 2400

RAW_KLINE_COLUMNS = [
    "Open_Time",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Close_Time",
    "Quote_Asset_Volume",
    "Number_of_Trades",
    "Taker_Buy_Base_Asset_Volume",
    "Taker_Buy_Quote_Asset_Volume",
    "Ignore",
]

OUTPUT_KLINE_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Close_Time",
    "Quote_Asset_Volume",
    "Number_of_Trades",
    "Taker_Buy_Base_Asset_Volume",
    "Taker_Buy_Quote_Asset_Volume",
]

NUMERIC_KLINE_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "Quote_Asset_Volume",
    "Number_of_Trades",
    "Taker_Buy_Base_Asset_Volume",
    "Taker_Buy_Quote_Asset_Volume",
]

DepthSide = Literal["bid", "ask"]
DepthValueField = Literal["price", "qty"]


@dataclass(frozen=True)
class DepthLevelQuote:
    symbol: str
    level: int
    bid_price: Optional[Decimal]
    bid_qty: Optional[Decimal]
    ask_price: Optional[Decimal]
    ask_qty: Optional[Decimal]
    event_time_ms: int
    transaction_time_ms: int
    local_recv_time_ms: int
    receive_latency_ms: int
    final_update_id: int
    is_stale: bool
    sequence_gap: bool
    depth_incomplete: Optional[bool] = None

    def value(self, side: DepthSide, field: DepthValueField) -> Optional[Decimal]:
        normalized_side = normalize_depth_side(side)
        normalized_field = normalize_depth_value_field(field)
        if normalized_side == "bid":
            return self.bid_price if normalized_field == "price" else self.bid_qty
        return self.ask_price if normalized_field == "price" else self.ask_qty


def normalize_depth_side(side: str) -> DepthSide:
    if not isinstance(side, str):
        raise ValueError("side must be 'bid' or 'ask'")
    normalized = side.strip().lower()
    if normalized not in {"bid", "ask"}:
        raise ValueError("side must be 'bid' or 'ask'")
    return cast(DepthSide, normalized)


def normalize_depth_value_field(field: str) -> DepthValueField:
    if not isinstance(field, str):
        raise ValueError("field must be 'price' or 'qty'")
    normalized = field.strip().lower()
    if normalized not in {"price", "qty"}:
        raise ValueError("field must be 'price' or 'qty'")
    return cast(DepthValueField, normalized)


def validate_depth_level(level: int, *, max_level: Optional[int] = None) -> int:
    if isinstance(level, bool) or not isinstance(level, int) or level < 1:
        raise ValueError("level must be a positive integer")
    if max_level is not None and level > max_level:
        raise ValueError(f"level must be <= {max_level}")
    return level


def validate_max_age_ms(max_age_ms: Optional[int]) -> Optional[int]:
    if max_age_ms is None:
        return None
    if isinstance(max_age_ms, bool) or not isinstance(max_age_ms, int) or max_age_ms < 0:
        raise ValueError("max_age_ms must be a non-negative integer or None")
    return max_age_ms


@dataclass(frozen=True)
class KlineServiceStatus:
    symbol: str
    window_size: int
    row_count: int
    running: bool
    ready: bool
    last_success_at: Optional[datetime]
    last_open_time: Optional[datetime]
    last_error: Optional[str]


@dataclass(frozen=True)
class RateLimiterStatus:
    limit: int
    effective_limit: int
    window_seconds: float
    used_weight: int
    available_weight: int
    cooldown_seconds: float
    observed_used_weight_1m: Optional[int]
    last_rate_limited_at: Optional[datetime]
    last_banned_at: Optional[datetime]


@dataclass(frozen=True)
class SymbolKlineStatus:
    symbol: str
    row_count: int
    ready: bool
    in_flight: bool
    last_success_at: Optional[datetime]
    last_open_time: Optional[datetime]
    last_error: Optional[str]


@dataclass(frozen=True)
class MultiKlineServiceStatus:
    symbols: Dict[str, SymbolKlineStatus]
    window_size: int
    running: bool
    ready: bool
    max_workers: int
    last_refresh_at: Optional[datetime]
    last_error: Optional[str]
    rate_limiter: Optional[RateLimiterStatus]


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def floor_to_minute_open_ms(timestamp_ms: int) -> int:
    return int(timestamp_ms // ONE_MINUTE_MS * ONE_MINUTE_MS)


def latest_closed_1m_open_time_ms(server_time_ms: int) -> int:
    return floor_to_minute_open_ms(server_time_ms) - ONE_MINUTE_MS
