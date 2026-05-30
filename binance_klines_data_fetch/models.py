"""Shared constants and small data models."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

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
