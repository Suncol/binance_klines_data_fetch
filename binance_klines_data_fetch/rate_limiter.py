"""Thread-safe weighted request limiter for Binance REST usage."""

from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Mapping, Optional, Tuple

from .errors import BinanceResponseError
from .models import DEFAULT_REQUEST_WEIGHT_LIMIT_PER_MINUTE, RateLimiterStatus


class RateLimitError(Exception):
    """Raised when a limiter request cannot be satisfied."""


def kline_request_weight(limit: int) -> int:
    """Return Binance USD-M `/fapi/v1/klines` request weight for a limit."""

    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise ValueError("limit must be a positive integer")
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


class WeightedRateLimiter:
    """A process-local rolling-window limiter using Binance request weights."""

    def __init__(
        self,
        *,
        limit: int = DEFAULT_REQUEST_WEIGHT_LIMIT_PER_MINUTE,
        window_seconds: float = 60.0,
        safety_margin: int = 0,
        cooldown_seconds: float = 60.0,
        ban_cooldown_seconds: float = 120.0,
        min_sleep_seconds: float = 0.001,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if isinstance(limit, bool) or int(limit) <= 0:
            raise ValueError("limit must be a positive integer")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if safety_margin < 0:
            raise ValueError("safety_margin must be >= 0")

        self.limit = int(limit)
        self.window_seconds = float(window_seconds)
        self.safety_margin = int(safety_margin)
        self.cooldown_seconds = max(0.0, float(cooldown_seconds))
        self.ban_cooldown_seconds = max(self.cooldown_seconds, float(ban_cooldown_seconds))
        self.min_sleep_seconds = max(0.0, float(min_sleep_seconds))
        self._clock = clock
        self._sleeper = sleeper

        self._lock = threading.RLock()
        self._events: Deque[Tuple[float, int]] = deque()
        self._used_weight = 0
        self._cooldown_until = 0.0
        self._observed_used_weight_1m: Optional[int] = None
        self._last_rate_limited_at: Optional[datetime] = None
        self._last_banned_at: Optional[datetime] = None

    @property
    def effective_limit(self) -> int:
        return max(1, self.limit - self.safety_margin)

    def set_limit(self, limit: int) -> None:
        if isinstance(limit, bool) or int(limit) <= 0:
            raise ValueError("limit must be a positive integer")
        with self._lock:
            self.limit = int(limit)
            self._prune(self._clock())

    def acquire(self, weight: int, *, timeout: Optional[float] = None) -> None:
        if isinstance(weight, bool) or int(weight) <= 0:
            raise ValueError("weight must be a positive integer")
        weight = int(weight)
        if weight > self.effective_limit:
            raise RateLimitError(f"weight {weight} exceeds effective limit {self.effective_limit}")

        deadline = None if timeout is None else self._clock() + max(0.0, float(timeout))
        while True:
            wait_seconds = 0.0
            with self._lock:
                now = self._clock()
                self._prune(now)
                cooldown_wait = max(0.0, self._cooldown_until - now)
                if cooldown_wait > 0:
                    wait_seconds = cooldown_wait
                elif self._used_weight + weight <= self.effective_limit:
                    self._events.append((now, weight))
                    self._used_weight += weight
                    return
                elif self._events:
                    oldest_ts = self._events[0][0]
                    wait_seconds = max(0.0, oldest_ts + self.window_seconds - now)
                else:
                    wait_seconds = self.window_seconds

                if deadline is not None and now + wait_seconds > deadline:
                    raise RateLimitError("timed out waiting for request weight capacity")

            self._sleeper(max(wait_seconds, self.min_sleep_seconds))

    def update_from_headers(self, headers: Mapping[str, Any]) -> None:
        used = _header_int(headers, "X-MBX-USED-WEIGHT-1M")
        if used is None:
            return
        with self._lock:
            self._observed_used_weight_1m = used

    def on_rate_limited(self, *, retry_after_seconds: Optional[float] = None) -> None:
        wait = self.cooldown_seconds if retry_after_seconds is None else max(0.0, float(retry_after_seconds))
        with self._lock:
            now = self._clock()
            self._cooldown_until = max(self._cooldown_until, now + wait)
            self._last_rate_limited_at = datetime.now(timezone.utc)

    def on_banned(self, *, retry_after_seconds: Optional[float] = None) -> None:
        wait = self.ban_cooldown_seconds if retry_after_seconds is None else max(0.0, float(retry_after_seconds))
        with self._lock:
            now = self._clock()
            self._cooldown_until = max(self._cooldown_until, now + wait)
            self._last_banned_at = datetime.now(timezone.utc)

    def status(self) -> RateLimiterStatus:
        with self._lock:
            now = self._clock()
            self._prune(now)
            cooldown_seconds = max(0.0, self._cooldown_until - now)
            available_weight = max(0, self.effective_limit - self._used_weight)
            return RateLimiterStatus(
                limit=self.limit,
                effective_limit=self.effective_limit,
                window_seconds=self.window_seconds,
                used_weight=self._used_weight,
                available_weight=available_weight,
                cooldown_seconds=cooldown_seconds,
                observed_used_weight_1m=self._observed_used_weight_1m,
                last_rate_limited_at=self._last_rate_limited_at,
                last_banned_at=self._last_banned_at,
            )

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0][0] <= cutoff:
            _, weight = self._events.popleft()
            self._used_weight -= weight
        if self._used_weight < 0:
            self._used_weight = 0


def request_weight_limit_from_exchange_info(data: Any) -> int:
    """Extract the 1-minute REQUEST_WEIGHT limit from `/fapi/v1/exchangeInfo`."""

    if not isinstance(data, dict):
        raise BinanceResponseError("exchangeInfo response must be a JSON object")
    rate_limits = data.get("rateLimits")
    if not isinstance(rate_limits, list):
        raise BinanceResponseError("exchangeInfo response missing rateLimits")

    for item in rate_limits:
        if not isinstance(item, dict):
            continue
        if item.get("rateLimitType") != "REQUEST_WEIGHT":
            continue
        if item.get("interval") != "MINUTE":
            continue
        try:
            interval_num = int(item.get("intervalNum", 1))
            limit = int(item["limit"])
        except Exception as exc:
            raise BinanceResponseError("invalid REQUEST_WEIGHT rate limit entry") from exc
        if interval_num == 1 and limit > 0:
            return limit

    raise BinanceResponseError("exchangeInfo response missing 1-minute REQUEST_WEIGHT limit")


def retry_after_seconds_from_headers(headers: Mapping[str, Any]) -> Optional[float]:
    raw = _header_value(headers, "Retry-After")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except Exception:
        return None


def _header_int(headers: Mapping[str, Any], name: str) -> Optional[int]:
    raw = _header_value(headers, name)
    if raw is None:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _header_value(headers: Mapping[str, Any], name: str) -> Optional[Any]:
    if name in headers:
        return headers[name]
    lower_name = name.lower()
    for key, value in headers.items():
        if str(key).lower() == lower_name:
            return value
    return None
