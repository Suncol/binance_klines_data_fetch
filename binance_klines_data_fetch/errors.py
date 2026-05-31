"""Exception types for Binance kline fetching."""

from __future__ import annotations

from typing import Optional


class BinanceKlineError(Exception):
    """Base exception for this package."""


class BinanceRequestError(BinanceKlineError):
    """Raised when an HTTP request cannot be completed."""


class BinanceResponseError(BinanceKlineError):
    """Raised when Binance returns malformed or unexpected data."""


class BinanceAPIError(BinanceKlineError):
    """Raised for Binance HTTP or JSON error payloads."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        code: Optional[int] = None,
        binance_message: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.binance_message = binance_message


class KlineServiceError(BinanceKlineError):
    """Raised for background service lifecycle and readiness errors."""


class BinanceDepthError(BinanceKlineError):
    """Base exception for futures depth stream handling."""


class BinanceDepthResponseError(BinanceDepthError):
    """Raised when a futures depth stream message is malformed."""


class DepthServiceError(BinanceDepthError):
    """Raised for background futures depth service lifecycle errors."""
