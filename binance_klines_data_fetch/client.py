"""REST client for Binance USD-M Futures 1m klines."""

from __future__ import annotations

import time
from typing import Any, Iterable, List, Optional

import pandas as pd
import requests

from .errors import BinanceAPIError, BinanceRequestError, BinanceResponseError
from .models import (
    DEFAULT_BASE_URL,
    EXCHANGE_INFO_PATH,
    INTERVAL_1M,
    KLINE_PATH,
    MAX_KLINE_LIMIT,
    NUMERIC_KLINE_COLUMNS,
    ONE_MINUTE_MS,
    OUTPUT_KLINE_COLUMNS,
    RAW_KLINE_COLUMNS,
    SERVER_TIME_PATH,
    latest_closed_1m_open_time_ms as calc_latest_closed_1m_open_time_ms,
    utc_now_ms,
)
from .rate_limiter import (
    WeightedRateLimiter,
    kline_request_weight,
    request_weight_limit_from_exchange_info,
    retry_after_seconds_from_headers,
)


def _normalize_symbol(symbol: str) -> str:
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string")
    return symbol.strip().upper()


def _validate_positive_int(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def klines_to_dataframe(rows: Iterable[Any]) -> pd.DataFrame:
    """Convert raw Binance kline rows into the package DataFrame schema."""

    rows_list = list(rows)
    if not rows_list:
        return pd.DataFrame(columns=OUTPUT_KLINE_COLUMNS).rename_axis("Open_Time")

    for idx, row in enumerate(rows_list):
        if not isinstance(row, (list, tuple)) or len(row) < len(RAW_KLINE_COLUMNS):
            raise BinanceResponseError(f"malformed kline row at index {idx}")

    try:
        normalized_rows = [list(row[: len(RAW_KLINE_COLUMNS)]) for row in rows_list]
        df = pd.DataFrame(normalized_rows, columns=RAW_KLINE_COLUMNS)
        df["Open_Time"] = pd.to_datetime(df["Open_Time"], unit="ms", utc=True)
        df["Close_Time"] = pd.to_datetime(df["Close_Time"], unit="ms", utc=True)
        for col in NUMERIC_KLINE_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="raise")
        df = df.drop(columns=["Ignore"])
        df = df.set_index("Open_Time")
        df.index.name = "Open_Time"
        df = df[OUTPUT_KLINE_COLUMNS].sort_index()
        df = df[~df.index.duplicated(keep="last")]
        return df
    except BinanceResponseError:
        raise
    except Exception as exc:  # pragma: no cover - pandas error details vary
        raise BinanceResponseError(f"failed to parse kline data: {exc}") from exc


class BinanceKlineClient:
    """Small Binance USD-M Futures REST client focused on closed 1m klines."""

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 10.0,
        session: Optional[requests.Session] = None,
        max_limit: int = MAX_KLINE_LIMIT,
        retries: int = 2,
        retry_backoff_seconds: float = 0.25,
        time_sync_interval_seconds: float = 300.0,
        proxies: Optional[dict[str, str]] = None,
        rate_limiter: Optional[WeightedRateLimiter] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self.session = session or requests.Session()
        self.max_limit = min(_validate_positive_int("max_limit", int(max_limit)), MAX_KLINE_LIMIT)
        self.retries = max(0, int(retries))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.time_sync_interval_seconds = max(1.0, float(time_sync_interval_seconds))
        self.proxies = proxies
        self.rate_limiter = rate_limiter
        self._server_time_offset_ms: Optional[int] = None
        self._last_time_sync_monotonic: Optional[float] = None

    def sync_time(self) -> int:
        """Fetch Binance server time and update the local clock offset estimate."""

        before = utc_now_ms()
        data = self._request_json(SERVER_TIME_PATH, params={}, request_weight=1)
        after = utc_now_ms()
        if not isinstance(data, dict) or "serverTime" not in data:
            raise BinanceResponseError("server time response missing serverTime")
        try:
            server_time_ms = int(data["serverTime"])
        except Exception as exc:
            raise BinanceResponseError("serverTime is not an integer") from exc

        local_midpoint_ms = (before + after) // 2
        self._server_time_offset_ms = server_time_ms - local_midpoint_ms
        self._last_time_sync_monotonic = time.monotonic()
        return server_time_ms

    def fetch_exchange_info(self) -> dict[str, Any]:
        data = self._request_json(EXCHANGE_INFO_PATH, params={}, request_weight=1)
        if not isinstance(data, dict):
            raise BinanceResponseError("exchangeInfo response must be a JSON object")
        return data

    def configure_rate_limiter_from_exchange_info(self) -> int:
        if self.rate_limiter is None:
            raise ValueError("rate_limiter is not configured")
        limit = request_weight_limit_from_exchange_info(self.fetch_exchange_info())
        self.rate_limiter.set_limit(limit)
        return limit

    def estimated_server_time_ms(self) -> int:
        if self._server_time_offset_ms is None or self._time_sync_expired():
            return self.sync_time()
        return utc_now_ms() + self._server_time_offset_ms

    def latest_closed_1m_open_time_ms(self) -> int:
        return calc_latest_closed_1m_open_time_ms(self.estimated_server_time_ms())

    def fetch_klines(
        self,
        symbol: str,
        *,
        interval: str = INTERVAL_1M,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 500,
    ) -> list[Any]:
        limit = _validate_positive_int("limit", int(limit))
        if limit > self.max_limit:
            raise ValueError(f"limit must be <= {self.max_limit}")

        params: dict[str, Any] = {
            "symbol": _normalize_symbol(symbol),
            "interval": interval,
            "limit": limit,
        }
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)

        data = self._request_json(KLINE_PATH, params=params, request_weight=kline_request_weight(limit))
        if not isinstance(data, list):
            raise BinanceResponseError("kline response must be a JSON array")
        return data

    def fetch_recent_closed_1m_klines(
        self,
        symbol: str,
        n: int,
        *,
        end_open_time_ms: Optional[int] = None,
        chunk_limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetch up to n most recent closed 1m klines for one futures symbol."""

        symbol = _normalize_symbol(symbol)
        n = _validate_positive_int("n", int(n))
        page_limit = self.max_limit
        if chunk_limit is not None:
            page_limit = min(_validate_positive_int("chunk_limit", int(chunk_limit)), self.max_limit)
        latest_open_ms = self.latest_closed_1m_open_time_ms() if end_open_time_ms is None else int(end_open_time_ms)
        start_open_ms = latest_open_ms - (n - 1) * ONE_MINUTE_MS
        final_end_ms = latest_open_ms + ONE_MINUTE_MS - 1

        rows: List[Any] = []
        cursor_ms = start_open_ms
        while cursor_ms <= latest_open_ms:
            remaining = ((latest_open_ms - cursor_ms) // ONE_MINUTE_MS) + 1
            limit = min(int(remaining), page_limit)
            batch = self.fetch_klines(
                symbol,
                interval=INTERVAL_1M,
                start_time_ms=cursor_ms,
                end_time_ms=final_end_ms,
                limit=limit,
            )
            if not batch:
                break

            rows.extend(batch)
            try:
                last_open = int(batch[-1][0])
            except Exception as exc:
                raise BinanceResponseError("kline row missing open time") from exc
            if last_open < cursor_ms:
                raise BinanceResponseError("kline response did not advance open time")
            cursor_ms = last_open + ONE_MINUTE_MS

        df = klines_to_dataframe(rows)
        if df.empty:
            return df

        start_ts = pd.to_datetime(start_open_ms, unit="ms", utc=True)
        end_ts = pd.to_datetime(latest_open_ms, unit="ms", utc=True)
        df = df[(df.index >= start_ts) & (df.index <= end_ts)]
        return df.tail(n).copy(deep=True)

    def _time_sync_expired(self) -> bool:
        if self._last_time_sync_monotonic is None:
            return True
        return (time.monotonic() - self._last_time_sync_monotonic) >= self.time_sync_interval_seconds

    def _request_json(self, path: str, *, params: dict[str, Any], request_weight: int = 1) -> Any:
        url = f"{self.base_url}{path}"
        last_error: Optional[BaseException] = None

        for attempt in range(self.retries + 1):
            try:
                if self.rate_limiter is not None:
                    self.rate_limiter.acquire(request_weight)
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout,
                    proxies=self.proxies,
                )
                status_code = int(response.status_code)
                if self.rate_limiter is not None:
                    self.rate_limiter.update_from_headers(response.headers)
                try:
                    data = response.json()
                except ValueError as exc:
                    response_error = BinanceResponseError(f"invalid JSON from Binance: {exc}")
                    self._notify_rate_limiter_of_status(status_code, response.headers)
                    if self._should_retry_status(status_code, attempt):
                        last_error = response_error
                        self._sleep_before_retry(attempt)
                        continue
                    raise response_error from exc

                if status_code >= 400:
                    api_error = self._api_error_from_response(status_code, data)
                    self._notify_rate_limiter_of_status(status_code, response.headers)
                    if self._should_retry_status(status_code, attempt):
                        last_error = api_error
                        self._sleep_before_retry(attempt)
                        continue
                    raise api_error

                if isinstance(data, dict) and "code" in data and "msg" in data:
                    raise BinanceAPIError(
                        f"Binance API error: {data.get('msg')} (code: {data.get('code')})",
                        code=self._safe_int(data.get("code")),
                        binance_message=str(data.get("msg")),
                    )

                return data
            except (BinanceAPIError, BinanceResponseError):
                raise
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = BinanceRequestError(f"request failed for {path}: {exc}")
                if attempt >= self.retries:
                    raise last_error from exc
                self._sleep_before_retry(attempt)
            except requests.RequestException as exc:
                raise BinanceRequestError(f"request failed for {path}: {exc}") from exc

        if last_error is not None:
            raise last_error
        raise BinanceRequestError(f"request failed for {path}")

    def _api_error_from_response(self, status_code: int, data: Any) -> BinanceAPIError:
        code = None
        msg = None
        if isinstance(data, dict):
            code = self._safe_int(data.get("code"))
            raw_msg = data.get("msg")
            msg = str(raw_msg) if raw_msg is not None else None
        detail = f": {msg}" if msg else ""
        return BinanceAPIError(
            f"Binance HTTP {status_code}{detail}",
            status_code=status_code,
            code=code,
            binance_message=msg,
        )

    def _should_retry_status(self, status_code: int, attempt: int) -> bool:
        if attempt >= self.retries:
            return False
        if status_code == 418:
            return False
        return status_code in {408, 429} or status_code >= 500

    def _sleep_before_retry(self, attempt: int) -> None:
        if self.retry_backoff_seconds <= 0:
            return
        time.sleep(self.retry_backoff_seconds * (2**attempt))

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None

    def _notify_rate_limiter_of_status(self, status_code: int, headers: Any) -> None:
        if self.rate_limiter is None:
            return
        retry_after = retry_after_seconds_from_headers(headers)
        if status_code == 429:
            self.rate_limiter.on_rate_limited(retry_after_seconds=retry_after)
        elif status_code == 418:
            self.rate_limiter.on_banned(retry_after_seconds=retry_after)


def fetch_recent_closed_1m_klines(
    symbol: str,
    n: int,
    *,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = 10.0,
    session: Optional[requests.Session] = None,
    retries: int = 2,
    retry_backoff_seconds: float = 0.25,
    proxies: Optional[dict[str, str]] = None,
    rate_limiter: Optional[WeightedRateLimiter] = None,
    chunk_limit: Optional[int] = None,
) -> pd.DataFrame:
    client = BinanceKlineClient(
        base_url=base_url,
        timeout=timeout,
        session=session,
        retries=retries,
        retry_backoff_seconds=retry_backoff_seconds,
        proxies=proxies,
        rate_limiter=rate_limiter,
    )
    return client.fetch_recent_closed_1m_klines(symbol, n, chunk_limit=chunk_limit)
