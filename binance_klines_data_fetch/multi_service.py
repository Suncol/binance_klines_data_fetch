"""Multi-symbol background-updating kline cache service."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional

import pandas as pd

from .client import BinanceKlineClient, _normalize_symbol, _validate_positive_int
from .errors import KlineServiceError
from .models import (
    DEFAULT_REQUEST_WEIGHT_LIMIT_PER_MINUTE,
    MultiKlineServiceStatus,
    ONE_MINUTE_MS,
    OUTPUT_KLINE_COLUMNS,
    SymbolKlineStatus,
)
from .rate_limiter import WeightedRateLimiter


class MultiSymbolKlineService:
    """Maintain rolling closed-1m kline caches for multiple symbols."""

    def __init__(
        self,
        *,
        symbols: Iterable[str],
        window_size: int,
        client: Optional[BinanceKlineClient] = None,
        rate_limiter: Optional[WeightedRateLimiter] = None,
        request_weight_limit_per_minute: Optional[int] = None,
        auto_configure_rate_limit: bool = True,
        max_workers: Optional[int] = None,
        refresh_interval_seconds: float = 2.0,
        max_backoff_seconds: float = 30.0,
        startup_timeout_seconds: float = 30.0,
        bootstrap_chunk_limit: int = 499,
    ) -> None:
        normalized_symbols = _normalize_symbols(symbols)
        if not normalized_symbols:
            raise ValueError("symbols must contain at least one symbol")

        self.window_size = _validate_positive_int("window_size", int(window_size))
        self.bootstrap_chunk_limit = _validate_positive_int("bootstrap_chunk_limit", int(bootstrap_chunk_limit))
        self.refresh_interval_seconds = max(0.1, float(refresh_interval_seconds))
        self.max_backoff_seconds = max(self.refresh_interval_seconds, float(max_backoff_seconds))
        self.startup_timeout_seconds = max(0.1, float(startup_timeout_seconds))
        self.max_workers = max(1, int(max_workers) if max_workers is not None else min(8, len(normalized_symbols)))
        self.auto_configure_rate_limit = bool(auto_configure_rate_limit and request_weight_limit_per_minute is None)
        self._rate_limit_configured = False

        if rate_limiter is None and client is not None:
            rate_limiter = getattr(client, "rate_limiter", None)
        if rate_limiter is None:
            rate_limiter = WeightedRateLimiter(
                limit=request_weight_limit_per_minute or DEFAULT_REQUEST_WEIGHT_LIMIT_PER_MINUTE
            )
        self.rate_limiter = rate_limiter
        self.client = client or BinanceKlineClient(rate_limiter=self.rate_limiter)
        if client is not None and hasattr(self.client, "rate_limiter"):
            self.client.rate_limiter = self.rate_limiter

        self._lock = threading.RLock()
        self._data: Dict[str, pd.DataFrame] = {
            symbol: _empty_frame() for symbol in normalized_symbols
        }
        self._last_success_at: Dict[str, Optional[datetime]] = {symbol: None for symbol in normalized_symbols}
        self._last_error: Dict[str, Optional[BaseException]] = {symbol: None for symbol in normalized_symbols}
        self._last_open_time_ms: Dict[str, Optional[int]] = {symbol: None for symbol in normalized_symbols}
        self._in_flight: set[str] = set()
        self._last_refresh_at: Optional[datetime] = None
        self._last_global_error: Optional[BaseException] = None

        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self, *, block_until_ready: bool = False, timeout: Optional[float] = None) -> None:
        if self.is_running:
            if block_until_ready:
                self._wait_until_ready(timeout)
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="binance-multi-kline-service",
            daemon=True,
        )
        self._thread.start()

        if block_until_ready:
            self._wait_until_ready(timeout)

    def stop(self, *, timeout: Optional[float] = 5.0) -> None:
        thread = self._thread
        if thread is None:
            return
        self._stop_event.set()
        if thread.is_alive():
            thread.join(timeout=timeout)
        if not thread.is_alive():
            self._thread = None

    def refresh_once(self) -> dict[str, bool]:
        self._configure_rate_limit_once()
        try:
            latest_closed_open_ms = self.client.latest_closed_1m_open_time_ms()
        except BaseException as exc:
            with self._lock:
                self._last_global_error = exc
            raise

        jobs: dict = {}
        results: dict[str, bool] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for symbol in self._symbols_snapshot():
                task = self._plan_symbol_refresh(symbol, latest_closed_open_ms)
                if task is None:
                    results[symbol] = False
                    continue
                self._mark_in_flight(symbol)
                future = executor.submit(self._fetch_symbol_frame, symbol, task, latest_closed_open_ms)
                jobs[future] = symbol

            for future in as_completed(jobs):
                symbol = jobs[future]
                try:
                    df = future.result()
                    changed = self._replace_symbol_cache(symbol, df)
                    results[symbol] = changed
                except BaseException as exc:
                    self._record_symbol_error(symbol, exc)
                    results[symbol] = False
                finally:
                    self._unmark_in_flight(symbol)

        with self._lock:
            self._last_refresh_at = datetime.now(timezone.utc)
            self._last_global_error = None
            self._update_ready_event_locked()
        return results

    def get_recent(self, symbol: str, n: Optional[int] = None) -> pd.DataFrame:
        symbol = _normalize_symbol(symbol)
        if n is None:
            n = self.window_size
        n = _validate_positive_int("n", int(n))
        if n > self.window_size:
            raise ValueError("n must be <= window_size")

        with self._lock:
            if symbol not in self._data:
                raise KeyError(f"unknown symbol: {symbol}")
            return self._data[symbol].tail(n).copy(deep=True)

    def get_all_recent(self, n: Optional[int] = None) -> dict[str, pd.DataFrame]:
        if n is None:
            n = self.window_size
        n = _validate_positive_int("n", int(n))
        if n > self.window_size:
            raise ValueError("n must be <= window_size")
        with self._lock:
            return {symbol: df.tail(n).copy(deep=True) for symbol, df in self._data.items()}

    def add_symbols(self, symbols: Iterable[str]) -> None:
        normalized = _normalize_symbols(symbols)
        with self._lock:
            for symbol in normalized:
                if symbol in self._data:
                    continue
                self._data[symbol] = _empty_frame()
                self._last_success_at[symbol] = None
                self._last_error[symbol] = None
                self._last_open_time_ms[symbol] = None
            self.max_workers = max(self.max_workers, min(8, max(1, len(self._data))))
            self._update_ready_event_locked()

    def remove_symbols(self, symbols: Iterable[str]) -> None:
        normalized = _normalize_symbols(symbols)
        with self._lock:
            for symbol in normalized:
                self._data.pop(symbol, None)
                self._last_success_at.pop(symbol, None)
                self._last_error.pop(symbol, None)
                self._last_open_time_ms.pop(symbol, None)
                self._in_flight.discard(symbol)
            self._update_ready_event_locked()

    def status(self) -> MultiKlineServiceStatus:
        with self._lock:
            symbols = {
                symbol: SymbolKlineStatus(
                    symbol=symbol,
                    row_count=len(df),
                    ready=not df.empty,
                    in_flight=symbol in self._in_flight,
                    last_success_at=self._last_success_at.get(symbol),
                    last_open_time=_ms_to_dt(self._last_open_time_ms.get(symbol)),
                    last_error=str(self._last_error[symbol]) if self._last_error.get(symbol) else None,
                )
                for symbol, df in self._data.items()
            }
            return MultiKlineServiceStatus(
                symbols=symbols,
                window_size=self.window_size,
                running=self.is_running,
                ready=self.is_ready,
                max_workers=self.max_workers,
                last_refresh_at=self._last_refresh_at,
                last_error=str(self._last_global_error) if self._last_global_error else None,
                rate_limiter=self.rate_limiter.status() if self.rate_limiter is not None else None,
            )

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    def __enter__(self) -> "MultiSymbolKlineService":
        self.start(block_until_ready=True)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def _wait_until_ready(self, timeout: Optional[float]) -> None:
        wait_timeout = self.startup_timeout_seconds if timeout is None else timeout
        if self._ready_event.wait(wait_timeout):
            return
        status = self.status()
        errors = {symbol: item.last_error for symbol, item in status.symbols.items() if item.last_error}
        detail = f"; symbol errors: {errors}" if errors else ""
        if status.last_error:
            detail = f"; last error: {status.last_error}{detail}"
        raise KlineServiceError(f"multi-symbol kline service did not become ready within {wait_timeout}s{detail}")

    def _run_loop(self) -> None:
        backoff = self.refresh_interval_seconds
        while not self._stop_event.is_set():
            try:
                self.refresh_once()
                backoff = self.refresh_interval_seconds
                wait_seconds = self.refresh_interval_seconds
            except Exception:
                wait_seconds = min(backoff, self.max_backoff_seconds)
                backoff = min(backoff * 2, self.max_backoff_seconds)
            self._stop_event.wait(wait_seconds)

    def _configure_rate_limit_once(self) -> None:
        if not self.auto_configure_rate_limit or self._rate_limit_configured:
            return
        configure = getattr(self.client, "configure_rate_limiter_from_exchange_info", None)
        if not callable(configure):
            self._rate_limit_configured = True
            return
        try:
            configure()
        except Exception as exc:
            with self._lock:
                self._last_global_error = exc
        finally:
            self._rate_limit_configured = True

    def _symbols_snapshot(self) -> list[str]:
        with self._lock:
            return list(self._data.keys())

    def _plan_symbol_refresh(self, symbol: str, latest_closed_open_ms: int) -> Optional[int]:
        with self._lock:
            if symbol in self._in_flight or symbol not in self._data:
                return None
            last_open_time_ms = self._last_open_time_ms.get(symbol)
            df = self._data[symbol]

        if df.empty or last_open_time_ms is None:
            return self.window_size
        if last_open_time_ms >= latest_closed_open_ms:
            return None
        missing_count = ((latest_closed_open_ms - last_open_time_ms) // ONE_MINUTE_MS)
        return max(1, min(int(missing_count), self.window_size))

    def _fetch_symbol_frame(self, symbol: str, n: int, latest_closed_open_ms: int) -> pd.DataFrame:
        return self.client.fetch_recent_closed_1m_klines(
            symbol,
            n,
            end_open_time_ms=latest_closed_open_ms,
            chunk_limit=self.bootstrap_chunk_limit,
        )

    def _mark_in_flight(self, symbol: str) -> None:
        with self._lock:
            self._in_flight.add(symbol)

    def _unmark_in_flight(self, symbol: str) -> None:
        with self._lock:
            self._in_flight.discard(symbol)

    def _replace_symbol_cache(self, symbol: str, df: pd.DataFrame) -> bool:
        if df is None:
            df = _empty_frame()
        df = df.copy(deep=True)
        if not df.empty:
            df = df.sort_index()
            df = df[~df.index.duplicated(keep="last")]

        with self._lock:
            old_df = self._data.get(symbol, _empty_frame())
            if old_df.empty:
                merged = df
            elif df.empty:
                merged = old_df
            else:
                merged = pd.concat([old_df, df]).sort_index()
                merged = merged[~merged.index.duplicated(keep="last")]
            merged = merged.tail(self.window_size).copy(deep=True)

            changed = not merged.equals(old_df)
            self._data[symbol] = merged
            self._last_error[symbol] = None
            self._last_success_at[symbol] = datetime.now(timezone.utc)
            self._last_open_time_ms[symbol] = (
                int(merged.index[-1].timestamp() * 1000) if not merged.empty else None
            )
            self._update_ready_event_locked()
            return changed

    def _record_symbol_error(self, symbol: str, exc: BaseException) -> None:
        with self._lock:
            if symbol in self._data:
                self._last_error[symbol] = exc
            self._update_ready_event_locked()

    def _update_ready_event_locked(self) -> None:
        if self._data and all(not df.empty for df in self._data.values()):
            self._ready_event.set()
        else:
            self._ready_event.clear()


def _normalize_symbols(symbols: Iterable[str]) -> list[str]:
    if isinstance(symbols, str):
        symbols = [symbols]
    seen = set()
    out = []
    for symbol in symbols:
        normalized = _normalize_symbol(symbol)
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_KLINE_COLUMNS).rename_axis("Open_Time")


def _ms_to_dt(value: Optional[int]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
