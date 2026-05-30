"""Background-updating kline cache service."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from .client import BinanceKlineClient, _normalize_symbol, _validate_positive_int
from .errors import KlineServiceError
from .models import KlineServiceStatus, ONE_MINUTE_MS, OUTPUT_KLINE_COLUMNS


class BinanceKlineService:
    """Maintain a rolling cache of closed 1m klines in a background thread."""

    def __init__(
        self,
        *,
        symbol: str,
        window_size: int,
        client: Optional[BinanceKlineClient] = None,
        refresh_interval_seconds: float = 2.0,
        max_backoff_seconds: float = 30.0,
        startup_timeout_seconds: float = 30.0,
    ) -> None:
        self.symbol = _normalize_symbol(symbol)
        self.window_size = _validate_positive_int("window_size", int(window_size))
        self.client = client or BinanceKlineClient()
        self.refresh_interval_seconds = max(0.1, float(refresh_interval_seconds))
        self.max_backoff_seconds = max(self.refresh_interval_seconds, float(max_backoff_seconds))
        self.startup_timeout_seconds = max(0.1, float(startup_timeout_seconds))

        self._lock = threading.RLock()
        self._df = pd.DataFrame(columns=OUTPUT_KLINE_COLUMNS).rename_axis("Open_Time")
        self._last_success_at: Optional[datetime] = None
        self._last_error: Optional[BaseException] = None
        self._last_open_time_ms: Optional[int] = None

        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self, *, block_until_ready: bool = False, timeout: Optional[float] = None) -> None:
        """Start the daemon refresh thread."""

        if self.is_running:
            if block_until_ready:
                self._wait_until_ready(timeout)
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"binance-kline-service-{self.symbol}",
            daemon=True,
        )
        self._thread.start()

        if block_until_ready:
            self._wait_until_ready(timeout)

    def stop(self, *, timeout: Optional[float] = 5.0) -> None:
        """Stop the refresh thread and wait briefly for it to exit."""

        thread = self._thread
        if thread is None:
            return
        self._stop_event.set()
        if thread.is_alive():
            thread.join(timeout=timeout)
        if not thread.is_alive():
            self._thread = None

    def refresh_once(self) -> bool:
        """Synchronously refresh the cache once.

        Returns True when cached data changed, False when it was already current.
        """

        try:
            changed = self._refresh_once_inner()
            with self._lock:
                self._last_error = None
            return changed
        except BaseException as exc:
            with self._lock:
                self._last_error = exc
            raise

    def get_recent(self, n: Optional[int] = None) -> pd.DataFrame:
        """Return a deep copy of the latest cached rows."""

        if n is None:
            n = self.window_size
        n = _validate_positive_int("n", int(n))
        if n > self.window_size:
            raise ValueError("n must be <= window_size")

        with self._lock:
            if self._df.empty:
                return self._df.copy(deep=True)
            return self._df.tail(n).copy(deep=True)

    def status(self) -> KlineServiceStatus:
        with self._lock:
            last_open_time = None
            if self._last_open_time_ms is not None:
                last_open_time = datetime.fromtimestamp(self._last_open_time_ms / 1000, tz=timezone.utc)
            return KlineServiceStatus(
                symbol=self.symbol,
                window_size=self.window_size,
                row_count=len(self._df),
                running=self.is_running,
                ready=self.is_ready,
                last_success_at=self._last_success_at,
                last_open_time=last_open_time,
                last_error=str(self._last_error) if self._last_error else None,
            )

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    @property
    def last_error(self) -> Optional[BaseException]:
        with self._lock:
            return self._last_error

    @property
    def last_success_at(self) -> Optional[datetime]:
        with self._lock:
            return self._last_success_at

    def __enter__(self) -> "BinanceKlineService":
        self.start(block_until_ready=True)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def _wait_until_ready(self, timeout: Optional[float]) -> None:
        wait_timeout = self.startup_timeout_seconds if timeout is None else timeout
        if self._ready_event.wait(wait_timeout):
            return
        last_error = self.last_error
        detail = f"; last error: {last_error}" if last_error else ""
        raise KlineServiceError(f"kline service did not become ready within {wait_timeout}s{detail}")

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

    def _refresh_once_inner(self) -> bool:
        latest_closed_open_ms = self.client.latest_closed_1m_open_time_ms()
        with self._lock:
            old_df = self._df.copy(deep=True)
            last_open_time_ms = self._last_open_time_ms

        if old_df.empty or last_open_time_ms is None:
            new_df = self.client.fetch_recent_closed_1m_klines(
                self.symbol,
                self.window_size,
                end_open_time_ms=latest_closed_open_ms,
            )
            self._replace_cache(new_df)
            return not new_df.empty

        if last_open_time_ms >= latest_closed_open_ms:
            return False

        missing_count = ((latest_closed_open_ms - last_open_time_ms) // ONE_MINUTE_MS)
        missing_count = max(1, min(int(missing_count), self.window_size))
        inc_df = self.client.fetch_recent_closed_1m_klines(
            self.symbol,
            missing_count,
            end_open_time_ms=latest_closed_open_ms,
        )
        if inc_df.empty:
            return False

        merged = pd.concat([old_df, inc_df]).sort_index()
        merged = merged[~merged.index.duplicated(keep="last")]
        merged = merged.tail(self.window_size)
        self._replace_cache(merged)
        return True

    def _replace_cache(self, df: pd.DataFrame) -> None:
        df = df.copy(deep=True)
        if not df.empty:
            df = df.sort_index()
            df = df[~df.index.duplicated(keep="last")]
            df = df.tail(self.window_size)
            last_open_time_ms = int(df.index[-1].timestamp() * 1000)
        else:
            last_open_time_ms = None

        with self._lock:
            self._df = df
            self._last_open_time_ms = last_open_time_ms
            self._last_success_at = datetime.now(timezone.utc)
            if not df.empty:
                self._ready_event.set()
