"""Background-updating Binance USD-M Futures partial depth service."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal, Optional

import numpy as np
import pandas as pd
import websockets
from websockets.exceptions import ConnectionClosed

from .errors import BinanceDepthResponseError, DepthServiceError
from .models import (
    DepthFrameNumericType,
    DepthLevelQuote,
    DepthSide,
    DepthValueField,
    normalize_depth_frame_numeric_type,
    normalize_depth_side,
    normalize_depth_value_field,
    utc_now_ms,
    validate_depth_level,
    validate_max_age_ms,
)

DepthLevel = Literal[5, 10, 20]
DepthSpeedMs = Literal[100, 250, 500]

DEFAULT_FUTURES_WS_BASE_URL = "wss://fstream.binance.com"
FUTURES_PUBLIC_WS_ENDPOINT = "public"
MAX_STREAMS_PER_CONNECTION = 1024
VALID_DEPTH_LEVELS = {5, 10, 20}
VALID_DEPTH_SPEEDS_MS = {100, 250, 500}


@dataclass(frozen=True)
class DepthPriceLevel:
    level: int
    price: Decimal
    qty: Decimal


@dataclass(frozen=True)
class DepthSnapshot:
    symbol: str
    event_time_ms: int
    transaction_time_ms: int
    local_recv_time_ms: int
    receive_latency_ms: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    bids: tuple[DepthPriceLevel, ...]
    asks: tuple[DepthPriceLevel, ...]
    mid_price: Optional[Decimal]
    spread: Optional[Decimal]
    spread_bps: Optional[Decimal]
    is_stale: bool
    sequence_gap: bool
    stale_reason: Optional[str] = None
    stale_since_ms: Optional[int] = None

    def get_level_quote(self, level: int = 1) -> DepthLevelQuote:
        level = validate_depth_level(level)
        bid = self.bids[level - 1] if level <= len(self.bids) else None
        ask = self.asks[level - 1] if level <= len(self.asks) else None

        return DepthLevelQuote(
            symbol=self.symbol,
            level=level,
            bid_price=bid.price if bid is not None else None,
            bid_qty=bid.qty if bid is not None else None,
            ask_price=ask.price if ask is not None else None,
            ask_qty=ask.qty if ask is not None else None,
            event_time_ms=self.event_time_ms,
            transaction_time_ms=self.transaction_time_ms,
            local_recv_time_ms=self.local_recv_time_ms,
            receive_latency_ms=self.receive_latency_ms,
            final_update_id=self.final_update_id,
            is_stale=self.is_stale,
            sequence_gap=self.sequence_gap,
            depth_incomplete=None,
        )


@dataclass(frozen=True)
class BinanceDepthConfig:
    symbols: Iterable[str]
    levels: DepthLevel = 5
    speed_ms: DepthSpeedMs = 100
    base_url: str = DEFAULT_FUTURES_WS_BASE_URL
    endpoint: str = FUTURES_PUBLIC_WS_ENDPOINT
    read_timeout_seconds: Optional[float] = 60.0
    reconnect_initial_delay_seconds: float = 1.0
    reconnect_max_delay_seconds: float = 60.0
    reconnect_jitter_seconds: float = 0.25
    startup_timeout_seconds: float = 30.0
    max_queue: int = 8192

    def __post_init__(self) -> None:
        symbols = _normalize_symbols(self.symbols)
        if not symbols:
            raise ValueError("symbols must contain at least one symbol")
        if len(symbols) > MAX_STREAMS_PER_CONNECTION:
            raise ValueError(f"symbols must contain at most {MAX_STREAMS_PER_CONNECTION} symbols")
        if self.levels not in VALID_DEPTH_LEVELS:
            raise ValueError(f"levels must be one of {sorted(VALID_DEPTH_LEVELS)}, got {self.levels}")
        if self.speed_ms not in VALID_DEPTH_SPEEDS_MS:
            raise ValueError(f"speed_ms must be one of {sorted(VALID_DEPTH_SPEEDS_MS)}, got {self.speed_ms}")
        if self.read_timeout_seconds is not None and self.read_timeout_seconds <= 0:
            raise ValueError("read_timeout_seconds must be positive or None")
        if self.reconnect_initial_delay_seconds <= 0:
            raise ValueError("reconnect_initial_delay_seconds must be positive")
        if self.reconnect_max_delay_seconds < self.reconnect_initial_delay_seconds:
            raise ValueError("reconnect_max_delay_seconds must be >= reconnect_initial_delay_seconds")
        if self.reconnect_jitter_seconds < 0:
            raise ValueError("reconnect_jitter_seconds must be >= 0")
        if self.startup_timeout_seconds <= 0:
            raise ValueError("startup_timeout_seconds must be positive")
        if self.max_queue <= 0:
            raise ValueError("max_queue must be positive")

        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))
        object.__setattr__(self, "endpoint", self.endpoint.strip("/"))


@dataclass(frozen=True)
class DepthServiceStatus:
    symbols: tuple[str, ...]
    running: bool
    ready: bool
    connected: bool
    url: str
    last_connect_at: Optional[datetime]
    last_message_at: Optional[datetime]
    reconnect_attempts: int
    last_error: Optional[str]


class BinanceFuturesDepthService:
    """Maintain latest top-N futures bid and ask levels in a background thread."""

    def __init__(
        self,
        config: BinanceDepthConfig,
        *,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.config = config
        self.logger = logger or logging.getLogger(__name__)

        self._lock = threading.RLock()
        self._snapshots: dict[str, DepthSnapshot] = {}
        self._last_u_by_symbol: dict[str, int] = {}
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._connected = False
        self._last_connect_at: Optional[datetime] = None
        self._last_message_at: Optional[datetime] = None
        self._last_error: Optional[BaseException] = None
        self._reconnect_attempts = 0

    def build_url(self) -> str:
        streams = "/".join(self._stream_name(symbol) for symbol in self.config.symbols)
        return f"{self.config.base_url}/{self.config.endpoint}/stream?streams={streams}"

    def start(self, *, block_until_ready: bool = False, timeout: Optional[float] = None) -> None:
        """Start the daemon receiver thread."""

        if self.is_running:
            if block_until_ready:
                self._wait_until_ready(timeout)
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_thread,
            name="binance-futures-depth-service",
            daemon=True,
        )
        self._thread.start()

        if block_until_ready:
            self._wait_until_ready(timeout)

    def stop(self, *, timeout: Optional[float] = 5.0) -> None:
        """Stop the receiver thread and mark cached depth as stale."""

        thread = self._thread
        if thread is None:
            self._mark_all_stale("stopped")
            return

        self._stop_event.set()
        if thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=timeout)
        if not thread.is_alive():
            self._thread = None
        self._mark_all_stale("stopped")

    def get_latest(self, symbol: str) -> Optional[DepthSnapshot]:
        symbol = _normalize_symbol(symbol)
        with self._lock:
            return self._snapshots.get(symbol)

    def get_latest_snapshot(self, symbol: str) -> Optional[DepthSnapshot]:
        return self.get_latest(symbol)

    def get_latest_level_quote(
        self,
        symbol: str,
        level: int = 1,
        *,
        require_not_stale: bool = True,
        require_sequence_continuity: bool = False,
        max_age_ms: Optional[int] = None,
    ) -> Optional[DepthLevelQuote]:
        level = validate_depth_level(level, max_level=int(self.config.levels))
        max_age_ms = validate_max_age_ms(max_age_ms)
        snapshot = self.get_latest(symbol)
        if snapshot is None:
            return None
        if not self._snapshot_passes_level_filters(
            snapshot,
            require_not_stale=require_not_stale,
            require_sequence_continuity=require_sequence_continuity,
            max_age_ms=max_age_ms,
        ):
            return None
        return snapshot.get_level_quote(level)

    def get_latest_level_value(
        self,
        symbol: str,
        side: DepthSide,
        level: int = 1,
        field: DepthValueField = "price",
        *,
        require_not_stale: bool = True,
        require_sequence_continuity: bool = False,
        max_age_ms: Optional[int] = None,
    ) -> Optional[Decimal]:
        normalized_side = normalize_depth_side(side)
        normalized_field = normalize_depth_value_field(field)
        quote = self.get_latest_level_quote(
            symbol,
            level=level,
            require_not_stale=require_not_stale,
            require_sequence_continuity=require_sequence_continuity,
            max_age_ms=max_age_ms,
        )
        if quote is None:
            return None
        return quote.value(normalized_side, normalized_field)

    def get_latest_depth_frame(
        self,
        levels: Optional[int] = None,
        *,
        include_status: bool = True,
        require_sequence_continuity: bool = False,
        max_age_ms: Optional[int] = None,
        numeric_type: DepthFrameNumericType = "decimal",
    ) -> pd.DataFrame:
        frame_levels = validate_depth_level(
            levels if levels is not None else int(self.config.levels),
            max_level=int(self.config.levels),
        )
        max_age_ms = validate_max_age_ms(max_age_ms)
        numeric_type = normalize_depth_frame_numeric_type(numeric_type)
        rows = [
            self._depth_frame_row(
                symbol,
                levels=frame_levels,
                include_status=include_status,
                require_sequence_continuity=require_sequence_continuity,
                max_age_ms=max_age_ms,
                numeric_type=numeric_type,
            )
            for symbol in self.config.symbols
        ]
        return pd.DataFrame(
            rows,
            index=pd.Index(self.config.symbols, name="symbol"),
        )

    def get_all_latest(self) -> dict[str, DepthSnapshot]:
        with self._lock:
            return dict(self._snapshots)

    @property
    def latest_depth_table(self) -> dict[str, DepthSnapshot]:
        return self.get_all_latest()

    def status(self) -> DepthServiceStatus:
        with self._lock:
            return DepthServiceStatus(
                symbols=tuple(self.config.symbols),
                running=self.is_running,
                ready=self.is_ready,
                connected=self._connected,
                url=self.build_url(),
                last_connect_at=self._last_connect_at,
                last_message_at=self._last_message_at,
                reconnect_attempts=self._reconnect_attempts,
                last_error=str(self._last_error) if self._last_error else None,
            )

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    def __enter__(self) -> "BinanceFuturesDepthService":
        self.start(block_until_ready=True)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def _stream_name(self, symbol: str) -> str:
        stream = f"{symbol.lower()}@depth{self.config.levels}"
        if self.config.speed_ms == 250:
            return stream
        return f"{stream}@{self.config.speed_ms}ms"

    @staticmethod
    def _snapshot_passes_level_filters(
        snapshot: DepthSnapshot,
        *,
        require_not_stale: bool,
        require_sequence_continuity: bool,
        max_age_ms: Optional[int],
    ) -> bool:
        if require_not_stale and snapshot.is_stale:
            return False
        if require_sequence_continuity and snapshot.sequence_gap:
            return False
        if max_age_ms is not None and utc_now_ms() - snapshot.local_recv_time_ms > max_age_ms:
            return False
        return True

    def _depth_frame_row(
        self,
        symbol: str,
        *,
        levels: int,
        include_status: bool,
        require_sequence_continuity: bool,
        max_age_ms: Optional[int],
        numeric_type: DepthFrameNumericType,
    ) -> dict[str, Any]:
        snapshot = self.get_latest(symbol)
        has_snapshot = snapshot is not None
        transaction_time_ms = np.nan if snapshot is None else snapshot.transaction_time_ms
        is_stale = True if snapshot is None else snapshot.is_stale
        sequence_gap = False if snapshot is None else snapshot.sequence_gap
        if snapshot is None:
            depth_incomplete = True
        else:
            depth_incomplete = len(snapshot.bids) < levels or len(snapshot.asks) < levels
        can_use_numeric_data = (
            snapshot is not None
            and self._snapshot_passes_level_filters(
                snapshot,
                require_not_stale=True,
                require_sequence_continuity=require_sequence_continuity,
                max_age_ms=max_age_ms,
            )
        )

        row: dict[str, Any] = {}
        if can_use_numeric_data:
            assert snapshot is not None
            bids = snapshot.bids
            asks = snapshot.asks
            for level in range(1, levels + 1):
                index = level - 1
                bid = bids[index] if index < len(bids) else None
                ask = asks[index] if index < len(asks) else None
                row[f"bid{level}"] = _depth_frame_value(bid.price if bid is not None else None, numeric_type)
                row[f"bid{level}_qty"] = _depth_frame_value(bid.qty if bid is not None else None, numeric_type)
                row[f"ask{level}"] = _depth_frame_value(ask.price if ask is not None else None, numeric_type)
                row[f"ask{level}_qty"] = _depth_frame_value(ask.qty if ask is not None else None, numeric_type)
        else:
            for level in range(1, levels + 1):
                row[f"bid{level}"] = np.nan
                row[f"bid{level}_qty"] = np.nan
                row[f"ask{level}"] = np.nan
                row[f"ask{level}_qty"] = np.nan

        if include_status:
            row.update(
                {
                    "has_snapshot": has_snapshot,
                    "transaction_time_ms": transaction_time_ms,
                    "is_stale": is_stale,
                    "sequence_gap": sequence_gap,
                    "depth_incomplete": depth_incomplete,
                }
            )

        return row

    def _wait_until_ready(self, timeout: Optional[float]) -> None:
        wait_timeout = self.config.startup_timeout_seconds if timeout is None else timeout
        if self._ready_event.wait(wait_timeout):
            return
        status = self.status()
        detail = f"; last error: {status.last_error}" if status.last_error else ""
        raise DepthServiceError(f"depth service did not become ready within {wait_timeout}s{detail}")

    def _run_thread(self) -> None:
        try:
            asyncio.run(self._run_forever())
        finally:
            self._set_connected(False)
            if self._stop_event.is_set():
                self._mark_all_stale("stopped")

    async def _run_forever(self) -> None:
        url = self.build_url()
        reconnect_delay = self.config.reconnect_initial_delay_seconds

        while not self._stop_event.is_set():
            self._record_connect_attempt()
            try:
                self.logger.info("Connecting to Binance Futures depth stream: url=%s", url)
                async with websockets.connect(
                    url,
                    ping_interval=None,
                    max_queue=self.config.max_queue,
                    close_timeout=5,
                ) as websocket:
                    self._record_connected()
                    reconnect_delay = self.config.reconnect_initial_delay_seconds
                    await self._receive_messages(websocket)
            except ConnectionClosed as exc:
                self.logger.warning(
                    "WebSocket connection closed: code=%s reason=%s",
                    getattr(exc, "code", None),
                    getattr(exc, "reason", None),
                )
                self._record_error(exc)
                self._mark_all_stale("connection_closed")
            except OSError as exc:
                self.logger.warning("Network-level WebSocket error: %r", exc)
                self._record_error(exc)
                self._mark_all_stale("network_error")
            except Exception as exc:
                self.logger.exception("Unexpected WebSocket error: %r", exc)
                self._record_error(exc)
                self._mark_all_stale("unexpected_error")
            finally:
                self._set_connected(False)

            if self._stop_event.is_set():
                break

            jitter = random.uniform(0, self.config.reconnect_jitter_seconds)
            sleep_seconds = min(
                reconnect_delay + jitter,
                self.config.reconnect_max_delay_seconds,
            )
            self.logger.warning(
                "Reconnecting Binance Futures depth stream after %.2f seconds",
                sleep_seconds,
            )
            await self._sleep_until_stopped(sleep_seconds)
            reconnect_delay = min(
                reconnect_delay * 2,
                self.config.reconnect_max_delay_seconds,
            )

    async def _receive_messages(self, websocket: Any) -> None:
        last_recv_monotonic = time.monotonic()

        while not self._stop_event.is_set():
            timeout = self._next_recv_timeout(last_recv_monotonic)
            try:
                raw = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                if self._read_timeout_elapsed(last_recv_monotonic):
                    self.logger.warning(
                        "No WebSocket message received for %.1f seconds; reconnecting",
                        self.config.read_timeout_seconds,
                    )
                    self._mark_all_stale("read_timeout")
                    return
                continue

            last_recv_monotonic = time.monotonic()
            self._handle_raw_message(raw)

    def _next_recv_timeout(self, last_recv_monotonic: float) -> float:
        if self.config.read_timeout_seconds is None:
            return 1.0
        remaining = self.config.read_timeout_seconds - (time.monotonic() - last_recv_monotonic)
        return max(0.1, min(1.0, remaining))

    def _read_timeout_elapsed(self, last_recv_monotonic: float) -> bool:
        if self.config.read_timeout_seconds is None:
            return False
        return (time.monotonic() - last_recv_monotonic) >= self.config.read_timeout_seconds

    async def _sleep_until_stopped(self, sleep_seconds: float) -> None:
        end = time.monotonic() + sleep_seconds
        while not self._stop_event.is_set():
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 0.5))

    def _handle_raw_message(self, raw: str | bytes) -> None:
        try:
            snapshot = self._parse_depth_message(raw)
        except Exception as exc:
            self.logger.warning("Failed to parse depth message: error=%r raw_prefix=%s", exc, _raw_prefix(raw))
            self._record_error(exc)
            return

        with self._lock:
            snapshot = self._with_sequence_status_locked(snapshot)
            self._snapshots[snapshot.symbol] = snapshot
            self._last_message_at = datetime.now(timezone.utc)
            self._last_error = None
            self._update_ready_event_locked()

    def _parse_depth_message(self, raw: str | bytes | Mapping[str, Any]) -> DepthSnapshot:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            try:
                message = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise BinanceDepthResponseError(f"invalid JSON from depth stream: {exc}") from exc
        elif isinstance(raw, Mapping):
            message = raw
        else:
            raise BinanceDepthResponseError("depth message must be a JSON string, bytes, or mapping")

        if not isinstance(message, Mapping):
            raise BinanceDepthResponseError("depth message must decode to a JSON object")
        data = message.get("data", message)
        if not isinstance(data, Mapping):
            raise BinanceDepthResponseError("depth message data must be a JSON object")

        missing = [key for key in ("s", "E", "T", "U", "u", "pu", "b", "a") if key not in data]
        if missing:
            raise BinanceDepthResponseError(f"depth message missing required fields: {missing}")

        local_recv_time_ms = utc_now_ms()
        symbol = _normalize_symbol(str(data["s"]))
        event_time_ms = _coerce_int(data["E"], "E")
        transaction_time_ms = _coerce_int(data["T"], "T")
        bids = _parse_price_qty_array(data["b"], side="bid", max_levels=self.config.levels)
        asks = _parse_price_qty_array(data["a"], side="ask", max_levels=self.config.levels)

        mid_price: Optional[Decimal] = None
        spread: Optional[Decimal] = None
        spread_bps: Optional[Decimal] = None
        if bids and asks:
            spread = asks[0].price - bids[0].price
            mid_price = (bids[0].price + asks[0].price) / Decimal("2")
            if mid_price != 0:
                spread_bps = spread / mid_price * Decimal("10000")

        if len(bids) < 2 or len(asks) < 2:
            self.logger.warning(
                "Depth message has fewer than 2 levels: symbol=%s bids=%d asks=%d",
                symbol,
                len(bids),
                len(asks),
            )

        return DepthSnapshot(
            symbol=symbol,
            event_time_ms=event_time_ms,
            transaction_time_ms=transaction_time_ms,
            local_recv_time_ms=local_recv_time_ms,
            receive_latency_ms=local_recv_time_ms - event_time_ms,
            first_update_id=_coerce_int(data["U"], "U"),
            final_update_id=_coerce_int(data["u"], "u"),
            previous_final_update_id=_coerce_int(data["pu"], "pu"),
            bids=bids,
            asks=asks,
            mid_price=mid_price,
            spread=spread,
            spread_bps=spread_bps,
            is_stale=False,
            sequence_gap=False,
        )

    def _with_sequence_status_locked(self, snapshot: DepthSnapshot) -> DepthSnapshot:
        previous_u = self._last_u_by_symbol.get(snapshot.symbol)
        sequence_gap = previous_u is not None and snapshot.previous_final_update_id != previous_u

        if sequence_gap:
            self.logger.warning(
                "Depth sequence gap detected: symbol=%s previous_u=%s current_pu=%s current_u=%s",
                snapshot.symbol,
                previous_u,
                snapshot.previous_final_update_id,
                snapshot.final_update_id,
            )

        self._last_u_by_symbol[snapshot.symbol] = snapshot.final_update_id
        if sequence_gap == snapshot.sequence_gap:
            return snapshot
        return replace(snapshot, sequence_gap=sequence_gap)

    def _record_connect_attempt(self) -> None:
        with self._lock:
            self._reconnect_attempts += 1

    def _record_connected(self) -> None:
        with self._lock:
            self._connected = True
            self._last_connect_at = datetime.now(timezone.utc)
            self._last_error = None
            self._last_u_by_symbol.clear()
            self._update_ready_event_locked()

    def _record_error(self, exc: BaseException) -> None:
        with self._lock:
            self._last_error = exc
            self._update_ready_event_locked()

    def _set_connected(self, value: bool) -> None:
        with self._lock:
            self._connected = value
            self._update_ready_event_locked()

    def _mark_all_stale(self, reason: str) -> None:
        now_ms = utc_now_ms()
        with self._lock:
            for symbol, snapshot in list(self._snapshots.items()):
                self._snapshots[symbol] = replace(
                    snapshot,
                    is_stale=True,
                    stale_reason=reason,
                    stale_since_ms=now_ms,
                )
            self._ready_event.clear()

    def _update_ready_event_locked(self) -> None:
        ready = self._connected and self._last_error is None and not self._stop_event.is_set()
        if ready:
            self._ready_event.set()
        else:
            self._ready_event.clear()


def _normalize_symbol(symbol: str) -> str:
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string")
    return symbol.strip().upper()


def _normalize_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    if isinstance(symbols, str):
        symbols = [symbols]

    seen: set[str] = set()
    normalized_symbols: list[str] = []
    for symbol in symbols:
        normalized = _normalize_symbol(symbol)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_symbols.append(normalized)
    return tuple(normalized_symbols)


def _depth_frame_value(value: Optional[Decimal], numeric_type: DepthFrameNumericType) -> Decimal | float:
    if value is None:
        return np.nan
    if numeric_type == "float":
        return float(value)
    return value


def _parse_price_qty_array(
    values: Any,
    *,
    side: Literal["bid", "ask"],
    max_levels: int,
) -> tuple[DepthPriceLevel, ...]:
    if not isinstance(values, list):
        raise BinanceDepthResponseError(f"{side} levels must be a JSON array")

    parsed: list[tuple[Decimal, Decimal]] = []
    for index, row in enumerate(values):
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            raise BinanceDepthResponseError(f"malformed {side} level at index {index}")
        try:
            price = Decimal(str(row[0]))
            qty = Decimal(str(row[1]))
        except Exception as exc:
            raise BinanceDepthResponseError(f"invalid {side} price/qty at index {index}") from exc
        parsed.append((price, qty))

    parsed.sort(key=lambda item: item[0], reverse=side == "bid")
    return tuple(
        DepthPriceLevel(level=level, price=price, qty=qty)
        for level, (price, qty) in enumerate(parsed[:max_levels], start=1)
    )


def _coerce_int(value: Any, field_name: str) -> int:
    try:
        return int(value)
    except Exception as exc:
        raise BinanceDepthResponseError(f"depth field {field_name} must be an integer") from exc


def _raw_prefix(raw: Any) -> str:
    if isinstance(raw, bytes):
        return raw[:500].decode("utf-8", errors="replace")
    return str(raw)[:500]
