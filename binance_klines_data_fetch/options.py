"""Utilities for Binance Options symbol universes."""

from __future__ import annotations

import re
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal, Optional, cast

import requests

from .errors import BinanceAPIError, BinanceRequestError, BinanceResponseError
from .rate_limiter import WeightedRateLimiter, retry_after_seconds_from_headers

OPTIONS_REST_BASE_URL = "https://eapi.binance.com"
OPTIONS_EXCHANGE_INFO_PATH = "/eapi/v1/exchangeInfo"
DEFAULT_OPTIONS_WS_BASE_URL = "wss://fstream.binance.com"
OPTIONS_PUBLIC_WS_ENDPOINT = "public"
MAX_OPTIONS_STREAMS_PER_CONNECTION = 200
VALID_OPTIONS_DEPTH_LEVELS = {5, 10, 20}
VALID_OPTIONS_DEPTH_SPEEDS_MS = {100, 500}

OptionType = Literal["CALL", "PUT"]
OptionDepthLevel = Literal[5, 10, 20]
OptionDepthSpeedMs = Literal[100, 500]

OPTION_SYMBOL_RE = re.compile(
    r"^(?P<base>[A-Z0-9]+)-(?P<expiry>\d{6})-(?P<strike>[0-9]+(?:\.[0-9]+)?)-(?P<cp>[CP])$"
)


@dataclass(frozen=True)
class OptionSymbol:
    symbol: str
    underlying: str
    base_asset: str
    quote_asset: str
    expiry: datetime
    expiry_date: str
    expiry_yymmdd: str
    strike: Decimal
    option_type: OptionType
    cp: str
    status: str
    price_scale: int
    quantity_scale: int
    min_qty: Decimal
    max_qty: Decimal
    unit: Decimal
    tick_size: Optional[Decimal] = None
    step_size: Optional[Decimal] = None


class BinanceOptionsClient:
    """Small Binance Options REST client for public exchange information."""

    def __init__(
        self,
        *,
        base_url: str = OPTIONS_REST_BASE_URL,
        timeout: float = 10.0,
        session: Optional[requests.Session] = None,
        retries: int = 2,
        retry_backoff_seconds: float = 0.25,
        proxies: Optional[dict[str, str]] = None,
        rate_limiter: Optional[WeightedRateLimiter] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = float(timeout)
        self.session = session or requests.Session()
        self.retries = max(0, int(retries))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.proxies = proxies
        self.rate_limiter = rate_limiter

    def fetch_exchange_info(self) -> dict[str, Any]:
        data = self._request_json(OPTIONS_EXCHANGE_INFO_PATH, params={}, request_weight=1)
        if not isinstance(data, dict):
            raise BinanceResponseError("options exchangeInfo response must be a JSON object")
        return data

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
                    response_error = BinanceResponseError(f"invalid JSON from Binance Options: {exc}")
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
                        f"Binance Options API error: {data.get('msg')} (code: {data.get('code')})",
                        code=self._safe_int(data.get("code")),
                        binance_message=str(data.get("msg")),
                    )

                return data
            except (BinanceAPIError, BinanceResponseError):
                raise
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = BinanceRequestError(f"request failed for Binance Options {path}: {exc}")
                if attempt >= self.retries:
                    raise last_error from exc
                self._sleep_before_retry(attempt)
            except requests.RequestException as exc:
                raise BinanceRequestError(f"request failed for Binance Options {path}: {exc}") from exc

        if last_error is not None:
            raise last_error
        raise BinanceRequestError(f"request failed for Binance Options {path}")

    def _api_error_from_response(self, status_code: int, data: Any) -> BinanceAPIError:
        code = None
        msg = None
        if isinstance(data, dict):
            code = self._safe_int(data.get("code"))
            raw_msg = data.get("msg")
            msg = str(raw_msg) if raw_msg is not None else None
        detail = f": {msg}" if msg else ""
        return BinanceAPIError(
            f"Binance Options HTTP {status_code}{detail}",
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

    def _notify_rate_limiter_of_status(self, status_code: int, headers: Any) -> None:
        if self.rate_limiter is None:
            return
        retry_after = retry_after_seconds_from_headers(headers)
        if status_code == 429:
            self.rate_limiter.on_rate_limited(retry_after_seconds=retry_after)
        elif status_code == 418:
            self.rate_limiter.on_banned(retry_after_seconds=retry_after)

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None


def get_option_universe(
    *,
    client: Optional[BinanceOptionsClient] = None,
    exchange_info: Optional[dict[str, Any]] = None,
) -> list[OptionSymbol]:
    """Return Binance Options symbols parsed from /eapi/v1/exchangeInfo."""

    info = _resolve_options_exchange_info(client=client, exchange_info=exchange_info)
    return [parse_option_symbol_record(item) for item in _iter_option_symbol_items(info)]


def get_trading_option_symbols(
    *,
    client: Optional[BinanceOptionsClient] = None,
    exchange_info: Optional[dict[str, Any]] = None,
    underlying: Optional[str] = None,
) -> list[str]:
    """Return active Binance Options symbol strings, optionally filtered by underlying."""

    universe = get_option_universe(client=client, exchange_info=exchange_info)
    return [opt.symbol for opt in filter_options(universe, underlying=underlying, status="TRADING")]


def parse_option_symbol_record(item: dict[str, Any]) -> OptionSymbol:
    if not isinstance(item, dict):
        raise BinanceResponseError("optionSymbols item must be a JSON object")

    symbol = _require_str(item, "symbol").upper()
    match = OPTION_SYMBOL_RE.match(symbol)
    if not match:
        raise BinanceResponseError(f"unexpected Binance Options symbol format: {symbol}")

    base_asset = match.group("base")
    expiry_yymmdd = match.group("expiry")
    expiry_date = _yymmdd_to_iso(expiry_yymmdd)
    strike_from_symbol = Decimal(match.group("strike"))
    cp = match.group("cp")

    option_type = _require_str(item, "side").upper()
    if option_type not in {"CALL", "PUT"}:
        raise BinanceResponseError(f"unknown Binance Options side: {option_type}")
    expected_cp = "C" if option_type == "CALL" else "P"
    if cp != expected_cp:
        raise BinanceResponseError(f"option side mismatch: symbol={symbol}, cp={cp}, side={option_type}")

    strike_from_field = _require_decimal(item, "strikePrice")
    if strike_from_symbol != strike_from_field:
        raise BinanceResponseError(
            f"option strike mismatch: symbol={symbol}, "
            f"from_symbol={strike_from_symbol}, from_field={strike_from_field}"
        )

    return OptionSymbol(
        symbol=symbol,
        underlying=_require_str(item, "underlying").upper(),
        base_asset=base_asset,
        quote_asset=_require_str(item, "quoteAsset").upper(),
        expiry=_ms_to_utc(_require_int(item, "expiryDate")),
        expiry_date=expiry_date,
        expiry_yymmdd=expiry_yymmdd,
        strike=strike_from_field,
        option_type=cast(OptionType, option_type),
        cp=cp,
        status=_require_str(item, "status").upper(),
        price_scale=_require_int(item, "priceScale"),
        quantity_scale=_require_int(item, "quantityScale"),
        min_qty=_require_decimal(item, "minQty"),
        max_qty=_require_decimal(item, "maxQty"),
        unit=_require_decimal(item, "unit"),
        tick_size=_filter_decimal(item, "PRICE_FILTER", "tickSize"),
        step_size=_filter_decimal(item, "LOT_SIZE", "stepSize"),
    )


def filter_options(
    universe: Iterable[OptionSymbol],
    *,
    underlying: Optional[str] = None,
    status: Optional[str] = "TRADING",
    option_type: Optional[str] = None,
    expiry: Optional[datetime] = None,
    min_expiry: Optional[datetime] = None,
    max_expiry: Optional[datetime] = None,
    min_strike: Optional[Decimal] = None,
    max_strike: Optional[Decimal] = None,
) -> list[OptionSymbol]:
    """Filter option symbols and return a stable sorted list."""

    underlying_filter = _normalize_optional_upper(underlying)
    status_filter = _normalize_optional_upper(status)
    option_type_filter = _normalize_optional_upper(option_type)
    if option_type_filter is not None and option_type_filter not in {"CALL", "PUT"}:
        raise ValueError("option_type must be CALL, PUT, or None")

    min_strike_decimal = _optional_decimal(min_strike)
    max_strike_decimal = _optional_decimal(max_strike)

    result = []
    for opt in universe:
        if status_filter is not None and opt.status != status_filter:
            continue
        if underlying_filter is not None and opt.underlying != underlying_filter:
            continue
        if option_type_filter is not None and opt.option_type != option_type_filter:
            continue
        if expiry is not None and opt.expiry != expiry:
            continue
        if min_expiry is not None and opt.expiry < min_expiry:
            continue
        if max_expiry is not None and opt.expiry > max_expiry:
            continue
        if min_strike_decimal is not None and opt.strike < min_strike_decimal:
            continue
        if max_strike_decimal is not None and opt.strike > max_strike_decimal:
            continue
        result.append(opt)

    return _sort_options(result)


def select_nearest_expiries(options: Iterable[OptionSymbol], count: int = 1) -> list[datetime]:
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("count must be a positive integer")
    return sorted({opt.expiry for opt in options})[:count]


def select_near_atm_options(
    options: Iterable[OptionSymbol],
    spot: Decimal,
    n_strikes_each_side: int = 5,
) -> list[OptionSymbol]:
    if isinstance(n_strikes_each_side, bool) or not isinstance(n_strikes_each_side, int) or n_strikes_each_side < 0:
        raise ValueError("n_strikes_each_side must be a non-negative integer")

    options_list = list(options)
    strikes = sorted({opt.strike for opt in options_list})
    if not strikes:
        return []

    spot_decimal = Decimal(str(spot))
    atm_strike = min(strikes, key=lambda strike: abs(strike - spot_decimal))
    atm_index = strikes.index(atm_strike)
    lo = max(0, atm_index - n_strikes_each_side)
    hi = min(len(strikes), atm_index + n_strikes_each_side + 1)
    selected_strikes = set(strikes[lo:hi])

    return _sort_options(opt for opt in options_list if opt.strike in selected_strikes)


def build_options_depth_streams(
    options_or_symbols: Iterable[OptionSymbol | str],
    *,
    levels: OptionDepthLevel = 5,
    speed_ms: OptionDepthSpeedMs = 100,
) -> list[str]:
    if levels not in VALID_OPTIONS_DEPTH_LEVELS:
        raise ValueError(f"levels must be one of {sorted(VALID_OPTIONS_DEPTH_LEVELS)}, got {levels}")
    if speed_ms not in VALID_OPTIONS_DEPTH_SPEEDS_MS:
        raise ValueError(f"speed_ms must be one of {sorted(VALID_OPTIONS_DEPTH_SPEEDS_MS)}, got {speed_ms}")

    symbols = _normalize_option_symbols(options_or_symbols)
    return [f"{symbol.lower()}@depth{levels}@{speed_ms}ms" for symbol in symbols]


def build_options_combined_stream_urls(
    streams: Iterable[str],
    *,
    base_url: str = DEFAULT_OPTIONS_WS_BASE_URL,
    endpoint: str = OPTIONS_PUBLIC_WS_ENDPOINT,
    max_streams_per_connection: int = MAX_OPTIONS_STREAMS_PER_CONNECTION,
) -> list[str]:
    if (
        isinstance(max_streams_per_connection, bool)
        or not isinstance(max_streams_per_connection, int)
        or max_streams_per_connection <= 0
    ):
        raise ValueError("max_streams_per_connection must be a positive integer")
    if max_streams_per_connection > MAX_OPTIONS_STREAMS_PER_CONNECTION:
        raise ValueError(f"max_streams_per_connection must be <= {MAX_OPTIONS_STREAMS_PER_CONNECTION}")

    normalized_streams = [_normalize_stream_name(stream) for stream in streams]
    if not normalized_streams:
        return []

    base = base_url.rstrip("/")
    path = endpoint.strip("/")
    urls = []
    for group in _chunked(normalized_streams, max_streams_per_connection):
        urls.append(f"{base}/{path}/stream?streams={'/'.join(group)}")
    return urls


def _resolve_options_exchange_info(
    *,
    client: Optional[BinanceOptionsClient],
    exchange_info: Optional[dict[str, Any]],
) -> dict[str, Any]:
    if exchange_info is not None:
        return exchange_info
    if client is None:
        client = BinanceOptionsClient()
    return client.fetch_exchange_info()


def _iter_option_symbol_items(exchange_info: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(exchange_info, dict):
        raise BinanceResponseError("options exchangeInfo response must be a JSON object")
    raw_symbols = exchange_info.get("optionSymbols")
    if not isinstance(raw_symbols, list):
        raise BinanceResponseError("options exchangeInfo response missing optionSymbols")
    return raw_symbols


def _sort_options(options: Iterable[OptionSymbol]) -> list[OptionSymbol]:
    return sorted(options, key=lambda opt: (opt.underlying, opt.expiry, opt.strike, opt.option_type, opt.symbol))


def _normalize_option_symbols(options_or_symbols: Iterable[OptionSymbol | str]) -> tuple[str, ...]:
    if isinstance(options_or_symbols, str):
        options_or_symbols = [options_or_symbols]

    seen: set[str] = set()
    symbols: list[str] = []
    for item in options_or_symbols:
        symbol = item.symbol if isinstance(item, OptionSymbol) else str(item)
        normalized = _normalize_option_symbol(symbol)
        if normalized in seen:
            continue
        seen.add(normalized)
        symbols.append(normalized)
    return tuple(symbols)


def _normalize_option_symbol(symbol: str) -> str:
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string")
    return symbol.strip().upper()


def _normalize_stream_name(stream: str) -> str:
    if not isinstance(stream, str) or not stream.strip():
        raise ValueError("stream names must be non-empty strings")
    return stream.strip().lower()


def _normalize_optional_upper(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    return normalized or None


def _optional_decimal(value: Optional[Decimal]) -> Optional[Decimal]:
    if value is None:
        return None
    return Decimal(str(value))


def _require_str(item: dict[str, Any], field_name: str) -> str:
    value = item.get(field_name)
    if value is None or not str(value).strip():
        raise BinanceResponseError(f"optionSymbols item missing {field_name}")
    return str(value).strip()


def _require_int(item: dict[str, Any], field_name: str) -> int:
    if field_name not in item:
        raise BinanceResponseError(f"optionSymbols item missing {field_name}")
    try:
        return int(item[field_name])
    except Exception as exc:
        raise BinanceResponseError(f"optionSymbols field {field_name} must be an integer") from exc


def _require_decimal(item: dict[str, Any], field_name: str) -> Decimal:
    if field_name not in item:
        raise BinanceResponseError(f"optionSymbols item missing {field_name}")
    try:
        return Decimal(str(item[field_name]))
    except Exception as exc:
        raise BinanceResponseError(f"optionSymbols field {field_name} must be decimal-compatible") from exc


def _filter_decimal(item: dict[str, Any], filter_type: str, key: str) -> Optional[Decimal]:
    filters = item.get("filters", [])
    if not isinstance(filters, list):
        raise BinanceResponseError("optionSymbols field filters must be a JSON array")
    for filter_item in filters:
        if not isinstance(filter_item, dict):
            continue
        if filter_item.get("filterType") != filter_type or key not in filter_item:
            continue
        try:
            return Decimal(str(filter_item[key]))
        except Exception as exc:
            raise BinanceResponseError(f"optionSymbols filter {filter_type}.{key} must be decimal-compatible") from exc
    return None


def _ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _yymmdd_to_iso(yymmdd: str) -> str:
    try:
        yy = int(yymmdd[0:2])
        mm = int(yymmdd[2:4])
        dd = int(yymmdd[4:6])
        return datetime(2000 + yy, mm, dd, tzinfo=timezone.utc).date().isoformat()
    except Exception as exc:
        raise BinanceResponseError(f"invalid Binance Options expiry in symbol: {yymmdd}") from exc


def _chunked(items: list[str], chunk_size: int) -> list[list[str]]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]
