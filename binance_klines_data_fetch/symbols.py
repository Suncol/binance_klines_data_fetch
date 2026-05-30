"""Utilities for Binance USD-M futures symbol universes."""

from __future__ import annotations

from typing import Any, Iterable, Optional

import pandas as pd

from .client import BinanceKlineClient
from .errors import BinanceResponseError

UM_PERPETUAL_INFO_COLUMNS = [
    "symbol",
    "pair",
    "contractType",
    "status",
    "baseAsset",
    "quoteAsset",
    "marginAsset",
    "onboardDate",
    "deliveryDate",
]


def get_um_perpetual_symbols(
    *,
    client: Optional[BinanceKlineClient] = None,
    exchange_info: Optional[dict[str, Any]] = None,
    quote_assets: Optional[Iterable[str]] = None,
) -> list[str]:
    """Return active Binance USD-M perpetual futures symbols."""

    info = _resolve_exchange_info(client=client, exchange_info=exchange_info)
    symbols = [
        str(item["symbol"]).upper()
        for item in _iter_um_perpetual_symbol_items(info, quote_assets=quote_assets)
    ]
    return sorted(dict.fromkeys(symbols))


def get_um_perpetual_symbol_info(
    *,
    client: Optional[BinanceKlineClient] = None,
    exchange_info: Optional[dict[str, Any]] = None,
    quote_assets: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Return metadata for active Binance USD-M perpetual futures symbols."""

    info = _resolve_exchange_info(client=client, exchange_info=exchange_info)
    rows = []
    for item in _iter_um_perpetual_symbol_items(info, quote_assets=quote_assets):
        rows.append({column: item.get(column) for column in UM_PERPETUAL_INFO_COLUMNS})

    df = pd.DataFrame(rows, columns=UM_PERPETUAL_INFO_COLUMNS)
    if df.empty:
        return df

    for col in ("onboardDate", "deliveryDate"):
        df[col] = pd.to_datetime(df[col], unit="ms", utc=True, errors="coerce")
    return df.sort_values("symbol").reset_index(drop=True)


def _resolve_exchange_info(
    *,
    client: Optional[BinanceKlineClient],
    exchange_info: Optional[dict[str, Any]],
) -> dict[str, Any]:
    if exchange_info is not None:
        return exchange_info
    if client is None:
        client = BinanceKlineClient()
    return client.fetch_exchange_info()


def _iter_um_perpetual_symbol_items(
    exchange_info: dict[str, Any],
    *,
    quote_assets: Optional[Iterable[str]],
) -> list[dict[str, Any]]:
    if not isinstance(exchange_info, dict):
        raise BinanceResponseError("exchangeInfo response must be a JSON object")
    raw_symbols = exchange_info.get("symbols")
    if not isinstance(raw_symbols, list):
        raise BinanceResponseError("exchangeInfo response missing symbols")

    quote_filter = None
    if quote_assets is not None:
        quote_filter = {str(asset).upper() for asset in quote_assets}

    out = []
    for item in raw_symbols:
        if not isinstance(item, dict):
            continue
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("status") != "TRADING":
            continue
        symbol = item.get("symbol")
        if not symbol:
            continue
        if quote_filter is not None and str(item.get("quoteAsset", "")).upper() not in quote_filter:
            continue
        out.append(item)
    return out
