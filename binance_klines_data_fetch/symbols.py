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

DEFAULT_CLASSIFICATION_CONTRACT_TYPES = ("PERPETUAL", "TRADIFI_PERPETUAL")
DEFAULT_UNKNOWN_CLASSIFICATION_LABEL = "UNKNOWN"


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


def get_um_futures_classification_maps(
    *,
    client: Optional[BinanceKlineClient] = None,
    exchange_info: Optional[dict[str, Any]] = None,
    quote_assets: Optional[Iterable[str]] = None,
    contract_types: Optional[Iterable[str]] = DEFAULT_CLASSIFICATION_CONTRACT_TYPES,
    status: Optional[str] = "TRADING",
    unknown_label: str = DEFAULT_UNKNOWN_CLASSIFICATION_LABEL,
) -> dict[str, dict[str, list[str]]]:
    """Return Binance USD-M futures symbols grouped by official classification fields."""

    info = _resolve_exchange_info(client=client, exchange_info=exchange_info)
    unknown = _normalize_label(unknown_label) or DEFAULT_UNKNOWN_CLASSIFICATION_LABEL
    type_buckets: dict[str, set[str]] = {}
    subtype_buckets: dict[str, set[str]] = {}

    for item in _iter_um_symbol_items(
        info,
        quote_assets=quote_assets,
        contract_types=contract_types,
        status=status,
    ):
        symbol = str(item["symbol"]).upper()
        type_label = _normalize_label(item.get("underlyingType")) or unknown
        type_buckets.setdefault(type_label, set()).add(symbol)

        for subtype_label in _normalize_label_list(item.get("underlyingSubType"), unknown):
            subtype_buckets.setdefault(subtype_label, set()).add(symbol)

    return {
        "underlyingType": _sorted_bucket_map(type_buckets),
        "underlyingSubType": _sorted_bucket_map(subtype_buckets),
    }


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
    return _iter_um_symbol_items(
        exchange_info,
        quote_assets=quote_assets,
        contract_types=("PERPETUAL",),
        status="TRADING",
    )


def _iter_um_symbol_items(
    exchange_info: dict[str, Any],
    *,
    quote_assets: Optional[Iterable[str]],
    contract_types: Optional[Iterable[str]],
    status: Optional[str],
) -> list[dict[str, Any]]:
    if not isinstance(exchange_info, dict):
        raise BinanceResponseError("exchangeInfo response must be a JSON object")
    raw_symbols = exchange_info.get("symbols")
    if not isinstance(raw_symbols, list):
        raise BinanceResponseError("exchangeInfo response missing symbols")

    quote_filter = _normalize_filter_values(quote_assets)
    contract_type_filter = _normalize_filter_values(contract_types)
    status_filter = _normalize_label(status)
    if status_filter is not None:
        status_filter = status_filter.upper()

    out = []
    for item in raw_symbols:
        if not isinstance(item, dict):
            continue
        contract_type = str(item.get("contractType", "")).upper()
        if contract_type_filter is not None and contract_type not in contract_type_filter:
            continue
        item_status = str(item.get("status", "")).upper()
        if status_filter is not None and item_status != status_filter:
            continue
        symbol = item.get("symbol")
        if not symbol:
            continue
        if quote_filter is not None and str(item.get("quoteAsset", "")).upper() not in quote_filter:
            continue
        out.append(item)
    return out


def _normalize_filter_values(values: Optional[Iterable[str]]) -> Optional[set[str]]:
    if values is None:
        return None
    if isinstance(values, str):
        raw_values = [values]
    else:
        raw_values = values
    return {label.upper() for value in raw_values if (label := _normalize_label(value))}


def _normalize_label(value: Any) -> Optional[str]:
    if value is None:
        return None
    label = str(value).strip()
    return label or None


def _normalize_label_list(value: Any, unknown_label: str) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        labels = [_normalize_label(item) for item in value]
    else:
        labels = [_normalize_label(value)]
    normalized = sorted(dict.fromkeys(label for label in labels if label))
    return normalized or [unknown_label]


def _sorted_bucket_map(buckets: dict[str, set[str]]) -> dict[str, list[str]]:
    return {key: sorted(symbols) for key, symbols in sorted(buckets.items())}
