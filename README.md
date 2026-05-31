# Binance Klines Data Fetch

Independent Python module for fetching Binance USD-M Futures closed 1-minute klines through REST API.

## What It Does

- Fetches recent `1m` klines for one symbol from `https://fapi.binance.com`.
- Uses `GET /fapi/v1/klines`.
- Excludes the current unfinished candle by using Binance server time from `GET /fapi/v1/time`.
- Returns a pandas `DataFrame`.
- Can keep a rolling cache updated in a background thread.

## Install Dependencies

```bash
pip install -r requirements.txt
```

The current local `.venv` already contains `requests` and `pandas`, so tests can run without installing anything.

## One-Shot Fetch

```python
from binance_klines_data_fetch import fetch_recent_closed_1m_klines

df = fetch_recent_closed_1m_klines("BTCUSDT", 100)
print(df.tail())
```

## Background Updating Cache

```python
from binance_klines_data_fetch import BinanceKlineService

service = BinanceKlineService(symbol="BTCUSDT", window_size=500)
service.start(block_until_ready=True)

try:
    latest_100 = service.get_recent(100)
    print(latest_100.tail())
finally:
    service.stop()
```

`get_recent(n)` returns the latest cached closed candles. The background thread polls REST every 2 seconds by default and only fetches missing tail candles after bootstrap.

## Multi-Symbol Background Cache

```python
from binance_klines_data_fetch import MultiSymbolKlineService

service = MultiSymbolKlineService(
    symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    window_size=500,
    max_workers=4,
)
service.start(block_until_ready=True)

try:
    btc = service.get_recent("BTCUSDT", 100)
    all_latest = service.get_all_recent(100)
    print(btc.tail())
    print(all_latest.keys())
finally:
    service.stop()
```

The multi-symbol service uses a shared process-local `WeightedRateLimiter`. For Binance USD-M `/fapi/v1/klines`, request weight is based on `limit`: `<100 => 1`, `100..499 => 2`, `500..1000 => 5`, and `>1000 => 10`. The Binance single-request `limit` max is still 1500; larger windows are paginated.

## Full USD-M Perpetual Market

```python
from binance_klines_data_fetch import (
    MultiSymbolKlineService,
    get_um_futures_classification_maps,
    get_um_perpetual_symbols,
    get_um_perpetual_symbol_info,
)

symbols = get_um_perpetual_symbols()
info = get_um_perpetual_symbol_info()
classifications = get_um_futures_classification_maps()

print(len(symbols))
print(info[["symbol", "baseAsset", "quoteAsset"]].head())
print(classifications["underlyingType"].keys())
print(classifications["underlyingSubType"].get("AI", [])[:10])

service = MultiSymbolKlineService.for_um_perpetual_market(
    window_size=100,
    max_workers=8,
)
service.start(block_until_ready=True, timeout=180)

try:
    latest = service.get_all_recent(1)
    print(latest.keys())
finally:
    service.stop()
```

The full-market helpers read Binance USD-M `/fapi/v1/exchangeInfo` and keep only symbols where `contractType == "PERPETUAL"` and `status == "TRADING"`.
The classification helper uses the same endpoint and groups `TRADING` `PERPETUAL` and `TRADIFI_PERPETUAL` contracts by Binance `underlyingType` and `underlyingSubType`; untagged subtype values are grouped under `UNKNOWN`.

## Futures Partial Depth Background Cache

```python
from binance_klines_data_fetch import BinanceDepthConfig, BinanceFuturesDepthService

service = BinanceFuturesDepthService(
    BinanceDepthConfig(
        symbols=["BTCUSDT", "ETHUSDT"],
        levels=5,
        speed_ms=100,
    )
)
service.start(block_until_ready=True)

try:
    snapshot = service.get_latest("BTCUSDT")
    if snapshot is not None and not snapshot.is_stale and not snapshot.sequence_gap:
        bid1 = snapshot.bids[0]
        bid2 = snapshot.bids[1]
        ask1 = snapshot.asks[0]
        ask2 = snapshot.asks[1]
        print(bid1, bid2, ask1, ask2)
finally:
    service.stop()
```

The depth service uses Binance USD-M Futures partial book depth streams, not diff-depth local order book reconstruction. Supported `levels` values are `5`, `10`, and `20`; supported `speed_ms` values are `100`, `250`, and `500`. `speed_ms=250` maps to the no-suffix stream name such as `btcusdt@depth5`. The service keeps only the latest in-memory snapshot per symbol and marks snapshots stale on disconnect, timeout, or stop. It does not write CSV, Parquet, database rows, or any periodic sampler output.

## Returned DataFrame

- Index: UTC `DatetimeIndex`, name `Open_Time`, ascending.
- Columns: `Open`, `High`, `Low`, `Close`, `Volume`, `Close_Time`, `Quote_Asset_Volume`, `Number_of_Trades`, `Taker_Buy_Base_Asset_Volume`, `Taker_Buy_Quote_Asset_Volume`.
- Price and volume columns are numeric.

## Tests

```bash
python -m unittest discover -v
```

Automated tests mock HTTP responses and do not call Binance.
