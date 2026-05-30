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

## Returned DataFrame

- Index: UTC `DatetimeIndex`, name `Open_Time`, ascending.
- Columns: `Open`, `High`, `Low`, `Close`, `Volume`, `Close_Time`, `Quote_Asset_Volume`, `Number_of_Trades`, `Taker_Buy_Base_Asset_Volume`, `Taker_Buy_Quote_Asset_Volume`.
- Price and volume columns are numeric.

## Tests

```bash
python -m unittest discover -v
```

Automated tests mock HTTP responses and do not call Binance.
