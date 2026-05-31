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

The depth service uses Binance USD-M Futures partial book depth streams, not diff-depth local order book reconstruction. Supported `levels` values are `5`, `10`, and `20`; supported `speed_ms` values are `100`, `250`, and `500`. `speed_ms=250` maps to the no-suffix stream name such as `btcusdt@depth5`. The service keeps only the latest in-memory snapshot per symbol and marks snapshots stale on disconnect, timeout, or stop. `status().ready` is a connection-level flag: it means the WebSocket is connected and has no current connection error. Always check `get_latest(symbol)` and the snapshot-level `is_stale` / `sequence_gap` flags before using a specific symbol. The service does not write CSV, Parquet, database rows, or any periodic sampler output.

## Binance Options Symbols

```python
from decimal import Decimal

from binance_klines_data_fetch import (
    build_options_combined_stream_urls,
    build_options_depth_streams,
    filter_options,
    get_option_universe,
    select_near_atm_options,
    select_nearest_expiries,
)

universe = get_option_universe()

btc_trading = filter_options(
    universe,
    underlying="BTCUSDT",
    status="TRADING",
)
nearest_expiry = select_nearest_expiries(btc_trading, count=1)[0]
nearest_expiry_options = filter_options(btc_trading, expiry=nearest_expiry)
selected = select_near_atm_options(
    nearest_expiry_options,
    spot=Decimal("105000"),
    n_strikes_each_side=5,
)

streams = build_options_depth_streams(selected, levels=5, speed_ms=100)
urls = build_options_combined_stream_urls(streams)

print([opt.symbol for opt in selected][:5])
print(urls[0])
```

Options symbol discovery uses Binance Options `GET /eapi/v1/exchangeInfo` from `https://eapi.binance.com` and reads official `optionSymbols` fields such as `symbol`, `expiryDate`, `strikePrice`, `side`, `underlying`, `status`, `priceScale`, `quantityScale`, and filters. Symbol strings follow the Binance format `<BASE>-<YYMMDD>-<STRIKE>-<C|P>`, for example `BTC-251226-110000-C`, but production code should not hand-build symbols. Fetch the official universe, keep `status == "TRADING"`, then construct Options WebSocket depth streams as `{symbol.lower()}@depth5@100ms`.

Options partial depth supports `levels` values `5`, `10`, and `20`, and `speed_ms` values `100` and `500`. Options combined stream URLs use `wss://fstream.binance.com/public/stream?streams=...` and are split at Binance's 200 streams per connection limit.

## Binance Options Top-N Depth Background Cache

```python
from decimal import Decimal

from binance_klines_data_fetch import (
    BinanceOptionsDepthConfig,
    BinanceOptionsDepthService,
    filter_options,
    get_option_universe,
    select_near_atm_options,
    select_nearest_expiries,
)

universe = get_option_universe()
btc_trading = filter_options(universe, underlying="BTCUSDT", status="TRADING")
nearest_expiry = select_nearest_expiries(btc_trading, count=1)[0]
near_expiry = filter_options(btc_trading, expiry=nearest_expiry)
selected = select_near_atm_options(
    near_expiry,
    spot=Decimal("105000"),
    n_strikes_each_side=2,
)

service = BinanceOptionsDepthService(
    BinanceOptionsDepthConfig(
        symbols=selected,
        levels=5,
        speed_ms=100,
    )
)
service.start(block_until_ready=True)

try:
    snapshot = service.get_latest(selected[0].symbol)
    if snapshot is not None and not snapshot.is_stale and not snapshot.sequence_gap:
        bid1 = snapshot.bids[0] if len(snapshot.bids) > 0 else None
        bid2 = snapshot.bids[1] if len(snapshot.bids) > 1 else None
        ask1 = snapshot.asks[0] if len(snapshot.asks) > 0 else None
        ask2 = snapshot.asks[1] if len(snapshot.asks) > 1 else None
        print(snapshot.symbol, bid1, bid2, ask1, ask2, snapshot.depth_incomplete)
finally:
    service.stop()
```

The Options depth service uses Binance Options partial book depth streams. It stores the latest in-memory top-N snapshot per option symbol and tracks sequence gaps with `U/u/pu`. `status().ready` is a connection-level flag: it means the WebSocket is connected and has no current connection error. A thin or inactive option can still have no snapshot yet, so always check `get_latest(symbol)` before using a specific contract. Options order books are often thin, so `depth_incomplete=True` is normal; always check `len(snapshot.bids)` and `len(snapshot.asks)` before reading bid2 or ask2.

## Returned DataFrame

- Index: UTC `DatetimeIndex`, name `Open_Time`, ascending.
- Columns: `Open`, `High`, `Low`, `Close`, `Volume`, `Close_Time`, `Quote_Asset_Volume`, `Number_of_Trades`, `Taker_Buy_Base_Asset_Volume`, `Taker_Buy_Quote_Asset_Volume`.
- Price and volume columns are numeric.

## Tests

```bash
python -m unittest discover -v
```

Automated tests mock HTTP responses and do not call Binance.
