from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from binance_klines_data_fetch.errors import BinanceResponseError
from binance_klines_data_fetch.options import (
    BinanceOptionsClient,
    build_options_combined_stream_urls,
    build_options_depth_streams,
    filter_options,
    get_option_universe,
    get_trading_option_symbols,
    parse_option_symbol_record,
    select_near_atm_options,
    select_nearest_expiries,
)


def utc_ms(year: int, month: int, day: int, hour: int = 8) -> int:
    return int(datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp() * 1000)


def make_option_record(
    *,
    symbol: str = "BTC-251226-110000-C",
    side: str = "CALL",
    strike_price: str = "110000",
    underlying: str = "BTCUSDT",
    quote_asset: str = "USDT",
    expiry_ms: int = utc_ms(2025, 12, 26),
    status: str = "TRADING",
):
    return {
        "expiryDate": expiry_ms,
        "filters": [
            {
                "filterType": "PRICE_FILTER",
                "minPrice": "0.02",
                "maxPrice": "80000.01",
                "tickSize": "0.01",
            },
            {
                "filterType": "LOT_SIZE",
                "minQty": "0.01",
                "maxQty": "100",
                "stepSize": "0.01",
            },
        ],
        "symbol": symbol,
        "side": side,
        "strikePrice": strike_price,
        "underlying": underlying,
        "unit": 1,
        "minQty": "0.01",
        "maxQty": "100",
        "priceScale": 2,
        "quantityScale": 2,
        "quoteAsset": quote_asset,
        "status": status,
    }


EXCHANGE_INFO = {
    "optionSymbols": [
        make_option_record(symbol="BTC-251226-110000-C", side="CALL", strike_price="110000"),
        make_option_record(symbol="BTC-251226-110000-P", side="PUT", strike_price="110000"),
        make_option_record(symbol="BTC-251226-100000-C", side="CALL", strike_price="100000"),
        make_option_record(
            symbol="BTC-260327-120000-C",
            side="CALL",
            strike_price="120000",
            expiry_ms=utc_ms(2026, 3, 27),
        ),
        make_option_record(symbol="ETH-251226-4000-C", side="CALL", strike_price="4000", underlying="ETHUSDT"),
        make_option_record(symbol="BTC-251226-90000-P", side="PUT", strike_price="90000", status="BREAK"),
    ]
}


class FakeResponse:
    def __init__(self, status_code, payload, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None, proxies=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout, "proxies": proxies})
        if not self.responses:
            raise AssertionError("unexpected HTTP call")
        return self.responses.pop(0)


class OptionsTests(unittest.TestCase):
    def test_parse_option_symbol_record_validates_and_normalizes_fields(self):
        opt = parse_option_symbol_record(make_option_record())

        self.assertEqual(opt.symbol, "BTC-251226-110000-C")
        self.assertEqual(opt.underlying, "BTCUSDT")
        self.assertEqual(opt.base_asset, "BTC")
        self.assertEqual(opt.quote_asset, "USDT")
        self.assertEqual(opt.expiry_yymmdd, "251226")
        self.assertEqual(opt.expiry_date, "2025-12-26")
        self.assertEqual(opt.expiry, datetime(2025, 12, 26, 8, tzinfo=timezone.utc))
        self.assertEqual(opt.strike, Decimal("110000"))
        self.assertEqual(opt.option_type, "CALL")
        self.assertEqual(opt.cp, "C")
        self.assertEqual(opt.status, "TRADING")
        self.assertEqual(opt.price_scale, 2)
        self.assertEqual(opt.quantity_scale, 2)
        self.assertEqual(opt.min_qty, Decimal("0.01"))
        self.assertEqual(opt.max_qty, Decimal("100"))
        self.assertEqual(opt.unit, Decimal("1"))
        self.assertEqual(opt.tick_size, Decimal("0.01"))
        self.assertEqual(opt.step_size, Decimal("0.01"))

    def test_parse_option_symbol_record_rejects_bad_symbol_side_and_strike(self):
        with self.assertRaises(BinanceResponseError):
            parse_option_symbol_record(make_option_record(symbol="BTCUSDT"))
        with self.assertRaises(BinanceResponseError):
            parse_option_symbol_record(make_option_record(symbol="BTC-251226-110000-C", side="PUT"))
        with self.assertRaises(BinanceResponseError):
            parse_option_symbol_record(make_option_record(symbol="BTC-251226-110000-C", strike_price="100000"))

    def test_get_option_universe_validates_response_shape(self):
        universe = get_option_universe(exchange_info=EXCHANGE_INFO)

        self.assertEqual(len(universe), 6)
        self.assertEqual(universe[0].symbol, "BTC-251226-110000-C")

        with self.assertRaises(BinanceResponseError):
            get_option_universe(exchange_info={"optionSymbols": {}})
        with self.assertRaises(BinanceResponseError):
            get_option_universe(exchange_info={})

    def test_filter_options_filters_status_underlying_type_expiry_and_strike(self):
        universe = get_option_universe(exchange_info=EXCHANGE_INFO)
        btc_trading = filter_options(universe, underlying="btcusdt")

        self.assertEqual(
            [opt.symbol for opt in btc_trading],
            [
                "BTC-251226-100000-C",
                "BTC-251226-110000-C",
                "BTC-251226-110000-P",
                "BTC-260327-120000-C",
            ],
        )

        calls = filter_options(btc_trading, option_type="call", min_strike=Decimal("105000"))
        self.assertEqual([opt.symbol for opt in calls], ["BTC-251226-110000-C", "BTC-260327-120000-C"])

        expiry = datetime(2025, 12, 26, 8, tzinfo=timezone.utc)
        puts = filter_options(btc_trading, option_type="PUT", expiry=expiry)
        self.assertEqual([opt.symbol for opt in puts], ["BTC-251226-110000-P"])

        all_statuses = filter_options(universe, underlying="BTCUSDT", status=None, max_strike=Decimal("90000"))
        self.assertEqual([opt.symbol for opt in all_statuses], ["BTC-251226-90000-P"])

    def test_get_trading_option_symbols_returns_symbol_strings(self):
        symbols = get_trading_option_symbols(exchange_info=EXCHANGE_INFO, underlying="BTCUSDT")

        self.assertEqual(
            symbols,
            [
                "BTC-251226-100000-C",
                "BTC-251226-110000-C",
                "BTC-251226-110000-P",
                "BTC-260327-120000-C",
            ],
        )

    def test_select_expiries_and_near_atm_options(self):
        universe = get_option_universe(exchange_info=EXCHANGE_INFO)
        btc_trading = filter_options(universe, underlying="BTCUSDT")

        expiries = select_nearest_expiries(btc_trading, count=2)
        self.assertEqual(expiries[0], datetime(2025, 12, 26, 8, tzinfo=timezone.utc))
        self.assertEqual(expiries[1], datetime(2026, 3, 27, 8, tzinfo=timezone.utc))

        nearest_expiry_options = filter_options(btc_trading, expiry=expiries[0])
        near_atm = select_near_atm_options(nearest_expiry_options, spot=Decimal("108000"), n_strikes_each_side=0)
        self.assertEqual([opt.symbol for opt in near_atm], ["BTC-251226-110000-C", "BTC-251226-110000-P"])

    def test_build_options_depth_streams_validates_options_depth_rules(self):
        universe = get_option_universe(exchange_info=EXCHANGE_INFO)

        streams = build_options_depth_streams(
            [universe[0], "btc-251226-110000-p", "BTC-251226-110000-P"],
            levels=5,
            speed_ms=100,
        )

        self.assertEqual(
            streams,
            [
                "btc-251226-110000-c@depth5@100ms",
                "btc-251226-110000-p@depth5@100ms",
            ],
        )

        with self.assertRaises(ValueError):
            build_options_depth_streams(["BTC-251226-110000-C"], levels=7)
        with self.assertRaises(ValueError):
            build_options_depth_streams(["BTC-251226-110000-C"], speed_ms=250)

    def test_build_options_combined_stream_urls_chunks_at_200_streams(self):
        streams = [f"btc-251226-{index}-c@depth5@100ms" for index in range(201)]

        urls = build_options_combined_stream_urls(streams)

        self.assertEqual(len(urls), 2)
        self.assertTrue(urls[0].startswith("wss://fstream.binance.com/public/stream?streams="))
        self.assertEqual(urls[0].count("@depth5@100ms"), 200)
        self.assertEqual(urls[1].count("@depth5@100ms"), 1)

        with self.assertRaises(ValueError):
            build_options_combined_stream_urls(streams, max_streams_per_connection=201)

    def test_options_client_fetches_exchange_info_from_eapi(self):
        session = FakeSession([FakeResponse(200, {"optionSymbols": []})])
        client = BinanceOptionsClient(session=session, retries=0, retry_backoff_seconds=0)

        info = client.fetch_exchange_info()

        self.assertEqual(info, {"optionSymbols": []})
        self.assertEqual(session.calls[0]["url"], "https://eapi.binance.com/eapi/v1/exchangeInfo")
        self.assertEqual(session.calls[0]["params"], {})


if __name__ == "__main__":
    unittest.main()
