from __future__ import annotations

import unittest

import pandas as pd

from binance_klines_data_fetch.errors import BinanceResponseError
from binance_klines_data_fetch.symbols import (
    get_um_perpetual_symbol_info,
    get_um_perpetual_symbols,
)


EXCHANGE_INFO = {
    "symbols": [
        {
            "symbol": "BTCUSDT",
            "pair": "BTCUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "onboardDate": 1_600_000_000_000,
            "deliveryDate": 4_102_444_800_000,
        },
        {
            "symbol": "ETHUSDT",
            "pair": "ETHUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "ETH",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "onboardDate": 1_600_000_000_000,
            "deliveryDate": 4_102_444_800_000,
        },
        {
            "symbol": "BTCUSDC",
            "pair": "BTCUSDC",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDC",
            "marginAsset": "USDC",
            "onboardDate": 1_700_000_000_000,
            "deliveryDate": 4_102_444_800_000,
        },
        {
            "symbol": "BTCUSDT_260327",
            "pair": "BTCUSDT",
            "contractType": "CURRENT_QUARTER",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
        },
        {
            "symbol": "OLDUSDT",
            "pair": "OLDUSDT",
            "contractType": "PERPETUAL",
            "status": "BREAK",
            "baseAsset": "OLD",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
        },
    ]
}


class SymbolsTests(unittest.TestCase):
    def test_get_um_perpetual_symbols_filters_contract_type_and_status(self):
        symbols = get_um_perpetual_symbols(exchange_info=EXCHANGE_INFO)

        self.assertEqual(symbols, ["BTCUSDC", "BTCUSDT", "ETHUSDT"])

    def test_get_um_perpetual_symbols_filters_quote_assets(self):
        symbols = get_um_perpetual_symbols(exchange_info=EXCHANGE_INFO, quote_assets=["USDT"])

        self.assertEqual(symbols, ["BTCUSDT", "ETHUSDT"])

    def test_get_um_perpetual_symbol_info_returns_metadata_frame(self):
        df = get_um_perpetual_symbol_info(exchange_info=EXCHANGE_INFO, quote_assets=["USDT"])

        self.assertEqual(df["symbol"].tolist(), ["BTCUSDT", "ETHUSDT"])
        self.assertIn("contractType", df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df["onboardDate"]))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df["deliveryDate"]))

    def test_malformed_exchange_info_raises_response_error(self):
        with self.assertRaises(BinanceResponseError):
            get_um_perpetual_symbols(exchange_info={"symbols": {}})


if __name__ == "__main__":
    unittest.main()
