from __future__ import annotations

import unittest

import pandas as pd

from binance_klines_data_fetch.errors import BinanceResponseError
from binance_klines_data_fetch.symbols import (
    get_um_futures_classification_maps,
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

CLASSIFICATION_EXCHANGE_INFO = {
    "symbols": [
        {
            "symbol": "ethusdt",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "quoteAsset": "USDT",
            "underlyingType": "COIN",
            "underlyingSubType": ["Layer-1"],
        },
        {
            "symbol": "TAOUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "quoteAsset": "USDT",
            "underlyingType": "COIN",
            "underlyingSubType": ["AI", "Alpha"],
        },
        {
            "symbol": "BTCUSDC",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "quoteAsset": "USDC",
            "underlyingType": "COIN",
            "underlyingSubType": ["USDC"],
        },
        {
            "symbol": "OPENAIUSDT",
            "contractType": "TRADIFI_PERPETUAL",
            "status": "TRADING",
            "quoteAsset": "USDT",
            "underlyingType": "PREMARKET",
            "underlyingSubType": ["Pre-IPO", "TradFi"],
        },
        {
            "symbol": "NVDAUSDT",
            "contractType": "TRADIFI_PERPETUAL",
            "status": "TRADING",
            "quoteAsset": "USDT",
            "underlyingType": "EQUITY",
            "underlyingSubType": ["TradFi"],
        },
        {
            "symbol": "UNTAGGEDUSDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
            "quoteAsset": "USDT",
            "underlyingType": "",
            "underlyingSubType": [],
        },
        {
            "symbol": "BTCUSDT_260327",
            "contractType": "CURRENT_QUARTER",
            "status": "TRADING",
            "quoteAsset": "USDT",
            "underlyingType": "COIN",
            "underlyingSubType": ["PoW"],
        },
        {
            "symbol": "OLDUSDT",
            "contractType": "PERPETUAL",
            "status": "BREAK",
            "quoteAsset": "USDT",
            "underlyingType": "COIN",
            "underlyingSubType": ["Meme"],
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

    def test_get_um_futures_classification_maps_groups_types_and_subtypes(self):
        maps = get_um_futures_classification_maps(exchange_info=CLASSIFICATION_EXCHANGE_INFO)

        self.assertEqual(
            maps["underlyingType"],
            {
                "COIN": ["BTCUSDC", "ETHUSDT", "TAOUSDT"],
                "EQUITY": ["NVDAUSDT"],
                "PREMARKET": ["OPENAIUSDT"],
                "UNKNOWN": ["UNTAGGEDUSDT"],
            },
        )
        self.assertEqual(maps["underlyingSubType"]["AI"], ["TAOUSDT"])
        self.assertEqual(maps["underlyingSubType"]["Alpha"], ["TAOUSDT"])
        self.assertEqual(maps["underlyingSubType"]["Layer-1"], ["ETHUSDT"])
        self.assertEqual(maps["underlyingSubType"]["Pre-IPO"], ["OPENAIUSDT"])
        self.assertEqual(maps["underlyingSubType"]["TradFi"], ["NVDAUSDT", "OPENAIUSDT"])
        self.assertEqual(maps["underlyingSubType"]["UNKNOWN"], ["UNTAGGEDUSDT"])
        self.assertNotIn("PoW", maps["underlyingSubType"])
        self.assertNotIn("Meme", maps["underlyingSubType"])

    def test_get_um_futures_classification_maps_filters_quote_assets(self):
        maps = get_um_futures_classification_maps(
            exchange_info=CLASSIFICATION_EXCHANGE_INFO,
            quote_assets=["USDT"],
        )

        self.assertEqual(maps["underlyingType"]["COIN"], ["ETHUSDT", "TAOUSDT"])
        self.assertNotIn("USDC", maps["underlyingSubType"])

    def test_get_um_futures_classification_maps_can_limit_contract_types(self):
        maps = get_um_futures_classification_maps(
            exchange_info=CLASSIFICATION_EXCHANGE_INFO,
            contract_types=("PERPETUAL",),
        )

        self.assertNotIn("EQUITY", maps["underlyingType"])
        self.assertNotIn("PREMARKET", maps["underlyingType"])
        self.assertNotIn("TradFi", maps["underlyingSubType"])

    def test_get_um_futures_classification_maps_returns_stable_order(self):
        maps = get_um_futures_classification_maps(exchange_info=CLASSIFICATION_EXCHANGE_INFO)

        self.assertEqual(list(maps["underlyingType"]), sorted(maps["underlyingType"]))
        self.assertEqual(list(maps["underlyingSubType"]), sorted(maps["underlyingSubType"]))
        self.assertEqual(
            maps["underlyingType"]["COIN"],
            sorted(maps["underlyingType"]["COIN"]),
        )


if __name__ == "__main__":
    unittest.main()
