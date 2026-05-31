from __future__ import annotations

import json
import unittest
from decimal import Decimal

from binance_klines_data_fetch import (
    BinanceDepthConfig,
    BinanceDepthResponseError,
    BinanceFuturesDepthService,
    DepthSnapshot,
)


def make_depth_payload(
    *,
    symbol: str = "BTCUSDT",
    event_time_ms: int = 1_700_000_000_001,
    transaction_time_ms: int = 1_700_000_000_000,
    first_update_id: int = 10,
    final_update_id: int = 12,
    previous_final_update_id: int = 9,
):
    return {
        "e": "depthUpdate",
        "E": event_time_ms,
        "T": transaction_time_ms,
        "s": symbol,
        "U": first_update_id,
        "u": final_update_id,
        "pu": previous_final_update_id,
        "b": [
            ["100.0", "1.5"],
            ["101.0", "2.5"],
            ["99.5", "3.0"],
        ],
        "a": [
            ["102.0", "1.0"],
            ["101.5", "4.0"],
            ["103.0", "2.0"],
        ],
    }


class BinanceFuturesDepthServiceTests(unittest.TestCase):
    def test_build_url_maps_speed_suffixes_and_normalizes_symbols(self):
        service = BinanceFuturesDepthService(
            BinanceDepthConfig(symbols=["btcusdt", "ETHUSDT", "BTCUSDT"], levels=5, speed_ms=100)
        )
        self.assertEqual(tuple(service.config.symbols), ("BTCUSDT", "ETHUSDT"))
        self.assertEqual(
            service.build_url(),
            "wss://fstream.binance.com/public/stream?streams=btcusdt@depth5@100ms/ethusdt@depth5@100ms",
        )

        url_250 = BinanceFuturesDepthService(
            BinanceDepthConfig(symbols=["BTCUSDT"], levels=10, speed_ms=250)
        ).build_url()
        self.assertTrue(url_250.endswith("streams=btcusdt@depth10"))

        url_500 = BinanceFuturesDepthService(
            BinanceDepthConfig(symbols=["BTCUSDT"], levels=20, speed_ms=500)
        ).build_url()
        self.assertTrue(url_500.endswith("streams=btcusdt@depth20@500ms"))

    def test_config_rejects_invalid_levels_speeds_and_symbol_counts(self):
        with self.assertRaises(ValueError):
            BinanceDepthConfig(symbols=["BTCUSDT"], levels=7)
        with self.assertRaises(ValueError):
            BinanceDepthConfig(symbols=["BTCUSDT"], speed_ms=200)
        with self.assertRaises(ValueError):
            BinanceDepthConfig(symbols=[])
        with self.assertRaises(ValueError):
            BinanceDepthConfig(symbols=[f"SYM{i}" for i in range(1025)])

    def test_parse_combined_payload_sorts_levels_and_uses_decimal(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))
        payload = {"stream": "btcusdt@depth5@100ms", "data": make_depth_payload()}

        snapshot = service._parse_depth_message(json.dumps(payload))

        self.assertIsInstance(snapshot, DepthSnapshot)
        self.assertEqual(snapshot.symbol, "BTCUSDT")
        self.assertEqual([level.price for level in snapshot.bids], [Decimal("101.0"), Decimal("100.0"), Decimal("99.5")])
        self.assertEqual([level.price for level in snapshot.asks], [Decimal("101.5"), Decimal("102.0"), Decimal("103.0")])
        self.assertEqual(snapshot.bids[0].qty, Decimal("2.5"))
        self.assertEqual(snapshot.asks[0].qty, Decimal("4.0"))
        self.assertEqual(snapshot.spread, Decimal("0.5"))
        self.assertEqual(snapshot.mid_price, Decimal("101.25"))
        self.assertEqual(snapshot.spread_bps, Decimal("0.5") / Decimal("101.25") * Decimal("10000"))
        self.assertFalse(snapshot.is_stale)
        self.assertFalse(snapshot.sequence_gap)

    def test_parse_raw_payload_without_combined_wrapper(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))

        snapshot = service._parse_depth_message(json.dumps(make_depth_payload()))

        self.assertEqual(snapshot.first_update_id, 10)
        self.assertEqual(snapshot.final_update_id, 12)
        self.assertEqual(snapshot.previous_final_update_id, 9)

    def test_malformed_depth_payload_raises_response_error(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))

        with self.assertRaises(BinanceDepthResponseError):
            service._parse_depth_message(json.dumps({"data": {"s": "BTCUSDT"}}))
        with self.assertRaises(BinanceDepthResponseError):
            service._parse_depth_message("{not-json")

    def test_connected_status_controls_readiness_without_snapshots(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT", "ETHUSDT"], levels=5))

        self.assertFalse(service.is_ready)

        service._record_connected()

        self.assertTrue(service.is_ready)
        self.assertEqual(service.get_all_latest(), {})

        service._set_connected(False)

        self.assertFalse(service.is_ready)

    def test_sequence_gap_sets_flag_until_next_contiguous_message(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))
        service._record_connected()

        service._handle_raw_message(json.dumps(make_depth_payload(final_update_id=12, previous_final_update_id=9)))
        first = service.get_latest("BTCUSDT")
        self.assertIsNotNone(first)
        self.assertFalse(first.sequence_gap)
        self.assertTrue(service.is_ready)

        service._handle_raw_message(
            json.dumps(make_depth_payload(first_update_id=13, final_update_id=14, previous_final_update_id=11))
        )
        second = service.get_latest("BTCUSDT")
        self.assertIsNotNone(second)
        self.assertTrue(second.sequence_gap)
        self.assertFalse(second.is_stale)
        self.assertTrue(service.is_ready)

        service._handle_raw_message(
            json.dumps(make_depth_payload(first_update_id=15, final_update_id=16, previous_final_update_id=14))
        )
        third = service.get_latest("BTCUSDT")
        self.assertIsNotNone(third)
        self.assertFalse(third.sequence_gap)
        self.assertTrue(service.is_ready)

    def test_stale_marking_keeps_last_snapshot_but_clears_readiness(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))
        service._record_connected()
        service._handle_raw_message(json.dumps(make_depth_payload()))

        service._mark_all_stale("connection_closed")

        snapshot = service.get_latest("BTCUSDT")
        self.assertIsNotNone(snapshot)
        self.assertTrue(snapshot.is_stale)
        self.assertEqual(snapshot.stale_reason, "connection_closed")
        self.assertIsNotNone(snapshot.stale_since_ms)
        self.assertFalse(service.is_ready)

    def test_latest_depth_table_returns_dict_copy(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))
        service._handle_raw_message(json.dumps(make_depth_payload()))

        table = service.latest_depth_table
        table.clear()

        self.assertIsNotNone(service.get_latest("BTCUSDT"))


if __name__ == "__main__":
    unittest.main()
