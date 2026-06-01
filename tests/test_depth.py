from __future__ import annotations

import json
import unittest
from dataclasses import replace
from decimal import Decimal
from unittest.mock import patch

import pandas as pd

from binance_klines_data_fetch import (
    BinanceDepthConfig,
    BinanceDepthResponseError,
    BinanceFuturesDepthService,
    DepthLevelQuote,
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

    def test_latest_level_quote_and_value_accessors(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))

        self.assertIsNone(service.get_latest_level_quote("BTCUSDT", level=1))

        service._handle_raw_message(json.dumps(make_depth_payload()))

        quote = service.get_latest_level_quote("btcusdt", level=2)

        self.assertIsInstance(quote, DepthLevelQuote)
        self.assertEqual(quote.symbol, "BTCUSDT")
        self.assertEqual(quote.level, 2)
        self.assertEqual(quote.bid_price, Decimal("100.0"))
        self.assertEqual(quote.bid_qty, Decimal("1.5"))
        self.assertEqual(quote.ask_price, Decimal("102.0"))
        self.assertEqual(quote.ask_qty, Decimal("1.0"))
        self.assertIsNone(quote.depth_incomplete)
        self.assertEqual(quote.value("bid", "price"), Decimal("100.0"))
        self.assertEqual(quote.value("ask", "qty"), Decimal("1.0"))
        self.assertEqual(
            service.get_latest_level_value("BTCUSDT", side="BID", level=2, field="PRICE"),
            Decimal("100.0"),
        )
        self.assertEqual(
            service.get_latest_level_value("BTCUSDT", side="ask", level=2, field="qty"),
            Decimal("1.0"),
        )

    def test_latest_level_accessors_filter_stale_gap_and_age(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))
        service._record_connected()
        service._handle_raw_message(json.dumps(make_depth_payload(final_update_id=12, previous_final_update_id=9)))

        self.assertIsNotNone(service.get_latest_level_quote("BTCUSDT"))

        service._mark_all_stale("connection_closed")

        self.assertIsNone(service.get_latest_level_quote("BTCUSDT"))
        stale_quote = service.get_latest_level_quote("BTCUSDT", require_not_stale=False)
        self.assertIsNotNone(stale_quote)
        self.assertTrue(stale_quote.is_stale)

        service._handle_raw_message(
            json.dumps(make_depth_payload(first_update_id=13, final_update_id=14, previous_final_update_id=11))
        )

        self.assertIsNotNone(service.get_latest_level_quote("BTCUSDT"))
        self.assertIsNone(service.get_latest_level_quote("BTCUSDT", require_sequence_continuity=True))

        snapshot = service.get_latest("BTCUSDT")
        self.assertIsNotNone(snapshot)
        service._snapshots["BTCUSDT"] = replace(snapshot, local_recv_time_ms=0, sequence_gap=False)

        self.assertIsNone(service.get_latest_level_quote("BTCUSDT", max_age_ms=1))

    def test_latest_level_accessors_validate_inputs(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))
        service._handle_raw_message(json.dumps(make_depth_payload()))

        with self.assertRaises(ValueError):
            service.get_latest_level_quote("BTCUSDT", level=0)
        with self.assertRaises(ValueError):
            service.get_latest_level_quote("BTCUSDT", level=6)
        with self.assertRaises(ValueError):
            service.get_latest_level_value("BTCUSDT", side="middle")
        with self.assertRaises(ValueError):
            service.get_latest_level_value("BTCUSDT", side="bid", field="size")
        with self.assertRaises(ValueError):
            service.get_latest_level_quote("BTCUSDT", max_age_ms=-1)

    def test_latest_depth_frame_keeps_all_symbols_and_uses_nan_for_missing_data(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT", "ETHUSDT"], levels=5))
        service._handle_raw_message(json.dumps(make_depth_payload(symbol="BTCUSDT")))

        frame = service.get_latest_depth_frame(levels=2)

        self.assertEqual(list(frame.index), ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(frame.index.name, "symbol")
        self.assertEqual(
            list(frame.columns),
            [
                "bid1",
                "bid1_qty",
                "ask1",
                "ask1_qty",
                "bid2",
                "bid2_qty",
                "ask2",
                "ask2_qty",
                "has_snapshot",
                "transaction_time_ms",
                "is_stale",
                "sequence_gap",
                "depth_incomplete",
            ],
        )
        self.assertEqual(frame.loc["BTCUSDT", "bid1"], Decimal("101.0"))
        self.assertEqual(frame.loc["BTCUSDT", "bid1_qty"], Decimal("2.5"))
        self.assertEqual(frame.loc["BTCUSDT", "ask2"], Decimal("102.0"))
        self.assertEqual(frame["bid1"].dtype, object)
        self.assertEqual(frame["bid1_qty"].dtype, object)
        self.assertEqual(frame["ask2"].dtype, object)
        self.assertTrue(frame.loc["BTCUSDT", "has_snapshot"])
        self.assertEqual(frame.loc["BTCUSDT", "transaction_time_ms"], 1_700_000_000_000)
        self.assertFalse(frame.loc["BTCUSDT", "is_stale"])
        self.assertFalse(frame.loc["BTCUSDT", "sequence_gap"])
        self.assertFalse(frame.loc["BTCUSDT", "depth_incomplete"])
        self.assertTrue(pd.isna(frame.loc["ETHUSDT", "bid1"]))
        self.assertTrue(pd.isna(frame.loc["ETHUSDT", "ask2_qty"]))
        self.assertFalse(frame.loc["ETHUSDT", "has_snapshot"])
        self.assertTrue(pd.isna(frame.loc["ETHUSDT", "transaction_time_ms"]))
        self.assertTrue(frame.loc["ETHUSDT", "is_stale"])
        self.assertTrue(frame.loc["ETHUSDT", "depth_incomplete"])

    def test_latest_depth_frame_reads_snapshot_levels_directly(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))
        service._handle_raw_message(json.dumps(make_depth_payload()))

        with patch.object(
            DepthSnapshot,
            "get_level_quote",
            side_effect=AssertionError("get_latest_depth_frame should not allocate DepthLevelQuote"),
        ):
            frame = service.get_latest_depth_frame(levels=2)

        self.assertEqual(frame.loc["BTCUSDT", "bid1"], Decimal("101.0"))
        self.assertEqual(frame.loc["BTCUSDT", "bid2_qty"], Decimal("1.5"))
        self.assertEqual(frame.loc["BTCUSDT", "ask1"], Decimal("101.5"))
        self.assertEqual(frame.loc["BTCUSDT", "ask2_qty"], Decimal("1.0"))

    def test_latest_depth_frame_can_return_decimal_numeric_values(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT", "ETHUSDT"], levels=5))
        service._handle_raw_message(json.dumps(make_depth_payload()))

        frame = service.get_latest_depth_frame(levels=2)

        self.assertEqual(frame.loc["BTCUSDT", "bid1"], Decimal("101.0"))
        self.assertEqual(frame.loc["BTCUSDT", "bid2_qty"], Decimal("1.5"))
        self.assertEqual(frame.loc["BTCUSDT", "ask1"], Decimal("101.5"))
        self.assertEqual(frame.loc["BTCUSDT", "ask2_qty"], Decimal("1.0"))
        self.assertTrue(pd.isna(frame.loc["ETHUSDT", "bid1"]))
        self.assertEqual(frame["bid1"].dtype, object)

    def test_latest_depth_frame_uses_nan_for_stale_gap_and_age_filters(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))
        service._record_connected()
        service._handle_raw_message(json.dumps(make_depth_payload(final_update_id=12, previous_final_update_id=9)))

        service._mark_all_stale("connection_closed")
        stale_frame = service.get_latest_depth_frame(levels=1)

        self.assertTrue(pd.isna(stale_frame.loc["BTCUSDT", "bid1"]))
        self.assertTrue(stale_frame.loc["BTCUSDT", "has_snapshot"])
        self.assertEqual(stale_frame.loc["BTCUSDT", "transaction_time_ms"], 1_700_000_000_000)
        self.assertTrue(stale_frame.loc["BTCUSDT", "is_stale"])

        service._handle_raw_message(
            json.dumps(make_depth_payload(first_update_id=13, final_update_id=14, previous_final_update_id=11))
        )
        gap_frame = service.get_latest_depth_frame(levels=1, require_sequence_continuity=True)

        self.assertTrue(pd.isna(gap_frame.loc["BTCUSDT", "bid1"]))
        self.assertEqual(gap_frame.loc["BTCUSDT", "transaction_time_ms"], 1_700_000_000_000)
        self.assertTrue(gap_frame.loc["BTCUSDT", "sequence_gap"])

        snapshot = service.get_latest("BTCUSDT")
        self.assertIsNotNone(snapshot)
        service._snapshots["BTCUSDT"] = replace(snapshot, local_recv_time_ms=0, sequence_gap=False)
        old_frame = service.get_latest_depth_frame(levels=1, max_age_ms=1)

        self.assertTrue(pd.isna(old_frame.loc["BTCUSDT", "bid1"]))
        self.assertEqual(old_frame.loc["BTCUSDT", "transaction_time_ms"], 1_700_000_000_000)
        self.assertFalse(old_frame.loc["BTCUSDT", "is_stale"])

    def test_latest_depth_frame_options(self):
        service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=["BTCUSDT"], levels=5))
        service._handle_raw_message(json.dumps(make_depth_payload()))

        frame = service.get_latest_depth_frame(levels=1, include_status=False)

        self.assertEqual(list(frame.columns), ["bid1", "bid1_qty", "ask1", "ask1_qty"])

        with self.assertRaises(ValueError):
            service.get_latest_depth_frame(levels=0)
        with self.assertRaises(ValueError):
            service.get_latest_depth_frame(levels=6)
        with self.assertRaises(ValueError):
            service.get_latest_depth_frame(max_age_ms=-1)
        with self.assertRaises(ValueError):
            service.get_latest_depth_frame(numeric_type="int")

        upper_case_frame = service.get_latest_depth_frame(levels=1, numeric_type="FLOAT")
        self.assertEqual(upper_case_frame.loc["BTCUSDT", "bid1"], 101.0)
        self.assertTrue(pd.api.types.is_float_dtype(upper_case_frame["bid1"]))


if __name__ == "__main__":
    unittest.main()
