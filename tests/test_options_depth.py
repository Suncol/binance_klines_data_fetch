from __future__ import annotations

import asyncio
import json
import unittest
from dataclasses import replace
from decimal import Decimal
from unittest.mock import patch

import pandas as pd

from binance_klines_data_fetch import (
    BinanceDepthResponseError,
    BinanceOptionsDepthConfig,
    BinanceOptionsDepthService,
    DepthLevelQuote,
    OptionDepthPriceLevel,
    OptionsDepthSnapshot,
    build_options_depth_service_configs,
)
from binance_klines_data_fetch.options import parse_option_symbol_record


def make_option_record():
    return {
        "expiryDate": 1_766_736_000_000,
        "filters": [],
        "symbol": "BTC-251226-110000-C",
        "side": "CALL",
        "strikePrice": "110000",
        "underlying": "BTCUSDT",
        "unit": 1,
        "minQty": "0.01",
        "maxQty": "100",
        "priceScale": 2,
        "quantityScale": 2,
        "quoteAsset": "USDT",
        "status": "TRADING",
    }


def make_options_depth_payload(
    *,
    symbol: str = "BTC-251226-110000-C",
    event_time_ms: int = 1_763_041_762_942,
    transaction_time_ms: int = 1_763_041_762_900,
    first_update_id: int = 10,
    final_update_id: int = 12,
    previous_final_update_id: int = 9,
    bids=None,
    asks=None,
):
    return {
        "e": "depthUpdate",
        "E": event_time_ms,
        "T": transaction_time_ms,
        "s": symbol,
        "U": first_update_id,
        "u": final_update_id,
        "pu": previous_final_update_id,
        "b": bids
        if bids is not None
        else [
            ["1100.000", "0.6000"],
            ["1200.000", "0.1000"],
            ["1000.000", "0.3000"],
        ],
        "a": asks
        if asks is not None
        else [
            ["1300.000", "0.6000"],
            ["1250.000", "0.2000"],
        ],
    }


class FakeConnectContext:
    def __init__(self, websocket):
        self.websocket = websocket

    async def __aenter__(self):
        return self.websocket

    async def __aexit__(self, exc_type, exc, tb):
        return False


class StopAfterMessageWebSocket:
    def __init__(self, service: BinanceOptionsDepthService, payload):
        self.service = service
        self.payload = payload

    async def recv(self):
        self.service._stop_event.set()
        return json.dumps(self.payload)


class HangingWebSocket:
    async def recv(self):
        await asyncio.sleep(10)


def make_fast_reconnect_service(*, read_timeout_seconds: float = 60.0) -> BinanceOptionsDepthService:
    return BinanceOptionsDepthService(
        BinanceOptionsDepthConfig(
            symbols=["BTC-251226-110000-C"],
            levels=5,
            speed_ms=100,
            read_timeout_seconds=read_timeout_seconds,
            reconnect_initial_delay_seconds=0.001,
            reconnect_max_delay_seconds=0.001,
            reconnect_jitter_seconds=0,
        )
    )


class BinanceOptionsDepthServiceTests(unittest.TestCase):
    def test_config_rejects_invalid_levels_speeds_and_symbol_counts(self):
        with self.assertRaises(ValueError):
            BinanceOptionsDepthConfig(symbols=[])
        with self.assertRaises(ValueError):
            BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=7)
        with self.assertRaises(ValueError):
            BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], speed_ms=250)
        with self.assertRaises(ValueError):
            BinanceOptionsDepthConfig(symbols=[f"BTC-251226-{index}-C" for index in range(201)])

    def test_config_accepts_option_symbol_objects_and_deduplicates(self):
        option_symbol = parse_option_symbol_record(make_option_record())

        config = BinanceOptionsDepthConfig(
            symbols=[option_symbol, "btc-251226-110000-c", "ETH-251226-4000-P"],
            levels=10,
            speed_ms=500,
        )

        self.assertEqual(tuple(config.symbols), ("BTC-251226-110000-C", "ETH-251226-4000-P"))
        self.assertEqual(config.base_url, "wss://fstream.binance.com")
        self.assertEqual(config.endpoint, "public")

    def test_build_url_uses_lowercase_options_depth_streams(self):
        service = BinanceOptionsDepthService(
            BinanceOptionsDepthConfig(
                symbols=["BTC-251226-110000-C", "ETH-251226-4000-P"],
                levels=20,
                speed_ms=500,
            )
        )

        self.assertEqual(
            service.build_url(),
            "wss://fstream.binance.com/public/stream?streams="
            "btc-251226-110000-c@depth20@500ms/eth-251226-4000-p@depth20@500ms",
        )

    def test_parse_combined_payload_sorts_levels_and_marks_incomplete_depth(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        payload = {"stream": "btc-251226-110000-c@depth5@100ms", "data": make_options_depth_payload()}

        snapshot = service._parse_depth_message(json.dumps(payload))

        self.assertIsInstance(snapshot, OptionsDepthSnapshot)
        self.assertEqual(snapshot.symbol, "BTC-251226-110000-C")
        self.assertEqual([level.price for level in snapshot.bids], [Decimal("1200.000"), Decimal("1100.000"), Decimal("1000.000")])
        self.assertEqual([level.price for level in snapshot.asks], [Decimal("1250.000"), Decimal("1300.000")])
        self.assertEqual(snapshot.bids[0].qty, Decimal("0.1000"))
        self.assertEqual(snapshot.asks[0].qty, Decimal("0.2000"))
        self.assertEqual(snapshot.spread, Decimal("50.000"))
        self.assertEqual(snapshot.mid_price, Decimal("1225.000"))
        self.assertEqual(snapshot.spread_bps, Decimal("50.000") / Decimal("1225.000") * Decimal("10000"))
        self.assertEqual(snapshot.expected_levels, 5)
        self.assertTrue(snapshot.depth_incomplete)
        self.assertFalse(snapshot.is_stale)
        self.assertFalse(snapshot.sequence_gap)

    def test_parse_raw_payload_can_have_complete_depth(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        payload = make_options_depth_payload(
            bids=[[str(1200 - index), "0.1"] for index in range(5)],
            asks=[[str(1300 + index), "0.2"] for index in range(5)],
        )

        snapshot = service._parse_depth_message(json.dumps(payload))

        self.assertEqual(len(snapshot.bids), 5)
        self.assertEqual(len(snapshot.asks), 5)
        self.assertFalse(snapshot.depth_incomplete)

    def test_malformed_depth_payload_raises_response_error(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))

        with self.assertRaises(BinanceDepthResponseError):
            service._parse_depth_message(json.dumps({"data": {"s": "BTC-251226-110000-C"}}))
        with self.assertRaises(BinanceDepthResponseError):
            service._parse_depth_message("{not-json")
        with self.assertRaises(BinanceDepthResponseError):
            service._parse_depth_message(json.dumps({**make_options_depth_payload(), "b": "bad"}))

    def test_connected_status_controls_readiness_without_snapshots(self):
        service = BinanceOptionsDepthService(
            BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C", "BTC-251226-110000-P"], levels=5)
        )

        self.assertFalse(service.is_ready)

        service._record_connected()

        self.assertTrue(service.is_ready)
        self.assertEqual(service.get_all_latest(), {})

        service._set_connected(False)

        self.assertFalse(service.is_ready)

    def test_sequence_gap_sets_flag_until_next_contiguous_message(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        service._record_connected()

        service._handle_raw_message(json.dumps(make_options_depth_payload(final_update_id=12, previous_final_update_id=9)))
        first = service.get_latest("btc-251226-110000-c")
        self.assertIsNotNone(first)
        self.assertFalse(first.sequence_gap)
        self.assertTrue(first.depth_incomplete)
        self.assertTrue(service.is_ready)

        service._handle_raw_message(
            json.dumps(make_options_depth_payload(first_update_id=13, final_update_id=14, previous_final_update_id=11))
        )
        second = service.get_latest("BTC-251226-110000-C")
        self.assertIsNotNone(second)
        self.assertTrue(second.sequence_gap)
        self.assertFalse(second.is_stale)
        self.assertTrue(service.is_ready)

        service._handle_raw_message(
            json.dumps(make_options_depth_payload(first_update_id=15, final_update_id=16, previous_final_update_id=14))
        )
        third = service.get_latest("BTC-251226-110000-C")
        self.assertIsNotNone(third)
        self.assertFalse(third.sequence_gap)
        self.assertTrue(service.is_ready)

    def test_stale_marking_keeps_last_snapshot_but_clears_readiness(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        service._record_connected()
        service._handle_raw_message(json.dumps(make_options_depth_payload()))

        service._mark_all_stale("connection_closed")

        snapshot = service.get_latest("BTC-251226-110000-C")
        self.assertIsNotNone(snapshot)
        self.assertTrue(snapshot.is_stale)
        self.assertEqual(snapshot.stale_reason, "connection_closed")
        self.assertIsNotNone(snapshot.stale_since_ms)
        self.assertFalse(service.is_ready)

    def test_latest_depth_table_returns_dict_copy(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        service._handle_raw_message(json.dumps(make_options_depth_payload()))

        table = service.latest_depth_table
        table.clear()

        self.assertIsNotNone(service.get_latest("BTC-251226-110000-C"))

    def test_latest_level_quote_handles_sparse_options_book(self):
        option_symbol = parse_option_symbol_record(make_option_record())
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=[option_symbol], levels=5))
        payload = make_options_depth_payload(
            bids=[],
            asks=[
                ["1300.000", "0.6000"],
                ["1250.000", "0.2000"],
            ],
        )

        self.assertIsNone(service.get_latest_level_quote(option_symbol, level=1))

        service._handle_raw_message(json.dumps(payload))

        quote = service.get_latest_level_quote(option_symbol, level=2)

        self.assertIsInstance(quote, DepthLevelQuote)
        self.assertEqual(quote.symbol, "BTC-251226-110000-C")
        self.assertEqual(quote.level, 2)
        self.assertIsNone(quote.bid_price)
        self.assertIsNone(quote.bid_qty)
        self.assertEqual(quote.ask_price, Decimal("1300.000"))
        self.assertEqual(quote.ask_qty, Decimal("0.6000"))
        self.assertTrue(quote.depth_incomplete)
        self.assertEqual(
            service.get_latest_level_value(option_symbol, side="ask", level=2, field="qty"),
            Decimal("0.6000"),
        )
        self.assertIsNone(service.get_latest_level_value(option_symbol, side="bid", level=1, field="price"))

    def test_latest_level_accessors_filter_stale_gap_and_age(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        service._record_connected()
        service._handle_raw_message(json.dumps(make_options_depth_payload(final_update_id=12, previous_final_update_id=9)))

        self.assertIsNotNone(service.get_latest_level_quote("BTC-251226-110000-C"))

        service._mark_all_stale("connection_closed")

        self.assertIsNone(service.get_latest_level_quote("BTC-251226-110000-C"))
        stale_quote = service.get_latest_level_quote("BTC-251226-110000-C", require_not_stale=False)
        self.assertIsNotNone(stale_quote)
        self.assertTrue(stale_quote.is_stale)

        service._handle_raw_message(
            json.dumps(make_options_depth_payload(first_update_id=13, final_update_id=14, previous_final_update_id=11))
        )

        self.assertIsNotNone(service.get_latest_level_quote("BTC-251226-110000-C"))
        self.assertIsNone(
            service.get_latest_level_quote("BTC-251226-110000-C", require_sequence_continuity=True)
        )

        snapshot = service.get_latest("BTC-251226-110000-C")
        self.assertIsNotNone(snapshot)
        service._snapshots["BTC-251226-110000-C"] = replace(snapshot, local_recv_time_ms=0, sequence_gap=False)

        self.assertIsNone(service.get_latest_level_quote("BTC-251226-110000-C", max_age_ms=1))

    def test_latest_level_accessors_validate_inputs(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        service._handle_raw_message(json.dumps(make_options_depth_payload()))

        with self.assertRaises(ValueError):
            service.get_latest_level_quote("BTC-251226-110000-C", level=0)
        with self.assertRaises(ValueError):
            service.get_latest_level_quote("BTC-251226-110000-C", level=6)
        with self.assertRaises(ValueError):
            service.get_latest_level_value("BTC-251226-110000-C", side="middle")
        with self.assertRaises(ValueError):
            service.get_latest_level_value("BTC-251226-110000-C", side="bid", field="size")
        with self.assertRaises(ValueError):
            service.get_latest_level_quote("BTC-251226-110000-C", max_age_ms=-1)

    def test_latest_depth_frame_keeps_all_symbols_and_handles_sparse_books(self):
        service = BinanceOptionsDepthService(
            BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C", "BTC-251226-110000-P"], levels=5)
        )
        service._handle_raw_message(
            json.dumps(
                make_options_depth_payload(
                    symbol="BTC-251226-110000-C",
                    bids=[],
                    asks=[
                        ["1300.000", "0.6000"],
                        ["1250.000", "0.2000"],
                    ],
                )
            )
        )

        frame = service.get_latest_depth_frame(levels=2)

        self.assertEqual(list(frame.index), ["BTC-251226-110000-C", "BTC-251226-110000-P"])
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
        self.assertTrue(pd.isna(frame.loc["BTC-251226-110000-C", "bid1"]))
        self.assertTrue(pd.isna(frame.loc["BTC-251226-110000-C", "bid2_qty"]))
        self.assertEqual(frame.loc["BTC-251226-110000-C", "ask1"], Decimal("1250.000"))
        self.assertEqual(frame.loc["BTC-251226-110000-C", "ask2"], Decimal("1300.000"))
        self.assertTrue(pd.api.types.is_float_dtype(frame["bid1"]))
        self.assertEqual(frame["ask1"].dtype, object)
        self.assertEqual(frame["ask2"].dtype, object)
        self.assertTrue(frame.loc["BTC-251226-110000-C", "has_snapshot"])
        self.assertEqual(frame.loc["BTC-251226-110000-C", "transaction_time_ms"], 1_763_041_762_900)
        self.assertTrue(frame.loc["BTC-251226-110000-C", "depth_incomplete"])
        self.assertTrue(pd.isna(frame.loc["BTC-251226-110000-P", "ask1"]))
        self.assertFalse(frame.loc["BTC-251226-110000-P", "has_snapshot"])
        self.assertTrue(pd.isna(frame.loc["BTC-251226-110000-P", "transaction_time_ms"]))
        self.assertTrue(frame.loc["BTC-251226-110000-P", "is_stale"])

    def test_latest_depth_frame_reads_snapshot_levels_directly(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        service._handle_raw_message(
            json.dumps(
                make_options_depth_payload(
                    bids=[],
                    asks=[
                        ["1300.000", "0.6000"],
                        ["1250.000", "0.2000"],
                    ],
                )
            )
        )

        with patch.object(
            OptionsDepthSnapshot,
            "get_level_quote",
            side_effect=AssertionError("get_latest_depth_frame should not allocate DepthLevelQuote"),
        ):
            frame = service.get_latest_depth_frame(levels=2)

        self.assertTrue(pd.isna(frame.loc["BTC-251226-110000-C", "bid1"]))
        self.assertTrue(pd.isna(frame.loc["BTC-251226-110000-C", "bid2_qty"]))
        self.assertEqual(frame.loc["BTC-251226-110000-C", "ask1"], Decimal("1250.000"))
        self.assertEqual(frame.loc["BTC-251226-110000-C", "ask2_qty"], Decimal("0.6000"))

    def test_latest_depth_frame_can_return_decimal_numeric_values(self):
        service = BinanceOptionsDepthService(
            BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C", "BTC-251226-110000-P"], levels=5)
        )
        service._handle_raw_message(
            json.dumps(
                make_options_depth_payload(
                    bids=[],
                    asks=[
                        ["1300.000", "0.6000"],
                        ["1250.000", "0.2000"],
                    ],
                )
            )
        )

        frame = service.get_latest_depth_frame(levels=2)

        self.assertTrue(pd.isna(frame.loc["BTC-251226-110000-C", "bid1"]))
        self.assertEqual(frame.loc["BTC-251226-110000-C", "ask1"], Decimal("1250.000"))
        self.assertEqual(frame.loc["BTC-251226-110000-C", "ask2_qty"], Decimal("0.6000"))
        self.assertTrue(pd.isna(frame.loc["BTC-251226-110000-P", "ask1"]))
        self.assertEqual(frame["ask1"].dtype, object)

    def test_latest_depth_frame_uses_nan_for_stale_gap_and_age_filters(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        service._record_connected()
        service._handle_raw_message(json.dumps(make_options_depth_payload(final_update_id=12, previous_final_update_id=9)))

        service._mark_all_stale("connection_closed")
        stale_frame = service.get_latest_depth_frame(levels=1)

        self.assertTrue(pd.isna(stale_frame.loc["BTC-251226-110000-C", "bid1"]))
        self.assertTrue(stale_frame.loc["BTC-251226-110000-C", "has_snapshot"])
        self.assertEqual(stale_frame.loc["BTC-251226-110000-C", "transaction_time_ms"], 1_763_041_762_900)
        self.assertTrue(stale_frame.loc["BTC-251226-110000-C", "is_stale"])

        service._handle_raw_message(
            json.dumps(make_options_depth_payload(first_update_id=13, final_update_id=14, previous_final_update_id=11))
        )
        gap_frame = service.get_latest_depth_frame(levels=1, require_sequence_continuity=True)

        self.assertTrue(pd.isna(gap_frame.loc["BTC-251226-110000-C", "bid1"]))
        self.assertEqual(gap_frame.loc["BTC-251226-110000-C", "transaction_time_ms"], 1_763_041_762_900)
        self.assertTrue(gap_frame.loc["BTC-251226-110000-C", "sequence_gap"])

        snapshot = service.get_latest("BTC-251226-110000-C")
        self.assertIsNotNone(snapshot)
        service._snapshots["BTC-251226-110000-C"] = replace(snapshot, local_recv_time_ms=0, sequence_gap=False)
        old_frame = service.get_latest_depth_frame(levels=1, max_age_ms=1)

        self.assertTrue(pd.isna(old_frame.loc["BTC-251226-110000-C", "bid1"]))
        self.assertEqual(old_frame.loc["BTC-251226-110000-C", "transaction_time_ms"], 1_763_041_762_900)
        self.assertFalse(old_frame.loc["BTC-251226-110000-C", "is_stale"])

    def test_latest_depth_frame_options(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
        service._handle_raw_message(json.dumps(make_options_depth_payload()))

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
        self.assertEqual(upper_case_frame.loc["BTC-251226-110000-C", "ask1"], 1250.0)
        self.assertTrue(pd.api.types.is_float_dtype(upper_case_frame["ask1"]))

    def test_build_options_depth_service_configs_chunks_streams(self):
        configs = build_options_depth_service_configs(
            [f"BTC-251226-{index}-C" for index in range(201)],
            levels=5,
            speed_ms=100,
        )

        self.assertEqual(len(configs), 2)
        self.assertEqual(len(tuple(configs[0].symbols)), 200)
        self.assertEqual(len(tuple(configs[1].symbols)), 1)

        with self.assertRaises(ValueError):
            build_options_depth_service_configs(["BTC-251226-110000-C"], max_streams_per_connection=201)

    def test_price_level_type_is_exported(self):
        level = OptionDepthPriceLevel(level=1, price=Decimal("1.0"), qty=Decimal("2.0"))

        self.assertEqual(level.price, Decimal("1.0"))

    def test_run_forever_reconnects_after_network_error(self):
        async def scenario():
            service = make_fast_reconnect_service()
            connect_calls = []

            def fake_connect(*args, **kwargs):
                connect_calls.append({"args": args, "kwargs": kwargs})
                if len(connect_calls) == 1:
                    raise OSError("temporary network failure")
                return FakeConnectContext(
                    StopAfterMessageWebSocket(
                        service,
                        make_options_depth_payload(final_update_id=20, previous_final_update_id=19),
                    )
                )

            with patch("binance_klines_data_fetch.options_depth.websockets.connect", side_effect=fake_connect):
                await asyncio.wait_for(service._run_forever(), timeout=1.0)

            snapshot = service.get_latest("BTC-251226-110000-C")
            self.assertIsNotNone(snapshot)
            self.assertFalse(snapshot.is_stale)
            self.assertFalse(snapshot.sequence_gap)
            self.assertEqual(snapshot.final_update_id, 20)
            self.assertEqual(service.status().reconnect_attempts, 2)
            self.assertEqual(len(connect_calls), 2)
            self.assertIsNone(connect_calls[0]["kwargs"]["ping_interval"])
            self.assertEqual(connect_calls[0]["kwargs"]["max_queue"], service.config.max_queue)

        asyncio.run(scenario())

    def test_run_forever_reconnects_after_read_timeout(self):
        async def scenario():
            service = make_fast_reconnect_service(read_timeout_seconds=0.0001)
            connect_calls = []
            stale_reasons = []
            original_mark_all_stale = service._mark_all_stale

            def recording_mark_all_stale(reason):
                stale_reasons.append(reason)
                original_mark_all_stale(reason)

            def fake_connect(*args, **kwargs):
                connect_calls.append({"args": args, "kwargs": kwargs})
                if len(connect_calls) == 1:
                    return FakeConnectContext(HangingWebSocket())
                return FakeConnectContext(
                    StopAfterMessageWebSocket(
                        service,
                        make_options_depth_payload(final_update_id=30, previous_final_update_id=29),
                    )
                )

            service._next_recv_timeout = lambda _last_recv_monotonic: 0.001
            service._mark_all_stale = recording_mark_all_stale

            with patch("binance_klines_data_fetch.options_depth.websockets.connect", side_effect=fake_connect):
                await asyncio.wait_for(service._run_forever(), timeout=1.0)

            snapshot = service.get_latest("BTC-251226-110000-C")
            self.assertIsNotNone(snapshot)
            self.assertFalse(snapshot.is_stale)
            self.assertEqual(snapshot.final_update_id, 30)
            self.assertIn("read_timeout", stale_reasons)
            self.assertEqual(service.status().reconnect_attempts, 2)
            self.assertEqual(len(connect_calls), 2)

        asyncio.run(scenario())

    def test_run_forever_stops_without_extra_reconnect_when_stop_requested_after_failure(self):
        async def scenario():
            service = make_fast_reconnect_service()
            connect_calls = []

            def fake_connect(*args, **kwargs):
                connect_calls.append({"args": args, "kwargs": kwargs})
                service._stop_event.set()
                raise OSError("stop after first failure")

            with patch("binance_klines_data_fetch.options_depth.websockets.connect", side_effect=fake_connect):
                await asyncio.wait_for(service._run_forever(), timeout=1.0)

            self.assertEqual(service.status().reconnect_attempts, 1)
            self.assertEqual(len(connect_calls), 1)
            self.assertFalse(service.status().connected)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
