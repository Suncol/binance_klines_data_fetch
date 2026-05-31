from __future__ import annotations

import asyncio
import json
import unittest
from decimal import Decimal
from unittest.mock import patch

from binance_klines_data_fetch import (
    BinanceDepthResponseError,
    BinanceOptionsDepthConfig,
    BinanceOptionsDepthService,
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

    def test_sequence_gap_sets_flag_until_next_contiguous_message(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))

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
        self.assertFalse(service.is_ready)

        service._handle_raw_message(
            json.dumps(make_options_depth_payload(first_update_id=15, final_update_id=16, previous_final_update_id=14))
        )
        third = service.get_latest("BTC-251226-110000-C")
        self.assertIsNotNone(third)
        self.assertFalse(third.sequence_gap)
        self.assertTrue(service.is_ready)

    def test_stale_marking_keeps_last_snapshot_but_clears_readiness(self):
        service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=["BTC-251226-110000-C"], levels=5))
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
