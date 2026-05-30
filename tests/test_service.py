from __future__ import annotations

import unittest

from binance_klines_data_fetch.client import klines_to_dataframe
from binance_klines_data_fetch.errors import BinanceRequestError, KlineServiceError
from binance_klines_data_fetch.models import ONE_MINUTE_MS
from binance_klines_data_fetch.service import BinanceKlineService

from tests.test_client import make_kline


class FakeClient:
    def __init__(self):
        self.latest_open = 120_000
        self.fetch_calls = []
        self.fail_next = False

    def latest_closed_1m_open_time_ms(self):
        return self.latest_open

    def fetch_recent_closed_1m_klines(self, symbol, n, *, end_open_time_ms=None):
        self.fetch_calls.append((symbol, n, end_open_time_ms))
        if self.fail_next:
            self.fail_next = False
            raise BinanceRequestError("network down")
        end_open_time_ms = self.latest_open if end_open_time_ms is None else end_open_time_ms
        start = end_open_time_ms - (n - 1) * ONE_MINUTE_MS
        return klines_to_dataframe([make_kline(start + i * ONE_MINUTE_MS) for i in range(n)])


class BinanceKlineServiceTests(unittest.TestCase):
    def test_refresh_once_bootstraps_then_incrementally_merges_and_trims(self):
        client = FakeClient()
        service = BinanceKlineService(symbol="BTCUSDT", window_size=3, client=client)

        changed = service.refresh_once()
        self.assertTrue(changed)
        self.assertEqual(len(service.get_recent()), 3)

        client.latest_open += ONE_MINUTE_MS
        changed = service.refresh_once()
        df = service.get_recent()

        self.assertTrue(changed)
        self.assertEqual(len(df), 3)
        self.assertEqual(client.fetch_calls[-1][1], 1)
        self.assertEqual(int(df.index[-1].timestamp() * 1000), client.latest_open)

    def test_refresh_error_keeps_previous_cache(self):
        client = FakeClient()
        service = BinanceKlineService(symbol="BTCUSDT", window_size=3, client=client)
        service.refresh_once()
        before = service.get_recent()

        client.latest_open += ONE_MINUTE_MS
        client.fail_next = True
        with self.assertRaises(BinanceRequestError):
            service.refresh_once()

        after = service.get_recent()
        self.assertTrue(before.equals(after))
        self.assertIsNotNone(service.last_error)

    def test_start_stop_background_thread(self):
        client = FakeClient()
        service = BinanceKlineService(
            symbol="BTCUSDT",
            window_size=3,
            client=client,
            refresh_interval_seconds=0.1,
            startup_timeout_seconds=2.0,
        )

        service.start(block_until_ready=True)
        try:
            self.assertTrue(service.is_running)
            self.assertTrue(service.is_ready)
            self.assertEqual(len(service.get_recent(2)), 2)
        finally:
            service.stop()

        self.assertFalse(service.is_running)

    def test_start_block_until_ready_times_out(self):
        class AlwaysFailClient(FakeClient):
            def fetch_recent_closed_1m_klines(self, symbol, n, *, end_open_time_ms=None):
                raise BinanceRequestError("still down")

        service = BinanceKlineService(
            symbol="BTCUSDT",
            window_size=3,
            client=AlwaysFailClient(),
            refresh_interval_seconds=0.1,
            startup_timeout_seconds=0.2,
        )
        try:
            with self.assertRaises(KlineServiceError):
                service.start(block_until_ready=True)
        finally:
            service.stop()


if __name__ == "__main__":
    unittest.main()
