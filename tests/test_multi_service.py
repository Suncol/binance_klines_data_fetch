from __future__ import annotations

import threading
import unittest

from binance_klines_data_fetch.client import klines_to_dataframe
from binance_klines_data_fetch.errors import BinanceRequestError
from binance_klines_data_fetch.models import ONE_MINUTE_MS
from binance_klines_data_fetch.multi_service import MultiSymbolKlineService

from tests.test_client import make_kline


class MultiFakeClient:
    def __init__(self):
        self.latest_open = 120_000
        self.fetch_calls = []
        self.fail_symbols = set()
        self.configure_called = 0
        self._lock = threading.Lock()

    def configure_rate_limiter_from_exchange_info(self):
        self.configure_called += 1
        return 2400

    def latest_closed_1m_open_time_ms(self):
        return self.latest_open

    def fetch_recent_closed_1m_klines(self, symbol, n, *, end_open_time_ms=None, chunk_limit=None):
        with self._lock:
            self.fetch_calls.append((symbol, n, end_open_time_ms, chunk_limit))
        if symbol in self.fail_symbols:
            raise BinanceRequestError(f"{symbol} failed")
        end_open_time_ms = self.latest_open if end_open_time_ms is None else end_open_time_ms
        start = end_open_time_ms - (n - 1) * ONE_MINUTE_MS
        return klines_to_dataframe([make_kline(start + i * ONE_MINUTE_MS) for i in range(n)])


class MultiSymbolKlineServiceTests(unittest.TestCase):
    def test_refresh_bootstraps_multiple_symbols(self):
        client = MultiFakeClient()
        service = MultiSymbolKlineService(
            symbols=["btcusdt", "ETHUSDT", "BTCUSDT"],
            window_size=3,
            client=client,
            auto_configure_rate_limit=False,
            max_workers=2,
        )

        result = service.refresh_once()
        all_data = service.get_all_recent()
        status = service.status()

        self.assertEqual(set(result), {"BTCUSDT", "ETHUSDT"})
        self.assertTrue(result["BTCUSDT"])
        self.assertTrue(result["ETHUSDT"])
        self.assertEqual(len(all_data["BTCUSDT"]), 3)
        self.assertEqual(len(all_data["ETHUSDT"]), 3)
        self.assertTrue(status.ready)

    def test_one_symbol_failure_keeps_old_cache_and_updates_other_symbols(self):
        client = MultiFakeClient()
        service = MultiSymbolKlineService(
            symbols=["BTCUSDT", "ETHUSDT"],
            window_size=3,
            client=client,
            auto_configure_rate_limit=False,
            max_workers=2,
        )
        service.refresh_once()
        eth_before = service.get_recent("ETHUSDT")

        client.latest_open += ONE_MINUTE_MS
        client.fail_symbols.add("ETHUSDT")
        result = service.refresh_once()
        status = service.status()

        self.assertTrue(result["BTCUSDT"])
        self.assertFalse(result["ETHUSDT"])
        self.assertEqual(int(service.get_recent("BTCUSDT").index[-1].timestamp() * 1000), client.latest_open)
        self.assertTrue(eth_before.equals(service.get_recent("ETHUSDT")))
        self.assertIsNotNone(status.symbols["ETHUSDT"].last_error)

    def test_add_and_remove_symbols(self):
        client = MultiFakeClient()
        service = MultiSymbolKlineService(
            symbols=["BTCUSDT"],
            window_size=2,
            client=client,
            auto_configure_rate_limit=False,
        )

        service.refresh_once()
        service.add_symbols(["SOLUSDT"])
        result = service.refresh_once()
        service.remove_symbols(["BTCUSDT"])

        self.assertFalse(result["BTCUSDT"])
        self.assertTrue(result["SOLUSDT"])
        self.assertEqual(len(service.get_recent("SOLUSDT")), 2)
        with self.assertRaises(KeyError):
            service.get_recent("BTCUSDT")

    def test_auto_configure_rate_limit_runs_once(self):
        client = MultiFakeClient()
        service = MultiSymbolKlineService(symbols=["BTCUSDT"], window_size=2, client=client)

        service.refresh_once()
        service.refresh_once()

        self.assertEqual(client.configure_called, 1)


if __name__ == "__main__":
    unittest.main()
