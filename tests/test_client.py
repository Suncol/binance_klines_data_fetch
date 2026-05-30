from __future__ import annotations

import unittest

import pandas as pd

from binance_klines_data_fetch.client import BinanceKlineClient, klines_to_dataframe
from binance_klines_data_fetch.errors import BinanceAPIError, BinanceResponseError
from binance_klines_data_fetch.models import ONE_MINUTE_MS, latest_closed_1m_open_time_ms


class FakeResponse:
    def __init__(self, status_code, payload, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = str(payload)

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


class RecordingLimiter:
    def __init__(self):
        self.acquired = []
        self.headers = []
        self.rate_limited = []
        self.banned = []

    def acquire(self, weight):
        self.acquired.append(weight)

    def update_from_headers(self, headers):
        self.headers.append(headers)

    def on_rate_limited(self, *, retry_after_seconds=None):
        self.rate_limited.append(retry_after_seconds)

    def on_banned(self, *, retry_after_seconds=None):
        self.banned.append(retry_after_seconds)


def make_kline(open_ms: int, close_value: str = "1.5"):
    return [
        open_ms,
        "1.0",
        "2.0",
        "0.5",
        close_value,
        "10.0",
        open_ms + ONE_MINUTE_MS - 1,
        "15.0",
        12,
        "5.0",
        "7.5",
        "0",
    ]


class BinanceKlineClientTests(unittest.TestCase):
    def test_klines_to_dataframe_schema(self):
        open_ms = 1_700_000_000_000
        df = klines_to_dataframe([make_kline(open_ms)])

        self.assertEqual(df.index.name, "Open_Time")
        self.assertEqual(len(df), 1)
        self.assertIn("Close_Time", df.columns)
        self.assertTrue(pd.api.types.is_float_dtype(df["Close"]))
        self.assertEqual(float(df.iloc[0]["Close"]), 1.5)

    def test_fetch_recent_uses_closed_boundary(self):
        server_time_ms = 1_700_000_123_456
        latest_open = latest_closed_1m_open_time_ms(server_time_ms)
        rows = [
            make_kline(latest_open - 2 * ONE_MINUTE_MS),
            make_kline(latest_open - ONE_MINUTE_MS),
            make_kline(latest_open),
        ]
        session = FakeSession(
            [
                FakeResponse(200, {"serverTime": server_time_ms}),
                FakeResponse(200, rows),
            ]
        )
        client = BinanceKlineClient(session=session, retries=0, retry_backoff_seconds=0)

        df = client.fetch_recent_closed_1m_klines("btcusdt", 3)

        self.assertEqual(len(df), 3)
        params = session.calls[1]["params"]
        self.assertEqual(params["symbol"], "BTCUSDT")
        self.assertEqual(params["interval"], "1m")
        self.assertEqual(params["limit"], 3)
        self.assertEqual(params["startTime"], latest_open - 2 * ONE_MINUTE_MS)
        self.assertEqual(params["endTime"], latest_open + ONE_MINUTE_MS - 1)

    def test_fetch_recent_paginates_above_binance_limit(self):
        n = 1501
        server_time_ms = 1_700_100_000_000
        latest_open = latest_closed_1m_open_time_ms(server_time_ms)
        start_open = latest_open - (n - 1) * ONE_MINUTE_MS
        first_batch = [make_kline(start_open + i * ONE_MINUTE_MS) for i in range(1500)]
        second_batch = [make_kline(latest_open)]
        session = FakeSession(
            [
                FakeResponse(200, {"serverTime": server_time_ms}),
                FakeResponse(200, first_batch),
                FakeResponse(200, second_batch),
            ]
        )
        client = BinanceKlineClient(session=session, retries=0, retry_backoff_seconds=0)

        df = client.fetch_recent_closed_1m_klines("ETHUSDT", n)

        self.assertEqual(len(df), n)
        self.assertEqual(session.calls[1]["params"]["limit"], 1500)
        self.assertEqual(session.calls[2]["params"]["limit"], 1)

    def test_binance_api_error_is_raised(self):
        session = FakeSession([FakeResponse(400, {"code": -1121, "msg": "Invalid symbol."})])
        client = BinanceKlineClient(session=session, retries=0, retry_backoff_seconds=0)

        with self.assertRaises(BinanceAPIError) as cm:
            client.fetch_klines("BAD", limit=1)

        self.assertEqual(cm.exception.status_code, 400)
        self.assertEqual(cm.exception.code, -1121)

    def test_retryable_non_json_5xx_is_retried(self):
        session = FakeSession(
            [
                FakeResponse(503, ValueError("html")),
                FakeResponse(200, [make_kline(1_700_000_000_000)]),
            ]
        )
        client = BinanceKlineClient(session=session, retries=1, retry_backoff_seconds=0)

        rows = client.fetch_klines("BTCUSDT", limit=1)

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(session.calls), 2)

    def test_fetch_klines_uses_weighted_limiter(self):
        limiter = RecordingLimiter()
        session = FakeSession(
            [FakeResponse(200, [make_kline(1_700_000_000_000)], headers={"X-MBX-USED-WEIGHT-1M": "5"})]
        )
        client = BinanceKlineClient(
            session=session,
            retries=0,
            retry_backoff_seconds=0,
            rate_limiter=limiter,
        )

        client.fetch_klines("BTCUSDT", limit=500)

        self.assertEqual(limiter.acquired, [5])
        self.assertEqual(limiter.headers[0]["X-MBX-USED-WEIGHT-1M"], "5")

    def test_chunk_limit_controls_pagination_size(self):
        latest_open = 1_700_100_000_000
        n = 501
        start_open = latest_open - (n - 1) * ONE_MINUTE_MS
        first_batch = [make_kline(start_open + i * ONE_MINUTE_MS) for i in range(499)]
        second_start = start_open + 499 * ONE_MINUTE_MS
        second_batch = [make_kline(second_start + i * ONE_MINUTE_MS) for i in range(2)]
        session = FakeSession([FakeResponse(200, first_batch), FakeResponse(200, second_batch)])
        client = BinanceKlineClient(session=session, retries=0, retry_backoff_seconds=0)

        df = client.fetch_recent_closed_1m_klines(
            "BTCUSDT",
            n,
            end_open_time_ms=latest_open,
            chunk_limit=499,
        )

        self.assertEqual(len(df), n)
        self.assertEqual(session.calls[0]["params"]["limit"], 499)
        self.assertEqual(session.calls[1]["params"]["limit"], 2)

    def test_429_notifies_limiter(self):
        limiter = RecordingLimiter()
        session = FakeSession(
            [FakeResponse(429, {"code": -1003, "msg": "Too many requests."}, headers={"Retry-After": "7"})]
        )
        client = BinanceKlineClient(
            session=session,
            retries=0,
            retry_backoff_seconds=0,
            rate_limiter=limiter,
        )

        with self.assertRaises(BinanceAPIError):
            client.fetch_klines("BTCUSDT", limit=1)

        self.assertEqual(limiter.rate_limited, [7.0])

    def test_malformed_rows_raise_response_error(self):
        with self.assertRaises(BinanceResponseError):
            klines_to_dataframe([[1, 2, 3]])


if __name__ == "__main__":
    unittest.main()
