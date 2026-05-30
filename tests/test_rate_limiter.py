from __future__ import annotations

import unittest

from binance_klines_data_fetch.errors import BinanceResponseError
from binance_klines_data_fetch.rate_limiter import (
    WeightedRateLimiter,
    kline_request_weight,
    request_weight_limit_from_exchange_info,
)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class WeightedRateLimiterTests(unittest.TestCase):
    def test_kline_weight_boundaries(self):
        cases = {
            1: 1,
            99: 1,
            100: 2,
            499: 2,
            500: 5,
            1000: 5,
            1001: 10,
            1500: 10,
        }
        for limit, expected in cases.items():
            with self.subTest(limit=limit):
                self.assertEqual(kline_request_weight(limit), expected)

    def test_acquire_waits_until_window_has_capacity(self):
        clock = FakeClock()
        limiter = WeightedRateLimiter(
            limit=3,
            window_seconds=60,
            clock=clock,
            sleeper=clock.sleep,
            min_sleep_seconds=0,
        )

        limiter.acquire(2)
        limiter.acquire(1)
        limiter.acquire(1)

        self.assertEqual(clock.sleeps, [60.0])
        self.assertEqual(limiter.status().used_weight, 1)

    def test_header_observation_and_cooldown_status(self):
        clock = FakeClock()
        limiter = WeightedRateLimiter(
            limit=10,
            window_seconds=60,
            clock=clock,
            sleeper=clock.sleep,
            min_sleep_seconds=0,
        )

        limiter.update_from_headers({"x-mbx-used-weight-1m": "8"})
        limiter.on_rate_limited(retry_after_seconds=5)
        status = limiter.status()

        self.assertEqual(status.observed_used_weight_1m, 8)
        self.assertEqual(status.cooldown_seconds, 5)

    def test_exchange_info_limit_parse(self):
        data = {
            "rateLimits": [
                {"rateLimitType": "ORDERS", "interval": "MINUTE", "intervalNum": 1, "limit": 1200},
                {"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 2400},
            ]
        }

        self.assertEqual(request_weight_limit_from_exchange_info(data), 2400)

    def test_exchange_info_limit_parse_rejects_missing_limit(self):
        with self.assertRaises(BinanceResponseError):
            request_weight_limit_from_exchange_info({"rateLimits": []})


if __name__ == "__main__":
    unittest.main()
