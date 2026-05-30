#!/usr/bin/env python3
"""Minimal import usage example."""

from __future__ import annotations

import time

from binance_klines_data_fetch import (
    BinanceKlineService,
    MultiSymbolKlineService,
    fetch_recent_closed_1m_klines,
)


def one_shot() -> None:
    df = fetch_recent_closed_1m_klines("BTCUSDT", 5)
    print(df)


def background_service() -> None:
    service = BinanceKlineService(symbol="BTCUSDT", window_size=100)
    try:
        service.start(block_until_ready=True)
        while True:
            df = service.get_recent(10)
            print(df.tail(1))
            time.sleep(10)
    finally:
        service.stop()


def multi_symbol_background_service() -> None:
    service = MultiSymbolKlineService(symbols=["BTCUSDT", "ETHUSDT"], window_size=100)
    try:
        service.start(block_until_ready=True)
        print(service.get_recent("BTCUSDT", 10).tail(1))
        print(service.get_all_recent(10).keys())
    finally:
        service.stop()


if __name__ == "__main__":
    one_shot()
