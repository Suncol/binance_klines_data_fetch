"""Benchmark depth DataFrame construction without network access.

This script preloads synthetic latest depth snapshots, then measures
get_latest_depth_frame() only. It is intended for local regression checks, not
for exchange or WebSocket throughput benchmarking.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import statistics
import time
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from binance_klines_data_fetch import (
    BinanceDepthConfig,
    BinanceFuturesDepthService,
    BinanceOptionsDepthConfig,
    BinanceOptionsDepthService,
)


def _futures_payload(symbol: str, *, final_update_id: int, levels: int) -> str:
    return json.dumps(
        {
            "e": "depthUpdate",
            "E": 1_700_000_000_000,
            "T": 1_700_000_000_000,
            "s": symbol,
            "U": final_update_id - 1,
            "u": final_update_id,
            "pu": final_update_id - 1,
            "b": [[str(100_000 - level), "1.2345"] for level in range(levels)],
            "a": [[str(100_001 + level), "2.3456"] for level in range(levels)],
        }
    )


def _options_payload(symbol: str, *, final_update_id: int, levels: int) -> str:
    return json.dumps(
        {
            "e": "depthUpdate",
            "E": 1_700_000_000_000,
            "T": 1_700_000_000_000,
            "s": symbol,
            "U": final_update_id - 1,
            "u": final_update_id,
            "pu": final_update_id - 1,
            "b": [[str(1_200 - level), "0.1234"] for level in range(levels)],
            "a": [[str(1_201 + level), "0.2345"] for level in range(levels)],
        }
    )


def _load_futures_service(symbol_count: int, levels: int) -> BinanceFuturesDepthService:
    symbols = tuple(f"SYM{index:04d}USDT" for index in range(symbol_count))
    service = BinanceFuturesDepthService(BinanceDepthConfig(symbols=symbols, levels=levels))
    for index, symbol in enumerate(symbols):
        service._handle_raw_message(_futures_payload(symbol, final_update_id=1_000 + index, levels=levels))
    return service


def _load_options_service(symbol_count: int, levels: int) -> BinanceOptionsDepthService:
    symbols = tuple(f"BTC-251226-{100_000 + index}-C" for index in range(symbol_count))
    service = BinanceOptionsDepthService(BinanceOptionsDepthConfig(symbols=symbols, levels=levels))
    for index, symbol in enumerate(symbols):
        service._handle_raw_message(_options_payload(symbol, final_update_id=1_000 + index, levels=levels))
    return service


def _measure_depth_frame(
    name: str,
    service: BinanceFuturesDepthService | BinanceOptionsDepthService,
    *,
    levels: int,
    iterations: int,
    warmup: int,
    include_status: bool,
    numeric_type: str,
) -> None:
    for _ in range(warmup):
        service.get_latest_depth_frame(levels=levels, include_status=include_status, numeric_type=numeric_type)

    row_count = len(tuple(service.config.symbols))
    durations_ns: list[int] = []
    total_rows = 0
    for _ in range(iterations):
        start_ns = time.perf_counter_ns()
        frame = service.get_latest_depth_frame(levels=levels, include_status=include_status, numeric_type=numeric_type)
        elapsed_ns = time.perf_counter_ns() - start_ns
        durations_ns.append(elapsed_ns)
        total_rows += len(frame)

    total_seconds = sum(durations_ns) / 1_000_000_000
    mean_ms = statistics.fmean(durations_ns) / 1_000_000
    median_ms = statistics.median(durations_ns) / 1_000_000
    best_ms = min(durations_ns) / 1_000_000
    worst_ms = max(durations_ns) / 1_000_000
    calls_per_second = iterations / total_seconds if total_seconds > 0 else float("inf")
    rows_per_second = total_rows / total_seconds if total_seconds > 0 else float("inf")

    print(f"{name}:")
    print(
        f"  symbols={row_count} levels={levels} include_status={include_status} "
        f"numeric_type={numeric_type} iterations={iterations}"
    )
    print(f"  mean_ms={mean_ms:.3f} median_ms={median_ms:.3f} best_ms={best_ms:.3f} worst_ms={worst_ms:.3f}")
    print(f"  calls_per_second={calls_per_second:.2f} rows_per_second={rows_per_second:.0f}")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--futures-symbols", type=int, default=1024)
    parser.add_argument("--options-symbols", type=int, default=200)
    parser.add_argument("--levels", type=int, choices=(5, 10, 20), default=20)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--numeric-type", choices=("float", "decimal"), default="decimal")
    parser.add_argument("--no-status", action="store_true", help="exclude status columns from the benchmarked frame")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.futures_symbols < 1:
        raise SystemExit("--futures-symbols must be positive")
    if args.options_symbols < 1:
        raise SystemExit("--options-symbols must be positive")
    if args.iterations < 1:
        raise SystemExit("--iterations must be positive")
    if args.warmup < 0:
        raise SystemExit("--warmup must be non-negative")

    logging.disable(logging.CRITICAL)
    include_status = not args.no_status

    futures_service = _load_futures_service(args.futures_symbols, args.levels)
    options_service = _load_options_service(args.options_symbols, args.levels)

    _measure_depth_frame(
        "futures get_latest_depth_frame",
        futures_service,
        levels=args.levels,
        iterations=args.iterations,
        warmup=args.warmup,
        include_status=include_status,
        numeric_type=args.numeric_type,
    )
    _measure_depth_frame(
        "options get_latest_depth_frame",
        options_service,
        levels=args.levels,
        iterations=args.iterations,
        warmup=args.warmup,
        include_status=include_status,
        numeric_type=args.numeric_type,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
