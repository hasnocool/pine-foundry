#!/usr/bin/env python3
# scripts/benchmark_event_storage.py
"""Benchmark Pine Foundry JSONL against DuckDB and Parquet.

The benchmark deliberately lives outside the scanner binary. It reads the
existing data/events/YYYY-MM-DD.jsonl event fabric and materializes optional
research copies without changing the live journal or scanner hot path.

Python 3.12+.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Callable, Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark Pine Foundry JSONL, DuckDB, and Parquet storage."
    )
    parser.add_argument(
        "--events-dir",
        type=Path,
        default=Path("data/events"),
        help="Directory containing YYYY-MM-DD.jsonl files.",
    )
    parser.add_argument(
        "--pattern",
        default="*.jsonl",
        help="Glob pattern relative to --events-dir.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/research"),
        help="Directory for DuckDB and Parquet research artifacts.",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=None,
        help="Optional DuckDB path. Defaults to <output-dir>/pine_foundry.duckdb.",
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        default=None,
        help="Optional Parquet path. Defaults to <output-dir>/events.parquet.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Measured runs per query (default: 5).",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Limit input files for focused tests; 0 means all files.",
    )
    parser.add_argument(
        "--mode",
        choices=("benchmark", "materialize"),
        default="benchmark",
        help="Benchmark existing/materialized data or only materialize it.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing DuckDB/Parquet research artifacts.",
    )
    return parser.parse_args()


def discover_files(events_dir: Path, pattern: str, max_files: int) -> list[Path]:
    files = sorted(path for path in events_dir.glob(pattern) if path.is_file())
    if max_files > 0:
        files = files[:max_files]
    return files


def total_bytes(paths: Iterable[Path]) -> int:
    return sum(path.stat().st_size for path in paths)


def python_jsonl_summary(paths: Iterable[Path]) -> tuple[int, dict[str, int]]:
    counts: dict[str, int] = {}
    rows = 0
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                record = json.loads(line)
                kind = str(record.get("kind", "unknown"))
                counts[kind] = counts.get(kind, 0) + 1
                rows += 1
    return rows, counts


def timed(runs: int, function: Callable[[], object]) -> list[float]:
    durations = []
    for _ in range(max(1, runs)):
        started = time.perf_counter()
        function()
        durations.append(time.perf_counter() - started)
    return durations


def median_ms(durations: list[float]) -> float:
    return statistics.median(durations) * 1000.0


def require_duckdb():
    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "DuckDB Python package is required. Install the current stable client "
            "with: python3 -m pip install duckdb"
        ) from exc
    return duckdb


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def input_glob(events_dir: Path, pattern: str) -> str:
    return sql_path(events_dir / pattern)


def main() -> int:
    args = parse_args()
    if args.runs < 1:
        raise SystemExit("--runs must be >= 1")

    input_files = discover_files(args.events_dir, args.pattern, args.max_files)
    if not input_files:
        raise SystemExit(f"No event files matched {args.events_dir / args.pattern}")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    database = args.database or (args.output_dir / "pine_foundry.duckdb")
    parquet = args.parquet or (args.output_dir / "events.parquet")

    print(f"Input files: {len(input_files)}")
    print(f"Input bytes: {total_bytes(input_files):,}")
    print("Input rows: scanning JSONL once...")

    jsonl_started = time.perf_counter()
    row_count, jsonl_counts = python_jsonl_summary(input_files)
    jsonl_elapsed_ms = (time.perf_counter() - jsonl_started) * 1000.0
    print(f"Rows: {row_count:,}")
    print(f"JSONL Python scan: {jsonl_elapsed_ms:.2f} ms")

    duckdb = require_duckdb()
    connection = duckdb.connect(str(database))

    if args.force:
        connection.close()
        if database.exists():
            database.unlink()
        if parquet.exists():
            parquet.unlink()
        connection = duckdb.connect(str(database))

    glob = input_glob(args.events_dir, args.pattern)
    print(f"DuckDB version: {duckdb.__version__}")

    connection.execute("DROP TABLE IF EXISTS events")
    materialize_started = time.perf_counter()
    connection.execute(
        f"""
        CREATE TABLE events AS
        SELECT *
        FROM read_ndjson_auto('{glob}')
        """
    )
    materialize_ms = (time.perf_counter() - materialize_started) * 1000.0

    connection.execute(
        f"""
        COPY events
        TO '{sql_path(parquet)}'
        (FORMAT parquet, COMPRESSION zstd)
        """
    )

    table_rows = int(connection.execute("SELECT count(*) FROM events").fetchone()[0])
    if table_rows != row_count:
        raise RuntimeError(
            f"row count mismatch: JSONL={row_count}, DuckDB={table_rows}"
        )

    query = "SELECT kind, count(*) AS rows FROM events GROUP BY kind ORDER BY kind"
    parquet_query = f"""
        SELECT kind, count(*) AS rows
        FROM read_parquet('{sql_path(parquet)}')
        GROUP BY kind
        ORDER BY kind
    """
    duckdb_jsonl_query = f"""
        SELECT kind, count(*) AS rows
        FROM read_ndjson_auto('{glob}')
        GROUP BY kind
        ORDER BY kind
    """

    benchmarks = []
    for name, operation in (
        ("duckdb_jsonl_scan", lambda: connection.execute(duckdb_jsonl_query).fetchall()),
        ("duckdb_table_scan", lambda: connection.execute(query).fetchall()),
        ("parquet_scan", lambda: connection.execute(parquet_query).fetchall()),
    ):
        durations = timed(args.runs, operation)
        benchmarks.append(
            {
                "name": name,
                "median_ms": round(median_ms(durations), 3),
                "runs": len(durations),
                "min_ms": round(min(durations) * 1000.0, 3),
                "max_ms": round(max(durations) * 1000.0, 3),
            }
        )

    connection.close()

    result = {
        "duckdb_version": duckdb.__version__,
        "input_files": len(input_files),
        "input_bytes": total_bytes(input_files),
        "rows": row_count,
        "jsonl_python_scan_ms": round(jsonl_elapsed_ms, 3),
        "duckdb_materialize_ms": round(materialize_ms, 3),
        "jsonl_counts": dict(sorted(jsonl_counts.items())),
        "artifacts": {
            "duckdb": str(database),
            "parquet": str(parquet),
            "duckdb_bytes": database.stat().st_size if database.exists() else 0,
            "parquet_bytes": parquet.stat().st_size if parquet.exists() else 0,
        },
        "benchmarks": benchmarks if args.mode == "benchmark" else [],
    }

    print("\nStorage summary:")
    print(json.dumps(result, indent=2, sort_keys=True))

    print("\nInterpretation:")
    print(
        "JSONL remains the authoritative append-oriented event journal. "
        "Use the measured DuckDB/Parquet results to decide whether a durable "
        "research mirror is worth the extra storage/build complexity."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
