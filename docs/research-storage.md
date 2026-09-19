# Research storage

Pine Foundry keeps the live event fabric in asynchronous JSONL. That remains
the append-oriented source of truth and is intentionally not replaced by a
database in the scanner hot path.

## Benchmark

Use Python 3.12+ and the current stable DuckDB Python client.

DuckDB's Python client is currently on the 1.5.5 stable line; pre-release
builds may be newer but are not used as the default research baseline.

~~~text
python3 -m pip install duckdb
python3 scripts/benchmark_event_storage.py
~~~

The benchmark measures:

- JSONL parsing in Python.
- DuckDB querying directly over the JSONL files.
- DuckDB querying over a materialized table.
- Parquet querying through DuckDB.
- Materialization time.
- JSONL, DuckDB and Parquet byte sizes.

Focused runs can limit the number of event files:

~~~text
python3 scripts/benchmark_event_storage.py --max-files 3 --runs 10
~~~

Materialization without reporting measured query timings:

~~~text
python3 scripts/benchmark_event_storage.py --mode materialize
~~~

Artifacts default to:

~~~text
data/research/pine_foundry.duckdb
data/research/events.parquet
~~~

The data directory is ignored by Git, so local research artifacts are not
committed.

## Promotion rule

Do not make DuckDB or Parquet the live journal simply because it is available.
Promote it to a first-class research layer only after measuring:

- append throughput and journal backpressure,
- storage size,
- replay throughput,
- filtered event-study query latency,
- rebuild/materialization cost,
- operational complexity on the target machine.

The intended architecture is:

~~~text
             live providers
                  |
                  v
        normalized EventJournal
                  |
            async JSONL
                  |
          +-------+--------+
          |                |
          v                v
       replay        research mirror
                           |
                    +------+------+
                    |             |
                 DuckDB        Parquet
                    |             |
                    +------+------+
                           v
                     event studies
~~~

The research mirror is downstream of the event fabric. It must never block
market ingestion.
