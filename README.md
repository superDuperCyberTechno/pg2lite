# pg2lite

pg2lite is a small CLI tool that converts PostgreSQL dump files into a SQLite database file.

Quick usage

    pg2lite dump.pgdump

Default behavior: the tool writes a `.sqlite` file next to the input file (same name). Override with:

    pg2lite -o out.sqlite dump.pgdump

Useful flags and env vars

- `-v`, `--verbose` : print progress logs for COPY and batch INSERT operations.
- `-o`, `--output`  : explicit output path for the resulting SQLite file.
- `PG2LITE_BATCH`   : set the preferred batch size for multi-row INSERTs (default `500`, automatically limited by SQLite max variables).

Features

- Parses and translates common SQL (CREATE TABLE, INSERT) using `sqlparser` and falls back to safe textual transforms when necessary.
- Supports PostgreSQL `COPY ... FROM stdin` in both text (tab-separated + backslash escapes) and CSV modes; CSV parsing is streamed and robust (handles quoted fields, embedded newlines and escaped quotes).
- Batches INSERTs for performance and applies SQLite performance pragmas (`synchronous=OFF`, `journal_mode=MEMORY`, `temp_store=MEMORY`) during import.
- Detects and maps PostgreSQL sequences (CREATE SEQUENCE, `setval`, `ALTER SEQUENCE ... OWNED BY`) into SQLite `AUTOINCREMENT` behavior and updates `sqlite_sequence` accordingly.
- Supports PostgreSQL custom-format dumps: if a dump file has the `PGDMP` header the tool will try to run `pg_restore` to extract plain SQL (requires `pg_restore` on `PATH`).

Building and testing

- Build: `cargo build` (or `cargo build --release` for better performance).
- Run the CLI: `./target/debug/pg2lite examples/sample.pgsql` (or the release binary).
- Tests: `cargo test`.
- Perf test (ignored by default): `PG2LITE_PERF_ROWS=100000 cargo test -- --ignored --nocapture`.

Limitations and notes

- The converter is conservative and intentionally skips or simplifies complex objects (extensions, functions, some index/trigger definitions). Expect edge cases on very complex schemas.
- COPY parsing currently treats the literal `\N` as NULL in both text and CSV modes; custom `NULL` tokens or non-standard encodings may require additional handling.
- `pg_restore` is required to extract SQL from PostgreSQL custom-format dumps; if unavailable the tool returns a helpful error message.

Contributions and feedback welcome. Open issues or PRs for new edge cases and features.
