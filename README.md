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

Examples

- The `examples/` directory includes a variety of sample dumps used by the tests. In particular
  `examples/employees.sql.gz` is a custom-format pg_dump of the sample "employees" database and
  is used by `tests/employees.rs` to exercise conversion of a real custom-format archive.

Testing notes

- Run the test suite with: `cargo test`.
- Some integration tests (for example the employees conversion test) require `pg_restore` to be
  available on PATH because they operate on PostgreSQL custom-format archives. If `pg_restore` is
  not available the test will skip with a short message. On CI you should install the PostgreSQL
  client tools (or ensure `pg_restore` is present) to run the full test set.

Development notes

- The converter emits helpful verbose logs when invoked with `-v` / `--verbose` — this is useful
  when inspecting why a COPY block or CREATE TABLE was skipped or translated.
- There are conservative textual transforms and sqlparser-based translations; if you add new
  translation logic please include tests in `tests/` and keep commits small and focused.

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
