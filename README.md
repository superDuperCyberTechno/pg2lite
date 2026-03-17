# pg2lite

pg2lite is a small CLI tool to convert PostgreSQL SQL dumps into a SQLite database file.

Usage

    pg2lite dump.pgdump

By default the tool will perform the conversion and write a SQLite database file next to the input file with the same name and a `.sqlite` extension. You can override the output path with `-o` / `--output`:

    pg2lite -o out.sqlite dump.pgdump

Examples

    # convert the provided example dump
    pg2lite examples/sample.pgsql

Notes

- The converter is intentionally conservative: it skips unsupported statements (COPY, ALTER, COMMENT, etc.) and applies basic transformations for `SERIAL`, boolean literals and `E''` string escapes.
- The project is early — expect edge cases. Contributions and bug reports welcome.
