# General
- This project aims to create a CLI application used to convert a PostgreSQL database dump into a valid SQLite database.
- The application should be called using the following syntax: `pg2lite dump.pgdump`. The output will simply be a new file with an identical file name, but with a new file extension: `.sqlite`.
- The application must support the standard SQL dumps (the default from `pg_dump`), the custom PostgreSQL format may be supported when the basic functionality is done.
- Every feature must be committed with Git, as granular as possible.
- Any commit must leave the project in a workable state without yielding any errors.

# Development
- `cargo` is available for debugging.
- After every, individual working feature has been implemented and tested, propose a Git commit message and offer to commit. Do not actually commit before the commit message has been approved and all tests pass.
- Never propose to push to origin, there is no origin.
- For every feature about to be implemented, serve a single sentence describing what its purpose is and what functions/files are added/changed, for approval.
