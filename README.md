# AINotify Relay

A single-user Rust relay for AINotify, backed by SQLite.

## Run locally

Build with `cargo build --locked --release`. Start the binary with these environment variables:

| Variable | Value |
| --- | --- |
| `AINOTIFY_LISTEN` | Default `127.0.0.1:8787` |
| `AINOTIFY_DB` | Backupable SQLite path, default `ainotify.sqlite3` |
| `AINOTIFY_READER_TOKEN` | A random reader token, 24 or more characters |
| `AINOTIFY_WRITERS` | JSON object mapping device IDs to distinct random writer tokens |

Run behind an HTTPS reverse proxy for devices outside this machine. Keep the database on local persistent storage. The database contains the final replies whenever a collector has text forwarding enabled. A token change takes effect after restarting the relay with the updated configuration.

See [protocol v1](docs/protocol.md). Run `./scripts/local-ci.ps1` to check the Rust project on Windows.
