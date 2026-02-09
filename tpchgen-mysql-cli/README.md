# tpch-mysql (Rust)

Single-binary replacement for `tpch-mysql` (Python). It:

- generates TPC-H data files using `tpchgen-cli` (tab-delimited CSV with header by default)
- loads them into MySQL via **client-side** `LOAD DATA LOCAL INFILE`
- optionally runs TPC-H queries (Q1..Q22) and records timings

## Notes

- `gen` writes data files to local disk, and `load` uses **client-side** `LOAD DATA LOCAL INFILE`, so the generated files must be readable by the `tpch-mysql` process (this machine), not by `mysqld`.
- Your MySQL server must allow `LOAD DATA LOCAL INFILE` (some deployments disable it for security reasons).

## Build

```bash
cargo build -p tpchgen-mysql-cli --release
```

Binary: `target/release/tpch-mysql`

## Portable Linux (static) build

If you copy a Linux build to another machine and see errors like `GLIBC_2.xx not found`, build a
statically-linked musl binary instead.

### Option A: Docker (recommended)

From the repo root:

```bash
./scripts/build-tpch-mysql-static-linux.sh
```

Output: `target/x86_64-unknown-linux-musl/release/tpch-mysql`

### Option B: Native on Linux

Install musl tooling + target, then build:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build -p tpchgen-mysql-cli --release --locked --target x86_64-unknown-linux-musl
```

## Usage

```bash
./target/release/tpch-mysql all \\
  --host 127.0.0.1 --port 3307 --user root --password 123123 --database dingo \\
  --data-dir /path/to/tpch \\
  --scale-factor 1
```

Run queries:

```bash
./target/release/tpch-mysql run \\
  --host <host> --port <port> --user <user> --password <password> --database <db> \\
  --all --output /path/to/tpch_timings.json \\
  --monitor-pid <pid>
```

### Memory monitoring (optional)

If you pass `--monitor-pid <pid>`, `tpch-mysql` will sample that **local** process's RSS
(`VmRSS` from `/proc/<pid>/status`) while each query executes and include the stats in the JSON
output.

- Linux only (requires `/proc`)
- Output fields are per-query:
  - `monitor_rss_start_kb`, `monitor_rss_end_kb`, `monitor_rss_peak_kb`, `monitor_samples`
  - `monitor_error` if sampling fails (e.g. process exits)

### One-command bench

Run `schema + gen + load (with truncate) + run` in a single command:

```bash
./target/release/tpch-mysql bench \\
  --host 127.0.0.1 --port 3307 --user root --password 123123 --database dingo \\
  --data-dir /tmp/tpch --scale-factor 0.001 \\
  --monitor-pid <pid> \\
  --query 1,6,19
```

By default this:

- drops existing tables before creating schema (`--drop-existing=true`)
- truncates tables before loading (`--truncate`)
- sleeps 30 minutes between load and run (`--sleep-seconds`, set to `0` to disable)
- writes timings to `./tpch_timings.json`
- runs all queries (Q1..Q22); pass `--query ...` to run a subset, or `--all` explicitly
- runs a precheck step before benchmarking (see below)

### Query precheck

By default, `run` performs a **precheck** step before benchmarking:

- Creates session-scoped **temporary** TPC-H tables (empty) and executes the selected queries once to validate SQL compatibility.
  - This does not modify your real tables or data.
  - If anything fails, `run` exits immediately.

If your server does not support `TEMPORARY TABLE` (e.g. some MySQL-compatible systems), use `--no-precheck`.

To skip precheck:

```bash
./target/release/tpch-mysql run --no-precheck ...
```
