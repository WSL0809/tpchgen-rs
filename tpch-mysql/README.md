# tpch-mysql

Generate TPC-H data using `tpchgen` (PyO3 bindings in `../tpchgen-py`) and load it into MySQL using `LOAD DATA INFILE` (server-side).

## Requirements

- Run this on the MySQL server machine (the generated files must be readable by `mysqld` for `LOAD DATA INFILE`).
- The MySQL user needs privileges to create tables and to read server-side files (`FILE` privilege), and the server may restrict import paths via `secure_file_priv`.

## Usage

```bash
cd tpch-mysql
uv run tpch-mysql all \
  --host <host> --port <port> --user <user> --password <password> --database <db> \
  --data-dir /home/jason/wangsl/tpch \
  --scale-factor 1
```

Run queries and record timings:

```bash
uv run tpch-mysql run \
  --host <host> --port <port> --user <user> --password <password> --database <db> \
  --all --output /tmp/tpch_timings.json
```

Run individual steps:

```bash
uv run tpch-mysql schema --host ... --port ... --user ... --password ... --database ...
uv run tpch-mysql gen    --data-dir /home/jason/wangsl/tpch --scale-factor 1
uv run tpch-mysql load   --host ... --port ... --user ... --password ... --database ... --data-dir /home/jason/wangsl/tpch
```

`load` defaults to `--ignore-header-lines 1` because `tpchgen` CSV output includes a header row.
