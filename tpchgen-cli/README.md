# TPC-H Data Generator CLI

`tpchgen-cli` is a high-performance, parallel TPC-H data generator command line
tool

This tool is more than 10x faster than the next fastest TPCH generator we know
of (`duckdb`). On a 2023 Mac M3 Max laptop, it easily generates data faster than
can be written to SSD. See [BENCHMARKS.md] for more details on performance and
benchmarking.

[BENCHMARKS.md]: https://github.com/clflushopt/tpchgen-rs/blob/main/benchmarks/BENCHMARKS.md

* See the tpchgen [README.md](https://github.com/clflushopt/tpchgen-rs) for
project details
* Watch this [awesome demo](https://www.youtube.com/watch?v=UYIC57hlL14)  by
[@alamb](https://github.com/alamb) to see `tpchgen-cli` in action
* Read the companion blog post in the
[Datafusion
blog](https://datafusion.apache.org/blog/2025/04/10/fastest-tpch-generator/) to learn about the project's history
* Try it yourself by following the instructions below

## Install via `pip`

```shell
pip install tpchgen-cli
```

## Install via `uv`

```shell
uv tool install tpchgen-cli 
```

## Install via Rust

[Install Rust](https://www.rust-lang.org/tools/install) and compile

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
RUSTFLAGS='-C target-cpu=native' cargo install tpchgen-cli
```

## Examples

```shell
# Scale Factor 10, all tables, in `tbl`(csv like) format in the `sf10` directory
# (10GB, 8 files, 60M lineitem rows)
tpchgen-cli -s 10 --output-dir sf10

# Scale Factor 10, all tables, in CSV format (default delimiter `,`)
tpchgen-cli -s 10 --format=csv --output-dir sf10_csv

# Scale Factor 10, lineitem table, in tab-delimited CSV (TSV) format
tpchgen-cli -s 10 --tables lineitem --format=csv --delimiter '\\t' --output-dir sf10_tsv

# Scale Factor 10, partition 2 and 3 of 10 in sf10 directory
#
# partitioned/
# ├── lineitem
# │   ├── lineitem.2.tbl
# │   └── lineitem.3.tbl
# └── orders
#    ├── orders.2.tbl
#    └── orders.3.tbl
#     
for PART in `seq 2 3`; do
  tpchgen-cli --tables lineitem,orders --scale-factor=10 --output-dir partitioned --parts 10 --part $PART
done
```

## Performance

See [BENCHMARKS.md] for performance and benchmarking details.
