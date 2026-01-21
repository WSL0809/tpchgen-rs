# tpchgen-rs

[![Apache licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/clflushopt/tpchgen-rs/blob/main/LICENSE
[actions-badge]: https://github.com/clflushopt/tpchgen-rs/actions/workflows/rust.yml/badge.svg
[actions-url]: https://github.com/clflushopt/tpchgen-rs/actions?query=branch%3Amain

Blazing fast [TPCH] benchmark data generator, in pure Rust with zero dependencies.

[TPCH]: https://www.tpc.org/tpch/

## Features

1. Blazing Speed 🚀
2. Obsessively Tested 📋
3. Fully parallel, streaming, constant memory usage 🧠

## Try it now

The easiest way to use this software is via the [`tpchgen-cli`] tool.

## Performance

[`tpchgen-cli`] is more than 10x faster than the next fastest TPCH generator we
know of. On a 2023 Mac M3 Max laptop, it easily generates data faster than can
be written to SSD. See [BENCHMARKS.md](./benchmarks/BENCHMARKS.md) for more
details on performance and benchmarking.

[`tpchgen-cli`]: ./tpchgen-cli/README.md


## Answers

The core `tpchgen` crate provides answers for queries 1 to 22 and for a scale factor
of 1. The answers exposed were derived from the [TPC-H Tools](https://www.tpc.org/)
official distribution.

## Testing

This crate has extensive tests to ensure correctness and produces exactly the
same, byte-for-byte output as the original [`dbgen`] implementation. We compare
the output of this crate with [`dbgen`] as part of every checkin. See
[TESTING.md](TESTING.md) for more details on testing methodology

## Crates

- [`tpchgen`](tpchgen): the core data generator logic for TPC-H. It has no
  dependencies and is easy to embed in other Rust project.

- [`tpchgen-cli`](tpchgen-cli) is a [`dbgen`] compatible CLI tool that generates
  benchmark dataset using multiple processes.

[`dbgen`]: https://github.com/electrum/tpch-dbgen

## Contributing

Pull requests are welcome. For major changes, please open an issue first for
discussion. See our [contributors guide](CONTRIBUTING.md) for more details.

## Architecture

Please see [architecture guide](ARCHITECTURE.md) for details on how the code
is structured.

## License

The project is licensed under the [APACHE 2.0](LICENSE) license.

## References

- The TPC-H Specification, see the specification [page](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
- The Original `dbgen` Implementation you must submit an official request to access the software `dbgen` at their official [website](https://www.tpc.org/tpch/)
