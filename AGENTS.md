# Repository Guidelines

## Project Structure

This is a Rust workspace (`Cargo.toml`) with multiple crates:

- `tpchgen/`: core TPC-H data generation library (kept dependency-free by design).
- `tpchgen-cli/`: `dbgen`-compatible CLI.

Other notable paths:

- `tests/`: repository-level scripts (e.g. `tests/conformance.sh`).
- `benchmarks/`: benchmarking docs and helper scripts/patches.
- `patches/`: upstream/reference patches used for reproducibility.
 - `tpchgen-py/`: PyO3 / `maturin` Python package (excluded from the Rust workspace).

## Build, Test, and Development Commands

- `cargo build`: build the workspace (debug).
- `cargo build --release`: optimized build (recommended for perf work).
- `cargo test --workspace`: run all unit/integration/doc tests.
- `cargo run -p tpchgen-cli --release -- --help`: run the CLI from source.
- `cargo fmt --all`: format with rustfmt (toolchain pinned in `rust-toolchain.toml`).
- `cargo clippy --workspace --all-targets -- -D warnings`: lint with clippy.
- `typos`: spell-check (configured by `.typos.toml`).

## Coding Style & Naming Conventions

- Use `rustfmt` output as the source of truth; avoid manual formatting debates.
- Keep `tpchgen/` embeddable: avoid adding dependencies unless there is a strong reason.
- Follow Rust naming conventions (types `CamelCase`, functions/modules `snake_case`, CLI flags `kebab-case`).

## Testing Guidelines

- Tests use Rust’s built-in test harness (`cargo test`); there is no explicit coverage gate.
- Prefer adding tests close to the crate you change (unit tests in `src/` or integration tests in `*/tests/*.rs`).
- Run conformance checks when changing generation logic:
  - `./tests/conformance.sh` (requires Docker; writes to `/tmp`; compares against a `dbgen` image).
- See `TESTING.md` for byte-for-byte verification guidance and published checksums.

## Commit & Pull Request Guidelines

- Match existing commit style: short, imperative summaries, often with a type/scope prefix (e.g. `feat: ...`, `docs: ...`, `cli: ...`, `chore(deps): ...`).
- PRs should explain the change, link issues when applicable, and include benchmarks for performance-sensitive edits.
- Before requesting review: run `cargo fmt`, `cargo clippy ...`, `cargo test --workspace`, and `typos`.
