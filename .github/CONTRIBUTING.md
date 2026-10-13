# Contributing

ripclone is open to contributions. The project is dual-licensed under MIT and Apache-2.0; by contributing, you agree that your contributions will be dual-licensed under the same terms.

## Getting started

Requirements: the Rust toolchain pinned in `rust-toolchain.toml`,
`protoc`, `cmake` (for `zlib-ng`), and `git`. On Linux, `rust/.cargo/config.toml`
links with `mold`; `scripts/install-build-deps.sh` installs it. Without cmake,
build with `--no-default-features` to use stock zlib.

```bash
cd rust
cargo build --release
cargo test
```

## What to work on

- Current direction is in [`docs/internal/rebuild/PLAN.md`](../docs/internal/rebuild/PLAN.md)
  and open issues.
- Good first issues: tests, documentation, CLI ergonomics, and benchmark improvements.
- Larger work should start with a brief discussion in an issue or PR so we can align on direction.

## Submitting changes

1. Open an issue or PR describing the change.
2. Run the exact checks CI runs: **`scripts/ci.sh`** (lint + tests + e2e). Running
   this is the best way to avoid "passed locally, failed in CI" — it uses the
   same commands and the pinned toolchain (`rust-toolchain.toml`) as CI. Run a
   single stage with `scripts/ci.sh lint|test|e2e|flake`.
3. Add tests for new behavior.
4. Keep commits focused and the diff minimal.

## Questions

Open an issue or start a discussion. We're happy to help.
