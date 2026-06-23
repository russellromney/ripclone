# Contributing

ripclone is open to contributions. The project is licensed under the Elastic License 2.0; by contributing, you agree that your contributions will be under the same license.

## Getting started

```bash
cd rust
cargo build --release
cargo test
```

## What to work on

- Check [`ROADMAP.md`](ROADMAP.md) for current direction.
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

> Tests run in **parallel** (as in CI), so they must not depend on global
> process state (e.g. shared env vars) leaking between tests. `scripts/ci.sh flake`
> re-runs the suite to surface such races.

## Questions

Open an issue or start a discussion. We're happy to help.
