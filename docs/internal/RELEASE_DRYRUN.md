# OSS release dry run

Use this checklist before the first public release and before any later release
that changes packaging or the wire protocol. Do not publish the final tag until
one release candidate passes every required row.

## 1. Prepare the release candidate

1. Set the package version in `rust/Cargo.toml`, for example `0.1.0-rc.1`.
2. Run `cargo update -w` in `rust/` so `Cargo.lock` has the same package version.
3. Commit those two changes and run the normal pull-request checks.
4. Confirm the tag you intend to create is exactly `v` plus that version.

The release workflow rejects a tag that does not match `rust/Cargo.toml`. A
release candidate must exercise every advertised publishing channel; do not
skip crates.io or PyPI and call the dry run complete.

Before tagging, record green results for:

```sh
bash scripts/audit_release_surface.sh
bash scripts/ci.sh all
cd rust && cargo +1.96.0 check --locked
```

Also record the current giant-repository concurrency proof and the private
large-history signed-URL expiry/refresh/resume proof. Those two rows use release
binaries built from the candidate commit.

## 2. Publish the candidate

Creating and pushing a tag is an operator action:

```sh
git tag v0.1.0-rc.1
git push origin v0.1.0-rc.1
```

Wait for `.github/workflows/release.yml` to finish. The GitHub Release must have
one archive and checksum for each supported platform:

- `linux-x86_64`
- `linux-arm64`
- `macos-x86_64`
- `macos-arm64`

Each archive must contain exactly `ripclone`, `ripclone-server`,
`ripclone-worker`, and the two license files. The release must also contain
`install.sh`. Confirm that crates.io and PyPI received the same candidate
version.

## 3. Test the shell installer

Run the installer on clean x86-64 and arm64 Linux systems and on clean x86-64
and arm64 macOS systems. Install system Git first because the server, worker,
and editable clone paths require it.

Example for Ubuntu:

```sh
docker run --rm -it ubuntu:24.04 bash -lc '
  apt-get update && apt-get install -y ca-certificates curl git &&
  curl -fsSL https://github.com/russellromney/ripclone/releases/download/v0.1.0-rc.1/install.sh | sh &&
  ~/.local/bin/ripclone --version &&
  ~/.local/bin/ripclone-server --version &&
  ~/.local/bin/ripclone-worker --help >/dev/null'
```

Repeat on Alpine with `apk add ca-certificates curl git`. Confirm checksum
verification succeeds and each program starts without a missing-library error.
On macOS, confirm the programs do not depend on Homebrew libraries.

Also run the installer without Git. It must print the documented warning. A
Files-only clone must remain usable; server, worker, and editable clone startup
must fail immediately with a clear system-Git error.

## 4. Test Cargo and PyPI installs

On both the minimum Rust version and the current stable version:

```sh
cargo install ripclone --version 0.1.0-rc.1 --locked
```

Confirm Cargo installs exactly `ripclone`, `ripclone-server`, and
`ripclone-worker`. The developer-only benchmark and proxy programs must not be
installed.

In a clean Python 3.12 environment on Linux x86-64 and both macOS
architectures:

```sh
python -m pip install ripclone==0.1.0rc1
ripclone --version
```

The install must use a wheel rather than compiling from an sdist. Run
`auditwheel show` on the Linux wheel and confirm it satisfies its declared
manylinux policy. Linux arm64 users install the release archive or use Cargo;
there is no Linux arm64 wheel in this release.

## 5. Test client/server compatibility

Run a candidate client against a candidate server and complete a real clone.
Then run one deliberate wire-version mismatch. The matching pair must work; the
mismatch must fail clearly and must not start a clone or build. No old wire
implementation should be present or selected.

If the CLI has a default public server, confirm that its `/v1/version` endpoint
is online and compatible before publishing the final release.

## 6. Test uninstall and retry

Follow the README uninstall instructions on one test system. Confirm the three
programs and the documented local state are removed.

Fix every failure, increment the candidate version, and repeat the complete
checklist. Publish `v0.1.0` only after one candidate passes every row without a
skip.
