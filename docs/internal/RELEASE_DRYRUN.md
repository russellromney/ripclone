# Release dry-run checklist (F3)

Internal runbook for cutting a ripclone pre-release and proving the published
artifacts install and run on clean machines. Everything here is a **USER** step —
it cuts real tags, spins up clean OS containers, and audits published wheels.
The automatable repo-hygiene and identity checks are already done in the F3 PR;
what's left is the release-machinery verification below.

Depends on **F2** (static/vendored-C binaries or an install-time preflight, and
adding `ripclone-worker` to the release tarball). See the open items at the
bottom before trusting a green run.

## 0. Preconditions

- [ ] F2 merged: release binaries either link C deps statically/vendored or
      `install.sh` runs a preflight that prints the exact `apt`/`brew` line.
- [ ] Release identity confirmed: GitHub remote is `russellromney/ripclone`
      (verified in F3 — `install.sh`/README URLs already match). No repo rename
      needed.
- [ ] Working tree clean; `scripts/ci.sh all` green on the tag commit.

## 1. Cut a pre-release tag

```sh
git tag v0.1.0-rc.1
git push origin v0.1.0-rc.1
```

- [ ] `.github/workflows/release.yml` runs to completion for the tag.
- [ ] GitHub Release has, per platform (`linux-x86_64`, `macos-arm64`,
      `macos-x86_64`): `ripclone-<platform>.tar.gz` + `.sha256`.
- [ ] `install.sh` is attached to the release.
- [ ] `crates-io` job published (or was intentionally skipped for an rc).
- [ ] `pypi-publish` uploaded the wheels (or intentionally skipped for an rc).

## 2. Shell installer on a clean Ubuntu container

```sh
docker run --rm -it ubuntu:24.04 bash -lc '
  apt-get update && apt-get install -y curl ca-certificates &&
  curl -fsSL https://github.com/russellromney/ripclone/releases/download/v0.1.0-rc.1/install.sh | sh &&
  ~/.local/bin/ripclone --version'
```

- [ ] Installer downloads the tarball, verifies the checksum, installs binaries.
- [ ] `ripclone --version` prints a version (not a missing-shared-lib crash).
      If it fails on `libgit2`/`libssl`, F2's static build or preflight is not
      done — stop and finish F2.
- [ ] Repeat with a **minimal** base (`ubuntu:24.04` with nothing extra) to prove
      the preflight/static story, not just a fat image.

## 3. Shell installer on clean macOS

On a fresh macOS (or a VM with no dev tooling / no Homebrew libs):

```sh
curl -fsSL https://github.com/russellromney/ripclone/releases/download/v0.1.0-rc.1/install.sh | sh
~/.local/bin/ripclone --version
```

- [ ] Installs and runs on both arm64 (macos-14 asset) and x86_64 (macos-13).
- [ ] If it needs `brew install libgit2 openssl@3`, that requirement is printed
      by the preflight and documented in the README Install section.

## 4. pip wheel + manylinux audit

```sh
# clean container, no build toolchain
docker run --rm -it python:3.12-slim bash -lc '
  pip install ripclone==0.1.0rc1 &&
  ripclone --version'
```

- [ ] `pip install ripclone` pulls a **prebuilt wheel** (not an sdist that
      compiles) on Linux x86_64 and both macOS arches.
- [ ] The installed `ripclone` runs.
- [ ] manylinux audit on the Linux wheel:
      `pip download --no-deps ripclone==0.1.0rc1 && auditwheel show ripclone-*.whl`
      — the wheel is tagged `manylinux_2_28` and lists no disallowed external
      shared libraries beyond the manylinux policy allowlist. (This is the check
      F2's vendored-C build is meant to make pass; a dynamically-linked build
      will show `libgit2`/`libssl` as external and fail the audit.)

## 5. Version compatibility check against the dev server

```sh
ripclone version            # prints CLI + server versions and a compatibility verdict
```

- [ ] Point at a running dev/staging server (`--server` or `RIPCLONE_SERVER`) and
      confirm the verdict is "compatible" for the matching version, and a
      deliberate mismatch reports incompatible with an actionable message.

## 6. Uninstall

- [ ] Follow the README **Uninstall** section on one of the test machines and
      confirm it removes binaries + config/data with nothing left behind.

## 7. Repeat until boring

Fix whatever breaks, re-tag (`-rc.2`, …), rerun sections 2–6. Only cut the real
`v0.1.0` once an rc passes 2–6 clean on every platform.

---

### Known open items (as of F3)

- **F2 not yet reflected in release machinery.** `release.yml` (build + package
  steps) and `install.sh` copy only `ripclone`, `ripclone-server`,
  `git-remote-ripclone` — **`ripclone-worker` is missing from the tarball**, but
  the README Install line claims all four binaries. F2 owns adding the worker and
  the static/preflight work. Sections 2–4 will surface the dynamic-linking
  failures until F2 lands.
- The release binaries currently link libgit2/openssl/zstd **dynamically**
  (`release.yml` header). Until F2 changes that, the manylinux audit in section 4
  is expected to fail — do not treat that as a release blocker before F2.
