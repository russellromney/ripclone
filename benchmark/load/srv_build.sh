# usage: srv_build.sh REV...  -- release-build ripclone + ripclone-server per REV into /opt/rc/bin/<rev>, one at a time
set -uo pipefail
. /root/.cargo/env
for rev in "$@"; do
  cd /opt/rc/v/$rev/rust
  rustup show active-toolchain >/dev/null 2>&1 || rustup toolchain install
  s=$(date +%s)
  if cargo build --release --locked --bin ripclone --bin ripclone-server > /root/build_$rev.log 2>&1; then
    mkdir -p /opt/rc/bin/$rev; cp target/release/ripclone target/release/ripclone-server /opt/rc/bin/$rev/
    echo "BUILD_OK $rev $(( $(date +%s) - s ))s"
  else echo "BUILD_FAIL $rev $(( $(date +%s) - s ))s"; tail -20 /root/build_$rev.log; fi
done
echo BUILDS_DONE
