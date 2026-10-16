# usage: srv_setup.sh REV...  -- server box: deps, rust, a worktree per REV under /opt/rc/v/<rev>
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q build-essential pkg-config protobuf-compiler git curl sqlite3 jq cmake libssl-dev libgit2-dev libfuse-dev mold python3
snap install aws-cli --classic
mkdir -p /opt/rc/v /data/run && cd /opt/rc
[ -d src ] || git clone -q https://github.com/russellromney/ripclone src
git -C src fetch -q origin
for rev in "$@"; do [ -d /opt/rc/v/$rev ] || git -C src worktree add -f /opt/rc/v/$rev $rev; done
[ -x /root/.cargo/bin/rustup ] || curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none --profile minimal
[ -f /root/bench_token ] || head -c 32 /dev/urandom | base64 | tr -dc A-Za-z0-9 > /root/bench_token
echo SETUP_DONE
