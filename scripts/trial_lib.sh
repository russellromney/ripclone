# Shared by trial_gitea.sh and trial_github.sh: checks, a ripclone server, and
# cleanup. Sourced, not run.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${RIPCLONE_BIN_DIR:-$ROOT/rust/target/release}"
for bin in ripclone ripclone-server; do
  test -x "$BIN/$bin" || {
    echo "error: missing $BIN/$bin; build with 'cargo build --release --bins' or set RIPCLONE_BIN_DIR" >&2
    exit 1
  }
done
command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 1; }

# Keep this machine's git config, and its credential helpers, out of the
# trial: a system osxkeychain helper would store the trial's tokens.
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0
export RIPCLONE_NO_METRICS=1

REAL_HOME="$HOME"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/ripclone-trial.XXXXXX")"
FAILS=0
PIDS=()
SECRETS=()

step() { printf '\n=== %s\n' "$*"; }
ok() { printf 'ok: %s\n' "$*"; }
bad() { printf 'FAIL: %s\n' "$*"; FAILS=$((FAILS + 1)); }
# Print stdin with every registered secret replaced.
scrub() {
  local line secret
  while IFS= read -r line; do
    for secret in "${SECRETS[@]}"; do line="${line//"$secret"/<token>}"; done
    printf '%s\n' "$line"
  done
}
# Run a command that is expected to fail and print its last lines.
expect_failure() {
  local out status
  out="$("$@" 2>&1)"; status=$?
  printf '%s\n' "$out" | tail -2 | scrub
  [ "$status" -ne 0 ] && ok "refused (exit $status)" || bad "succeeded: $*"
}

trial_cleanup() {
  local pid
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null; done
  for pid in "${PIDS[@]}"; do wait "$pid" 2>/dev/null; done
  [ -n "${KEEP_TRIAL:-}" ] && echo "kept $WORK" || rm -rf "$WORK"
}

# start_server <name> [env assignments...]: start a ripclone server with its
# own data, home, empty git config and local storage, on a port the OS
# picks. Sets SERVER_URL.
start_server() {
  local name="$1" log="$WORK/$1.log" dir="$WORK/$1"
  shift
  mkdir -p "$dir/home"
  # Local storage only. S3 settings in the environment (soup sets them) would
  # upload the trial's private repo content to a real bucket; the config's
  # backend = "local" wins over them.
  printf '[storage]\nbackend = "local"\n' >"$dir/config.toml"
  env HOME="$dir/home" RIPCLONE_CONFIG="$dir/config.toml" GIT_CONFIG_GLOBAL=/dev/null \
    RIPCLONE_POLL_INTERVAL_SECS=0 RUST_LOG=info "$@" \
    "$BIN/ripclone-server" --host 127.0.0.1 --port 0 --cas-dir "$dir/cas" \
    --repo-root "$dir/repos" --control-db "$dir/control.db" >"$log" 2>&1 &
  PIDS+=($!)
  SERVER_URL=""
  for _ in $(seq 1 100); do
    SERVER_URL="$(sed -n 's/.*ripclone server listening on \(http:[^ ]*\).*/\1/p' "$log" | head -1)"
    [ -n "$SERVER_URL" ] && curl -fsS -m 1 "$SERVER_URL/readyz" >/dev/null 2>&1 && break
    SERVER_URL=""
    sleep 0.1
  done
  [ -n "$SERVER_URL" ] || { bad "server $name did not start"; scrub <"$log" | tail -5; exit 1; }
  grep -q "using local storage" "$log" || { bad "server $name is not on local storage"; exit 1; }
}

# The user's shell: their own HOME and global git config.
use_user_home() {
  export HOME="$WORK/user" GIT_CONFIG_GLOBAL="$WORK/user/.gitconfig"
  mkdir -p "$HOME"
  git config --global user.name "Trial User"
  git config --global user.email trial@example.com
  cd "$HOME"
}

rc() { "$BIN/ripclone" "$@"; }

trial_finish() {
  step "server logs"
  local log
  for log in "$WORK"/*.log; do
    [ -e "$log" ] || continue
    grep -E " (WARN|ERROR) " "$log" | scrub | cut -c1-240 | head -10
    for secret in "${SECRETS[@]}"; do
      grep -qF "$secret" "$log" && bad "a token is in $(basename "$log")"
    done
  done
  printf '\nfailures: %s\n' "$FAILS"
  [ "$FAILS" -eq 0 ]
}
