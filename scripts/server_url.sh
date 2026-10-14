# shellcheck shell=bash
# Sourced by test scripts. Start ripclone-server with `--port 0` and
# RUST_LOG including `ripclone::server=info`, then:
#
#   SERVER_URL="$(server_url "$log" "$pid")" || fail
#
# Prints the bound URL from the server's "listening on" line once it answers
# /healthz. Returns 1 if the server exits or is not ready within 20 s. Binding
# port 0 avoids picking a port another process already holds.
server_url() { # log pid
  local _ url=""
  for _ in $(seq 1 200); do
    kill -0 "$2" 2>/dev/null || return 1
    if [ -z "$url" ]; then
      url="$(grep -o 'listening on http://[0-9.:]*' "$1" 2>/dev/null | head -1 | sed 's/^listening on //')"
    fi
    if [ -n "$url" ] && curl -fsS -o /dev/null "$url/healthz" 2>/dev/null; then
      echo "$url"
      return 0
    fi
    sleep 0.1
  done
  return 1
}
