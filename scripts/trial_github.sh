#!/usr/bin/env bash
# A user's read-only trial of ripclone against private GitHub repos:
#
#   RIPCLONE_GITHUB_TOKEN=... scripts/trial_github.sh owner/private-repo...
#
# The server reads the repos with RIPCLONE_GITHUB_TOKEN. The user's git
# authenticates with their gh login (`gh auth git-credential`), or with
# RIPCLONE_TRIAL_GIT_TOKEN when set. Each repo gets add, full, depth-1 and
# files clones, compared with `git clone` (history, tree, local config,
# reflog), then pull and fetch. Nothing is pushed. Also checks a missing repo
# and a server without a token. Needs jq and release binaries
# (RIPCLONE_BIN_DIR, default rust/target/release). KEEP_TRIAL=1 keeps the work
# directory. Exits non-zero on any failed check.
set -o pipefail
source "$(dirname "${BASH_SOURCE[0]}")/trial_lib.sh"
[ -n "${RIPCLONE_GITHUB_TOKEN:-}" ] || { echo "error: set RIPCLONE_GITHUB_TOKEN" >&2; exit 2; }
[ $# -gt 0 ] || { echo "usage: $0 owner/private-repo..." >&2; exit 2; }
SECRETS=("$RIPCLONE_GITHUB_TOKEN" "${RIPCLONE_TRIAL_GIT_TOKEN:-$RIPCLONE_GITHUB_TOKEN}")
trap trial_cleanup EXIT

step "the repos are private"
for repo in "$@"; do
  git ls-remote "https://github.com/$repo.git" HEAD >/dev/null 2>&1 \
    && bad "$repo is readable anonymously" || ok "$repo refuses anonymous reads"
done

step "server with the GitHub token"
export RIPCLONE_SERVER_TOKEN="$(openssl rand -hex 32)"
SECRETS+=("$RIPCLONE_SERVER_TOKEN")
start_server server \
  RIPCLONE_PROVIDERS="{\"providers\":[{\"id\":\"github\",\"kind\":\"github\",\"host\":\"github.com\",\"token\":\"$RIPCLONE_GITHUB_TOKEN\"}]}"
export RIPCLONE_SERVER="$SERVER_URL"
ok "$SERVER_URL"

use_user_home
if [ -n "${RIPCLONE_TRIAL_GIT_TOKEN:-}" ]; then
  git config --global credential.helper "store --file=$HOME/.git-credentials"
  printf 'https://x-access-token:%s@github.com\n' "$RIPCLONE_TRIAL_GIT_TOKEN" >"$HOME/.git-credentials"
else
  # gh keeps its token in the login keychain, found through the real HOME.
  git config --global credential.https://github.com.helper "!HOME='$REAL_HOME' gh auth git-credential"
fi

for repo in "$@"; do
  name="${repo#*/}"
  url="https://github.com/$repo.git"
  step "$repo"
  tip="$(git ls-remote "$url" HEAD | cut -f1)"
  [ -n "$tip" ] && ok "the user's git reads it" || { bad "the user's git cannot read $repo"; continue; }
  rc add "$repo" --wait 2>&1 | tail -1
  rc clone "$url" 2>&1 | tail -1
  git clone -q "$url" "$name-git"
  [ "$(git -C "$name" rev-parse HEAD)" = "$tip" ] && ok "full clone at the tip" || bad "full clone tip"
  [ "$(git -C "$name" rev-list --count HEAD)" = "$(git -C "$name-git" rev-list --count HEAD)" ] \
    && ok "history equals git clone's" || bad "history"
  [ -z "$(git -C "$name" status --porcelain)" ] && ok "status clean" || bad "status"
  diff -rq --exclude=.git "$name" "$name-git" >/dev/null && ok "tree equals git clone's" || bad "tree"
  git -C "$name" fsck --no-progress >/dev/null 2>&1 && ok "fsck clean" || bad "fsck"
  diff <(git -C "$name" config --local --list | grep -v '^core.checkstat=') \
    <(git -C "$name-git" config --local --list) && ok "local config equals git clone's" || bad "config"
  diff <(git -C "$name" reflog --format='%H %gs') <(git -C "$name-git" reflog --format='%H %gs') \
    && ok "reflog equals git clone's" || bad "reflog"
  git -C "$name" pull -q --ff-only && ok "pull" || bad "pull"
  git -C "$name" fetch -q && ok "fetch" || bad "fetch"
  rc clone "$repo" "$name-shallow" --depth 1 2>&1 | tail -1
  [ "$(git -C "$name-shallow" rev-parse --is-shallow-repository)" = true ] \
    && [ "$(git -C "$name-shallow" rev-parse HEAD)" = "$tip" ] && ok "depth 1 at the tip" || bad "depth 1"
  git -C "$name-shallow" pull -q --ff-only && ok "depth 1 pulls" || bad "depth 1 pull"
  rc clone "$repo" "$name-files" --mode files 2>&1 | tail -1
  diff -rq --exclude=.git "$name-files" "$name-git" >/dev/null && ok "files tree equals git clone's" || bad "files tree"
done

step "refusals"
expect_failure rc add "$1-no-such-repo" --wait
start_server no-token
expect_failure env RIPCLONE_SERVER="$SERVER_URL" "$BIN/ripclone" add "$1" --wait
expect_failure env RIPCLONE_SERVER="$SERVER_URL" "$BIN/ripclone" clone "$1" no-token
[ ! -e no-token ] && ok "no directory from a server without a token" || bad "no-token written"

trial_finish
