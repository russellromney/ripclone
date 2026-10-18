#!/usr/bin/env bash
# A user's trial of ripclone against private repos on a local, digest-pinned
# Gitea: provider setup, add by URL, full, depth-1, files and branch clones,
# push and pull with the user's git, a clone right after a push, and the
# refusals. Needs Docker, jq and release binaries (RIPCLONE_BIN_DIR, default
# rust/target/release). SRC (default this checkout) is pushed as the private
# repo; it needs at least six commits of history at HEAD. KEEP_TRIAL=1 keeps
# the work directory. Exits non-zero on any failed check.
set -o pipefail
source "$(dirname "${BASH_SOURCE[0]}")/trial_lib.sh"
command -v docker >/dev/null || { echo "error: Docker is required" >&2; exit 1; }

SRC="${SRC:-$ROOT}"
IMAGE="gitea/gitea:1.24.6@sha256:2edc102cbb636ae1ddac5fa0c715aa5b03079dee13ac6800b2cef6d4e912e718"
CONTAINER="ripclone-trial-gitea-$$"
cleanup() { docker rm -f "$CONTAINER" >/dev/null 2>&1; trial_cleanup; }
trap cleanup EXIT

step "Gitea with users ci and other, neither an admin"
docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null
docker run --rm -d --name "$CONTAINER" -p 127.0.0.1::3000 \
  -e GITEA__security__INSTALL_LOCK=true -e GITEA__database__DB_TYPE=sqlite3 \
  -e GITEA__server__HTTP_PORT=3000 -e GITEA__server__ROOT_URL=http://127.0.0.1:3000/ \
  -e GITEA__service__DISABLE_REGISTRATION=true "$IMAGE" >/dev/null
PORT="$(docker port "$CONTAINER" 3000/tcp | awk -F: 'NR==1 {print $NF}')"
GITEA="http://127.0.0.1:$PORT"
for _ in $(seq 1 60); do curl -fsS -m 2 "$GITEA/api/v1/version" >/dev/null 2>&1 && break; sleep 1; done
token_for() {
  docker exec -u git "$CONTAINER" gitea admin user create --username "$1" \
    --password "$1-password-123" --email "$1@example.com" --must-change-password=false >/dev/null
  curl -fsS -m 10 -X POST -H 'Content-Type: application/json' -u "$1:$1-password-123" \
    -d '{"name":"trial","scopes":["write:repository","write:user","read:repository","read:user"]}' \
    "$GITEA/api/v1/users/$1/tokens" | jq -r .sha1
}
CI_TOKEN="$(token_for ci)"
OTHER_TOKEN="$(token_for other)"
SECRETS=("$CI_TOKEN" "$OTHER_TOKEN")
for token in "$CI_TOKEN" "$OTHER_TOKEN"; do
  [ -n "$token" ] && [ "$token" != null ] || { bad "Gitea tokens"; exit 1; }
done
ok "$GITEA"
new_repo() { # new_repo <token> <name> [extra json]
  curl -fsS -m 10 -X POST -H "Authorization: token $1" -H 'Content-Type: application/json' \
    -d "{\"name\":\"$2\",\"private\":true,\"default_branch\":\"main\"$3}" "$GITEA/api/v1/user/repos" >/dev/null
}

step "private repo ci/app: HEAD of $SRC as main, HEAD~5 as release"
new_repo "$CI_TOKEN" app
# A large push over chunked HTTP breaks against Gitea in Docker.
AUTHED="http://ci:$CI_TOKEN@127.0.0.1:$PORT/ci/app.git"
git -C "$SRC" -c http.postBuffer=209715200 push -q "$AUTHED" \
  "$(git -C "$SRC" rev-parse HEAD):refs/heads/main" \
  "$(git -C "$SRC" rev-parse HEAD~5):refs/heads/release" 2>&1 | grep -v '^remote:' | scrub
TIP="$(git -C "$SRC" rev-parse HEAD)"
RELEASE="$(git -C "$SRC" rev-parse HEAD~5)"
COUNT="$(git -C "$SRC" rev-list --count HEAD)"
URL="$GITEA/ci/app.git"
[ "$(git ls-remote "$AUTHED" refs/heads/main | cut -f1)" = "$TIP" ] && ok "pushed $COUNT commits" || { bad "push"; exit 1; }
git ls-remote "$URL" >/dev/null 2>&1 && bad "ci/app is readable anonymously" || ok "anonymous reads refused"

step "server with ci's token"
export RIPCLONE_SERVER_TOKEN="$(openssl rand -hex 32)"
SECRETS+=("$RIPCLONE_SERVER_TOKEN")
start_server server "$(printf '[providers.gitea]\nkind = "gitea"\nhost = "%s"\ntoken = "%s"' "$GITEA" "$CI_TOKEN")"
export RIPCLONE_SERVER="$SERVER_URL"
ok "$SERVER_URL"

# The user's git holds ci's credentials, for pull, push and top-up.
use_user_home
git config --global credential.helper "store --file=$HOME/.git-credentials"
printf 'http://ci:%s@127.0.0.1:%s\n' "$CI_TOKEN" "$PORT" >"$HOME/.git-credentials"

step "provider add and test"
rc provider add gitea --kind gitea --host "$GITEA" >/dev/null && ok "provider added" || bad "provider add"
rc provider test gitea ci/app 2>&1 | scrub | grep -q "HEAD is $TIP" && ok "provider test reads HEAD" || bad "provider test"

step "add by URL, then a full clone into the default directory"
rc add "$URL" --wait 2>&1 | tail -1
rc clone "$URL" 2>&1 | tail -1
[ "$(git -C app rev-parse HEAD)" = "$TIP" ] && ok "HEAD is the upstream tip" || bad "HEAD"
[ "$(git -C app rev-list --count HEAD)" = "$COUNT" ] && ok "full history" || bad "history"
[ -z "$(git -C app status --porcelain)" ] && ok "status clean" || bad "status"
git -C app fsck --no-progress >/dev/null 2>&1 && ok "fsck clean" || bad "fsck"
[ "$(git -C app rev-parse --abbrev-ref '@{upstream}')" = origin/main ] && ok "main tracks origin/main" || bad "upstream"
[ "$(git -C app reflog --format='%H %gs')" = "$TIP clone: from $URL" ] && ok "reflog like git clone" || bad "reflog"
[ "$(git -C app config remote.origin.url)" = "$URL" ] && ok "origin is the upstream" || bad "origin url"

step "depth 1, files and a branch"
rc clone "$URL" app-shallow --depth 1 2>&1 | tail -1
[ "$(git -C app-shallow rev-parse --is-shallow-repository)" = true ] && ok "depth 1 is shallow" || bad "depth 1"
[ "$(git -C app-shallow config remote.origin.fetch)" = "+refs/heads/main:refs/remotes/origin/main" ] \
  && ok "depth 1 fetches only main" || bad "depth 1 refspec"
rc clone "$URL" app-files --mode files 2>&1 | tail -1
diff -rq --exclude=.git app app-files >/dev/null && ok "files tree equals the full checkout" || bad "files tree"
rc clone gitea:ci/app app-release --branch release 2>&1 | tail -1
[ "$(git -C app-release rev-parse HEAD)" = "$RELEASE" ] && ok "--branch release" || bad "--branch release"

step "push from one clone, pull in another"
echo "trial" >app/TRIAL.txt
git -C app add TRIAL.txt && git -C app commit -qm "Trial commit" && git -C app push -q 2>&1 | grep -v '^remote:'
NEW_TIP="$(git -C app rev-parse HEAD)"
[ "$NEW_TIP" != "$TIP" ] && [ "$(git ls-remote "$URL" refs/heads/main | cut -f1)" = "$NEW_TIP" ] \
  && ok "push reached Gitea" || bad "push"
git -C app-shallow pull -q --ff-only && [ "$(git -C app-shallow rev-parse HEAD)" = "$NEW_TIP" ] \
  && ok "depth 1 pulls" || bad "depth 1 pull"

step "a clone right after the push"
rc sync "$URL" 2>&1 | tail -1
rc clone "$URL" app-new 2>&1 | tail -1
[ "$(git -C app-new rev-parse HEAD 2>/dev/null)" = "$NEW_TIP" ] && ok "has the pushed commit" || bad "new clone"
git -C app-new fsck --no-progress >/dev/null 2>&1 && ok "fsck clean" || bad "fsck new clone"

step "refusals"
expect_failure rc clone gitea:ci/nope nope
[ ! -e nope ] && ok "no directory for a missing repo" || bad "nope written"
new_repo "$OTHER_TOKEN" secret ',"auto_init":true'
expect_failure rc add gitea:other/secret --wait
expect_failure rc clone gitea:other/secret secret
[ ! -e secret ] && ok "no directory for an unreadable repo" || bad "secret written"
expect_failure rc clone "$URL" app
expect_failure env RIPCLONE_SERVER_TOKEN=wrong-token "$BIN/ripclone" clone "$URL" wrong-token
[ ! -e wrong-token ] && ok "no directory with a wrong server token" || bad "wrong-token written"
start_server no-token "$(printf '[providers.gitea]\nkind = "gitea"\nhost = "%s"' "$GITEA")"
expect_failure env RIPCLONE_SERVER="$SERVER_URL" "$BIN/ripclone" add "$URL" --wait
expect_failure env RIPCLONE_SERVER="$SERVER_URL" "$BIN/ripclone" clone "$URL" no-token
[ ! -e no-token ] && ok "no directory from a server without a token" || bad "no-token written"

trial_finish
