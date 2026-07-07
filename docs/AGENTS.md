# Agents & CI

ripclone's best-fit user is an **agent fleet on a giant repo** — hundreds of
throwaway VMs, each cloning a large repo, doing work, and being torn down. The
primitives for that path already exist (`--depth 1`, `--mode files`, `--temp`,
token-in-env); this page ties them into one coherent surface and gives you a
copy-paste quickstart.

## TL;DR

- Set `RIPCLONE_AGENT=1` on your fleet VMs. It flips the defaults to fleet-sane:
  **depth-1 history**, **never any interactive prompt**, cache reuse on.
- Put the token in the environment (`RIPCLONE_SERVER_TOKEN`). No `ripclone
  login` round-trip — the headless path needs no TTY.
- Choose your clone shape: **depth-1 editable** (default in agent mode) if the
  agent runs git commands, **`--mode files`** if it only reads/edits the working
  tree, **`--depth 0` (full history)** only when the agent genuinely walks
  history (rare).

## Agent mode: `RIPCLONE_AGENT=1`

`ripclone clone` defaults to a **full editable clone** (git-parity history) —
that is the right default for a human at a terminal. Agents are different: on a
1.3M-commit monorepo, an agent almost never wants full history, and it must
never block on a prompt.

Agent mode makes that explicit:

```bash
export RIPCLONE_AGENT=1
```

or, in `~/.config/ripclone/config.toml` (or a project `ripclone.toml`):

```toml
agent = true
```

When agent mode is on:

| Behavior            | Human default        | Agent-mode default          |
| ------------------- | -------------------- | --------------------------- |
| History depth       | full (`--depth 0`)   | **`--depth 1`** (HEAD only)  |
| Interactive prompts | may prompt on a TTY  | **never** (fail fast)        |
| Cache reuse         | on                   | on                          |
| `--temp` tmpfs      | opt-in               | opt-in, fully supported      |

The env var wins over the config default: `RIPCLONE_AGENT=0` turns agent mode
off even when the config sets `agent = true`.

**This is a deliberate, explicit switch — not a silent size-based heuristic.**
ripclone will never quietly downgrade a human's clone to depth-1 because a repo
looks big; that would surprise people. You opt in, per fleet.

Explicit flags still win over the agent default. `RIPCLONE_AGENT=1 ripclone
clone owner/repo --depth 0` gives you full history; a `[clone] depth` value in
config also overrides the agent default.

## Depth-1 vs files vs full

| Shape                        | You get                                    | Use it for                              |
| ---------------------------- | ------------------------------------------ | --------------------------------------- |
| `--depth 1` (editable)       | a real, shallow git repo (HEAD only)       | agents that run `git diff/commit/log`   |
| `--mode files`               | working tree only, no `.git`               | pure worktree agents; files-only CI     |
| `--depth 0` / full editable  | full-history editable git repo             | **humans**, or agents that walk history |

Rule of thumb: **full history is for humans.** Agents want depth-1 (if they use
git) or files (if they don't). Agent mode picks depth-1 for you; add `--mode
files` when the agent never touches `.git`.

## Ephemeral fleet VMs: `--temp`

For throwaway VMs, `--temp` materializes the working tree in memory (tmpfs) for
a fast, disposable clone. It does not survive a reboot — which is exactly right
for a VM you are about to destroy. Linux only.

```bash
RIPCLONE_AGENT=1 ripclone clone owner/repo --temp
```

## Fleet quickstart

Drop this into a CI job or an agent VM's provisioning script. It installs
ripclone, puts the token in the environment, and clones headless — no login, no
prompt, no TTY required.

```bash
# 1. Install (pinned release recommended for reproducible fleets)
curl -fsSL https://github.com/russellromney/ripclone/releases/latest/download/install.sh | sh

# 2. Point at your server + drop in the token (paste from your VM's secret store)
export RIPCLONE_SERVER=https://ripclone.com          # or your self-hosted URL
export RIPCLONE_SERVER_TOKEN=rc_xxx                  # agent token, from a secret
export RIPCLONE_AGENT=1                               # fleet-sane defaults

# 3a. Editable agent (runs git): depth-1 clone, headless
ripclone clone owner/repo

# 3b. Pure worktree agent (no git): files-only, fastest
ripclone clone owner/repo --mode files

# 3c. Ephemeral VM you'll tear down: in-memory tmpfs clone (Linux)
ripclone clone owner/repo --temp
```

For a **private** repo, add the upstream credential (never printed, sent as
`X-Upstream-Token`):

```bash
export RIPCLONE_UPSTREAM_TOKEN=ghp_xxx
ripclone clone my-org/private-repo
```

## Machine-parseable access errors

An agent fleet needs to tell "wait and retry" apart from "this needs a paid
plan." ripclone's non-2xx responses carry a JSON body `{ "error", "code" }` and
the CLI exits non-zero with an actionable hint. The paid-plan / paywall cases
carry the subscribe URL so a fleet can detect and route them without scraping
prose:

| Status | `code`           | Meaning / next step                         |
| ------ | ---------------- | ------------------------------------------- |
| 401    | —                | not authenticated → `ripclone login` / token |
| 402    | —                | paid plan required → subscribe at ripclone.com |
| 403    | `no_plan`        | org needs a plan → owner subscribes at ripclone.com |
| 403    | `no_access`      | you lack host access to this repo           |
| 404    | `repo_not_added` | repo not built yet → `ripclone add <repo>`  |
| 429    | —                | rate limited → back off and retry           |
| 502/503| —                | briefly unavailable → retry shortly         |

The managed cloud also sets an `X-Ripclone-Upgrade` header with an upgrade nudge
when relevant. Parse the exit code and the `code` field; treat 402/403-with-a-
subscribe-URL as "needs billing," not a transient failure.
