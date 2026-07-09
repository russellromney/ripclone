# Provider-driven broker + CLI host cleanup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make credential-broker selection provider-driven and remove hardcoded GitHub host assumptions from the CLI, with zero behavior change for the GitHub default path.

**Architecture:** Add a composite `ProviderAwareBroker` that dispatches by provider instance id (fallback to `StaticBroker`), and load `ProviderRegistry` once in the CLI so host/kind lookups replace hardcoded strings.

**Tech Stack:** Rust, Cargo, `anyhow`, `secrecy`, `clap`, git worktree at `../turbogit-chore-provider-driven-broker`, branch `chore/provider-driven-broker` off `origin/main`.

## Global Constraints

- ZERO behavior change for the GitHub default path.
- Do not touch the webhook layer or `ProviderInstance` clone-URL/auth logic.
- Ripclone’s own GitHub release URLs stay hardcoded.
- Add unit tests for broker selection and CLI host resolution.
- Run `cargo fmt --check` and targeted `cargo clippy`.
- Lean testing only; no full release suite / no `--all-targets`.
- Open a PR; do not merge.

---

## File map

- `rust/src/auth/broker.rs` — add `ProviderAwareBroker`, rewrite `broker_from_env`, add selection tests.
- `rust/src/bin/cli.rs` — load registry in `main`, resolve host from registry, resolve GitHub shape from provider kind, clean up provider add/list, add/update tests.

---

### Task 1: Add `ProviderAwareBroker` in `broker.rs`

**Files:**
- Modify: `rust/src/auth/broker.rs`

**Interfaces:**
- Consumes: `CredentialBroker` trait, `StaticBroker`, `GitHubAppBroker`, `ProviderRegistry`, `RepoId`.
- Produces: `ProviderAwareBroker::new`, `ProviderAwareBroker::register`, `ProviderAwareBroker as CredentialBroker`, updated `broker_from_env`.

- [ ] **Step 1: Add the composite broker struct and impl before `broker_from_env`**

Insert the following immediately before `pub fn broker_from_env` (around line 343):

```rust
/// Provider-driven broker dispatch.
///
/// Holds a provider-agnostic `StaticBroker` fallback and a map of provider
/// instance ids to dynamic brokers (e.g. `GitHubAppBroker`). Selection is
/// resolved per `RepoId`, so adding a new provider-specific broker is only a
/// registration change, not a change to the dispatch logic.
pub struct ProviderAwareBroker {
    registry: ProviderRegistry,
    dynamic: HashMap<String, Arc<dyn CredentialBroker>>,
    static_broker: StaticBroker,
}

impl ProviderAwareBroker {
    pub fn new(registry: ProviderRegistry) -> Self {
        let static_broker = StaticBroker::new(registry.clone());
        Self {
            registry,
            dynamic: HashMap::new(),
            static_broker,
        }
    }

    /// Register a dynamic broker for a specific provider instance id.
    pub fn register(
        mut self,
        provider_id: impl Into<String>,
        broker: Arc<dyn CredentialBroker>,
    ) -> Self {
        self.dynamic.insert(provider_id.into(), broker);
        self
    }

    fn broker_for(&self, repo_id: &RepoId) -> &dyn CredentialBroker {
        self.dynamic
            .get(repo_id.provider.as_str())
            .map(|b| b.as_ref())
            .unwrap_or(&self.static_broker)
    }
}

impl CredentialBroker for ProviderAwareBroker {
    fn fetch_credential(
        &self,
        repo_id: &RepoId,
        request_token: Option<&SecretString>,
    ) -> Result<Option<SecretString>> {
        self.broker_for(repo_id)
            .fetch_credential(repo_id, request_token)
    }
}
```

- [ ] **Step 2: Rewrite `broker_from_env` to use the composite broker**

Replace the existing function body with:

```rust
pub fn broker_from_env(registry: ProviderRegistry) -> Result<Arc<dyn CredentialBroker>> {
    let mut broker = ProviderAwareBroker::new(registry);
    if let Some(config) = GitHubAppConfig::from_env()? {
        let app_id = config.app_id.clone();
        let installation_id = config.installation_id;
        let gh_broker = Arc::new(GitHubAppBroker::new(config)?);
        broker = broker.register("github", gh_broker);
        info!(
            "using GitHub App credential broker (app_id={app_id}, installation_id={installation_id})"
        );
    }
    Ok(Arc::new(broker))
}
```

- [ ] **Step 3: Verify `cargo check` for the library**

Run:
```bash
cd rust && cargo check --lib
```

Expected: clean compile.

- [ ] **Step 4: Commit**

```bash
git add rust/src/auth/broker.rs
git commit -m "refactor(auth): provider-aware credential broker dispatch"
```

---

### Task 2: Add broker selection unit tests

**Files:**
- Modify: `rust/src/auth/broker.rs` (test module)

**Interfaces:**
- Consumes: `ProviderAwareBroker`, a test `CredentialBroker` implementation.
- Produces: passing unit tests proving provider-driven selection.

- [ ] **Step 1: Add a recording test broker**

Add inside the `#[cfg(test)] mod tests` block:

```rust
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingBroker {
        calls: AtomicUsize,
        token: SecretString,
    }

    impl RecordingBroker {
        fn new(token: &str) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                token: SecretString::from(token),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl CredentialBroker for RecordingBroker {
        fn fetch_credential(
            &self,
            _repo_id: &RepoId,
            _request_token: Option<&SecretString>,
        ) -> Result<Option<SecretString>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(self.token.clone()))
        }
    }
```

- [ ] **Step 2: Add selection tests**

Add the tests:

```rust
    #[test]
    fn provider_aware_dispatches_to_registered_broker() {
        let registry = ProviderRegistry::new();
        let custom = Arc::new(RecordingBroker::new("custom-token"));
        let broker = ProviderAwareBroker::new(registry).register("my-gitea", custom.clone());

        let repo = RepoId {
            provider: ProviderInstanceId::new("my-gitea"),
            path: "org/repo".to_string(),
        };
        let token = broker
            .fetch_credential(&repo, None)
            .unwrap()
            .expect("token from registered broker");
        assert_eq!(token.expose_secret(), "custom-token");
        assert_eq!(custom.call_count(), 1);
    }

    #[test]
    fn provider_aware_falls_back_to_static_broker() {
        let registry = ProviderRegistry::new();
        let custom = Arc::new(RecordingBroker::new("custom-token"));
        let broker = ProviderAwareBroker::new(registry).register("my-gitea", custom.clone());

        // A provider with no registered dynamic broker → StaticBroker → None.
        let repo = RepoId {
            provider: ProviderInstanceId::new("gitlab"),
            path: "group/proj".to_string(),
        };
        assert!(broker.fetch_credential(&repo, None).unwrap().is_none());
        assert_eq!(custom.call_count(), 0);
    }

    #[test]
    fn provider_aware_uses_github_app_for_github_instance() {
        let registry = ProviderRegistry::new();
        let gh_broker = Arc::new(RecordingBroker::new("gh-app-token"));
        let broker = ProviderAwareBroker::new(registry).register("github", gh_broker.clone());

        let token = broker
            .fetch_credential(&RepoId::github("o/r"), None)
            .unwrap()
            .expect("github app token");
        assert_eq!(token.expose_secret(), "gh-app-token");
        assert_eq!(gh_broker.call_count(), 1);
    }
```

- [ ] **Step 3: Run the broker tests**

Run:
```bash
cd rust && cargo test --lib auth::broker
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add rust/src/auth/broker.rs
git commit -m "test(auth): provider-aware broker selection"
```

---

### Task 3: Load `ProviderRegistry` once in `cli.rs` and update `provider_host`

**Files:**
- Modify: `rust/src/bin/cli.rs`

**Interfaces:**
- Consumes: `ProviderRegistry`, `provider_config::load_registry_with_token_store`.
- Produces: `provider_host(provider_id, registry)`, `clean_host(host)`, registry loaded in `main`.

- [ ] **Step 1: Load the registry in `main()`**

After the `server` and `default_provider` lines in `main()` (around line 918), add:

```rust
    let store = token_store().context("initialize token store")?;
    let provider_registry = ripclone::provider_config::load_registry_with_token_store(&store)
        .context("load provider registry")?;
```

Remove the later `let store = token_store()?` calls inside `resolve_upstream_token` and `run_provider_list` by passing the registry instead.

- [ ] **Step 2: Replace `provider_host` with registry-driven lookup**

Replace the existing `provider_host` function and add `clean_host`:

```rust
fn provider_host(
    provider_id: &str,
    registry: &ripclone::provider::ProviderRegistry,
) -> Option<String> {
    registry.get(provider_id).map(|p| clean_host(&p.host))
}

fn clean_host(host: &str) -> String {
    let h = host.trim_end_matches('/');
    h.strip_prefix("https://")
        .or_else(|| h.strip_prefix("http://"))
        .unwrap_or(h)
        .to_string()
}
```

- [ ] **Step 3: Update `resolve_upstream_token` to accept the registry**

Change the signature to:

```rust
async fn resolve_upstream_token(
    provider_id: &str,
    repo_path: &str,
    override_token: Option<&str>,
    registry: &ripclone::provider::ProviderRegistry,
) -> Result<Option<String>> {
```

Remove the internal `load_registry_with_token_store` call and use the passed `registry`:

```rust
    if let Some(host) = provider_host(provider_id, registry)
        && let Some(token) = git_credential_token(&host, repo_path).await?
    {
        return Ok(Some(token));
    }

    Ok(registry
        .token(provider_id)
        .map(|token| token.expose_secret().to_string()))
}
```

- [ ] **Step 4: Update call sites of `resolve_upstream_token`**

In the `Sync` and `Clone` branches, change:

```rust
let upstream_token =
    resolve_upstream_token(&provider, &repo_path, args.token.as_deref()).await?;
```

to:

```rust
let upstream_token =
    resolve_upstream_token(&provider, &repo_path, args.token.as_deref(), &provider_registry)
        .await?;
```

- [ ] **Step 5: Verify `cargo check --bin ripclone`**

Run:
```bash
cd rust && cargo check --bin ripclone
```

Expected: compile errors from unresolved `resolve_repo` calls (fixed in next task).

- [ ] **Step 6: Commit the partial plumbing**

```bash
git add rust/src/bin/cli.rs
git commit -m "refactor(cli): load provider registry once and resolve host from it"
```

---

### Task 4: Resolve GitHub-shaped repo paths by provider kind

**Files:**
- Modify: `rust/src/bin/cli.rs`

**Interfaces:**
- Consumes: `ProviderRegistry`, `ProviderKind`.
- Produces: `resolve_repo(repo, default_provider, registry)`.

- [ ] **Step 1: Update `resolve_repo` signature and logic**

Change:

```rust
fn resolve_repo(repo: &str, default_provider: &str) -> Result<(String, String)> {
```

to:

```rust
fn resolve_repo(
    repo: &str,
    default_provider: &str,
    registry: &ripclone::provider::ProviderRegistry,
) -> Result<(String, String)> {
```

Update the body:

```rust
    let (provider_override, path) = parse_repo_arg(repo);
    let provider = provider_override.unwrap_or_else(|| default_provider.to_string());
    let is_github = registry
        .get(&provider)
        .map(|p| p.kind == ProviderKind::GitHub)
        .unwrap_or(false);
    let repo_path = if is_github {
        let (owner, name) = parse_repo(&path)?;
        format!("{owner}/{name}")
    } else {
        path
    };
    Ok((provider, repo_path))
}
```

- [ ] **Step 2: Update all `resolve_repo` call sites**

For each of `Sync`, `Clone`, `Cat`, `Snapshot::Create`, `Prefetch`, `BuildArchive`, `Worktree` (repo arg), and `TrainDictionary`, pass `&provider_registry` as the third argument:

```rust
let (provider, repo_path) = resolve_repo(&repo, &default_provider, &provider_registry)?;
```

- [ ] **Step 3: Update the `Worktree` `--repo` fallback branch**

Change:

```rust
                    if default_provider == "github" {
```

to:

```rust
                    let is_github = provider_registry
                        .get(&default_provider)
                        .map(|p| p.kind == ProviderKind::GitHub)
                        .unwrap_or(false);
                    if is_github {
```

- [ ] **Step 4: Update existing `resolve_repo` unit tests**

Update the tests to construct a registry and pass it:

```rust
    fn test_registry() -> ripclone::provider::ProviderRegistry {
        ripclone::provider::ProviderRegistry::new()
    }

    #[test]
    fn resolve_repo_defaults_to_github() {
        let registry = test_registry();
        let (provider, repo_path) = resolve_repo("oven-sh/bun", "github", &registry).unwrap();
        assert_eq!(provider, "github");
        assert_eq!(repo_path, "oven-sh/bun");
    }

    #[test]
    fn resolve_repo_overrides_provider_from_prefix() {
        let registry = test_registry();
        let (provider, repo_path) =
            resolve_repo("gitlab:oven-sh/bun", "github", &registry).unwrap();
        assert_eq!(provider, "gitlab");
        assert_eq!(repo_path, "oven-sh/bun");
    }

    #[test]
    fn resolve_repo_preserves_non_github_path() {
        let registry = test_registry();
        let (provider, repo_path) =
            resolve_repo("group/sub/repo", "gitlab", &registry).unwrap();
        assert_eq!(provider, "gitlab");
        assert_eq!(repo_path, "group/sub/repo");
    }
```

- [ ] **Step 5: Verify `cargo check --bin ripclone`**

Run:
```bash
cd rust && cargo check --bin ripclone
```

Expected: clean compile.

- [ ] **Step 6: Commit**

```bash
git add rust/src/bin/cli.rs
git commit -m "refactor(cli): resolve github-shaped repo paths by provider kind"
```

---

### Task 5: Clean up `provider add` and `provider list`

**Files:**
- Modify: `rust/src/bin/cli.rs`

**Interfaces:**
- Consumes: `ProviderRegistry` defaults.
- Produces: `run_provider_add` without preset host defaults, `run_provider_list` using registry host.

- [ ] **Step 1: Remove hardcoded preset hosts from `run_provider_add`**

Replace the host defaulting block in `run_provider_add`:

```rust
    let host = match host {
        Some(h) => Some(h),
        None => match kind_parsed {
            ProviderKind::GitHub => Some("github.com".to_string()),
            ProviderKind::GitLab => Some("gitlab.com".to_string()),
            ProviderKind::Bitbucket => Some("bitbucket.org".to_string()),
            ProviderKind::Gitea | ProviderKind::Generic => None,
        },
    };
```

with:

```rust
    // Preset kinds (GitHub/GitLab/Bitbucket) get their default host from
    // ProviderRegistry::merge_one when the registry is loaded; custom hosts
    // are still honored if supplied.
    let host = host;
```

Or simply remove the block and use the existing `host` variable.

- [ ] **Step 2: Update `run_provider_list` to resolve host from registry**

The function already builds `registry`. Replace the host line:

```rust
        let host = entry.host.as_deref().unwrap_or("-");
```

with:

```rust
        let host = registry
            .get(id)
            .map(|p| clean_host(&p.host))
            .unwrap_or_else(|| entry.host.clone().unwrap_or_else(|| "-".to_string()));
```

Update the print to use `host` as a `String`.

- [ ] **Step 3: Verify `cargo check --bin ripclone`**

Run:
```bash
cd rust && cargo check --bin ripclone
```

Expected: clean compile.

- [ ] **Step 4: Commit**

```bash
git add rust/src/bin/cli.rs
git commit -m "refactor(cli): provider add/list use registry defaults and resolved host"
```

---

### Task 6: Add/update CLI unit tests

**Files:**
- Modify: `rust/src/bin/cli.rs` (test module)

**Interfaces:**
- Consumes: `provider_host`, `clean_host`, `ProviderRegistry`.
- Produces: passing tests for custom-provider host and GitHub default host.

- [ ] **Step 1: Replace preset-host tests with registry-driven tests**

Replace `provider_host_uses_preset_defaults` with:

```rust
    #[test]
    fn provider_host_resolves_from_registry() {
        let mut registry = ripclone::provider::ProviderRegistry::new();
        registry.merge_one(ripclone::provider::ProviderConfig {
            id: "my-gitea".to_string(),
            kind: Some("gitea".to_string()),
            host: Some("https://gitea.example.com".to_string()),
            token: None,
            auth_template: None,
            auth_header_name: None,
        }).unwrap();

        assert_eq!(
            provider_host("my-gitea", &registry),
            Some("gitea.example.com".to_string())
        );
        assert_eq!(
            provider_host("github", &registry),
            Some("github.com".to_string())
        );
    }
```

- [ ] **Step 2: Add a test for `clean_host` edge cases**

```rust
    #[test]
    fn clean_host_strips_scheme_and_trailing_slash() {
        assert_eq!(clean_host("https://gitea.example.com"), "gitea.example.com");
        assert_eq!(clean_host("http://gitea.example.com/"), "gitea.example.com");
        assert_eq!(clean_host("gitea.example.com:3000/"), "gitea.example.com:3000");
        assert_eq!(clean_host("github.com"), "github.com");
    }
```

- [ ] **Step 3: Run CLI unit tests**

Run:
```bash
cd rust && cargo test --bin ripclone
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add rust/src/bin/cli.rs
git commit -m "test(cli): host resolution from registry"
```

---

### Task 7: Format, clippy, and lean test pass

**Files:**
- Modify: `rust/src/auth/broker.rs`, `rust/src/bin/cli.rs` as needed.

- [ ] **Step 1: Run `cargo fmt`**

```bash
cd rust && cargo fmt
```

- [ ] **Step 2: Verify fmt check passes**

```bash
cd rust && cargo fmt --check
```

Expected: no output / exit 0.

- [ ] **Step 3: Run targeted clippy**

```bash
cd rust && cargo clippy --bin ripclone --lib -- -D warnings
```

Expected: clean.

- [ ] **Step 4: Run lean tests**

```bash
cd rust && cargo test --lib auth::broker
```

```bash
cd rust && cargo test --bin ripclone
```

Expected: all pass.

- [ ] **Step 5: Commit any fmt/clippy fixes**

```bash
git add rust/src/auth/broker.rs rust/src/bin/cli.rs
git commit -m "style: fmt and clippy"
```

---

### Task 8: Push branch and open PR

**Files:**
- None (git/PR operations).

- [ ] **Step 1: Push the branch**

```bash
git push -u origin chore/provider-driven-broker
```

- [ ] **Step 2: Open a PR**

Use `gh pr create`:

```bash
gh pr create \
  --base main \
  --head chore/provider-driven-broker \
  --title "refactor(auth,cli): provider-driven broker selection and CLI host cleanup" \
  --body "Closes the GitHub-specific leaks in broker selection and CLI host resolution.

- Introduces ProviderAwareBroker to dispatch CredentialBroker by provider instance id.
- GitHubAppBroker is registered only for the github default instance; behavior is unchanged.
- CLI loads ProviderRegistry once and resolves host/kind from it instead of hardcoded strings.
- Adds unit tests for provider-aware broker selection and CLI host resolution.

No webhook or ProviderInstance auth logic changes."
```

- [ ] **Step 3: Report PR URL**

Capture the PR URL from `gh pr create` output and report it.

---

## Spec coverage self-check

| Spec requirement | Task |
|---|---|
| Provider-driven broker selection | Task 1, Task 2 |
| GitHubAppBroker behavior unchanged | Task 1 (no source edits) |
| StaticBroker fallback | Task 1 `ProviderAwareBroker` fallback |
| Future broker drop-in via registration | Task 1 `register` API |
| CLI host resolved from registry | Task 3 `provider_host` |
| GitHub default preserved | Task 4 kind check, Task 6 tests |
| `run_provider_add` no hardcoded hosts | Task 5 |
| `run_provider_list` resolved host | Task 5 |
| Release URLs untouched | No code changes to `run_update` |
| Unit tests for broker selection | Task 2 |
| Unit tests for CLI host | Task 6 |
| fmt/clippy clean | Task 7 |
| PR opened, not merged | Task 8 |
