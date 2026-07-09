# Provider-driven credential-broker selection + CLI host cleanup

## Goal

Close two GitHub-specific leaks in the ripclone Rust codebase while preserving the existing provider abstraction and the GitHub default behavior byte-for-byte.

1. Make `CredentialBroker` selection provider-driven in `rust/src/auth/broker.rs`.
2. Remove hardcoded GitHub host assumptions in `rust/src/bin/cli.rs` and resolve hosts from `ProviderRegistry` / `ProviderInstance`.

## Context

- `rust/src/provider.rs` already has `ProviderInstance` (host, clone URL, auth header) and `ProviderRegistry` (instances + static tokens). This design is sound and unchanged.
- `rust/src/auth/broker.rs` already has a provider-agnostic `CredentialBroker` trait, a `StaticBroker`, and a `GitHubAppBroker`. The trait is the right seam.
- The current leak is in `GitHubAppBroker::fetch_credential`, which branches on `!repo_id.is_github_default()`, and in `broker_from_env`, which selects the app broker globally based only on env vars.
- The CLI leak is in `provider_host`, `run_provider_add`, `run_provider_list`, `resolve_repo`, and the `Worktree` branch, which hardcode `"github.com"` or compare `provider == "github"`.

## Design

### 1. Provider-driven broker selection

Introduce a thin composite broker:

```rust
pub struct ProviderAwareBroker {
    registry: ProviderRegistry,
    dynamic: HashMap<String, Arc<dyn CredentialBroker>>,
    static_broker: StaticBroker,
}
```

- `ProviderAwareBroker::new(registry)` builds a `StaticBroker` from the registry for the fallback path.
- `register(provider_id, broker)` adds a provider-specific dynamic broker (e.g. `GitHubAppBroker`).
- `fetch_credential(repo_id, request_token)` looks up `repo_id.provider.as_str()` in `dynamic` and delegates; otherwise delegates to `StaticBroker`.

Rewrite `broker_from_env`:

```rust
pub fn broker_from_env(registry: ProviderRegistry) -> Result<Arc<dyn CredentialBroker>> {
    let mut broker = ProviderAwareBroker::new(registry);
    if let Some(config) = GitHubAppConfig::from_env()? {
        let app_id = config.app_id.clone();
        let installation_id = config.installation_id;
        let gh_broker = Arc::new(GitHubAppBroker::new(config)?);
        broker = broker.register("github", gh_broker);
        info!("using GitHub App credential broker (app_id={app_id}, installation_id={installation_id})");
    }
    Ok(Arc::new(broker))
}
```

`GitHubAppBroker` is not modified. When configured it is registered only under the `"github"` instance id, so its token-minting path runs for the same inputs as before. The `is_github_default()` guard inside it remains true for every call it receives.

A future `GitLabAppBroker` or `GiteaBroker` is added by constructing it and calling `.register("gitlab" | "my-gitea", ...)` in `broker_from_env`; the selection logic in `ProviderAwareBroker` needs no edit.

### 2. CLI host resolution

Load `ProviderRegistry` once in `main()`:

```rust
let provider_registry = ripclone::provider_config::load_registry_with_token_store(&token_store()?)
    .context("load provider registry")?;
```

Replace `provider_host` with a registry lookup:

```rust
fn provider_host(provider_id: &str, registry: &ProviderRegistry) -> Option<String> {
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

Update call sites:

- `resolve_repo(repo, default_provider, registry)` checks `registry.get(&provider).map(|p| p.kind == ProviderKind::GitHub).unwrap_or(false)` instead of `provider == "github"`. This correctly handles GitHub Enterprise instances configured with `kind = "github"` but a custom id/host.
- `run_provider_add` stops hardcoding preset hosts; preset kinds are allowed to omit `--host`, and `ProviderRegistry::merge_one` supplies the default.
- `run_provider_list` reads each provider’s host from `registry.get(id).host`.
- The `Worktree` branch checks the resolved provider kind instead of `default_provider == "github"`.

Ripclone’s own release-update URLs (`api.github.com/repos/russellromney/ripclone/releases/...`) are left untouched.

### 3. Testing

- **Broker selection unit test** in `rust/src/auth/broker.rs`:
  - Define a test-only `CredentialBroker` that records the `RepoId` and returns a sentinel token.
  - Register it for a non-github provider id.
  - Assert that fetching for that provider uses the registered broker.
  - Assert that fetching for `"github"` without a registered broker uses `StaticBroker`.
  - Assert that fetching for `"github"` with a registered `GitHubAppBroker` uses the app broker.
- **CLI host resolution unit test** in `rust/src/bin/cli.rs`:
  - Build a registry with a custom Gitea instance whose host is `https://gitea.example.com`.
  - Assert `provider_host("my-gitea", &registry) == Some("gitea.example.com")`.
  - Assert `provider_host("github", &registry) == Some("github.com")`.

### 4. Constraints preserved

- No webhook changes.
- No `ProviderInstance` clone-URL/auth changes.
- GitHub default path is behaviorally unchanged:
  - same `--token` / request-token precedence,
  - same `RIPCLONE_GITHUB_APP_*` env config,
  - same token minting and caching,
  - same logging.
- `cargo fmt --check` and targeted clippy clean.
- Lean testing only; no full release suite.

## Worktree

Implementation happens in branch `chore/provider-driven-broker` off `origin/main`, in a fresh git worktree at `../turbogit-chore-provider-driven-broker`.
