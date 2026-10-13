# Roadmap

## Simplification pass

A four-wave cleanup of `rust/src/server.rs` and related client code, aimed at removing indirection that has built up in production control flow.

1. Server test hooks collapsed into one seam: a `TestStage` enum + a single `test_hook(stage).await` call form, replacing 28 per-stage `admission_test_*` functions and the separate file-barrier helpers. [this PR]
2. One admission function behind `sync_repo_inner` / `sync_repo_at_revision` / `get_ref_inner` tail / `trigger_build`, and one `RefResponse` builder behind `ref_response` / `ref_response_from_manifest` / `sync_response_without_storage_read`.
3. Dead surface: the cfg-dead non-control branch in `enqueue_admitted_build`, the always-zero `build_queue_depth`, ~27 `git.rs` pub fns with no external caller, and one-impl traits `MetaDb`/`QueueDb`/`AccessVerifier`.
4. Client: hooks and three zero-caller pub fetch fns, after PRs 171/173/174 land.
