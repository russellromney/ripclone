//! In-memory [`ComputeProvider`] for tests. Records every `ensure_worker` call.

use super::{ComputeProvider, WorkerSpec};
use anyhow::{Result, bail};
use async_trait::async_trait;
use std::sync::Mutex;

/// Records calls; never talks to a real platform.
///
/// Optional fail-N mode: the next N `ensure_worker` calls return `Err` after
/// recording (or not) so autoscale backoff tests stay deterministic.
pub struct MockProvider {
    calls: Mutex<Vec<WorkerSpec>>,
    /// Remaining forced failures. Each failed call decrements by one.
    fail_remaining: Mutex<u32>,
    /// When true, failed calls are still appended to `calls` (observability).
    record_failures: Mutex<bool>,
}

impl MockProvider {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            fail_remaining: Mutex::new(0),
            record_failures: Mutex::new(true),
        }
    }

    /// Snapshot of every recorded `ensure_worker` call, in order.
    pub fn calls(&self) -> Vec<WorkerSpec> {
        self.calls.lock().expect("mock calls lock").clone()
    }

    /// Drop recorded history (does not clear fail-N state).
    pub fn reset(&self) {
        self.calls.lock().expect("mock calls lock").clear();
    }

    /// Force the next `n` `ensure_worker` calls to return `Err`.
    pub fn fail_next(&self, n: u32) {
        *self.fail_remaining.lock().expect("mock fail lock") = n;
    }

    /// Whether failed calls are still appended to [`Self::calls`] (default true).
    pub fn set_record_failures(&self, record: bool) {
        *self.record_failures.lock().expect("mock record lock") = record;
    }
}

impl Default for MockProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ComputeProvider for MockProvider {
    fn name(&self) -> &str {
        "mock"
    }

    async fn ensure_worker(&self, spec: &WorkerSpec) -> Result<()> {
        spec.validate()?;
        let should_fail = {
            let mut n = self.fail_remaining.lock().expect("mock fail lock");
            if *n > 0 {
                *n -= 1;
                true
            } else {
                false
            }
        };
        let record = if should_fail {
            *self.record_failures.lock().expect("mock record lock")
        } else {
            true
        };
        if record {
            // Snapshot isolation: clone so later mutation of the caller's env does
            // not rewrite history.
            self.calls
                .lock()
                .expect("mock calls lock")
                .push(spec.clone());
        }
        if should_fail {
            bail!("MockProvider: forced ensure_worker failure");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn spec(size: &str) -> WorkerSpec {
        let mut env = BTreeMap::new();
        env.insert("RIPCLONE_QUEUE".into(), "sqlite".into());
        WorkerSpec::new(size, env)
    }

    #[tokio::test]
    async fn records_ensure_worker_calls() {
        let mock = MockProvider::new();
        assert_eq!(mock.name(), "mock");

        let mut s = spec("small");
        mock.ensure_worker(&s).await.unwrap();
        mock.ensure_worker(&WorkerSpec::new(
            "large",
            BTreeMap::from([("FOO".into(), "bar".into())]),
        ))
        .await
        .unwrap();

        let calls = mock.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].size_class, "small");
        assert_eq!(
            calls[0].env.get("RIPCLONE_QUEUE").map(String::as_str),
            Some("sqlite")
        );
        assert_eq!(calls[1].size_class, "large");

        // Snapshot isolation.
        s.env.insert("RIPCLONE_QUEUE".into(), "mutated".into());
        assert_eq!(
            mock.calls()[0]
                .env
                .get("RIPCLONE_QUEUE")
                .map(String::as_str),
            Some("sqlite")
        );
    }

    #[tokio::test]
    async fn reset_clears_recorded_calls() {
        let mock = MockProvider::new();
        mock.ensure_worker(&spec("small")).await.unwrap();
        mock.reset();
        assert!(mock.calls().is_empty());
    }
}
