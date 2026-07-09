//! In-memory [`ComputeProvider`] for tests. Records every `ensure_worker` call.

use super::{ComputeProvider, WorkerSpec};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Mutex;

/// Records calls; never talks to a real platform.
pub struct MockProvider {
    calls: Mutex<Vec<WorkerSpec>>,
}

impl MockProvider {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }

    /// Snapshot of every recorded `ensure_worker` call, in order.
    pub fn calls(&self) -> Vec<WorkerSpec> {
        self.calls.lock().expect("mock calls lock").clone()
    }

    /// Drop recorded history.
    pub fn reset(&self) {
        self.calls.lock().expect("mock calls lock").clear();
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
        // Snapshot isolation: clone so later mutation of the caller's env does
        // not rewrite history.
        self.calls
            .lock()
            .expect("mock calls lock")
            .push(spec.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn spec(size: &str) -> WorkerSpec {
        let mut env = BTreeMap::new();
        env.insert("RIPCLONE_QUEUE".into(), "libsql".into());
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
            Some("libsql")
        );
        assert_eq!(calls[1].size_class, "large");

        // Snapshot isolation.
        s.env.insert("RIPCLONE_QUEUE".into(), "mutated".into());
        assert_eq!(
            mock.calls()[0]
                .env
                .get("RIPCLONE_QUEUE")
                .map(String::as_str),
            Some("libsql")
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
