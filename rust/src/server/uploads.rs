//! A build's artifact uploads. Each upload starts as soon as its artifact is
//! in the CAS, while the build goes on, and the build waits for all of them
//! before it returns its result, so nothing is published before its
//! artifacts are durable. One server-wide limit bounds the uploads running
//! at once across all builds.

use crate::cancel::Cancel;
use crate::cas::Cas;
use crate::storage::StorageRef;
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Uploads running at once across every build. Each streams one artifact
/// from disk, so this bounds connections and open files, not memory.
const UPLOAD_SLOTS: usize = 32;

fn upload_slots() -> Arc<Semaphore> {
    static SLOTS: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();
    SLOTS
        .get_or_init(|| Arc::new(Semaphore::new(UPLOAD_SLOTS)))
        .clone()
}

pub(super) struct Uploads {
    cas: Cas,
    storage: StorageRef,
    runtime: tokio::runtime::Handle,
    cancel: Option<Cancel>,
    state: Mutex<State>,
    bytes: Arc<AtomicU64>,
}

#[derive(Default)]
struct State {
    started: Vec<String>,
    seen: HashSet<String>,
    tasks: JoinSet<Result<()>>,
}

impl Uploads {
    /// Uploads to `storage` from `cas`, for the build running on this task.
    pub(super) fn new(cas: &Cas, storage: &StorageRef) -> Arc<Self> {
        Arc::new(Self {
            cas: cas.detached(),
            storage: storage.clone(),
            runtime: tokio::runtime::Handle::current(),
            cancel: Cancel::current(),
            state: Mutex::default(),
            bytes: Arc::default(),
        })
    }

    /// `cas`, starting the upload of each object it stores.
    pub(super) fn cas(self: &Arc<Self>) -> Cas {
        let uploads: Weak<Self> = Arc::downgrade(self);
        self.cas.with_on_store(Arc::new(move |hash: &str| {
            if let Some(uploads) = uploads.upgrade() {
                uploads.start(hash);
            }
        }))
    }

    /// Start uploading `hash` unless it already started. Callable from any
    /// thread.
    pub(super) fn start(&self, hash: &str) {
        if hash.is_empty() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.seen.insert(hash.to_string()) {
            return;
        }
        state.started.push(hash.to_string());
        let (storage, path, hash) = (self.storage.clone(), self.cas.path(hash), hash.to_string());
        let (cancel, bytes) = (self.cancel.clone(), self.bytes.clone());
        state.tasks.spawn_on(
            async move {
                let _slot = upload_slots()
                    .acquire_owned()
                    .await
                    .context("upload slots closed")?;
                if let Some(cancel) = &cancel {
                    cancel.check()?;
                }
                let start = Instant::now();
                storage
                    .put_file_async(&hash, &path)
                    .await
                    .with_context(|| format!("upload artifact {hash}"))?;
                let len = tokio::fs::metadata(&path).await.map_or(0, |m| m.len());
                crate::perf::record_storage_upload(start.elapsed(), len);
                bytes.fetch_add(len, Ordering::Relaxed);
                Ok(())
            },
            &self.runtime,
        );
    }

    /// Start every one of `hashes` not yet started.
    pub(super) fn start_all<'a>(&self, hashes: impl IntoIterator<Item = &'a String>) {
        for hash in hashes {
            self.start(hash);
        }
    }

    /// Bytes of the uploads finished so far.
    pub(super) fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Wait for every upload. The first failure is returned and stops the
    /// rest. Returns every hash uploaded.
    pub(super) async fn finish(&self) -> Result<Vec<String>> {
        loop {
            let mut tasks = std::mem::take(
                &mut self
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .tasks,
            );
            if tasks.is_empty() {
                break;
            }
            while let Some(joined) = tasks.join_next().await {
                joined.context("upload task")??;
            }
        }
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(state.started.clone())
    }
}
