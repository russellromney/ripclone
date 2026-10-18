//! Keeps storage deletion away from artifacts a build relies on.
//!
//! A build that finds an artifact already stored does not store it again:
//! the CAS keeps an intact object, and S3 storage skips an upload the bucket
//! has. Between that check and the publish that names the artifact, no row
//! references it, so a deletion could remove it under the build. A build
//! therefore pins each artifact before it checks for it, and keeps the pin
//! until it has published. Deletion skips a pinned artifact, and a pin waits
//! while its artifact is being deleted, so the build then stores it again.
//!
//! One server process owns its storage, so the pins are in memory.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex, PoisonError};

/// The server's pins and the artifacts being deleted.
#[derive(Default)]
pub struct Pins {
    state: Mutex<State>,
    deleted: Condvar,
}

#[derive(Default)]
struct State {
    /// Pins per artifact, across builds.
    pinned: HashMap<String, usize>,
    deleting: HashSet<String>,
}

impl Pins {
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    fn pin(&self, hash: &str) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while state.deleting.contains(hash) {
            state = self
                .deleted
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        *state.pinned.entry(hash.to_string()).or_default() += 1;
    }

    fn unpin(&self, hash: &str) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = state.pinned.get_mut(hash) {
            *count -= 1;
            if *count == 0 {
                state.pinned.remove(hash);
            }
        }
    }

    /// Start deleting `hash` unless a build pins it. Until
    /// [`end_delete`](Self::end_delete), pins of it wait.
    pub fn begin_delete(&self, hash: &str) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.pinned.contains_key(hash) {
            return false;
        }
        state.deleting.insert(hash.to_string())
    }

    pub fn end_delete(&self, hash: &str) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.deleting.remove(hash);
        self.deleted.notify_all();
    }
}

/// One build's pins, released when it is dropped.
pub struct PinSet {
    pins: Arc<Pins>,
    held: Mutex<HashSet<String>>,
}

impl PinSet {
    pub fn new(pins: &Arc<Pins>) -> Arc<Self> {
        Arc::new(Self {
            pins: Arc::clone(pins),
            held: Mutex::default(),
        })
    }

    /// Pin `hash` for this build. Waits while it is being deleted.
    pub fn pin(&self, hash: &str) {
        // Held while waiting, so another thread of this build that pins the
        // same artifact waits too instead of finding it already held.
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        if !held.contains(hash) {
            self.pins.pin(hash);
            held.insert(hash.to_string());
        }
    }

    /// Every artifact this build pinned.
    pub fn hashes(&self) -> Vec<String> {
        let held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        held.iter().cloned().collect()
    }
}

impl Drop for PinSet {
    fn drop(&mut self) {
        let held = self.held.get_mut().unwrap_or_else(PoisonError::into_inner);
        for hash in held.drain() {
            self.pins.unpin(&hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_pinned_artifact_is_not_deleted_until_every_build_releases_it() {
        let pins = Pins::new();
        let (a, b) = (PinSet::new(&pins), PinSet::new(&pins));
        a.pin("x");
        a.pin("x");
        b.pin("x");
        drop(a);
        assert!(!pins.begin_delete("x"), "b still pins it");
        drop(b);
        assert!(pins.begin_delete("x"));
        pins.end_delete("x");
    }

    #[test]
    fn a_pin_waits_for_its_artifact_to_be_deleted() {
        let pins = Pins::new();
        assert!(pins.begin_delete("x"));
        let build = PinSet::new(&pins);
        let pinning = {
            let build = Arc::clone(&build);
            std::thread::spawn(move || build.pin("x"))
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!pinning.is_finished(), "pinned during the delete");
        pins.end_delete("x");
        pinning.join().unwrap();
        assert_eq!(build.hashes(), ["x"]);
        assert!(!pins.begin_delete("x"));
    }
}
