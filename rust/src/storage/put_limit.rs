//! A limit on PUTs to the bucket at once that follows the bucket's
//! throttling. A throttled PUT halves it, once per round of requests: the
//! PUTs already in flight when it halved were sent at the old limit, so
//! their throttling does not halve it again. Each limit's worth of PUTs that
//! succeed raises it by one, up to its maximum.

use std::sync::{Mutex, PoisonError};
use tokio::sync::{Semaphore, SemaphorePermit};

pub(super) struct PutLimit {
    slots: Semaphore,
    max: usize,
    state: Mutex<State>,
}

struct State {
    limit: usize,
    /// PUTs that succeeded since the limit last changed.
    successes: usize,
    /// Counts the halvings: a PUT sent before the last one is from an
    /// older round.
    round: u64,
    /// Slots taken out of the limit while held: each is dropped on release
    /// instead of returned.
    debt: usize,
}

/// One PUT's slot. Report how it went with [`Self::succeeded`] or
/// [`Self::throttled`]; dropping it otherwise just frees the slot.
pub(super) struct PutPermit<'a> {
    limit: &'a PutLimit,
    permit: Option<SemaphorePermit<'a>>,
    round: u64,
}

impl PutLimit {
    pub(super) fn new(max: usize) -> Self {
        let max = max.max(1);
        Self {
            slots: Semaphore::new(max),
            max,
            state: Mutex::new(State {
                limit: max,
                successes: 0,
                round: 0,
                debt: 0,
            }),
        }
    }

    pub(super) async fn acquire(&self) -> PutPermit<'_> {
        let permit = (self.slots.acquire().await).expect("the PUT limit is never closed");
        PutPermit {
            limit: self,
            round: self.state().round,
            permit: Some(permit),
        }
    }

    /// PUTs allowed at once now.
    #[cfg(test)]
    pub(super) fn current(&self) -> usize {
        self.state().limit
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl PutPermit<'_> {
    pub(super) fn succeeded(self) {
        let mut state = self.limit.state();
        state.successes += 1;
        if state.successes >= state.limit && state.limit < self.limit.max {
            state.successes = 0;
            state.limit += 1;
            if state.debt > 0 {
                state.debt -= 1;
            } else {
                self.limit.slots.add_permits(1);
            }
        }
    }

    /// Halve the limit, unless this PUT was sent before the last halving.
    /// Returns the limit now.
    pub(super) fn throttled(self) -> usize {
        let mut state = self.limit.state();
        if self.round == state.round {
            let limit = (state.limit / 2).max(1);
            let cut = state.limit - limit;
            state.limit = limit;
            state.successes = 0;
            state.round += 1;
            state.debt += cut - self.limit.slots.forget_permits(cut);
        }
        state.limit
    }
}

impl Drop for PutPermit<'_> {
    fn drop(&mut self) {
        let mut state = self.limit.state();
        if let Some(permit) = self.permit.take()
            && state.debt > 0
        {
            state.debt -= 1;
            permit.forget();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn throttling_halves_once_per_round() {
        let limit = PutLimit::new(32);
        let held: Vec<_> = futures::future::join_all((0..32).map(|_| limit.acquire())).await;
        let mut held = held.into_iter();
        assert_eq!(held.next().unwrap().throttled(), 16);
        // The rest were sent at 32: they do not halve it again.
        for permit in held.by_ref().take(10) {
            assert_eq!(permit.throttled(), 16);
        }
        drop(held);
        assert_eq!(limit.slots.available_permits(), 16);

        // A PUT sent at 16 halves it again.
        assert_eq!(limit.acquire().await.throttled(), 8);
        assert_eq!(limit.slots.available_permits(), 8);
        assert_eq!(limit.acquire().await.throttled(), 4);
        assert_eq!(limit.acquire().await.throttled(), 2);
        assert_eq!(limit.acquire().await.throttled(), 1);
        assert_eq!(limit.acquire().await.throttled(), 1);
        assert_eq!(limit.slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn successes_raise_it_back_to_the_maximum() {
        let limit = PutLimit::new(4);
        limit.acquire().await.throttled();
        limit.acquire().await.throttled();
        assert_eq!(limit.current(), 1);
        // One success at 1, two at 2, three at 3.
        for (successes, then) in [(1, 2), (2, 3), (3, 4)] {
            for _ in 0..successes {
                limit.acquire().await.succeeded();
            }
            assert_eq!(limit.current(), then);
            assert_eq!(limit.slots.available_permits(), then);
        }
        for _ in 0..10 {
            limit.acquire().await.succeeded();
        }
        assert_eq!(limit.current(), 4, "never past the maximum");
        assert_eq!(limit.slots.available_permits(), 4);
    }

    #[tokio::test]
    async fn slots_held_while_it_shrinks_are_not_returned() {
        let limit = PutLimit::new(4);
        let a = limit.acquire().await;
        let b = limit.acquire().await;
        let c = limit.acquire().await;
        // 4 -> 2 with three held: the free one goes now, one more on release.
        assert_eq!(limit.acquire().await.throttled(), 2);
        assert_eq!(limit.slots.available_permits(), 0);
        drop(a);
        assert_eq!(limit.slots.available_permits(), 0);
        drop(b);
        drop(c);
        assert_eq!(limit.slots.available_permits(), 2);
    }
}
