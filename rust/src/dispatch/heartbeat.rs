//! Dead-man's switch: an external heartbeat monitor (healthchecks.io,
//! Cronitor, …) alerts on *silence*. The dispatcher pings it every *healthy*
//! reconcile cycle and stops pinging once it detects an outage its own
//! retries can't fix — so the external monitor's next missed check is the
//! alert. No `/metrics`, no dashboards: just a GET the operator already has
//! tooling to watch.
//!
//! Config: `RIPCLONE_HEARTBEAT_URL` (optional). Unset -> [`DeadMansSwitch`] is
//! fully inert (no pinging, no behavior change).

use tracing::warn;

/// Consecutive "wedged" reconciles (see [`DeadMansSwitch::on_reconcile`])
/// before the switch stops pinging. Small on purpose: a couple of retried
/// failures is normal self-healing; three in a row while work piles up is a
/// capacity/provider/DB outage the dispatcher's own backoff can't fix alone.
pub const WEDGED_CYCLES_TO_STOP: u32 = 3;

/// Per-loop dead-man's-switch state. One instance lives for the lifetime of
/// [`super::run_loop`].
pub struct DeadMansSwitch {
    url: Option<String>,
    client: reqwest::Client,
    consecutive_wedged: u32,
}

impl DeadMansSwitch {
    /// `RIPCLONE_HEARTBEAT_URL`, unset/empty -> inert switch.
    pub fn from_env() -> Self {
        Self::new(
            std::env::var("RIPCLONE_HEARTBEAT_URL")
                .ok()
                .filter(|s| !s.trim().is_empty()),
        )
    }

    pub fn new(url: Option<String>) -> Self {
        Self {
            url,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
            consecutive_wedged: 0,
        }
    }

    /// Current consecutive-wedged streak (tests / logs).
    pub fn consecutive_wedged(&self) -> u32 {
        self.consecutive_wedged
    }

    /// True when `RIPCLONE_HEARTBEAT_URL` (or an explicit `Some`) is set —
    /// the switch actually pings. False means it is fully inert (startup log).
    pub fn is_configured(&self) -> bool {
        self.url.is_some()
    }

    /// True once the streak has crossed [`WEDGED_CYCLES_TO_STOP`] — pinging is
    /// suppressed so the external monitor's silence fires the alert.
    pub fn is_stopped(&self) -> bool {
        self.consecutive_wedged >= WEDGED_CYCLES_TO_STOP
    }

    /// Feed this cycle's reconcile outcome. `depth`/`started`/`failed` are
    /// [`super::ReconcileOutcome::plan`]`.total_pending`, `.started`, `.failed`.
    ///
    /// "Wedged" = `depth > 0 && started == 0 && failed > 0`: dispatch is
    /// actively failing while work piles up (capacity/provider/DB outage) —
    /// not something natural retries fix. A healthy cycle (`started > 0` or
    /// `depth == 0`) resets the streak. Anything else (e.g. `depth > 0`,
    /// `started == 0`, `failed == 0` — backoff is skipping starts this pass)
    /// is neither a fresh failure nor proof of health, so it leaves an
    /// in-progress streak exactly where it was: it does not clear an alarm in
    /// progress, but it does not advance one either.
    ///
    /// Best-effort: a failed ping is logged and NEVER propagates — this must
    /// never break the reconcile loop.
    pub async fn on_reconcile(&mut self, depth: usize, started: usize, failed: usize) {
        let Some(url) = self.url.clone() else {
            return;
        };

        let wedged = depth > 0 && started == 0 && failed > 0;
        let healthy = started > 0 || depth == 0;
        if wedged {
            self.consecutive_wedged = self.consecutive_wedged.saturating_add(1);
        } else if healthy {
            self.consecutive_wedged = 0;
        }

        if self.is_stopped() {
            warn!(
                consecutive_wedged = self.consecutive_wedged,
                "dead-man's switch: un-self-healable outage detected \
                 (depth > 0, started == 0, failed > 0 for {} cycles) — \
                 suppressing heartbeat pings so the external monitor alerts",
                self.consecutive_wedged
            );
            return;
        }

        if let Err(e) = self.client.get(&url).send().await {
            warn!(err = %e, url, "dead-man's switch: heartbeat ping failed (best-effort, non-fatal)");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::routing::get;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn spawn_sink() -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = hits.clone();
        let app = Router::new().route(
            "/hb",
            get(move || {
                let hits = hits2.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    "ok"
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        tokio::task::yield_now().await;
        (format!("http://{addr}/hb"), hits)
    }

    #[tokio::test]
    async fn unset_url_never_pings_and_never_tracks_state() {
        let (url, hits) = spawn_sink().await;
        let mut switch = DeadMansSwitch::new(None);
        // Feed a wedged-shaped sequence far past the stop threshold.
        for _ in 0..(WEDGED_CYCLES_TO_STOP * 2) {
            switch.on_reconcile(1, 0, 1).await;
        }
        assert_eq!(
            switch.consecutive_wedged(),
            0,
            "inert switch tracks nothing"
        );
        assert!(!switch.is_stopped());
        // No requests ever reached the sink (url was never used).
        let _ = url; // sink URL intentionally unused by the switch
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn healthy_reconcile_pings() {
        let (url, hits) = spawn_sink().await;
        let mut switch = DeadMansSwitch::new(Some(url));
        // Empty queue (depth == 0) is healthy.
        switch.on_reconcile(0, 0, 0).await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // Started > 0 is also healthy.
        switch.on_reconcile(3, 2, 0).await;
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        assert_eq!(switch.consecutive_wedged(), 0);
    }

    #[tokio::test]
    async fn wedged_streak_stops_pinging_after_threshold() {
        let (url, hits) = spawn_sink().await;
        let mut switch = DeadMansSwitch::new(Some(url));
        // Warm up with one healthy ping.
        switch.on_reconcile(0, 0, 0).await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // Cycles under the threshold still ping (transient failures alone
        // are not yet an "un-self-healable" outage).
        for cycle in 1..WEDGED_CYCLES_TO_STOP {
            switch.on_reconcile(1, 0, 1).await;
            assert_eq!(
                hits.load(Ordering::SeqCst),
                1 + cycle as usize,
                "cycle {cycle} (under threshold) must still ping"
            );
        }
        assert!(!switch.is_stopped());

        // The Nth consecutive wedged cycle crosses the threshold: no ping.
        switch.on_reconcile(1, 0, 1).await;
        assert!(switch.is_stopped());
        let stopped_hits = hits.load(Ordering::SeqCst);
        assert_eq!(
            stopped_hits, WEDGED_CYCLES_TO_STOP as usize,
            "the Nth wedged cycle itself must not ping"
        );

        // Further wedged cycles stay silent.
        for _ in 0..5 {
            switch.on_reconcile(1, 0, 1).await;
        }
        assert_eq!(
            hits.load(Ordering::SeqCst),
            stopped_hits,
            "no new pings while stopped"
        );

        // A healthy cycle (started > 0) resets the streak and resumes pinging
        // on the SAME cycle.
        switch.on_reconcile(1, 1, 0).await;
        assert!(!switch.is_stopped());
        assert_eq!(switch.consecutive_wedged(), 0);
        assert_eq!(hits.load(Ordering::SeqCst), stopped_hits + 1);
    }

    #[tokio::test]
    async fn gray_zone_cycle_does_not_clear_an_in_progress_streak() {
        // depth > 0, started == 0, failed == 0 (e.g. backoff skipping starts
        // this pass): not a fresh failure, but not proof of health either.
        let (url, hits) = spawn_sink().await;
        let mut switch = DeadMansSwitch::new(Some(url));
        switch.on_reconcile(1, 0, 1).await; // wedged: streak = 1
        assert_eq!(switch.consecutive_wedged(), 1);
        switch.on_reconcile(1, 0, 0).await; // gray zone: streak unchanged
        assert_eq!(
            switch.consecutive_wedged(),
            1,
            "gray-zone cycle must not reset an in-progress wedged streak"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "gray zone still pings (not stopped yet)"
        );
    }

    #[tokio::test]
    async fn ping_failure_is_logged_and_never_propagates() {
        // Point at a closed local port: the GET fails, but on_reconcile must
        // not panic or return an error (there is nothing to propagate to).
        let mut switch = DeadMansSwitch::new(Some("http://127.0.0.1:1".into()));
        switch.on_reconcile(0, 0, 0).await;
        // Reaching here at all is the assertion: a failed ping never broke
        // the (implicit) reconcile loop.
    }
}
