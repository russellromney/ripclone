//! Build cancellation. A worker cancels a build whose lease it lost: the
//! build stops at its next [`Cancel::check`], and every git process group the
//! build is running is stopped at once.
//!
//! A git process group is stopped with SIGTERM, so git removes its lock
//! files (a SIGKILLed fetch leaves `refs/...lock` behind and the retry of
//! that commit fails), then SIGKILL after [`GRACE`].
//!
//! The token rides with the build as a task-local ([`Cancel::scope`]) and
//! with its blocking work as a thread-local ([`Cancel::enter`], done by
//! [`spawn_blocking`]). Code that fans work out to other threads enters it
//! there too. Git children started in their own process group register with
//! the current token while they run ([`wait_child`]).

use std::cell::RefCell;
use std::collections::HashSet;
use std::process::{Child, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// How long a git process group has to exit after SIGTERM before SIGKILL.
pub const GRACE: Duration = Duration::from_secs(2);

/// How often [`wait_child_watched`] runs its stop check.
pub const WATCH_EVERY: Duration = Duration::from_millis(500);

#[derive(Clone, Default)]
pub struct Cancel(Arc<Inner>);

#[derive(Default)]
struct Inner {
    cancelled: AtomicBool,
    /// Process groups of the running git children. Guarded together with
    /// their reaping, so a group id is never killed after its leader was
    /// reaped and the id could be reused.
    groups: Mutex<HashSet<i32>>,
}

/// The error a cancelled build stops with.
#[derive(Debug)]
pub struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("build cancelled: its lease was lost")
    }
}

impl std::error::Error for Cancelled {}

thread_local! {
    static CURRENT: RefCell<Option<Cancel>> = const { RefCell::new(None) };
}

tokio::task_local! {
    static TASK: Cancel;
}

impl Cancel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop the build: later checks fail and every registered git process
    /// group is stopped.
    pub fn cancel(&self) {
        if self.0.cancelled.swap(true, Ordering::SeqCst) {
            return;
        }
        {
            let groups = self.0.groups.lock().unwrap_or_else(PoisonError::into_inner);
            if groups.is_empty() {
                return;
            }
            for &group in groups.iter() {
                signal_group(group, Signal::Term);
            }
        }
        // A group still registered after the grace has an unreaped leader,
        // so its id is still its own.
        let inner = Arc::clone(&self.0);
        std::thread::spawn(move || {
            std::thread::sleep(GRACE);
            let groups = inner.groups.lock().unwrap_or_else(PoisonError::into_inner);
            for &group in groups.iter() {
                signal_group(group, Signal::Kill);
            }
        });
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::SeqCst)
    }

    /// Fail with [`Cancelled`] once the build is cancelled.
    pub fn check(&self) -> anyhow::Result<()> {
        if self.is_cancelled() {
            return Err(anyhow::Error::new(Cancelled));
        }
        Ok(())
    }

    /// Run `f` with this token as the thread's current one.
    pub fn enter<R>(&self, f: impl FnOnce() -> R) -> R {
        struct Restore(Option<Cancel>);
        impl Drop for Restore {
            fn drop(&mut self) {
                let previous = self.0.take();
                CURRENT.with(|current| *current.borrow_mut() = previous);
            }
        }
        let previous = CURRENT.with(|current| current.replace(Some(self.clone())));
        let _restore = Restore(previous);
        f()
    }

    /// Run the future `f` with this token as its task's current one.
    pub async fn scope<F: std::future::Future>(&self, f: F) -> F::Output {
        TASK.scope(self.clone(), f).await
    }

    /// The token of the build running on this thread or task, if any.
    pub fn current() -> Option<Cancel> {
        CURRENT
            .with(|current| current.borrow().clone())
            .or_else(|| TASK.try_with(Cancel::clone).ok())
    }

    /// [`Cancel::check`] on the current thread's token. Without one, never
    /// fails.
    pub fn check_current() -> anyhow::Result<()> {
        Self::current().map_or(Ok(()), |cancel| cancel.check())
    }
}

/// `tokio::task::spawn_blocking` that carries the current build's token into
/// the blocking thread.
pub fn spawn_blocking<R: Send + 'static>(
    f: impl FnOnce() -> R + Send + 'static,
) -> tokio::task::JoinHandle<R> {
    let cancel = Cancel::current();
    tokio::task::spawn_blocking(move || match cancel {
        Some(cancel) => cancel.enter(f),
        None => f(),
    })
}

/// How a child waited on by [`wait_child`] ended.
#[derive(Debug)]
pub enum Ended {
    Exited(ExitStatus),
    TimedOut,
    Cancelled,
}

/// Wait for `child`, which must lead its own process group
/// (`Command::process_group(0)`, which keeps the `posix_spawn` path). The
/// whole group is stopped when the current build is cancelled or `timeout`
/// passes, and killed once the child exits, so no helper (an HTTP
/// transport, `index-pack`) outlives it.
pub fn wait_child(child: &mut Child, timeout: Option<Duration>) -> std::io::Result<Ended> {
    wait_child_watched(child, timeout, None)
}

/// [`wait_child`] that also stops the group, as a timeout does, once `stop`
/// returns true. `stop` runs every [`WATCH_EVERY`] while the child runs;
/// the caller learns from its own state that it fired.
#[cfg(unix)]
pub fn wait_child_watched(
    child: &mut Child,
    timeout: Option<Duration>,
    stop: Option<&(dyn Fn() -> bool + Sync)>,
) -> std::io::Result<Ended> {
    use std::sync::mpsc;
    let group = i32::try_from(child.id()).map_err(std::io::Error::other)?;
    let cancel = Cancel::current();
    if let Some(cancel) = &cancel {
        let mut groups = cancel
            .0
            .groups
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if cancel.is_cancelled() {
            // Started after the cancel: it has done nothing worth cleaning.
            signal_group(group, Signal::Kill);
        }
        groups.insert(group);
    }
    let timed_out = AtomicBool::new(false);
    // Wait without reaping: until the zombie is reaped its pid, and so the
    // group id, cannot be reused, so every kill above and below is safe.
    // The watchdog ends inside the scope, before the reap.
    let exited = std::thread::scope(|scope| {
        let (done, wait) = mpsc::channel::<()>();
        if timeout.is_some() || stop.is_some() {
            let timed_out = &timed_out;
            scope.spawn(move || watchdog(group, timeout, stop, &wait, timed_out));
        }
        let exited = wait_without_reaping(group);
        drop(done);
        exited
    });
    if let Some(cancel) = &cancel {
        let mut groups = cancel
            .0
            .groups
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        groups.remove(&group);
    }
    signal_group(group, Signal::Kill);
    let status = child.wait()?;
    exited?;
    Ok(if timed_out.load(Ordering::SeqCst) {
        Ended::TimedOut
    } else if !status.success() && cancel.is_some_and(|cancel| cancel.is_cancelled()) {
        Ended::Cancelled
    } else {
        Ended::Exited(status)
    })
}

/// Stop `group` (SIGTERM, then SIGKILL after [`GRACE`]) once `timeout`
/// passes or `stop` returns true, unless `wait` disconnects first: the
/// child exited.
#[cfg(unix)]
fn watchdog(
    group: i32,
    timeout: Option<Duration>,
    stop: Option<&(dyn Fn() -> bool + Sync)>,
    wait: &std::sync::mpsc::Receiver<()>,
    timed_out: &AtomicBool,
) {
    use std::sync::mpsc::RecvTimeoutError;
    let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
    loop {
        let mut next = if stop.is_some() {
            WATCH_EVERY
        } else {
            Duration::MAX
        };
        if let Some(deadline) = deadline {
            next = next.min(deadline.saturating_duration_since(Instant::now()));
        }
        if !matches!(wait.recv_timeout(next), Err(RecvTimeoutError::Timeout)) {
            return;
        }
        let late = deadline.is_some_and(|deadline| Instant::now() >= deadline);
        if late || stop.is_some_and(|stop| stop()) {
            timed_out.store(late, Ordering::SeqCst);
            signal_group(group, Signal::Term);
            if let Err(RecvTimeoutError::Timeout) = wait.recv_timeout(GRACE) {
                signal_group(group, Signal::Kill);
            }
            return;
        }
    }
}

#[cfg(not(unix))]
pub fn wait_child_watched(
    child: &mut Child,
    _timeout: Option<Duration>,
    _stop: Option<&(dyn Fn() -> bool + Sync)>,
) -> std::io::Result<Ended> {
    child.wait().map(Ended::Exited)
}

#[cfg(unix)]
fn wait_without_reaping(pid: i32) -> std::io::Result<()> {
    loop {
        // SAFETY: `info` is a valid out-pointer for the duration of the call.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        if rc == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[derive(Clone, Copy)]
enum Signal {
    Term,
    Kill,
}

#[cfg(unix)]
fn signal_group(group: i32, signal: Signal) {
    let signal = match signal {
        Signal::Term => libc::SIGTERM,
        Signal::Kill => libc::SIGKILL,
    };
    // SAFETY: plain syscall. ESRCH only means the group is gone.
    unsafe {
        libc::kill(-group, signal);
    }
}

#[cfg(not(unix))]
fn signal_group(_group: i32, _signal: Signal) {}
