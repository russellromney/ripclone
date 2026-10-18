//! What a clone tells the user on stderr besides errors and warnings: notes
//! such as "warming", and, on a terminal, one progress line with the phase
//! and the bytes of the planned artifacts downloaded so far of their total.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// What a clone writes to stderr besides errors and warnings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Stderr {
    /// `--quiet`: nothing.
    Quiet,
    /// Notes only, one line each. The default, and what scripts see.
    #[default]
    Notes,
    /// Notes and a progress line redrawn in place. For a terminal only.
    Progress,
}

/// Wait before the first draw, so a fast clone draws nothing.
const FIRST_DRAW: Duration = Duration::from_millis(500);
const REDRAW: Duration = Duration::from_millis(200);

/// One clone's progress. Cheap to clone; every copy counts into the same
/// totals. Without a progress line it only prints notes.
#[derive(Clone)]
pub(super) struct Progress {
    mode: Stderr,
    state: Arc<State>,
}

struct State {
    done: AtomicU64,
    total: AtomicU64,
    line: Mutex<Line>,
}

struct Line {
    phase: String,
    /// What the phase is at, e.g. each output's state; it keeps the
    /// phase's seconds counting.
    detail: String,
    /// When the phase began, for the rate and the elapsed seconds.
    phase_since: Instant,
    /// Width of the line on screen now; 0 when none is drawn.
    drawn: usize,
    stopped: bool,
}

impl Progress {
    pub(super) fn new(mode: Stderr) -> Self {
        Self {
            mode,
            state: Arc::new(State {
                done: AtomicU64::new(0),
                total: AtomicU64::new(0),
                line: Mutex::new(Line {
                    phase: "resolving".to_string(),
                    detail: String::new(),
                    phase_since: Instant::now(),
                    drawn: 0,
                    stopped: false,
                }),
            }),
        }
    }

    /// New counts in the same mode, for the next clone.
    pub(super) fn fresh(&self) -> Self {
        Self::new(self.mode)
    }

    fn line(&self) -> std::sync::MutexGuard<'_, Line> {
        self.state.line.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Print a note on its own line, above the progress line.
    pub(super) fn note(&self, message: &str) {
        match self.mode {
            Stderr::Quiet => {}
            Stderr::Notes => eprintln!("ripclone: {message}"),
            Stderr::Progress => {
                let mut line = self.line();
                let mut err = std::io::stderr().lock();
                line.clear(&mut err);
                let _ = writeln!(err, "ripclone: {message}");
            }
        }
    }

    /// Print a warning on its own line, even with `--quiet`.
    pub(super) fn warn(&self, message: &str) {
        let mut err = std::io::stderr().lock();
        if self.mode == Stderr::Progress {
            self.line().clear(&mut err);
        }
        let _ = writeln!(err, "warning: {message}");
    }

    pub(super) fn phase(&self, phase: impl Into<String>) {
        if self.mode == Stderr::Progress {
            self.line().enter(phase.into());
        }
    }

    /// Show `detail` after the phase without restarting its seconds.
    pub(super) fn detail(&self, detail: String) {
        if self.mode == Stderr::Progress {
            self.line().detail = detail;
        }
    }

    /// Plan `bytes` more of download, and enter the download phase.
    pub(super) fn add_total(&self, bytes: u64) {
        self.state.total.fetch_add(bytes, Ordering::Relaxed);
        if self.mode == Stderr::Progress {
            self.line().enter(DOWNLOADING.to_string());
        }
    }

    pub(super) fn add(&self, bytes: u64) {
        self.state.done.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Take back bytes an artifact discarded to restart.
    pub(super) fn sub(&self, bytes: u64) {
        self.state.done.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Redraw the progress line until the returned guard drops, which
    /// clears it. Without a progress line, the guard does nothing.
    pub(super) fn draw(&self) -> Drawing {
        let task = (self.mode == Stderr::Progress).then(|| {
            let progress = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(FIRST_DRAW).await;
                let mut tick = tokio::time::interval(REDRAW);
                loop {
                    tick.tick().await;
                    progress.redraw();
                }
            })
        });
        Drawing {
            progress: self.clone(),
            task,
        }
    }

    fn redraw(&self) {
        let mut line = self.line();
        if line.stopped {
            return;
        }
        let text = render(
            &line.phase,
            &line.detail,
            self.state.done.load(Ordering::Relaxed),
            self.state.total.load(Ordering::Relaxed),
            line.phase_since.elapsed(),
        );
        // A line as wide as the terminal wraps, and `\r` then redraws only
        // its last row.
        let text = fit(text, terminal_width());
        let mut err = std::io::stderr().lock();
        // Spaces, not an erase code, cover a longer previous line, so a
        // terminal without ANSI support shows no escape bytes.
        let width = text.chars().count();
        let pad = line.drawn.saturating_sub(width);
        let _ = write!(err, "\r{text}{:pad$}", "");
        let _ = err.flush();
        line.drawn = width;
    }
}

impl Line {
    /// Blank the drawn line and return the cursor to its start.
    fn clear(&mut self, err: &mut impl Write) {
        if self.drawn > 0 {
            let _ = write!(err, "\r{:width$}\r", "", width = self.drawn);
            let _ = err.flush();
            self.drawn = 0;
        }
    }

    fn enter(&mut self, phase: String) {
        if self.phase != phase {
            self.phase = phase;
            self.detail.clear();
            self.phase_since = Instant::now();
        }
    }
}

/// Clears the progress line when dropped.
pub(super) struct Drawing {
    progress: Progress,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Drawing {
    fn drop(&mut self) {
        let Some(task) = self.task.take() else {
            return;
        };
        task.abort();
        let mut line = self.progress.line();
        line.stopped = true;
        line.clear(&mut std::io::stderr().lock());
    }
}

const DOWNLOADING: &str = "downloading";

/// One progress line, e.g.
/// `ripclone: downloading 412.3 MiB / 1.2 GiB (34%), 52.1 MiB/s`. Outside
/// the download, the phase, its detail and its seconds so far:
/// `ripclone: waiting for abc1234: Head ready, Files pending (3s)`.
fn render(phase: &str, detail: &str, done: u64, total: u64, elapsed: Duration) -> String {
    let mut text = format!("ripclone: {phase}");
    if !detail.is_empty() {
        text += &format!(": {detail}");
    }
    let seconds = elapsed.as_secs_f64();
    if phase == DOWNLOADING && total > 0 {
        let done = done.min(total);
        let percent = done * 100 / total;
        text += &format!(" {} / {} ({percent}%)", bytes(done), bytes(total));
        if seconds >= 1.0 {
            text += &format!(", {}/s", bytes((done as f64 / seconds) as u64));
        }
    } else if seconds >= 1.0 {
        text += &format!(" ({}s)", elapsed.as_secs());
    }
    text
}

/// Stderr's terminal width in columns, when it is a terminal.
fn terminal_width() -> Option<usize> {
    let mut size = libc::winsize {
        ws_row: 0,
        ws_col: 0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: TIOCGWINSZ writes one `winsize` through the pointer.
    let ok = unsafe { libc::ioctl(libc::STDERR_FILENO, libc::TIOCGWINSZ, &mut size) } == 0;
    (ok && size.ws_col > 0).then_some(usize::from(size.ws_col))
}

/// `text` cut to leave the terminal's last column free.
fn fit(text: String, width: Option<usize>) -> String {
    match width {
        Some(width) if text.chars().count() >= width => {
            text.chars().take(width.saturating_sub(1)).collect()
        }
        _ => text,
    }
}

fn bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut value = n as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn line_shows_bytes_while_downloading_and_seconds_otherwise() {
        let ms = Duration::from_millis;
        assert_eq!(
            render("resolving", "", 0, 0, ms(500)),
            "ripclone: resolving"
        );
        assert_eq!(
            render("resolving", "", 0, 0, ms(2500)),
            "ripclone: resolving (2s)"
        );
        assert_eq!(
            render(DOWNLOADING, "", 512, 2048, ms(500)),
            "ripclone: downloading 512 B / 2.0 KiB (25%)"
        );
        assert_eq!(
            render(DOWNLOADING, "", 300 << 20, 1200 << 20, ms(10_000)),
            "ripclone: downloading 300.0 MiB / 1.2 GiB (25%), 30.0 MiB/s"
        );
        // After the download the byte figures would read as the next
        // step's progress, so a top-up shows only its phase and seconds.
        assert_eq!(
            render(
                "fetching abc1234 from upstream",
                "",
                5 << 30,
                5 << 30,
                ms(7200)
            ),
            "ripclone: fetching abc1234 from upstream (7s)"
        );
        assert_eq!(
            render(
                "waiting for abc1234",
                "Head ready, Full pending",
                0,
                0,
                ms(3100)
            ),
            "ripclone: waiting for abc1234: Head ready, Full pending (3s)"
        );
    }

    #[test]
    fn line_leaves_the_last_column_free() {
        let line = "ripclone: downloading".to_string();
        assert_eq!(fit(line.clone(), None), line);
        assert_eq!(fit(line.clone(), Some(80)), line);
        assert_eq!(fit(line.clone(), Some(21)), "ripclone: downloadin");
        assert_eq!(fit(line, Some(10)), "ripclone:");
    }
}
