//! Temp files of a test process, removed even when the process is killed.
//!
//! A test's `TempDir` is removed when it drops, but a test process killed
//! mid-run (a timeout, Ctrl-C) drops nothing. So before `main`, each test
//! process points `tempfile` at a root of its own, `$TMPDIR/ripclone-test-<pid>`,
//! and removes the roots of test processes that are gone. At exit it removes
//! its root if the tests left it empty; anything still in it is a leak, which
//! `scripts/ci.sh test` reports.
//!
//! The unit tests and the contract suite both include this file.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const PREFIX: &str = "ripclone-test-";

static ROOT: OnceLock<PathBuf> = OnceLock::new();

// Run `init` before `main`, from the platform's initializer list.
#[used]
#[cfg_attr(target_os = "linux", unsafe(link_section = ".init_array"))]
#[cfg_attr(
    target_vendor = "apple",
    unsafe(link_section = "__DATA,__mod_init_func")
)]
static INIT: extern "C" fn() = init;

extern "C" fn init() {
    let system = std::env::temp_dir();
    sweep(&system);
    let root = system.join(format!("{PREFIX}{}", std::process::id()));
    // No live process but this one has its pid, so a root already there is a
    // dead process's.
    let _ = std::fs::remove_dir_all(&root);
    if let Err(error) = std::fs::create_dir(&root) {
        eprintln!("test temp root {}: {error}", root.display());
        return;
    }
    if let Err(current) = tempfile::env::override_temp_dir(&root) {
        eprintln!("test temp dir already set to {}", current.display());
        let _ = std::fs::remove_dir(&root);
        return;
    }
    let _ = ROOT.set(root);
    // SAFETY: `remove_root` is a plain function that stays valid until exit.
    unsafe { libc::atexit(remove_root) };
}

extern "C" fn remove_root() {
    if let Some(root) = ROOT.get() {
        let _ = std::fs::remove_dir(root);
    }
}

/// Remove the roots in `system` of test processes that no longer exist.
/// Roots of live processes, such as another test run, are left alone.
fn sweep(system: &Path) {
    let Ok(entries) = std::fs::read_dir(system) else {
        return;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix(PREFIX))
            .and_then(|pid| pid.parse::<libc::pid_t>().ok())
            .filter(|pid| *pid > 0)
        else {
            continue;
        };
        // Signal 0 checks for the process without signalling it; only ESRCH
        // means there is no such process.
        // SAFETY: kill with signal 0 has no effect on the target.
        let gone = unsafe { libc::kill(pid, 0) } != 0
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
        if gone {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_dirs_are_under_this_process_root() {
        let root = ROOT.get().expect("test temp root is set");
        assert_eq!(
            root.file_name().unwrap().to_str().unwrap(),
            format!("{PREFIX}{}", std::process::id())
        );
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(dir.path().parent(), Some(root.as_path()));
    }

    #[test]
    fn sweep_removes_only_roots_of_gone_processes() {
        let system = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let gone = child.id();
        child.wait().unwrap();
        let roots = [
            (format!("{PREFIX}{gone}"), false),
            (format!("{PREFIX}{}", std::process::id()), true),
            ("ripclone-test-x".to_string(), true),
            (".tmpAbC123".to_string(), true),
        ];
        for (name, _) in &roots {
            std::fs::create_dir_all(system.path().join(name).join("00")).unwrap();
        }

        sweep(system.path());

        for (name, kept) in &roots {
            assert_eq!(system.path().join(name).exists(), *kept, "{name}");
        }
    }
}
