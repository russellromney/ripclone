use anyhow::{Context, Result};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Whether a temp (in-memory, ephemeral) clone is available on this host.
///
/// Opt-in only. A temp clone is materialized on tmpfs (`/dev/shm`) via an
/// overlay mount that is left in place — so the working tree lives in RAM and
/// does NOT survive a reboot. That's ideal for ephemeral agent/CI machines but
/// a surprising durability change for a normal clone, so we never enable it
/// implicitly. Enable with the `--temp` CLI flag or `RIPCLONE_TEMP=1`.
pub fn is_available() -> bool {
    if cfg!(not(target_os = "linux")) {
        return false;
    }
    // Explicit opt-out wins, even if `RIPCLONE_TEMP` is set.
    if env_enabled("RIPCLONE_NO_OVERLAY") {
        return false;
    }
    if !env_enabled("RIPCLONE_TEMP") {
        return false;
    }
    Path::new("/dev/shm").is_dir()
}

fn env_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|v| {
            let v = v.trim();
            !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
        })
        .unwrap_or(false)
}

/// Directory to use for overlay lower/upper/work dirs. Defaults to `/dev/shm`
/// because it is almost always tmpfs and dramatically faster than rootfs on
/// cloud VMs.
pub fn staging_dir() -> PathBuf {
    std::env::var("RIPCLONE_STAGING_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/dev/shm"))
}

/// Available bytes on the filesystem that backs `path`.
// `statvfs` field widths differ by platform (the casts are needed on macOS,
// redundant on 64-bit Linux), so the cast is portability, not a clippy nit.
#[allow(clippy::unnecessary_cast)]
pub fn available_space(path: &Path) -> Option<u64> {
    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut buf: std::mem::MaybeUninit<libc::statvfs> = std::mem::MaybeUninit::uninit();
    unsafe {
        if libc::statvfs(c_path.as_ptr(), buf.as_mut_ptr()) != 0 {
            return None;
        }
        let buf = buf.assume_init();
        Some(buf.f_bavail as u64 * buf.f_bsize as u64)
    }
}

/// Directories needed for one overlay mount.
pub struct OverlayDirs {
    pub base: PathBuf,
    pub lower: PathBuf,
    pub upper: PathBuf,
    pub work: PathBuf,
    pub mount_point: PathBuf,
    /// Set once the overlay is mounted. Until then, dropping `OverlayDirs`
    /// removes the staging tree so a failed clone leaves nothing behind. After a
    /// successful mount the staging dirs must persist (the mount references
    /// them), so drop becomes a no-op.
    mounted: std::sync::atomic::AtomicBool,
}

impl Drop for OverlayDirs {
    fn drop(&mut self) {
        if !self.mounted.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = std::fs::remove_dir_all(&self.base);
            // `create` also creates the final mount point. Before a successful
            // mount it is ours and should not make a failed clone look
            // published. Remove only an empty directory so an external writer
            // racing with the clone cannot have its data deleted.
            let _ = std::fs::remove_dir(&self.mount_point);
        }
    }
}

impl OverlayDirs {
    /// Create lower/upper/work dirs under `staging_dir`. If the clone fails
    /// before [`mount_dirs`] succeeds, dropping this value removes the staging
    /// tree; after a successful mount it is kept.
    pub fn create(staging_dir: &Path, mount_point: &Path) -> Result<OverlayDirs> {
        // Keep the temp dir's path but manage cleanup ourselves via `Drop`
        // (keyed on `mounted`), so a mounted overlay's staging survives.
        let base = tempfile::Builder::new()
            .prefix("ripclone-overlay-")
            .tempdir_in(staging_dir)
            .context("create overlay staging directory")?
            .keep();
        let lower = base.join("lower");
        let upper = base.join("upper");
        let work = base.join("work");
        std::fs::create_dir_all(&lower)?;
        std::fs::create_dir_all(&upper)?;
        std::fs::create_dir_all(&work)?;
        std::fs::create_dir_all(mount_point)?;
        Ok(OverlayDirs {
            base,
            lower,
            upper,
            work,
            mount_point: mount_point.to_path_buf(),
            mounted: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Mark the staging tree as mounted so `Drop` keeps it.
    pub fn mark_mounted(&self) {
        self.mounted
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Do a cheap trial mount/unmount in a disposable directory to confirm the
/// kernel allows overlay mounts with tmpfs-backed upper/work dirs in this
/// environment.
pub fn test_mount(staging_dir: &Path) -> bool {
    let Ok(base) = tempfile::Builder::new()
        .prefix("ripclone-overlay-test-")
        .tempdir_in(staging_dir)
    else {
        return false;
    };
    let lower = base.path().join("lower");
    let upper = base.path().join("upper");
    let work = base.path().join("work");
    let merge = base.path().join("merge");
    if std::fs::create_dir_all(&lower).is_err()
        || std::fs::create_dir_all(&upper).is_err()
        || std::fs::create_dir_all(&work).is_err()
        || std::fs::create_dir_all(&merge).is_err()
    {
        return false;
    }
    if std::fs::write(lower.join("probe.txt"), b"ok").is_err() {
        return false;
    }
    if mount(&lower, &upper, &work, &merge).is_err() {
        return false;
    }
    // If the mount worked we should see the probe file.
    let ok = std::fs::read_to_string(merge.join("probe.txt"))
        .map(|s| s == "ok")
        .unwrap_or(false);
    let _ = Command::new("umount").arg(&merge).status();
    ok
}

/// Mount an overlayfs with the given lower/upper/work dirs at `mount_point`.
pub fn mount(lower: &Path, upper: &Path, work: &Path, mount_point: &Path) -> Result<()> {
    let options = format!(
        "lowerdir={},upperdir={},workdir={}",
        lower.display(),
        upper.display(),
        work.display()
    );
    let status = Command::new("mount")
        .arg("-t")
        .arg("overlay")
        .arg("overlay")
        .arg("-o")
        .arg(&options)
        .arg(mount_point)
        .status()
        .context("spawn overlay mount")?;
    if !status.success() {
        anyhow::bail!("overlay mount failed");
    }
    Ok(())
}

/// Mount helper that takes an `OverlayDirs`.
pub fn mount_dirs(dirs: &OverlayDirs) -> Result<()> {
    mount(&dirs.lower, &dirs.upper, &dirs.work, &dirs.mount_point)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unmounted_overlay_staging_is_removed_on_drop() {
        let staging = tempfile::tempdir().unwrap();
        let mp = tempfile::tempdir().unwrap();
        let mount_point = mp.path().join("mnt");
        let base = {
            let dirs = OverlayDirs::create(staging.path(), &mount_point).unwrap();
            assert!(dirs.base.exists());
            assert!(mount_point.exists());
            dirs.base.clone()
        };
        assert!(
            !base.exists(),
            "unmounted overlay staging must be removed on drop"
        );
        assert!(
            !mount_point.exists(),
            "unmounted overlay target must be removed on drop"
        );
    }

    #[test]
    fn mounted_overlay_staging_is_kept_on_drop() {
        let staging = tempfile::tempdir().unwrap();
        let mp = tempfile::tempdir().unwrap();
        let base = {
            let dirs = OverlayDirs::create(staging.path(), &mp.path().join("mnt")).unwrap();
            dirs.mark_mounted();
            dirs.base.clone()
        };
        assert!(
            base.exists(),
            "mounted overlay staging must be kept on drop"
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}
