use anyhow::{Context, Result};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Whether overlay staging is even worth trying on this host.
pub fn is_available() -> bool {
    if cfg!(not(target_os = "linux")) {
        return false;
    }
    if std::env::var_os("RIPCLONE_NO_OVERLAY").is_some() {
        return false;
    }
    Path::new("/dev/shm").is_dir()
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
pub fn available_space(path: &Path) -> Option<u64> {
    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut buf: std::mem::MaybeUninit<libc::statvfs> = std::mem::MaybeUninit::uninit();
    unsafe {
        if libc::statvfs(c_path.as_ptr(), buf.as_mut_ptr()) != 0 {
            return None;
        }
        let buf = buf.assume_init();
        Some(buf.f_bavail as u64 * buf.f_bsize)
    }
}

/// Directories needed for one overlay mount.
pub struct OverlayDirs {
    pub base: PathBuf,
    pub lower: PathBuf,
    pub upper: PathBuf,
    pub work: PathBuf,
    pub mount_point: PathBuf,
}

impl OverlayDirs {
    /// Create lower/upper/work dirs under `staging_dir`. The caller keeps the
    /// returned paths alive; dropping this value does not delete anything.
    pub fn create(staging_dir: &Path, mount_point: &Path) -> Result<OverlayDirs> {
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
        })
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
