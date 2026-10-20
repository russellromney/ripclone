//! The machine's capacity, for limits that scale with it.

use anyhow::{Context, Result};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

/// The size in bytes of the filesystem that holds `path`.
pub fn disk_bytes(path: &Path) -> Result<u64> {
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .with_context(|| format!("path {} has a NUL byte", path.display()))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("read the disk size of {}", path.display()));
    }
    let stat = unsafe { stat.assume_init() };
    // The field types differ by platform: 32-bit block counts on macOS.
    #[allow(clippy::useless_conversion)]
    let (blocks, block_size) = (u64::from(stat.f_blocks), u64::from(stat.f_frsize));
    Ok(blocks.saturating_mul(block_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_bytes_reads_the_filesystem_size() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = disk_bytes(dir.path()).unwrap();
        assert!(bytes > 1 << 20, "{bytes}");
        assert!(disk_bytes(&dir.path().join("missing")).is_err());
    }
}
