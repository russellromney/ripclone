//! Shared filesystem helpers used by archive extraction and worktree writers.
//!
//! These helpers are safety-critical: they validate manifest paths and create
//! parent directories while rejecting symlinks and traversal attempts. Keeping
//! one copy prevents drift between the two extraction paths.

use anyhow::{Context, Result};
use std::path::Path;

/// Convert a raw path byte slice to a `Path`. On Unix this preserves arbitrary
/// git path bytes; on other platforms we fall back to UTF-8.
pub fn path_from_bytes(bytes: &[u8]) -> &Path {
    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        Path::new(OsStr::from_bytes(bytes))
    }
    #[cfg(not(unix))]
    {
        let s = std::str::from_utf8(bytes).unwrap_or("<invalid utf8 path>");
        Path::new(s)
    }
}

/// Validate that `path` is a non-empty relative path with no `..` components
/// and no NUL bytes. This must be applied to every manifest path before any
/// filesystem operation.
pub fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        anyhow::bail!("path is empty");
    }
    if path.is_absolute() {
        anyhow::bail!("path is absolute: {}", path.display());
    }
    for comp in path.components() {
        match comp {
            std::path::Component::ParentDir => {
                anyhow::bail!("path contains parent-dir component: {}", path.display());
            }
            std::path::Component::Normal(_) => {}
            _ => {
                anyhow::bail!("path contains invalid component: {}", path.display());
            }
        }
    }
    if path.as_os_str().as_encoded_bytes().contains(&0) {
        anyhow::bail!("path contains NUL byte: {}", path.display());
    }
    Ok(())
}

/// Create a directory tree under `root` following only real directory
/// components. Any symlink encountered along the way is rejected.
pub fn safe_create_dir_all(root: &Path, rel: &Path) -> Result<()> {
    validate_relative_path(rel)?;
    let mut current = root.to_path_buf();
    for comp in rel.components() {
        if let std::path::Component::Normal(name) = comp {
            current.push(name);
            if current.is_symlink() {
                anyhow::bail!(
                    "refusing to follow symlinked directory: {}",
                    current.display()
                );
            }
            // Create unconditionally and tolerate a concurrent creator. Probing
            // with `exists()` first would race when several producer threads
            // create the same parent directory at once (one would win, the
            // others would hit EEXIST). `create_dir` + `AlreadyExists` is the
            // atomic form.
            match std::fs::create_dir(&current) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if current.is_symlink() {
                        anyhow::bail!(
                            "refusing to follow symlinked directory: {}",
                            current.display()
                        );
                    }
                    if !current.is_dir() {
                        anyhow::bail!("path is not a directory: {}", current.display());
                    }
                }
                Err(e) => {
                    return Err(e).with_context(|| format!("create dir {}", current.display()));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStrExt;

    #[test]
    fn validate_relative_path_accepts_simple_relative() {
        validate_relative_path(Path::new("src/foo/bar.txt")).unwrap();
    }

    #[test]
    fn validate_relative_path_rejects_empty() {
        assert!(validate_relative_path(Path::new("")).is_err());
    }

    #[test]
    fn validate_relative_path_rejects_absolute() {
        assert!(validate_relative_path(Path::new("/etc/passwd")).is_err());
    }

    #[test]
    fn validate_relative_path_rejects_parent_dir() {
        assert!(validate_relative_path(Path::new("../foo")).is_err());
        assert!(validate_relative_path(Path::new("foo/../bar")).is_err());
    }

    #[test]
    fn validate_relative_path_rejects_nul_byte() {
        let p = std::ffi::OsStr::from_bytes(b"foo/bar\0baz");
        assert!(validate_relative_path(Path::new(p)).is_err());
    }

    #[test]
    fn safe_create_dir_all_rejects_symlinked_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let real = root.join("real");
        std::fs::create_dir(&real).unwrap();
        let link = root.join("link");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, &link).unwrap();
        #[cfg(not(unix))]
        std::os::windows::fs::symlink_dir(&real, &link).unwrap();

        let rel = Path::new("link/nested/file.txt");
        assert!(safe_create_dir_all(root, rel).is_err());
    }
}
