//! `statx` bindings that work on both glibc and musl Linux targets.
//!
//! The `libc` crate exposes `statx` / `statx_timestamp` / `STATX_BASIC_STATS`
//! on glibc but not on musl (as of libc 0.2.186), and the Linux release binary
//! is now static-musl. `io_uring::types::statx` is an opaque placeholder that
//! defers to `libc::statx`, so it does not help either. The kernel
//! `struct statx` is a stable UAPI ABI shared by every C library, so on musl we
//! define the exact same 256-byte layout ourselves; on glibc we re-export
//! libc's definitions unchanged so that build stays byte-identical.

#[cfg(not(target_env = "musl"))]
pub use libc::{STATX_BASIC_STATS, statx, statx_timestamp};

#[cfg(target_env = "musl")]
pub use musl::{STATX_BASIC_STATS, statx, statx_timestamp};

// Layout guard on whichever definition is in scope — libc's on glibc, ours on
// musl. The kernel writes this buffer directly (the io_uring `Statx` op hands it
// a raw pointer), so a single wrong offset silently yields garbage sizes and
// timestamps rather than a compile or runtime error. Ground truth is the UAPI
// `struct statx` in linux/stat.h; `size_of == 256` alone does not pin the
// fields, so every public field is asserted individually.
const _: () = {
    use core::mem::{align_of, offset_of, size_of};

    assert!(size_of::<statx_timestamp>() == 16);
    assert!(align_of::<statx_timestamp>() == 8);
    assert!(offset_of!(statx_timestamp, tv_sec) == 0);
    assert!(offset_of!(statx_timestamp, tv_nsec) == 8);

    assert!(size_of::<statx>() == 256);
    assert!(align_of::<statx>() == 8);
    assert!(offset_of!(statx, stx_mask) == 0);
    assert!(offset_of!(statx, stx_blksize) == 4);
    assert!(offset_of!(statx, stx_attributes) == 8);
    assert!(offset_of!(statx, stx_nlink) == 16);
    assert!(offset_of!(statx, stx_uid) == 20);
    assert!(offset_of!(statx, stx_gid) == 24);
    assert!(offset_of!(statx, stx_mode) == 28);
    assert!(offset_of!(statx, stx_ino) == 32);
    assert!(offset_of!(statx, stx_size) == 40);
    assert!(offset_of!(statx, stx_blocks) == 48);
    assert!(offset_of!(statx, stx_attributes_mask) == 56);
    assert!(offset_of!(statx, stx_atime) == 64);
    assert!(offset_of!(statx, stx_btime) == 80);
    assert!(offset_of!(statx, stx_ctime) == 96);
    assert!(offset_of!(statx, stx_mtime) == 112);
    assert!(offset_of!(statx, stx_rdev_major) == 128);
    assert!(offset_of!(statx, stx_rdev_minor) == 132);
    assert!(offset_of!(statx, stx_dev_major) == 136);
    assert!(offset_of!(statx, stx_dev_minor) == 140);
    assert!(offset_of!(statx, stx_mnt_id) == 144);
    assert!(offset_of!(statx, stx_dio_mem_align) == 152);
    assert!(offset_of!(statx, stx_dio_offset_align) == 156);

    assert!(STATX_BASIC_STATS == 0x0000_07ff);
};

#[cfg(target_env = "musl")]
mod musl {
    // Mirrors the kernel UAPI `struct statx` / `struct statx_timestamp`
    // (linux/stat.h) and matches libc's glibc-side layout field-for-field. The
    // io_uring `Statx` op writes this buffer, so the layout must be exact.
    #[repr(C)]
    #[derive(Clone, Copy)]
    #[allow(non_camel_case_types)]
    pub struct statx_timestamp {
        pub tv_sec: i64,
        pub tv_nsec: u32,
        pub __statx_timestamp_pad1: [i32; 1],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    #[allow(non_camel_case_types)]
    pub struct statx {
        pub stx_mask: u32,
        pub stx_blksize: u32,
        pub stx_attributes: u64,
        pub stx_nlink: u32,
        pub stx_uid: u32,
        pub stx_gid: u32,
        pub stx_mode: u16,
        pub __statx_pad1: [u16; 1],
        pub stx_ino: u64,
        pub stx_size: u64,
        pub stx_blocks: u64,
        pub stx_attributes_mask: u64,
        pub stx_atime: statx_timestamp,
        pub stx_btime: statx_timestamp,
        pub stx_ctime: statx_timestamp,
        pub stx_mtime: statx_timestamp,
        pub stx_rdev_major: u32,
        pub stx_rdev_minor: u32,
        pub stx_dev_major: u32,
        pub stx_dev_minor: u32,
        pub stx_mnt_id: u64,
        pub stx_dio_mem_align: u32,
        pub stx_dio_offset_align: u32,
        pub __statx_pad3: [u64; 12],
    }

    // STATX_BASIC_STATS = the bitmask of the classic stat(2) fields.
    pub const STATX_BASIC_STATS: u32 = 0x0000_07ff;

    // Compile-time guard: the kernel struct is exactly 256 bytes; a layout
    // mistake here would silently corrupt the io_uring statx buffer. Per-field
    // offsets are asserted on the re-exported type in the parent module, so the
    // same guard also covers libc's glibc definition.
    const _: () = assert!(core::mem::size_of::<statx>() == 256);
    const _: () = assert!(core::mem::size_of::<statx_timestamp>() == 16);
}

#[cfg(test)]
mod tests {
    use super::{STATX_BASIC_STATS, statx};
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;

    /// Fill a `statx` buffer with the raw `statx(2)` syscall. Deliberately does
    /// not go through any libc wrapper: this is the same "kernel writes into our
    /// struct" path the io_uring `Statx` op takes, so it is what the layout has
    /// to be right for.
    fn raw_statx(path: &Path) -> statx {
        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // Poison the buffer instead of zeroing it: a field the kernel never
        // writes (because an offset is wrong) then shows up as 0xaa… rather than
        // as a plausible zero.
        let mut buf: statx = unsafe { std::mem::zeroed() };
        unsafe { std::ptr::write_bytes(std::ptr::from_mut(&mut buf), 0xaa, 1) };
        let rc = unsafe {
            libc::syscall(
                libc::SYS_statx,
                libc::AT_FDCWD,
                c_path.as_ptr(),
                0, // AT_STATX_SYNC_AS_STAT
                STATX_BASIC_STATS,
                std::ptr::from_mut(&mut buf),
            )
        };
        assert_eq!(
            rc,
            0,
            "statx({}) failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
        buf
    }

    /// Independent baseline: plain `stat(2)` through libc, which never touches
    /// our struct definition.
    fn raw_stat(path: &Path) -> libc::stat {
        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::stat(c_path.as_ptr(), &mut st) };
        assert_eq!(
            rc,
            0,
            "stat({}) failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
        st
    }

    fn make_dev(major: u32, minor: u32) -> u64 {
        let (major, minor) = (u64::from(major), u64::from(minor));
        ((major & 0x0000_0fff) << 8)
            | ((major & 0xffff_f000) << 32)
            | (minor & 0x0000_00ff)
            | ((minor & 0xffff_ff00) << 12)
    }

    /// Every field the code reads out of `statx` must agree with `stat(2)` for
    /// the same file. A shifted offset in the musl struct would surface here as
    /// a wrong size / inode / timestamp instead of silently corrupting the git
    /// index stat cache.
    // `libc::stat` field widths differ by target (e.g. `st_ino` is `u64` on musl
    // but `u32`/`u64` elsewhere), so the `as u64` casts are needed for a
    // cross-target compile even though a few are no-ops under musl.
    #[allow(clippy::unnecessary_cast)]
    fn assert_statx_agrees_with_stat(path: &Path) {
        let sx = raw_statx(path);
        let st = raw_stat(path);

        // The kernel must have filled every basic-stats field we ask for; if
        // STATX_BASIC_STATS were wrong, some of the comparisons below would be
        // comparing against the 0xaa poison.
        assert_eq!(
            sx.stx_mask & STATX_BASIC_STATS,
            STATX_BASIC_STATS,
            "kernel did not return all STATX_BASIC_STATS fields (mask {:#x})",
            sx.stx_mask
        );

        assert_eq!(u64::from(sx.stx_mode), st.st_mode as u64, "stx_mode");
        assert_eq!(sx.stx_ino, st.st_ino as u64, "stx_ino");
        assert_eq!(sx.stx_size, st.st_size as u64, "stx_size");
        assert_eq!(sx.stx_blocks, st.st_blocks as u64, "stx_blocks");
        assert_eq!(
            u64::from(sx.stx_blksize),
            st.st_blksize as u64,
            "stx_blksize"
        );
        assert_eq!(u64::from(sx.stx_nlink), st.st_nlink as u64, "stx_nlink");
        assert_eq!(sx.stx_uid, st.st_uid, "stx_uid");
        assert_eq!(sx.stx_gid, st.st_gid, "stx_gid");
        assert_eq!(
            make_dev(sx.stx_dev_major, sx.stx_dev_minor),
            st.st_dev as u64,
            "stx_dev_major/minor"
        );
        assert_eq!(sx.stx_mtime.tv_sec, st.st_mtime, "stx_mtime.tv_sec");
        assert_eq!(
            i64::from(sx.stx_mtime.tv_nsec),
            st.st_mtime_nsec,
            "stx_mtime.tv_nsec"
        );
        assert_eq!(sx.stx_ctime.tv_sec, st.st_ctime, "stx_ctime.tv_sec");
        assert_eq!(
            i64::from(sx.stx_ctime.tv_nsec),
            st.st_ctime_nsec,
            "stx_ctime.tv_nsec"
        );
        assert_eq!(sx.stx_atime.tv_sec, st.st_atime, "stx_atime.tv_sec");
    }

    /// atime and mtime are identical on a freshly written file, and so are most
    /// of the small integer fields (uid == gid == 0 under a container, nlink ==
    /// 1). That makes a "compare statx against stat" test blind to a field mixup:
    /// reading atime where mtime belongs still matches. Force every field this
    /// test relies on to a distinct value first.
    fn make_fields_distinguishable(path: &Path, link: &Path) {
        // nlink: 1 -> 2.
        std::fs::hard_link(path, link).unwrap();

        // atime != mtime, both with distinct nanoseconds, neither equal to the
        // ctime that this very call sets to "now".
        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let times = [
            libc::timespec {
                tv_sec: 1_000_000_000,
                tv_nsec: 111_111_111,
            },
            libc::timespec {
                tv_sec: 1_500_000_000,
                tv_nsec: 222_222_222,
            },
        ];
        let rc = unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0) };
        assert_eq!(
            rc,
            0,
            "utimensat failed: {}",
            std::io::Error::last_os_error()
        );
    }

    #[test]
    fn statx_fields_match_stat_for_a_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        // A distinctive, non-round size: a byte-swapped or shifted read of
        // stx_size cannot coincidentally match it.
        std::fs::write(&path, vec![b'x'; 1_234_567]).unwrap();
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o640))
            .unwrap();
        make_fields_distinguishable(&path, &dir.path().join("link"));

        let sx = raw_statx(&path);
        assert_eq!(sx.stx_size, 1_234_567, "stx_size read from wrong offset");
        assert_eq!(
            sx.stx_mode & 0o7777,
            0o640,
            "stx_mode read from wrong offset"
        );
        assert_eq!(
            sx.stx_mode & libc::S_IFMT as u16,
            libc::S_IFREG as u16,
            "stx_mode file type"
        );
        assert_eq!(sx.stx_nlink, 2, "stx_nlink read from wrong offset");
        assert_eq!(sx.stx_atime.tv_sec, 1_000_000_000, "stx_atime is not atime");
        assert_eq!(sx.stx_mtime.tv_sec, 1_500_000_000, "stx_mtime is not mtime");
        // ctime was set to "now" by utimensat, so it must differ from both.
        assert!(
            sx.stx_ctime.tv_sec != sx.stx_atime.tv_sec
                && sx.stx_ctime.tv_sec != sx.stx_mtime.tv_sec,
            "stx_ctime is not ctime (a={}, m={}, c={})",
            sx.stx_atime.tv_sec,
            sx.stx_mtime.tv_sec,
            sx.stx_ctime.tv_sec
        );

        assert_statx_agrees_with_stat(&path);
    }

    #[test]
    fn statx_fields_match_stat_for_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let sx = raw_statx(dir.path());
        assert_eq!(
            sx.stx_mode & libc::S_IFMT as u16,
            libc::S_IFDIR as u16,
            "stx_mode file type"
        );
        assert_statx_agrees_with_stat(dir.path());
    }

    /// The consumer's view: the index stat built from `statx` must be identical
    /// to the one built from `std::fs::Metadata` for the same file. This is the
    /// value that lands in the git index stat cache — if it disagrees, git sees
    /// every file as dirty.
    #[test]
    fn index_stat_from_statx_matches_index_stat_from_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::write(&path, b"hello statx").unwrap();
        // Without this, atime == mtime == ctime and the comparison below passes
        // even if the conversion reads the wrong timestamp.
        make_fields_distinguishable(&path, &dir.path().join("link"));

        let sx = raw_statx(&path);
        let from_statx = crate::git::materialized_path_stat_from_statx(b"file".to_vec(), &sx);
        let metadata = std::fs::metadata(&path).unwrap();
        let from_metadata =
            crate::git::materialized_path_stat_from_metadata(b"file".to_vec(), &metadata);

        assert_eq!(from_statx, from_metadata);
    }
}
