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
    // mistake here would silently corrupt the io_uring statx buffer.
    const _: () = assert!(core::mem::size_of::<statx>() == 256);
    const _: () = assert!(core::mem::size_of::<statx_timestamp>() == 16);
}
