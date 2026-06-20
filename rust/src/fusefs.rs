use crate::git;
use anyhow::Result;
use fuser::{
    FUSE_ROOT_ID, FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    ReplyOpen, Request,
};
use libc::{EINVAL, EIO, ENOENT};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};
use tracing::warn;

const TTL: Duration = Duration::from_secs(60);
const GIT_INODE: u64 = 2;
const BLOCK_SIZE: u64 = 512;

#[derive(Clone)]
struct InodeEntry {
    ino: u64,
    path: String,
    kind: FileType,
    size: u64,
    mode: u32,
}

#[derive(Clone)]
struct TreeEntryInfo {
    raw_mode: String,
    sha: String,
    obj_type: String,
}

#[derive(Clone)]
struct DirChild {
    name: String,
    raw_mode: String,
    sha: String,
    kind: FileType,
    mode: u32,
    size: u64,
}

pub struct RipcloneFs {
    owner: String,
    repo: String,
    branch: String,
    server: String,
    http: reqwest::blocking::Client,
    git_dir: PathBuf,
    commit: String,
    sizes: HashMap<String, u64>,
    inodes: Arc<Mutex<HashMap<u64, InodeEntry>>>,
    path_to_inode: Arc<Mutex<HashMap<String, u64>>>,
    next_inode: AtomicU64,
    next_git_inode: AtomicU64,
    blob_cache: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    overlay: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    deleted: Arc<Mutex<HashSet<String>>>,
    tree_entries: HashMap<String, TreeEntryInfo>,
    dir_children: HashMap<String, Vec<DirChild>>,
}

impl RipcloneFs {
    pub fn new<P: AsRef<Path>>(
        owner: String,
        repo: String,
        branch: String,
        server: String,
        git_dir: P,
        commit: String,
        sizes: HashMap<String, u64>,
    ) -> Self {
        let mut inodes = HashMap::new();
        let mut path_to_inode = HashMap::new();

        inodes.insert(
            FUSE_ROOT_ID,
            InodeEntry {
                ino: FUSE_ROOT_ID,
                path: "".to_string(),
                kind: FileType::Directory,
                size: 0,
                mode: 0o40755,
            },
        );
        path_to_inode.insert("".to_string(), FUSE_ROOT_ID);

        inodes.insert(
            GIT_INODE,
            InodeEntry {
                ino: GIT_INODE,
                path: ".git".to_string(),
                kind: FileType::Directory,
                size: 0,
                mode: 0o40755,
            },
        );
        path_to_inode.insert(".git".to_string(), GIT_INODE);

        // Build in-memory tree caches so directory reads and file lookups don't
        // need to spawn a `git ls-tree` process per path.
        let mut tree_entries = HashMap::new();
        let mut dir_children: HashMap<String, Vec<DirChild>> = HashMap::new();
        let mut dir_paths: HashSet<String> = HashSet::new();
        if let Ok(entries) = git::list_tree_entries(&git_dir, &commit) {
            for (path, raw_mode, sha, obj_type) in entries {
                tree_entries.insert(
                    path.clone(),
                    TreeEntryInfo {
                        raw_mode: raw_mode.clone(),
                        sha,
                        obj_type: obj_type.clone(),
                    },
                );

                let kind = if raw_mode.starts_with("04") {
                    FileType::Directory
                } else if raw_mode.starts_with("120") {
                    FileType::Symlink
                } else {
                    FileType::RegularFile
                };
                let size = if kind == FileType::Directory {
                    0
                } else {
                    sizes.get(&path).copied().unwrap_or(0)
                };
                let mode = mode_to_file_mode(&raw_mode, kind);

                let parent = std::path::Path::new(&path)
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_default();
                let name = std::path::Path::new(&path)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| path.clone());
                dir_children
                    .entry(parent.clone())
                    .or_default()
                    .push(DirChild {
                        name,
                        raw_mode,
                        sha: tree_entries[&path].sha.clone(),
                        kind,
                        mode,
                        size,
                    });

                // Infer parent directories from the file path.
                let mut p = parent;
                while !p.is_empty() {
                    dir_paths.insert(p.clone());
                    p = std::path::Path::new(&p)
                        .parent()
                        .map(|x| x.to_string_lossy().to_string())
                        .unwrap_or_default();
                }
            }
        }
        // Register inferred directories so lookup/readdir can find them.
        let mut sorted_dirs: Vec<String> = dir_paths.into_iter().collect();
        sorted_dirs.sort_by_key(|d| d.matches('/').count());
        for dir in sorted_dirs {
            tree_entries.insert(
                dir.clone(),
                TreeEntryInfo {
                    raw_mode: "040000".to_string(),
                    sha: String::new(),
                    obj_type: "tree".to_string(),
                },
            );
            let parent = std::path::Path::new(&dir)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            let name = std::path::Path::new(&dir)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| dir.clone());
            let child = DirChild {
                name,
                raw_mode: "040000".to_string(),
                sha: String::new(),
                kind: FileType::Directory,
                mode: 0o40755,
                size: 0,
            };
            let siblings = dir_children.entry(parent.clone()).or_default();
            if !siblings.iter().any(|c| c.name == child.name) {
                siblings.push(child);
            }
        }

        Self {
            owner,
            repo,
            branch,
            server,
            http: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            git_dir: git_dir.as_ref().to_path_buf(),
            commit,
            sizes,
            inodes: Arc::new(Mutex::new(inodes)),
            path_to_inode: Arc::new(Mutex::new(path_to_inode)),
            next_inode: AtomicU64::new(GIT_INODE + 1),
            next_git_inode: AtomicU64::new(1 << 60),
            blob_cache: Arc::new(Mutex::new(HashMap::new())),
            overlay: Arc::new(Mutex::new(HashMap::new())),
            deleted: Arc::new(Mutex::new(HashSet::new())),
            tree_entries,
            dir_children,
        }
    }

    fn alloc_inode(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }

    fn deterministic_inode(path: &str) -> u64 {
        if path.is_empty() {
            return FUSE_ROOT_ID;
        }
        if path == ".git" {
            return GIT_INODE;
        }
        // FNV-1a 64-bit over the path bytes for stable inode numbers.
        let mut h: u64 = 0xcbf29ce484222325;
        for b in path.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        // Avoid colliding with the reserved root / .git inodes.
        if h <= GIT_INODE {
            h += GIT_INODE + 1;
        }
        h
    }

    fn attr_for(&self, entry: &InodeEntry) -> FileAttr {
        let t = UNIX_EPOCH + Duration::from_secs(1);
        FileAttr {
            ino: entry.ino,
            size: entry.size,
            blocks: entry.size.div_ceil(BLOCK_SIZE),
            atime: t,
            mtime: t,
            ctime: t,
            crtime: UNIX_EPOCH,
            kind: entry.kind,
            perm: (entry.mode & 0o7777) as u16,
            nlink: if entry.kind == FileType::Directory {
                2
            } else {
                1
            },
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }

    fn get_or_create_inode(&self, path: &str, entry: &mut InodeEntry) -> u64 {
        let mut p2i = self.path_to_inode.lock().unwrap();
        if let Some(&ino) = p2i.get(path) {
            // Reuse the existing inode so stat(2) data stays stable for the path.
            entry.ino = ino;
            self.inodes.lock().unwrap().insert(ino, entry.clone());
            return ino;
        }
        let ino = if path == ".git" || path.starts_with(".git/") {
            self.next_git_inode.fetch_add(1, Ordering::SeqCst)
        } else {
            // Use sequential allocation to avoid inode collisions across the
            // 14k+ tree paths; stability is provided by path_to_inode reuse.
            self.alloc_inode()
        };
        entry.ino = ino;
        p2i.insert(path.to_string(), ino);
        self.inodes.lock().unwrap().insert(ino, entry.clone());
        ino
    }

    fn path_for_inode(&self, ino: u64) -> Option<String> {
        self.inodes
            .lock()
            .unwrap()
            .get(&ino)
            .map(|e| e.path.clone())
    }

    fn real_git_path(&self, path: &str) -> Option<PathBuf> {
        if path == ".git" || path.starts_with(".git/") {
            Some(self.git_dir.join(path))
        } else {
            None
        }
    }

    fn fetch_file(&self, path: &str) -> Result<Vec<u8>> {
        {
            let cache = self.blob_cache.lock().unwrap();
            if let Some(data) = cache.get(path) {
                return Ok(data.clone());
            }
        }
        // Try loose object in git dir first (only works if the skeleton included it).
        if let Some(sha) = self.tree_entry(path)?.map(|e| e.sha)
            && let Ok(data) = git::cat_file(&self.git_dir, &sha)
        {
            self.blob_cache
                .lock()
                .unwrap()
                .insert(path.to_string(), data.clone());
            return Ok(data);
        }
        let url = format!(
            "{}/v1/repos/{}/{}/cat?path={}&branch={}",
            self.server,
            self.owner,
            self.repo,
            urlencoding::encode(path),
            self.branch
        );
        let data = match self.http.get(&url).send() {
            Ok(resp) => {
                if !resp.status().is_success() {
                    anyhow::bail!("cat failed for {}: {}", path, resp.status());
                }
                resp.bytes()?.to_vec()
            }
            Err(e) => {
                return Err(e.into());
            }
        };
        self.blob_cache
            .lock()
            .unwrap()
            .insert(path.to_string(), data.clone());
        Ok(data)
    }

    fn tree_entry(&self, path: &str) -> Result<Option<TreeEntry>> {
        Ok(self.tree_entries.get(path).map(|e| TreeEntry {
            sha: e.sha.clone(),
            raw_mode: e.raw_mode.clone(),
        }))
    }

    fn list_tree_children(
        &self,
        dir_path: &str,
    ) -> Result<Vec<(String, String, String, u32, u64)>> {
        let children = self
            .dir_children
            .get(dir_path)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        Ok(children
            .iter()
            .map(|c| {
                (
                    c.name.clone(),
                    c.sha.clone(),
                    c.raw_mode.clone(),
                    c.mode,
                    c.size,
                )
            })
            .collect())
    }
}

#[derive(Clone)]
struct TreeEntry {
    sha: String,
    raw_mode: String,
}

fn mode_to_file_mode(raw: &str, kind: FileType) -> u32 {
    match kind {
        FileType::Directory => 0o40755,
        FileType::Symlink => 0o120777,
        FileType::RegularFile if raw.starts_with("100755") => 0o100755,
        _ => 0o100644,
    }
}

impl Filesystem for RipcloneFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy().to_string();

        if parent == FUSE_ROOT_ID && name_str == ".git" {
            let entry = InodeEntry {
                ino: GIT_INODE,
                path: ".git".to_string(),
                kind: FileType::Directory,
                size: 0,
                mode: 0o40755,
            };
            reply.entry(&TTL, &self.attr_for(&entry), 0);
            return;
        }

        let parent_path = match self.path_for_inode(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let child_path = if parent_path.is_empty() {
            name_str.clone()
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        // Fast path: if we already have a stable inode for this path, reuse it.
        if !self.deleted.lock().unwrap().contains(&child_path)
            && let Some(&ino) = self.path_to_inode.lock().unwrap().get(&child_path)
            && let Some(mut e) = self.inodes.lock().unwrap().get(&ino).cloned()
        {
            if let Some(data) = self.overlay.lock().unwrap().get(&child_path) {
                e.size = data.len() as u64;
            }
            reply.entry(&TTL, &self.attr_for(&e), 0);
            return;
        }

        // .git passthrough: stat the real file in the backing git dir.
        if child_path == ".git" || child_path.starts_with(".git/") {
            let real = self.git_dir.join(&child_path);
            if let Ok(meta) = std::fs::metadata(&real) {
                let kind = if meta.is_dir() {
                    FileType::Directory
                } else if meta.file_type().is_symlink() {
                    FileType::Symlink
                } else {
                    FileType::RegularFile
                };
                let mode = if meta.is_dir() {
                    0o40755
                } else if meta.file_type().is_symlink() {
                    0o120777
                } else if meta.permissions().mode() & 0o111 != 0 {
                    0o100755
                } else {
                    0o100644
                };
                let ino = self.alloc_inode();
                let mut e = InodeEntry {
                    ino,
                    path: child_path,
                    kind,
                    size: meta.len(),
                    mode,
                };
                let path = e.path.clone();
                let _ino = self.get_or_create_inode(&path, &mut e);
                reply.entry(&TTL, &self.attr_for(&e), 0);
            } else {
                reply.error(ENOENT);
            }
            return;
        }

        // Check overlay.
        if let Some(data) = self.overlay.lock().unwrap().get(&child_path) {
            let ino = self.alloc_inode();
            let mut entry = InodeEntry {
                ino,
                path: child_path,
                kind: FileType::RegularFile,
                size: data.len() as u64,
                mode: 0o100644,
            };
            let path = entry.path.clone();
            let _ino = self.get_or_create_inode(&path, &mut entry);
            reply.entry(&TTL, &self.attr_for(&entry), 0);
            return;
        }

        if self.deleted.lock().unwrap().contains(&child_path) {
            reply.error(ENOENT);
            return;
        }

        match self.tree_entry(&child_path) {
            Ok(Some(entry)) => {
                let kind = if entry.raw_mode.starts_with("04") {
                    FileType::Directory
                } else if entry.raw_mode.starts_with("120") {
                    FileType::Symlink
                } else {
                    FileType::RegularFile
                };
                let size = if kind == FileType::Directory {
                    0
                } else {
                    self.sizes.get(&child_path).copied().unwrap_or(0)
                };
                let mode = mode_to_file_mode(&entry.raw_mode, kind);
                let ino = self.alloc_inode();
                let mut e = InodeEntry {
                    ino,
                    path: child_path.clone(),
                    kind,
                    size,
                    mode,
                };
                let path = e.path.clone();
                let _ino = self.get_or_create_inode(&path, &mut e);
                reply.entry(&TTL, &self.attr_for(&e), 0);
            }
            Ok(None) => reply.error(ENOENT),
            Err(e) => {
                warn!("lookup error for {}: {}", child_path, e);
                reply.error(ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if ino == GIT_INODE {
            let entry = InodeEntry {
                ino: GIT_INODE,
                path: ".git".to_string(),
                kind: FileType::Directory,
                size: 0,
                mode: 0o40755,
            };
            reply.attr(&TTL, &self.attr_for(&entry));
            return;
        }

        if let Some(path) = self.path_for_inode(ino)
            && let Some(real) = self.real_git_path(&path)
            && let Ok(meta) = std::fs::symlink_metadata(&real)
        {
            let kind = if meta.is_dir() {
                FileType::Directory
            } else if meta.file_type().is_symlink() {
                FileType::Symlink
            } else {
                FileType::RegularFile
            };
            let mode = if meta.is_dir() {
                0o40755
            } else if meta.file_type().is_symlink() {
                0o120777
            } else if meta.permissions().mode() & 0o111 != 0 {
                0o100755
            } else {
                0o100644
            };
            let size = if meta.file_type().is_symlink() {
                std::fs::read_link(&real)
                    .map(|t| t.as_os_str().len() as u64)
                    .unwrap_or(0)
            } else {
                meta.len()
            };
            let entry = InodeEntry {
                ino,
                path,
                kind,
                size,
                mode,
            };
            reply.attr(&TTL, &self.attr_for(&entry));
            return;
        }

        let entry = match self.inodes.lock().unwrap().get(&ino).cloned() {
            Some(e) => e,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        reply.attr(&TTL, &self.attr_for(&entry));
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        let entry = match self.inodes.lock().unwrap().get(&ino).cloned() {
            Some(e) => e,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if entry.kind != FileType::Symlink {
            reply.error(EINVAL);
            return;
        }

        // .git passthrough: read the real symlink target from the backing git dir.
        if let Some(real) = self.real_git_path(&entry.path) {
            match std::fs::read_link(&real) {
                Ok(target) => reply.data(target.as_os_str().as_bytes()),
                Err(_) => reply.error(EIO),
            }
            return;
        }

        // Tree symlink: the blob content is the target path.
        match self.fetch_file(&entry.path) {
            Ok(data) => reply.data(&data),
            Err(e) => {
                warn!(
                    "readlink: failed to fetch symlink target {}: {}",
                    entry.path, e
                );
                reply.error(EIO);
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match self.path_for_inode(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut all_entries: Vec<(String, u64, FileType)> = Vec::new();

        // Determine parent inode for "..".
        let parent_ino = if path == ".git" {
            FUSE_ROOT_ID
        } else {
            let parent_path = std::path::Path::new(&path)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            self.path_to_inode
                .lock()
                .unwrap()
                .get(&parent_path)
                .copied()
                .unwrap_or(FUSE_ROOT_ID)
        };

        all_entries.push((".".to_string(), ino, FileType::Directory));
        all_entries.push(("..".to_string(), parent_ino, FileType::Directory));

        if ino == FUSE_ROOT_ID {
            all_entries.push((".git".to_string(), GIT_INODE, FileType::Directory));
        }

        if path == ".git" || path.starts_with(".git/") {
            // Passthrough .git directory recursively.
            let real = self.git_dir.join(&path);
            if let Ok(dir) = std::fs::read_dir(&real) {
                for entry in dir.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    let kind = if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        FileType::Directory
                    } else if entry.file_type().map(|t| t.is_symlink()).unwrap_or(false) {
                        FileType::Symlink
                    } else {
                        FileType::RegularFile
                    };
                    let child_path = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", path, name)
                    };
                    let mut ie = InodeEntry {
                        ino: 0,
                        path: child_path.clone(),
                        kind,
                        size: entry.metadata().map(|m| m.len()).unwrap_or(0),
                        mode: if kind == FileType::Directory {
                            0o40755
                        } else if kind == FileType::Symlink {
                            0o120777
                        } else {
                            0o100644
                        },
                    };
                    let ino = self.get_or_create_inode(&child_path, &mut ie);
                    all_entries.push((name, ino, kind));
                }
            }
        } else {
            // Prefetch directory blobs (disabled while debugging).
            // self.prefetch_directory(&path);

            let deleted = self.deleted.lock().unwrap();
            if let Ok(children) = self.list_tree_children(&path) {
                for (name, _sha, raw_mode, mode, size) in children {
                    let child_path = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{}", path, name)
                    };
                    if deleted.contains(&child_path) {
                        continue;
                    }
                    let kind = if raw_mode.starts_with("04") {
                        FileType::Directory
                    } else if raw_mode.starts_with("120") {
                        FileType::Symlink
                    } else {
                        FileType::RegularFile
                    };
                    let mut ie = InodeEntry {
                        ino: 0,
                        path: child_path.clone(),
                        kind,
                        size,
                        mode,
                    };
                    let ino = self.get_or_create_inode(&child_path, &mut ie);
                    all_entries.push((name, ino, kind));
                }
            }
        }

        for (i, (name, ino, kind)) in all_entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*ino, (i + 1) as i64, *kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let entry = match self.inodes.lock().unwrap().get(&ino).cloned() {
            Some(e) => e,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // .git passthrough.
        if let Some(real) = self.real_git_path(&entry.path) {
            match std::fs::read(&real) {
                Ok(data) => {
                    reply.data(read_slice(&data, offset, size));
                    return;
                }
                Err(_) => {
                    reply.error(ENOENT);
                    return;
                }
            }
        }

        if self.deleted.lock().unwrap().contains(&entry.path) {
            reply.error(ENOENT);
            return;
        }

        let data = if let Some(data) = self.overlay.lock().unwrap().get(&entry.path) {
            data.clone()
        } else if entry.kind == FileType::RegularFile {
            match self.fetch_file(&entry.path) {
                Ok(d) => {
                    if let Some(e) = self.inodes.lock().unwrap().get_mut(&ino) {
                        e.size = d.len() as u64;
                    }
                    d
                }
                Err(e) => {
                    warn!("read: failed to fetch file {}: {}", entry.path, e);
                    reply.error(ENOENT);
                    return;
                }
            }
        } else {
            reply.error(ENOENT);
            return;
        };

        reply.data(read_slice(&data, offset, size));
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let entry = match self.inodes.lock().unwrap().get(&ino).cloned() {
            Some(e) => e,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        // Passthrough writes to the real .git directory.
        if entry.path == ".git" || entry.path.starts_with(".git/") {
            let real = self.git_dir.join(&entry.path);
            match std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&real)
            {
                Ok(mut f) => {
                    if let Err(e) = f
                        .seek(SeekFrom::Start(offset as u64))
                        .and_then(|_| f.write_all(data))
                    {
                        warn!(".git write failed for {}: {}", entry.path, e);
                        reply.error(EIO);
                        return;
                    }
                    reply.written(data.len() as u32);
                }
                Err(e) => {
                    warn!(".git open for write failed for {}: {}", entry.path, e);
                    reply.error(EIO);
                }
            }
            return;
        }

        let mut overlay = self.overlay.lock().unwrap();
        let content = overlay.entry(entry.path.clone()).or_insert_with(|| {
            if entry.kind == FileType::RegularFile {
                return self.fetch_file(&entry.path).unwrap_or_default();
            }
            Vec::new()
        });

        let offset = offset as usize;
        if offset > content.len() {
            content.resize(offset, 0);
        }
        let end = offset + data.len();
        if end > content.len() {
            content.resize(end, 0);
        }
        content[offset..end].copy_from_slice(data);

        if let Some(entry) = self.inodes.lock().unwrap().get_mut(&ino) {
            entry.size = content.len() as u64;
        }

        reply.written(data.len() as u32);
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let parent_path = match self.path_for_inode(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let name_str = name.to_string_lossy().to_string();
        let child_path = if parent_path.is_empty() {
            name_str
        } else {
            format!("{}/{}", parent_path, name_str)
        };

        // Passthrough creates to the real .git directory.
        if child_path == ".git" || child_path.starts_with(".git/") {
            let real = self.git_dir.join(&child_path);
            match std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&real)
            {
                Ok(_) => {
                    let ino = self.alloc_inode();
                    let mut entry = InodeEntry {
                        ino,
                        path: child_path.clone(),
                        kind: FileType::RegularFile,
                        size: 0,
                        mode,
                    };
                    let ino = self.get_or_create_inode(&child_path, &mut entry);
                    reply.created(&TTL, &self.attr_for(&entry), 0, ino, flags as u32);
                }
                Err(e) => {
                    warn!(".git create failed for {}: {}", child_path, e);
                    reply.error(EIO);
                }
            }
            return;
        }

        let ino = self.alloc_inode();
        let mut entry = InodeEntry {
            ino,
            path: child_path.clone(),
            kind: FileType::RegularFile,
            size: 0,
            mode,
        };
        let ino = self.get_or_create_inode(&child_path, &mut entry);
        self.overlay.lock().unwrap().insert(child_path, Vec::new());
        reply.created(&TTL, &self.attr_for(&entry), 0, ino, flags as u32);
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let parent_path = match self.path_for_inode(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let child_path = if parent_path.is_empty() {
            name.to_string_lossy().to_string()
        } else {
            format!("{}/{}", parent_path, name.to_string_lossy())
        };
        // Passthrough unlinks to the real .git directory.
        if child_path == ".git" || child_path.starts_with(".git/") {
            let real = self.git_dir.join(&child_path);
            if let Err(e) = std::fs::remove_file(&real) {
                warn!(".git unlink failed for {}: {}", child_path, e);
                reply.error(EIO);
                return;
            }
            reply.ok();
            return;
        }

        self.overlay.lock().unwrap().remove(&child_path);
        self.deleted.lock().unwrap().insert(child_path);
        reply.ok();
    }
}

/// Compute the slice of `data` for a FUSE `read` call without panicking on
/// huge or negative offsets. Returns an empty slice when the offset is past
/// the end of the data.
fn read_slice(data: &[u8], offset: i64, size: u32) -> &[u8] {
    if offset < 0 {
        return &[];
    }
    let off = match usize::try_from(offset) {
        Ok(v) => v,
        Err(_) => return &[],
    };
    if off >= data.len() {
        return &[];
    }
    let size_usize = size as usize;
    let end = off
        .checked_add(size_usize)
        .map(|e| e.min(data.len()))
        .unwrap_or(data.len());
    &data[off..end]
}

pub fn mount<P: AsRef<Path>>(
    owner: &str,
    repo: &str,
    branch: &str,
    server: &str,
    git_dir: P,
    commit: &str,
    sizes: HashMap<String, u64>,
    mount_point: P,
) -> Result<()> {
    let fs = RipcloneFs::new(
        owner.to_string(),
        repo.to_string(),
        branch.to_string(),
        server.to_string(),
        git_dir.as_ref(),
        commit.to_string(),
        sizes,
    );
    let mountpoint = mount_point.as_ref().to_path_buf();
    fuser::mount2(fs, &mountpoint, &[])?;
    Ok(())
}
