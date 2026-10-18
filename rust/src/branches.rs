//! A repository's branches artifact and side pack, so a full clone has
//! every branch and tag as plain `git clone` does. A refresh builds both
//! from the mirror, which then holds every upstream ref; a full clone that
//! holds the default branch's commit installs the side pack and writes
//! every ref. See `docs/internal/rebuild/TAGS.md`.

use crate::clonepack::pb::{Branch, BranchesArtifact};
use anyhow::{Context, Result, ensure};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The branches of a full clone's response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchesRef {
    /// CAS id of the zstd-compressed `BranchesArtifact`.
    pub artifact: String,
    #[serde(default)]
    pub artifact_url: Option<String>,
    /// Absent when every branch and tag is in the default branch's history.
    #[serde(default)]
    pub side_pack: Option<SidePackRef>,
}

/// The side pack and its idx, with signed URLs when the backend signs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidePackRef {
    pub pack: String,
    pub pack_len: u64,
    #[serde(default)]
    pub pack_url: Option<String>,
    pub idx: String,
    pub idx_len: u64,
    #[serde(default)]
    pub idx_url: Option<String>,
}

/// The hash of a ref listing: the default branch, then every branch and
/// tag with the id its ref names. The mirror's ([`MirrorRefs`]) and
/// upstream's (`git::UpstreamRefs`) hash alike when they hold the same refs.
pub fn refs_hash(
    default_branch: Option<&str>,
    branches: &[(String, String)],
    tags: &[(String, String)],
) -> String {
    let mut text = format!("HEAD {}\n", default_branch.unwrap_or_default());
    for (name, id) in branches {
        text.push_str(&format!("{id} refs/heads/{name}\n"));
    }
    for (name, id) in tags {
        text.push_str(&format!("{id} refs/tags/{name}\n"));
    }
    crate::cas::hash(text.as_bytes())
}

/// The mirror's refs, read without touching an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorRefs {
    /// The branch the mirror's `HEAD` names, when it is one of `branches`.
    pub default_branch: Option<String>,
    /// `(name under refs/heads/, commit)`, sorted by name.
    pub branches: Vec<(String, String)>,
    /// As [`crate::tags::listing`] gives them.
    pub tags: Vec<(String, String)>,
    /// [`crate::tags::listing`]'s hash of `tags`.
    pub tags_hash: String,
}

impl MirrorRefs {
    pub fn read(repo: &Path) -> Result<Self> {
        let (tags, tags_hash) = crate::tags::listing(repo)?;
        let repo = crate::gix_util::open_repo(repo)?;
        let head = repo.head_name().context("read HEAD")?.and_then(|name| {
            name.as_bstr()
                .to_string()
                .strip_prefix("refs/heads/")
                .map(str::to_string)
        });
        let references = repo.references().context("read refs")?;
        let mut branches = Vec::new();
        for reference in references
            .local_branches()
            .map_err(|e| anyhow::anyhow!("list branches: {e}"))?
        {
            let reference = reference.map_err(|e| anyhow::anyhow!("read branch: {e}"))?;
            let Some(id) = reference.target().try_id().map(|id| id.to_string()) else {
                continue;
            };
            let name = reference.name().shorten().to_string();
            // As upstream's listing leaves it out.
            if crate::validation::validate_checkout_name(&name).is_err() {
                continue;
            }
            branches.push((name, id));
        }
        branches.sort();
        let default_branch = head.filter(|head| branches.iter().any(|(name, _)| name == head));
        Ok(Self {
            default_branch,
            branches,
            tags,
            tags_hash,
        })
    }

    /// [`refs_hash`] of these refs.
    pub fn hash(&self) -> String {
        refs_hash(self.default_branch.as_deref(), &self.branches, &self.tags)
    }

    /// The default branch's commit.
    pub fn base(&self) -> Option<&str> {
        let default = self.default_branch.as_deref()?;
        (self.branches.iter())
            .find(|(name, _)| name == default)
            .map(|(_, id)| id.as_str())
    }

    /// The ids the side pack completes: every branch's but the default
    /// branch's, and every tag's, sorted and deduplicated.
    fn side_tips(&self) -> Vec<&str> {
        let default = self.default_branch.as_deref();
        let mut tips: Vec<&str> = (self.branches.iter())
            .filter(|(name, _)| Some(name.as_str()) != default)
            .chain(&self.tags)
            .map(|(_, id)| id.as_str())
            .collect();
        tips.sort_unstable();
        tips.dedup();
        tips
    }

    /// The hash of [`Self::side_tips`]: a side pack built for refs of the
    /// same hash, against a commit in the default branch's history, still
    /// completes every ref.
    pub fn side_tips_hash(&self) -> String {
        crate::cas::hash(self.side_tips().join("\n").as_bytes())
    }

    /// The zstd-compressed artifact for these branches. The same refs
    /// always give the same bytes.
    pub fn build(&self) -> Result<Vec<u8>> {
        let artifact = BranchesArtifact {
            default_branch: self.default_branch.clone().unwrap_or_default(),
            base: self.base().unwrap_or_default().to_string(),
            branches: (self.branches.iter())
                .map(|(name, commit)| Branch {
                    name: name.clone(),
                    commit: commit.clone(),
                })
                .collect(),
        };
        zstd::encode_all(&artifact.encode_to_vec()[..], 3).context("compress branches artifact")
    }

    /// Write into `dir` the pack of every object reachable from a branch
    /// or tag but not from the default branch, and its idx. `None` when
    /// there is no default branch or nothing to pack.
    pub fn side_pack(&self, repo: &Path, dir: &Path) -> Result<Option<(PathBuf, PathBuf)>> {
        let Some(base) = self.base() else {
            return Ok(None);
        };
        let tips: Vec<&str> = (self.side_tips().into_iter())
            .filter(|id| *id != base)
            .collect();
        if tips.is_empty() {
            return Ok(None);
        }
        crate::git::pack_objects_excluding(repo, &tips, base, dir.join("side"))?;
        let mut pack = None;
        let mut idx = None;
        for entry in std::fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
            let path = entry?.path();
            match path.extension().and_then(|ext| ext.to_str()) {
                Some("pack") => pack = Some(path),
                Some("idx") => idx = Some(path),
                _ => {}
            }
        }
        let (pack, idx) = (
            pack.context("pack-objects wrote no pack")?,
            idx.context("pack-objects wrote no idx")?,
        );
        // A pack of no objects is its 12-byte header and 20-byte checksum.
        if std::fs::metadata(&pack)?.len() <= 32 {
            return Ok(None);
        }
        Ok(Some((pack, idx)))
    }
}

/// A full clone's branches, checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Branches {
    pub default_branch: Option<String>,
    /// The default branch's commit; the clone must hold it for the side
    /// pack to complete every ref.
    pub base: Option<String>,
    /// `(name, commit)`, sorted by name.
    pub branches: Vec<(String, String)>,
}

/// Decode and check a downloaded branches artifact: a name git would
/// refuse, an id that is not one, or a base that is not the default
/// branch's commit fails it whole.
pub fn decode(bytes: &[u8]) -> Result<Branches> {
    let integrity = "branches artifact integrity error";
    let raw = zstd::decode_all(bytes).context("decompress branches artifact")?;
    let artifact = BranchesArtifact::decode(&raw[..]).context("decode branches artifact")?;
    let mut branches = Vec::with_capacity(artifact.branches.len());
    for branch in artifact.branches {
        crate::validation::validate_checkout_name(&branch.name)
            .with_context(|| format!("{integrity}: invalid branch"))?;
        crate::tags::parse_id(&branch.commit)
            .with_context(|| format!("{integrity}: branch {}", branch.name))?;
        branches.push((branch.name, branch.commit));
    }
    branches.sort();
    ensure!(
        branches.windows(2).all(|pair| pair[0].0 != pair[1].0),
        "{integrity}: a branch is listed twice"
    );
    let default_branch = Some(artifact.default_branch).filter(|name| !name.is_empty());
    let base = Some(artifact.base).filter(|base| !base.is_empty());
    let default_commit = default_branch.as_ref().and_then(|default| {
        (branches.iter())
            .find(|(name, _)| name == default)
            .map(|(_, commit)| commit)
    });
    ensure!(
        default_branch.is_none() || default_commit.is_some(),
        "{integrity}: the default branch is not listed"
    );
    ensure!(
        base.as_ref() == default_commit,
        "{integrity}: the base is not the default branch's commit"
    );
    Ok(Branches {
        default_branch,
        base,
        branches,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn git(repo: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .env("GIT_COMMITTER_DATE", "2026-01-01T00:00:00Z")
            .env("GIT_AUTHOR_DATE", "2026-01-01T00:00:00Z")
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {out:?}");
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }

    /// A repo whose `main` has a tag, with a side branch carrying a tag of
    /// its own and a tag on a blob.
    fn repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        git(repo, &["init", "-q", "-b", "main"]);
        git(repo, &["config", "user.email", "t@example.com"]);
        git(repo, &["config", "user.name", "t"]);
        git(repo, &["commit", "-q", "--allow-empty", "-m", "a"]);
        git(repo, &["tag", "-a", "v1", "-m", "v1"]);
        git(repo, &["checkout", "-q", "-b", "side"]);
        std::fs::write(repo.join("f"), "side").unwrap();
        git(repo, &["add", "f"]);
        git(repo, &["commit", "-q", "-m", "side"]);
        git(repo, &["tag", "on-side"]);
        let blob = git(repo, &["hash-object", "-w", "--stdin"]);
        git(repo, &["tag", "-a", "on-blob", "-m", "blob", &blob]);
        git(repo, &["checkout", "-q", "main"]);
        dir
    }

    /// The mirror's refs hash as upstream's listing of the same refs; the
    /// artifact round-trips; the side pack holds exactly what the default
    /// branch's history lacks.
    #[test]
    fn side_pack_completes_every_ref() {
        let dir = repo();
        let repo = dir.path();
        let refs = MirrorRefs::read(repo).unwrap();
        assert_eq!(refs.default_branch.as_deref(), Some("main"));
        let names: Vec<&str> = refs.branches.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["main", "side"]);
        let main = git(repo, &["rev-parse", "main"]);
        assert_eq!(refs.base(), Some(main.as_str()));
        let upstream = crate::git::UpstreamRefs {
            default_branch: Some("main".into()),
            branches: refs.branches.clone(),
            tags: refs.tags.clone(),
        };
        assert_eq!(upstream.hash(), refs.hash());
        let renamed = crate::git::UpstreamRefs {
            default_branch: Some("side".into()),
            ..upstream
        };
        assert_ne!(renamed.hash(), refs.hash());

        let branches = decode(&refs.build().unwrap()).unwrap();
        assert_eq!(branches.base.as_deref(), Some(main.as_str()));
        assert_eq!(branches.branches, refs.branches);

        let out = tempfile::tempdir().unwrap();
        let (_, idx) = refs.side_pack(repo, out.path()).unwrap().unwrap();
        let index = gix::odb::pack::index::File::at(&idx, gix::hash::Kind::Sha1).unwrap();
        let mut packed: Vec<String> = (0..index.num_objects())
            .map(|i| index.oid_at_index(i).to_string())
            .collect();
        packed.sort();
        let tips: Vec<String> = ["side", "on-side", "on-blob", "v1"]
            .iter()
            .map(|name| git(repo, &["rev-parse", name]))
            .collect();
        let mut args = vec!["rev-list", "--objects", "--no-object-names"];
        args.extend(tips.iter().map(String::as_str));
        let exclude = format!("^{main}");
        args.push(&exclude);
        let mut wanted: Vec<String> = git(repo, &args).lines().map(str::to_string).collect();
        wanted.sort();
        assert_eq!(packed, wanted);
        assert!(
            packed.contains(&git(repo, &["rev-parse", "v1"])),
            "tag objects on main's history come along"
        );
    }

    /// Only `main`, and its tags: no side pack.
    #[test]
    fn no_side_pack_when_main_holds_everything() {
        let dir = repo();
        let repo = dir.path();
        git(repo, &["branch", "-D", "side"]);
        git(repo, &["tag", "-d", "on-side", "on-blob"]);
        git(repo, &["tag", "light"]);
        let refs = MirrorRefs::read(repo).unwrap();
        let out = tempfile::tempdir().unwrap();
        // The annotated tag `v1` is not in main's history, so it is packed.
        assert!(refs.side_pack(repo, out.path()).unwrap().is_some());
        git(repo, &["tag", "-d", "v1"]);
        let refs = MirrorRefs::read(repo).unwrap();
        let out = tempfile::tempdir().unwrap();
        assert_eq!(refs.side_pack(repo, out.path()).unwrap(), None);
    }

    /// A base that is not the default branch's commit, or an invalid name,
    /// fails the artifact.
    #[test]
    fn decode_refuses_inconsistent_artifacts() {
        let encode = |artifact: BranchesArtifact| {
            zstd::encode_all(&artifact.encode_to_vec()[..], 3).unwrap()
        };
        let branch = |name: &str, commit: &str| Branch {
            name: name.into(),
            commit: commit.into(),
        };
        let (a, b) = ("a".repeat(40), "b".repeat(40));
        let good = BranchesArtifact {
            default_branch: "main".into(),
            base: a.clone(),
            branches: vec![branch("main", &a), branch("next", &b)],
        };
        assert!(decode(&encode(good.clone())).is_ok());
        for bad in [
            BranchesArtifact {
                base: b.clone(),
                ..good.clone()
            },
            BranchesArtifact {
                default_branch: "gone".into(),
                ..good.clone()
            },
            BranchesArtifact {
                branches: vec![branch("main", &a), branch("bad..name", &b)],
                ..good.clone()
            },
            BranchesArtifact {
                branches: vec![branch("main", &a), branch("main", &b)],
                ..good.clone()
            },
            BranchesArtifact {
                branches: vec![branch("main", &a), branch("next", "zz")],
                ..good.clone()
            },
        ] {
            assert!(decode(&encode(bad)).is_err());
        }
    }
}
