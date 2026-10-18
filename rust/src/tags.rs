//! A repository's tags artifact: the server builds one from its mirror's
//! `refs/tags/`, and a clone keeps the tags whose peeled target is among
//! its objects, as `git clone` gets them. See
//! `docs/internal/rebuild/TAGS.md`.

use crate::clonepack::pb::{Tag, TagsArtifact};
use anyhow::{Context, Result, ensure};
use prost::Message;
use std::collections::BTreeMap;
use std::path::Path;

/// The longest tag-of-tag chain followed. Git itself stops peeling sooner.
const MAX_CHAIN: usize = 32;

/// The mirror's tags as `(name under refs/tags/, object id)`, sorted by
/// name, and a hash of that listing. Reads only refs, never objects, so a
/// publish with no tag change costs no more than this.
pub fn listing(repo: &Path) -> Result<(Vec<(String, String)>, String)> {
    let repo = crate::gix_util::open_repo(repo)?;
    let references = repo.references().context("read refs")?;
    let mut tags = Vec::new();
    for reference in references
        .tags()
        .map_err(|e| anyhow::anyhow!("list tags: {e}"))?
    {
        let reference = reference.map_err(|e| anyhow::anyhow!("read tag: {e}"))?;
        // A symbolic ref under refs/tags/ names no object; git clone skips it.
        let Some(id) = reference.target().try_id().map(|id| id.to_string()) else {
            continue;
        };
        let name = reference.name().shorten().to_string();
        // A name a clone would refuse to write is left out, so one odd tag
        // never fails every clone.
        if crate::validation::validate_tag_name(&name).is_err() {
            continue;
        }
        tags.push((name, id));
    }
    tags.sort();
    let mut text = String::new();
    for (name, id) in &tags {
        text.push_str(&format!("{id} {name}\n"));
    }
    let hash = crate::cas::hash(text.as_bytes());
    Ok((tags, hash))
}

/// The zstd-compressed artifact for `tags` (from [`listing`]). The same
/// tags always give the same bytes.
pub fn build(repo: &Path, tags: &[(String, String)]) -> Result<Vec<u8>> {
    let repo = crate::gix_util::open_repo(repo)?;
    let mut objects = BTreeMap::new();
    let mut out = Vec::with_capacity(tags.len());
    for (name, id) in tags {
        let mut current = gix::hash::ObjectId::from_hex(id.as_bytes())
            .with_context(|| format!("tag {name}: invalid object id {id}"))?;
        let mut chain = Vec::new();
        let mut target = None;
        // Up to MAX_CHAIN tag objects, then the target.
        for _ in 0..=MAX_CHAIN {
            let object = repo
                .find_object(current)
                .with_context(|| format!("tag {name}: find {current}"))?;
            if object.kind != gix::object::Kind::Tag {
                target = Some(current);
                break;
            }
            let data = object.data.clone();
            let tag = object
                .try_into_tag()
                .with_context(|| format!("tag {name}: {current} is not a tag"))?;
            let next = tag
                .decode()
                .with_context(|| format!("tag {name}: decode {current}"))?
                .target();
            chain.push((current.to_string(), data));
            current = next;
        }
        // A clone would refuse it, so it is left out rather than failing
        // every clone, as a name a clone would refuse is.
        let Some(target) = target else {
            tracing::warn!("tag {name}: more than {MAX_CHAIN} tag objects deep, left out");
            continue;
        };
        objects.extend(chain);
        let target = target.to_string();
        out.push(Tag {
            name: name.clone(),
            object: id.clone(),
            peeled: if target == *id { String::new() } else { target },
        });
    }
    let artifact = TagsArtifact {
        tags: out,
        objects: objects.into_values().collect(),
    };
    zstd::encode_all(&artifact.encode_to_vec()[..], 3).context("compress tags artifact")
}

/// Decode a downloaded tags artifact.
pub fn decode(bytes: &[u8]) -> Result<TagsArtifact> {
    let raw = zstd::decode_all(bytes).context("decompress tags artifact")?;
    TagsArtifact::decode(&raw[..]).context("decode tags artifact")
}

/// A SHA-1 object id.
pub(crate) type Id = [u8; 20];

/// One tag a clone keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeptTag {
    /// Under `refs/tags/`.
    pub name: String,
    /// What the ref names.
    pub object: String,
    /// `object` fully peeled; `object` itself for a lightweight tag.
    pub target: String,
    target_id: Id,
    /// The annotated tag objects from `object` down to `target`, as
    /// indexes into [`Kept::objects`].
    chain: Vec<u32>,
}

impl KeptTag {
    /// `target` as bytes.
    pub fn target_id(&self) -> &[u8; 20] {
        &self.target_id
    }
}

/// The tags a clone keeps, ready to write.
#[derive(Debug, Default)]
pub struct Kept {
    /// Sorted by name.
    pub tags: Vec<KeptTag>,
    /// Every tag object of the artifact, sorted by id.
    objects: Vec<(Id, Vec<u8>)>,
}

impl Kept {
    /// Keep only the tags `keep` accepts.
    pub fn retain(&mut self, keep: impl FnMut(&KeptTag) -> bool) {
        self.tags.retain(keep);
    }

    /// The tag objects the kept tags need, sorted by id.
    fn objects(&self) -> Vec<(&Id, &[u8])> {
        let mut needed: Vec<u32> = self
            .tags
            .iter()
            .flat_map(|tag| tag.chain.iter().copied())
            .collect();
        needed.sort_unstable();
        needed.dedup();
        needed
            .into_iter()
            .map(|index| {
                let (id, data) = &self.objects[index as usize];
                (id, data.as_slice())
            })
            .collect()
    }
}

/// Every tag of `artifact`, checked: a name git would refuse, an object id
/// that is not one, or a chain of tag objects that is missing or does not
/// end at `peeled` fails the whole artifact, so a bad one writes nothing.
/// Needs no clone objects, so it runs while they download; the caller then
/// keeps the tags whose target the clone holds with [`Kept::retain`].
pub fn check(artifact: TagsArtifact) -> Result<Kept> {
    let integrity = "tags artifact integrity error";
    let mut objects: Vec<(Id, Vec<u8>)> = artifact
        .objects
        .into_iter()
        .map(|data| (tag_object_id(&data), data))
        .collect();
    objects.sort_unstable_by_key(|(id, _)| *id);
    objects.dedup_by(|a, b| a.0 == b.0);
    let index: std::collections::HashMap<Id, u32> = (objects.iter().enumerate())
        .map(|(i, (id, _))| (*id, i as u32))
        .collect();
    let mut tags = Vec::with_capacity(artifact.tags.len());
    for tag in artifact.tags {
        crate::validation::validate_tag_name(&tag.name)
            .with_context(|| format!("{integrity}: invalid tag"))?;
        let object =
            parse_id(&tag.object).with_context(|| format!("{integrity}: tag {}", tag.name))?;
        let mut chain = Vec::new();
        let (target, target_id) = if tag.peeled.is_empty() {
            ensure!(
                !index.contains_key(&object),
                "{integrity}: tag {} names a tag object but has no target",
                tag.name
            );
            (tag.object.clone(), object)
        } else {
            let peeled =
                parse_id(&tag.peeled).with_context(|| format!("{integrity}: tag {}", tag.name))?;
            let mut current = object;
            while let Some(&i) = index.get(&current) {
                ensure!(
                    chain.len() < MAX_CHAIN,
                    "{integrity}: tag {} chain is too long",
                    tag.name
                );
                chain.push(i);
                current = tag_target(&objects[i as usize].1)
                    .with_context(|| format!("{integrity}: tag {}", tag.name))?;
            }
            ensure!(
                !chain.is_empty() && current == peeled,
                "{integrity}: tag {} does not peel to {}",
                tag.name,
                tag.peeled
            );
            (tag.peeled, peeled)
        };
        tags.push(KeptTag {
            name: tag.name,
            object: tag.object,
            target,
            target_id,
            chain,
        });
    }
    tags.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    ensure!(
        tags.windows(2).all(|pair| pair[0].name != pair[1].name),
        "{integrity}: a tag is listed twice"
    );
    Ok(Kept { tags, objects })
}

/// The objects of a clone's `.git`: its pack indexes, and the loose
/// objects a top-up may leave. Asking gix per object would check the disk
/// for a loose object on every miss, and in a shallow clone almost every
/// tag target misses.
pub struct Present {
    indexes: Vec<gix::odb::pack::index::File>,
    loose: std::collections::HashSet<Id>,
}

impl Present {
    pub fn scan(git_dir: &Path) -> Result<Self> {
        let objects = git_dir.join("objects");
        let mut indexes = Vec::new();
        let pack_dir = objects.join("pack");
        for entry in
            std::fs::read_dir(&pack_dir).with_context(|| format!("read {}", pack_dir.display()))?
        {
            let path = entry?.path();
            if path.extension().is_some_and(|ext| ext == "idx") {
                let index = gix::odb::pack::index::File::at(&path, gix::hash::Kind::Sha1)
                    .with_context(|| format!("open {}", path.display()))?;
                indexes.push(index);
            }
        }
        let mut loose = std::collections::HashSet::new();
        for entry in
            std::fs::read_dir(&objects).with_context(|| format!("read {}", objects.display()))?
        {
            let entry = entry?;
            let Some(prefix) = entry
                .file_name()
                .to_str()
                .filter(|n| n.len() == 2)
                .map(String::from)
            else {
                continue;
            };
            for object in std::fs::read_dir(entry.path())? {
                if let Some(rest) = object?.file_name().to_str()
                    && let Ok(id) = parse_id(&format!("{prefix}{rest}"))
                {
                    loose.insert(id);
                }
            }
        }
        Ok(Self { indexes, loose })
    }

    pub fn contains(&self, id: &Id) -> bool {
        let oid = gix::oid::from_bytes_unchecked(id);
        self.loose.contains(id) || self.indexes.iter().any(|index| index.lookup(oid).is_some())
    }

    /// [`Self::contains`] for a hex id; false for one that does not parse.
    pub fn contains_hex(&self, hex: &str) -> bool {
        parse_id(hex).is_ok_and(|id| self.contains(&id))
    }
}

/// A hex object id as bytes. Clones are SHA-1, so only lowercase 40-hex
/// ids parse.
pub(crate) fn parse_id(hex: &str) -> Result<Id> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            _ => None,
        }
    }
    let invalid = || anyhow::anyhow!("not a SHA-1 object id: {hex}");
    let bytes = hex.as_bytes();
    ensure!(bytes.len() == 40, invalid());
    let mut id = [0; 20];
    for (out, pair) in id.iter_mut().zip(bytes.chunks_exact(2)) {
        *out = (nibble(pair[0]).ok_or_else(invalid)? << 4) | nibble(pair[1]).ok_or_else(invalid)?;
    }
    Ok(id)
}

/// The object id git gives a tag object with this content.
fn tag_object_id(data: &[u8]) -> Id {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(format!("tag {}\0", data.len()).as_bytes());
    hasher.update(data);
    hasher.finalize().into()
}

/// The object a tag object names: its `object <id>` header line.
fn tag_target(data: &[u8]) -> Result<Id> {
    let line = data
        .split(|&b| b == b'\n')
        .next()
        .context("empty tag object")?;
    let id = line
        .strip_prefix(b"object ")
        .context("tag object does not start with an object line")?;
    parse_id(std::str::from_utf8(id).context("tag object id is not text")?)
}

/// Write `kept` into `git_dir`: the tag objects `present` lacks as one
/// pack and its idx in `objects/pack`, and the refs as a sorted
/// `packed-refs` with no peeled lines, which git peels itself, as `git
/// clone` writes them. `remotes` are `(branch, commit)` pairs, sorted by
/// name, written as `refs/remotes/origin/<branch>`. Loose objects would be
/// one file each; the pack holds only the kept objects, so `git fsck` finds
/// no dangling tag that `git clone` would not have.
pub fn write(
    git_dir: &Path,
    kept: &Kept,
    remotes: &[(String, String)],
    present: &Present,
) -> Result<()> {
    if kept.tags.is_empty() && remotes.is_empty() {
        return Ok(());
    }
    let path = git_dir.join("packed-refs");
    ensure!(
        !path.exists(),
        "{} already exists; refs would replace its refs",
        path.display()
    );
    let objects: Vec<_> = (kept.objects().into_iter())
        .filter(|(id, _)| !present.contains(id))
        .collect();
    if !objects.is_empty() {
        let (pack, idx, checksum) = pack(&objects)?;
        let dir = git_dir.join("objects").join("pack");
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        // The idx last: git ignores a pack until its idx is there.
        for (ext, bytes) in [("pack", pack), ("idx", idx)] {
            let file = dir.join(format!("pack-{checksum}.{ext}"));
            std::fs::write(&file, bytes).with_context(|| format!("write {}", file.display()))?;
        }
    }
    // `refs/remotes/` sorts before `refs/tags/`.
    let refs = (remotes.iter())
        .map(|(name, commit)| (format!("refs/remotes/origin/{name}"), commit.as_str()))
        .chain(
            (kept.tags.iter()).map(|tag| (format!("refs/tags/{}", tag.name), tag.object.as_str())),
        );
    write_packed_refs(&path, refs)
}

/// Write each kept tag as a ref straight to its target, with no objects, so
/// git can say which targets a commit reaches (`for-each-ref --merged`).
/// The caller removes the file before [`write`].
pub fn write_target_refs(git_dir: &Path, kept: &Kept) -> Result<()> {
    let refs =
        (kept.tags.iter()).map(|tag| (format!("refs/tags/{}", tag.name), tag.target.as_str()));
    write_packed_refs(&git_dir.join("packed-refs"), refs)
}

/// A sorted `packed-refs` with no peeled lines, which git peels itself.
/// `refs` are `(full name, id)`, sorted by name.
fn write_packed_refs<'a>(path: &Path, refs: impl Iterator<Item = (String, &'a str)>) -> Result<()> {
    let mut packed = String::from("# pack-refs with: sorted \n");
    for (name, id) in refs {
        packed.push_str(&format!("{id} {name}\n"));
    }
    std::fs::write(path, packed).with_context(|| format!("write {}", path.display()))
}

/// A version 2 pack of undeltified tag objects (sorted by id), its version
/// 2 idx, and its checksum. Each object is a stored (uncompressed) zlib
/// stream: tag objects are small and stay on local disk, and deflating
/// each one costs more than the rest of the clone's tag work.
fn pack(objects: &[(&Id, &[u8])]) -> Result<(Vec<u8>, Vec<u8>, String)> {
    use sha1::{Digest, Sha1};
    const OBJ_TAG: u8 = 4;
    let count = u32::try_from(objects.len()).context("too many tag objects")?;
    let raw: usize = objects.iter().map(|(_, data)| data.len() + 32).sum();
    let mut out = Vec::with_capacity(12 + raw + 20);
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    out.extend_from_slice(&count.to_be_bytes());
    let mut entries = Vec::with_capacity(objects.len());
    for (_, data) in objects {
        let start = out.len();
        let mut size = data.len();
        let mut byte = (OBJ_TAG << 4) | (size & 0x0f) as u8;
        size >>= 4;
        while size > 0 {
            out.push(byte | 0x80);
            byte = (size & 0x7f) as u8;
            size >>= 7;
        }
        out.push(byte);
        stored_zlib(data, &mut out);
        let mut crc = flate2::Crc::new();
        crc.update(&out[start..]);
        entries.push((start as u64, crc.sum()));
    }
    let checksum: [u8; 20] = Sha1::digest(&out).into();
    out.extend_from_slice(&checksum);

    let mut idx = Vec::with_capacity(8 + 256 * 4 + objects.len() * 28 + 40);
    idx.extend_from_slice(b"\xfftOc");
    idx.extend_from_slice(&2u32.to_be_bytes());
    let mut fanout = [0u32; 256];
    for (id, _) in objects {
        fanout[id[0] as usize] += 1;
    }
    let mut total = 0;
    for slot in fanout {
        total += slot;
        idx.extend_from_slice(&total.to_be_bytes());
    }
    for (id, _) in objects {
        idx.extend_from_slice(&id[..]);
    }
    for (_, crc) in &entries {
        idx.extend_from_slice(&crc.to_be_bytes());
    }
    let mut large = Vec::new();
    for (offset, _) in &entries {
        let small = match u32::try_from(*offset) {
            Ok(offset) if offset < 0x8000_0000 => offset,
            _ => {
                large.push(*offset);
                0x8000_0000 | u32::try_from(large.len() - 1).context("too many large offsets")?
            }
        };
        idx.extend_from_slice(&small.to_be_bytes());
    }
    for offset in large {
        idx.extend_from_slice(&offset.to_be_bytes());
    }
    idx.extend_from_slice(&checksum);
    let idx_checksum = Sha1::digest(&idx);
    idx.extend_from_slice(&idx_checksum);
    Ok((out, idx, hex::encode(checksum)))
}

/// `data` as a zlib stream of stored deflate blocks.
fn stored_zlib(data: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&[0x78, 0x01]);
    let mut blocks = data.chunks(0xffff).peekable();
    if blocks.peek().is_none() {
        out.extend_from_slice(&[1, 0, 0, 0xff, 0xff]);
    }
    while let Some(block) = blocks.next() {
        out.push(u8::from(blocks.peek().is_none()));
        let len = block.len() as u16;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes());
        out.extend_from_slice(block);
    }
    // Adler-32, summed in runs short enough that the sums cannot overflow.
    let (mut a, mut b) = (1u32, 0u32);
    for run in data.chunks(5552) {
        for &byte in run {
            a += u32::from(byte);
            b += a;
        }
        a %= 65521;
        b %= 65521;
    }
    out.extend_from_slice(&((b << 16) | a).to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn keep(artifact: TagsArtifact, has: impl Fn(&str) -> bool) -> Result<Kept> {
        let mut kept = check(artifact)?;
        kept.retain(|tag| has(&tag.target));
        Ok(kept)
    }

    fn git(dir: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .env("GIT_COMMITTER_DATE", "1700000000 +0000")
            .env("GIT_AUTHOR_DATE", "1700000000 +0000")
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {out:?}");
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }

    /// A repo with an annotated tag, a tag of it, a lightweight tag and a
    /// tag on a tree, on commit B; and an annotated tag on a side commit S.
    fn repo() -> (tempfile::TempDir, String, String) {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        git(d, &["init", "-q", "-b", "main"]);
        std::fs::write(d.join("a"), "a\n").unwrap();
        git(d, &["add", "a"]);
        git(d, &["commit", "-qm", "B"]);
        let b = git(d, &["rev-parse", "HEAD"]);
        git(d, &["tag", "-a", "-m", "v1", "v1"]);
        git(
            d,
            &[
                "-c",
                "advice.nestedTag=false",
                "tag",
                "-a",
                "-m",
                "of",
                "v1-of",
                "v1",
            ],
        );
        git(d, &["tag", "light"]);
        git(
            d,
            &[
                "tag",
                "-a",
                "-m",
                "tree",
                "on-tree",
                &format!("{b}^{{tree}}"),
            ],
        );
        git(d, &["checkout", "-q", "-b", "side"]);
        std::fs::write(d.join("s"), "s\n").unwrap();
        git(d, &["add", "s"]);
        git(d, &["commit", "-qm", "S"]);
        let s = git(d, &["rev-parse", "HEAD"]);
        git(d, &["tag", "-a", "-m", "side", "v-side"]);
        (dir, b, s)
    }

    #[test]
    fn listing_hash_follows_refs_and_the_artifact_is_deterministic() {
        let (dir, _, _) = repo();
        let (tags, hash) = listing(dir.path()).unwrap();
        let names: Vec<&str> = tags.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["light", "on-tree", "v-side", "v1", "v1-of"]);
        let built = build(dir.path(), &tags).unwrap();
        assert_eq!(build(dir.path(), &tags).unwrap(), built);
        let artifact = decode(&built).unwrap();
        // v1, v1-of (and v1 again inside its chain), on-tree, v-side.
        assert_eq!(artifact.objects.len(), 4);
        let light = artifact.tags.iter().find(|t| t.name == "light").unwrap();
        assert!(light.peeled.is_empty());

        git(dir.path(), &["tag", "-d", "light"]);
        let (_, after) = listing(dir.path()).unwrap();
        assert_ne!(after, hash);
    }

    #[test]
    fn keep_takes_the_tags_whose_target_is_present() {
        let (dir, b, s) = repo();
        let (tags, _) = listing(dir.path()).unwrap();
        let artifact = decode(&build(dir.path(), &tags).unwrap()).unwrap();
        let tree = git(dir.path(), &["rev-parse", &format!("{b}^{{tree}}")]);
        let present: BTreeSet<String> = [b.clone(), tree].into();
        let kept = keep(artifact.clone(), |id| present.contains(id)).unwrap();
        let names: Vec<&str> = kept.tags.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["light", "on-tree", "v1", "v1-of"]);
        assert_eq!(kept.objects().len(), 3);
        // Looked up in a repository: the source holds every target, an
        // empty one none.
        let source = Present::scan(&dir.path().join(".git")).unwrap();
        let mut all = check(artifact.clone()).unwrap();
        all.retain(|tag| source.contains(tag.target_id()));
        assert_eq!(all.tags.len(), 5);
        let empty = tempfile::tempdir().unwrap();
        git(empty.path(), &["init", "-q"]);
        let nothing = Present::scan(&empty.path().join(".git")).unwrap();
        let mut none = check(artifact.clone()).unwrap();
        none.retain(|tag| nothing.contains(tag.target_id()));
        assert!(none.tags.is_empty());
        assert!(!nothing.contains_hex(&b) && source.contains_hex(&b));
        let side = keep(artifact, |id| id == s).unwrap();
        let names: Vec<&str> = side.tags.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["v-side"]);

        // Written with its idx, the pack passes git's checks and git reads
        // the tags back as the repo has them.
        let clone = tempfile::tempdir().unwrap();
        git(clone.path(), &["init", "-q", "--bare"]);
        let remotes = [
            ("main".to_string(), b.clone()),
            ("side".to_string(), s.clone()),
        ];
        let empty = Present::scan(clone.path()).unwrap();
        write(clone.path(), &kept, &remotes, &empty).unwrap();
        let packed = std::fs::read_to_string(clone.path().join("packed-refs")).unwrap();
        let names: Vec<&str> = (packed.lines().skip(1))
            .map(|line| line.split_once(' ').unwrap().1)
            .collect();
        assert_eq!(
            names,
            [
                "refs/remotes/origin/main",
                "refs/remotes/origin/side",
                "refs/tags/light",
                "refs/tags/on-tree",
                "refs/tags/v1",
                "refs/tags/v1-of"
            ]
        );
        let packs = clone.path().join("objects/pack");
        let idx: Vec<_> = std::fs::read_dir(&packs)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "idx"))
            .collect();
        assert_eq!(idx.len(), 1);
        let verified = git(
            clone.path(),
            &["verify-pack", "-v", idx[0].to_str().unwrap()],
        );
        assert!(verified.contains("tag"), "{verified}");
        for name in ["v1", "v1-of", "on-tree"] {
            assert_eq!(
                git(clone.path(), &["rev-parse", &format!("refs/tags/{name}")]),
                git(dir.path(), &["rev-parse", &format!("refs/tags/{name}")])
            );
            assert_eq!(
                git(
                    clone.path(),
                    &["cat-file", "-p", &format!("refs/tags/{name}")]
                ),
                git(
                    dir.path(),
                    &["cat-file", "-p", &format!("refs/tags/{name}")]
                )
            );
        }
        assert!(
            write(clone.path(), &kept, &[], &empty).is_err(),
            "packed-refs exists"
        );
        // Tag objects the clone already holds are not written again.
        let again = tempfile::tempdir().unwrap();
        git(again.path(), &["init", "-q", "--bare"]);
        write(again.path(), &kept, &[], &source).unwrap();
        assert_eq!(
            std::fs::read_dir(again.path().join("objects/pack"))
                .unwrap()
                .count(),
            0
        );
    }

    /// Stored zlib streams inflate back to their data, across the 64 KiB
    /// block size and when empty.
    #[test]
    fn stored_zlib_round_trips() {
        use std::io::Read;
        for len in [0, 1, 1000, 0xffff, 0x10000, 200_000] {
            let data: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
            let mut stream = Vec::new();
            stored_zlib(&data, &mut stream);
            let mut back = Vec::new();
            flate2::read::ZlibDecoder::new(&stream[..])
                .read_to_end(&mut back)
                .unwrap();
            assert_eq!(back, data, "{len}");
        }
    }

    #[test]
    fn a_bad_artifact_keeps_nothing() {
        let (dir, b, _) = repo();
        let (tags, _) = listing(dir.path()).unwrap();
        let good = decode(&build(dir.path(), &tags).unwrap()).unwrap();
        let all = |_: &str| true;
        assert!(keep(good.clone(), all).is_ok());
        let v1 = good.tags.iter().position(|t| t.name == "v1").unwrap();
        let mut bad = Vec::new();
        for name in [
            format!("v1\n{b} refs/heads/main"),
            "../v1".into(),
            "v1.lock".into(),
        ] {
            let mut a = good.clone();
            a.tags[v1].name = name;
            bad.push(a);
        }
        let mut a = good.clone();
        a.tags[v1].object = "HEAD".into();
        bad.push(a);
        // Peels somewhere else.
        let mut a = good.clone();
        a.tags[v1].peeled = "f".repeat(40);
        bad.push(a);
        // A tag object is missing from the chain.
        let mut a = good.clone();
        a.objects.clear();
        bad.push(a);
        // An annotated tag claiming to be lightweight.
        let mut a = good.clone();
        a.tags[v1].peeled.clear();
        bad.push(a);
        for a in bad {
            let tag = a.tags[v1].clone();
            assert!(keep(a, all).is_err(), "{tag:?}");
        }
    }
}
