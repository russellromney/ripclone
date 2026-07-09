//! Config-driven job size classes for the build queue claim filter.
//!
//! Classes are an ordered list: each has a name, an inclusive byte threshold,
//! and a machine-spec label (for later cloud dispatch). Classification picks
//! the first class whose threshold covers the size; unknown size maps to the
//! last (largest) class so first builds never under-size.
//!
//! Launch ships `small | large`. Adding a lane or retuning a threshold is a
//! config change, never a code change.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// One size class in the ordered config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SizeClass {
    /// Operator-facing name (e.g. `small`, `large`). Used by `--max-size-class`.
    pub name: String,
    /// Inclusive upper bound in bytes for this class. The last class should use
    /// a large threshold (often `u64::MAX`) as the catch-all.
    pub max_bytes: u64,
    /// Machine-spec label for later cloud dispatch (e.g. `shared-cpu-2x:2048`).
    /// Unused by the OSS claim filter; carried so config is ready for cloud.
    #[serde(default)]
    pub machine: String,
}

/// Launch default: two lanes. Small covers repos/clonepacks up to 1 GiB; large
/// is the catch-all. Thresholds are retunable via config without a rebuild.
pub fn default_size_classes() -> Vec<SizeClass> {
    vec![
        SizeClass {
            name: "small".into(),
            max_bytes: 1 << 30, // 1 GiB
            machine: "shared-cpu-1x:1024".into(),
        },
        SizeClass {
            name: "large".into(),
            max_bytes: u64::MAX,
            machine: "shared-cpu-4x:8192".into(),
        },
    ]
}

/// Validate an ordered class list. Fails loudly on empty, blank names, or
/// duplicates — silent misconfig would route jobs to the wrong machines.
pub fn validate_size_classes(classes: &[SizeClass]) -> Result<()> {
    if classes.is_empty() {
        bail!("size_classes must contain at least one class");
    }
    let mut seen = std::collections::HashSet::new();
    for (i, c) in classes.iter().enumerate() {
        if c.name.trim().is_empty() {
            bail!("size_classes[{i}].name must be non-empty");
        }
        if !seen.insert(c.name.as_str()) {
            bail!("duplicate size class name {:?}", c.name);
        }
    }
    // Thresholds must be non-decreasing so "first match wins" is well-ordered.
    for w in classes.windows(2) {
        if w[0].max_bytes > w[1].max_bytes {
            bail!(
                "size_classes must be ordered by non-decreasing max_bytes \
                 ({:?}={} > {:?}={})",
                w[0].name,
                w[0].max_bytes,
                w[1].name,
                w[1].max_bytes
            );
        }
    }
    Ok(())
}

/// Map a byte size to a class rank (0-based index into `classes`).
///
/// - `None` size (no preflight / no prior clonepack) → last class (largest).
/// - Otherwise → first class with `max_bytes >= size`.
/// - If somehow past every threshold → last class.
pub fn classify_rank(size_bytes: Option<u64>, classes: &[SizeClass]) -> i64 {
    debug_assert!(!classes.is_empty());
    let last = (classes.len() - 1) as i64;
    let Some(n) = size_bytes else {
        return last;
    };
    for (i, c) in classes.iter().enumerate() {
        if n <= c.max_bytes {
            return i as i64;
        }
    }
    last
}

/// Class name for a rank, or the last class if out of range.
pub fn class_name(rank: i64, classes: &[SizeClass]) -> &str {
    classes
        .get(rank as usize)
        .or_else(|| classes.last())
        .map(|c| c.name.as_str())
        .unwrap_or("unknown")
}

/// Resolve `--max-size-class NAME` to an inclusive rank ceiling.
/// Unknown names fail loudly so a typo never silently claims everything.
pub fn rank_ceiling(name: &str, classes: &[SizeClass]) -> Result<i64> {
    classes
        .iter()
        .position(|c| c.name == name)
        .map(|i| i as i64)
        .with_context(|| {
            let known: Vec<_> = classes.iter().map(|c| c.name.as_str()).collect();
            format!("unknown size class {name:?}; configured classes: {known:?}")
        })
}

/// Best-effort prior clonepack byte total from lengths already stored on a
/// [`crate::RefInfo`]. Used at re-sync enqueue so classification needs no new
/// API call. Returns 0 when no sized artifacts are present (caller treats as
/// unknown → largest class).
pub fn prior_clonepack_bytes(info: &crate::RefInfo) -> u64 {
    let mut total = 0u64;
    for p in &info.head_base_packs {
        total = total.saturating_add(p.pack_len).saturating_add(p.idx_len);
    }
    for level in &info.history_levels {
        for p in &level.packs {
            total = total.saturating_add(p.pack_len).saturating_add(p.idx_len);
        }
    }
    for f in &info.archive_frames {
        total = total.saturating_add(f.compressed_len);
    }
    total
}

/// Load size classes: config list if non-empty, else launch defaults.
/// Also accepts `RIPCLONE_SIZE_CLASSES` JSON for env-only deploys.
pub fn load_size_classes(from_config: &[SizeClass]) -> Result<Vec<SizeClass>> {
    if let Ok(raw) = std::env::var("RIPCLONE_SIZE_CLASSES") {
        if !raw.trim().is_empty() {
            let classes: Vec<SizeClass> = serde_json::from_str(&raw)
                .with_context(|| format!("parse RIPCLONE_SIZE_CLASSES JSON: {raw}"))?;
            validate_size_classes(&classes)?;
            return Ok(classes);
        }
    }
    if from_config.is_empty() {
        let classes = default_size_classes();
        validate_size_classes(&classes)?;
        return Ok(classes);
    }
    validate_size_classes(from_config)?;
    Ok(from_config.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn two() -> Vec<SizeClass> {
        default_size_classes()
    }

    fn three() -> Vec<SizeClass> {
        vec![
            SizeClass {
                name: "small".into(),
                max_bytes: 100,
                machine: "s".into(),
            },
            SizeClass {
                name: "medium".into(),
                max_bytes: 1_000,
                machine: "m".into(),
            },
            SizeClass {
                name: "large".into(),
                max_bytes: u64::MAX,
                machine: "l".into(),
            },
        ]
    }

    #[test]
    fn two_class_config_classifies() {
        let c = two();
        assert_eq!(classify_rank(Some(0), &c), 0);
        assert_eq!(classify_rank(Some(1 << 30), &c), 0);
        assert_eq!(classify_rank(Some((1 << 30) + 1), &c), 1);
        assert_eq!(class_name(0, &c), "small");
        assert_eq!(class_name(1, &c), "large");
    }

    #[test]
    fn three_class_config_classifies() {
        let c = three();
        assert_eq!(class_name(classify_rank(Some(50), &c), &c), "small");
        assert_eq!(class_name(classify_rank(Some(100), &c), &c), "small");
        assert_eq!(class_name(classify_rank(Some(101), &c), &c), "medium");
        assert_eq!(class_name(classify_rank(Some(1_000), &c), &c), "medium");
        assert_eq!(class_name(classify_rank(Some(1_001), &c), &c), "large");
    }

    #[test]
    fn unknown_size_maps_to_largest() {
        let c = three();
        assert_eq!(classify_rank(None, &c), 2);
        assert_eq!(class_name(classify_rank(None, &c), &c), "large");
    }

    #[test]
    fn threshold_change_reclassifies() {
        let bytes = 500u64;
        let tight = three();
        assert_eq!(class_name(classify_rank(Some(bytes), &tight), &tight), "medium");
        // Raise medium threshold so 500 now fits small — pure config change.
        let retuned = vec![
            SizeClass {
                name: "small".into(),
                max_bytes: 600,
                machine: "s".into(),
            },
            SizeClass {
                name: "medium".into(),
                max_bytes: 1_000,
                machine: "m".into(),
            },
            SizeClass {
                name: "large".into(),
                max_bytes: u64::MAX,
                machine: "l".into(),
            },
        ];
        assert_eq!(
            class_name(classify_rank(Some(bytes), &retuned), &retuned),
            "small"
        );
    }

    #[test]
    fn rank_ceiling_resolves_names() {
        let c = three();
        assert_eq!(rank_ceiling("small", &c).unwrap(), 0);
        assert_eq!(rank_ceiling("medium", &c).unwrap(), 1);
        assert_eq!(rank_ceiling("large", &c).unwrap(), 2);
        assert!(rank_ceiling("xlarge", &c).is_err());
    }

    #[test]
    fn validate_rejects_empty_and_duplicates() {
        assert!(validate_size_classes(&[]).is_err());
        let dup = vec![
            SizeClass {
                name: "a".into(),
                max_bytes: 1,
                machine: String::new(),
            },
            SizeClass {
                name: "a".into(),
                max_bytes: 2,
                machine: String::new(),
            },
        ];
        assert!(validate_size_classes(&dup).is_err());
    }
}
