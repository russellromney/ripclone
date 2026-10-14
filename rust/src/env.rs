//! Numbers read from environment variables.
//!
//! A set value that does not parse is an error that names the variable. It
//! never falls back to the default, so a typo fails at startup instead of
//! running with a setting the operator did not choose.

use anyhow::{Context, Result};
use std::str::FromStr;

/// The non-negative integer in `key`, or `default` when `key` is unset.
pub fn number<T>(key: &str, default: T) -> Result<T>
where
    T: FromStr<Err = std::num::ParseIntError>,
{
    let value = match std::env::var(key) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(value)) => {
            anyhow::bail!("{key} must be a non-negative integer, got {value:?}")
        }
    };
    parse_number(key, value.as_deref(), default)
}

/// [`number`] for a value already read: `None` is unset.
pub fn parse_number<T>(key: &str, value: Option<&str>, default: T) -> Result<T>
where
    T: FromStr<Err = std::num::ParseIntError>,
{
    match value {
        None => Ok(default),
        Some(value) => value
            .trim()
            .parse()
            .with_context(|| format!("{key} must be a non-negative integer, got {value:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_number;

    #[test]
    fn unset_is_the_default_and_anything_unparsable_is_an_error() {
        assert_eq!(parse_number::<u64>("K", None, 7).unwrap(), 7);
        assert_eq!(parse_number::<u64>("K", Some(" 12 "), 7).unwrap(), 12);
        assert_eq!(parse_number::<u64>("K", Some("0"), 7).unwrap(), 0);
        for bad in ["", "ten", "-1", "1.5", "99999999999999999999"] {
            let err = format!("{:#}", parse_number::<u64>("K", Some(bad), 7).unwrap_err());
            assert!(
                err.starts_with(&format!("K must be a non-negative integer, got {bad:?}")),
                "{bad:?}: {err}"
            );
        }
        assert!(parse_number::<u32>("K", Some("4294967296"), 1).is_err());
    }
}
