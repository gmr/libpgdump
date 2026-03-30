#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArchiveVersion {
    pub major: u8,
    pub minor: u8,
    pub rev: u8,
}

impl ArchiveVersion {
    pub const fn new(major: u8, minor: u8, rev: u8) -> Self {
        Self { major, minor, rev }
    }
}

impl std::fmt::Display for ArchiveVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.rev)
    }
}

pub const MIN_VERSION: ArchiveVersion = ArchiveVersion::new(1, 12, 0);
pub const MAX_VERSION: ArchiveVersion = ArchiveVersion::new(1, 16, 0);

/// Map a PostgreSQL server version (major, minor) to the archive format version.
pub fn pg_version_to_archive_version(major: u32, minor: u32) -> Option<ArchiveVersion> {
    let combined = major * 100 + minor;
    match combined {
        900..=1002 => Some(ArchiveVersion::new(1, 12, 0)),
        1003..=1199 => Some(ArchiveVersion::new(1, 13, 0)),
        1200..=1599 => Some(ArchiveVersion::new(1, 14, 0)),
        1600..=1699 => Some(ArchiveVersion::new(1, 15, 0)),
        1700.. => Some(ArchiveVersion::new(1, 16, 0)),
        _ => None,
    }
}

/// Parse a PostgreSQL version string like "17.0" or "16.2" into (major, minor).
pub fn parse_pg_version(version: &str) -> Option<(u32, u32)> {
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    Some((major, minor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_ordering() {
        assert!(ArchiveVersion::new(1, 12, 0) < ArchiveVersion::new(1, 13, 0));
        assert!(ArchiveVersion::new(1, 14, 0) < ArchiveVersion::new(1, 16, 0));
        assert!(ArchiveVersion::new(1, 14, 0) == ArchiveVersion::new(1, 14, 0));
    }

    #[test]
    fn test_pg_version_mapping() {
        assert_eq!(
            pg_version_to_archive_version(9, 0),
            Some(ArchiveVersion::new(1, 12, 0))
        );
        assert_eq!(
            pg_version_to_archive_version(12, 0),
            Some(ArchiveVersion::new(1, 14, 0))
        );
        assert_eq!(
            pg_version_to_archive_version(16, 0),
            Some(ArchiveVersion::new(1, 15, 0))
        );
        assert_eq!(
            pg_version_to_archive_version(17, 0),
            Some(ArchiveVersion::new(1, 16, 0))
        );
        assert_eq!(
            pg_version_to_archive_version(18, 0),
            Some(ArchiveVersion::new(1, 16, 0))
        );
    }

    #[test]
    fn test_parse_pg_version() {
        assert_eq!(parse_pg_version("17.0"), Some((17, 0)));
        assert_eq!(parse_pg_version("16.2"), Some((16, 2)));
        assert_eq!(parse_pg_version("18"), Some((18, 0)));
    }
}
