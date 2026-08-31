use std::path::{Path, PathBuf};

/// Return the path to a test fixture file, or `None` if it doesn't exist.
/// Run `just bootstrap` to generate fixture files.
#[allow(dead_code)]
pub fn fixture_path(name: &str) -> Option<PathBuf> {
    let path = Path::new("build/data").join(name);
    if path.exists() { Some(path) } else { None }
}

/// Return the path to a checked-in archive under `tests/data`.
///
/// These are committed rather than generated because the PostgreSQL versions
/// that wrote them are no longer part of the CI matrix.
#[allow(dead_code)]
pub fn data_path(name: &str) -> PathBuf {
    Path::new("tests/data").join(name)
}
