use std::path::{Path, PathBuf};

/// Return the path to a test fixture file, or `None` if it doesn't exist.
/// Run `just bootstrap` to generate fixture files.
pub fn fixture_path(name: &str) -> Option<PathBuf> {
    let path = Path::new("build/data").join(name);
    if path.exists() { Some(path) } else { None }
}
