use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use libpgdump::dump::detect_file_format;
use libpgdump::error::Result;
use libpgdump::format::{ArchiveMetadata, custom, directory};
use libpgdump::{Format, OffsetState};

/// A simple utility to print archive header and TOC entries without loading data blocks.
fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let mut args = std::env::args();
    let program = args.next().unwrap_or_else(|| "pgdump-toc".to_string());

    let path_arg = match args.next() {
        Some(value) => value,
        None => {
            eprintln!("Usage: {program} <dump-path>");
            eprintln!("Print archive header and TOC entries without loading data blocks.");
            std::process::exit(2);
        }
    };

    if args.next().is_some() {
        eprintln!("Usage: {program} <dump-path>");
        std::process::exit(2);
    }

    let path = Path::new(&path_arg);
    let metadata = load_metadata(path)?;
    print_metadata(path, &metadata);
    Ok(())
}

fn load_metadata(path: &Path) -> Result<ArchiveMetadata> {
    match detect_file_format(path)? {
        Format::Tar => Err(libpgdump::Error::UnsupportedFormat(Format::Tar as u8)),
        Format::Directory => directory::read_metadata(path),
        Format::Custom => {
            let file = File::open(path)?;
            let mut reader = BufReader::new(file);
            custom::read_metadata(&mut reader)
        }
        _ => unreachable!(
            "detect_file_format should only return Tar, Directory, or Custom for files"
        ),
    }
}

fn print_metadata(path: &Path, metadata: &ArchiveMetadata) {
    let ts = &metadata.timestamp;
    let year = ts.year + 1900;
    let month = ts.month + 1;

    println!("Archive: {}", path.display());
    println!("  format: {:?}", metadata.header.format);
    println!("  version: {}", metadata.header.version);
    println!("  int_size: {}", metadata.header.int_size);
    println!("  off_size: {}", metadata.header.off_size);
    println!("  compression: {:?}", metadata.header.compression);
    println!("  dbname: {}", metadata.dbname);
    println!("  server_version: {}", metadata.server_version);
    println!("  dump_version: {}", metadata.dump_version);
    println!(
        "  timestamp: {:04}-{:02}-{:02} {:02}:{:02}:{:02} (is_dst={})",
        year, month, ts.day, ts.hour, ts.minute, ts.second, ts.is_dst
    );
    println!();

    println!("Entries: {}", metadata.entries.len());
    println!(
        "{:<6} {:<8} {:<20} {:<20} {:<30} {:<8} {:>4}",
        "id", "section", "type", "namespace", "tag", "data", "deps"
    );

    for entry in &metadata.entries {
        let has_data = if entry.filename.is_some()
            || (entry.had_dumper && entry.data_state != OffsetState::NoData)
        {
            "yes"
        } else {
            "no"
        };

        println!(
            "{:<6} {:<8} {:<20} {:<20} {:<30} {:<8} {:>4}",
            entry.dump_id,
            format!("{:?}", entry.section),
            entry.desc.as_str(),
            entry.namespace.as_deref().unwrap_or("-"),
            entry.tag.as_deref().unwrap_or("-"),
            has_data,
            entry.dependencies.len(),
        );
    }
}
