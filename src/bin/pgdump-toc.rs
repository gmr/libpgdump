use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use libpgdump::dump::detect_file_format;
use libpgdump::error::Result;
use libpgdump::format::custom_toc::read_toc;
use libpgdump::format::directory;
use libpgdump::{Format, OffsetState, TableOfContents};

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
    let toc = load_toc(path)?;
    print_toc(path, &toc);
    Ok(())
}

fn load_toc(path: &Path) -> Result<TableOfContents> {
    match detect_file_format(path)? {
        Format::Tar => Err(libpgdump::Error::UnsupportedFormat(Format::Tar as u8)),
        Format::Directory => directory::read_toc(path),
        Format::Custom => {
            let file = File::open(path)?;
            let mut reader = BufReader::new(file);
            read_toc(&mut reader)
        }
        _ => unreachable!(
            "detect_file_format should only return Tar, Directory, or Custom for files"
        ),
    }
}

fn print_toc(path: &Path, toc: &TableOfContents) {
    let ts = &toc.timestamp;
    let year = ts.year + 1900;
    let month = ts.month + 1;

    println!("Archive: {}", path.display());
    println!("  format: {:?}", toc.format);
    println!("  version: {}", toc.version);
    println!("  int_size: {}", toc.int_size);
    println!("  off_size: {}", toc.off_size);
    println!("  compression: {:?}", toc.compression);
    println!("  dbname: {}", toc.dbname);
    println!("  server_version: {}", toc.server_version);
    println!("  dump_version: {}", toc.dump_version);
    println!(
        "  timestamp: {:04}-{:02}-{:02} {:02}:{:02}:{:02} (is_dst={})",
        year, month, ts.day, ts.hour, ts.minute, ts.second, ts.is_dst
    );
    println!();

    println!("Entries: {}", toc.entries.len());
    println!(
        "{:<6} {:<8} {:<20} {:<20} {:<30} {:<8} {:>4}",
        "id", "section", "type", "namespace", "tag", "data", "deps"
    );

    for entry in &toc.entries {
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
