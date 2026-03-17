use std::path::PathBuf;
use std::process;

use clap::Parser;

/// pg2lite — convert PostgreSQL dump files to SQLite
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input PostgreSQL dump file
    input: PathBuf,

    /// Optional output SQLite file. If omitted, set by replacing input extension with `.sqlite`.
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Verbose logging for progress during conversion
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

}

fn main() {
    let args = Args::parse();

    if !args.input.exists() {
        eprintln!("error: input file '{}' does not exist", args.input.display());
        process::exit(2);
    }

    let out_path = match args.output {
        Some(p) => p,
        None => args.input.with_extension("sqlite"),
    };
    println!("{}", out_path.display());

    // Default behavior: perform the conversion and write the SQLite database.
    let verbose = args.verbose > 0;
    if let Err(e) = pg2lite::convert_dump_to_sqlite_with_verbose(&args.input, &out_path, verbose) {
        eprintln!("error: conversion failed: {}", e);
        process::exit(3);
    }
}
