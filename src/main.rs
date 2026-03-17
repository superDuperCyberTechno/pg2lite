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

    /// Actually perform the conversion and write the SQLite database.
    /// If omitted the program will only print the computed output path.
    #[arg(long)]
    apply: bool,
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

    if args.apply {
        if let Err(e) = convert_dump_to_sqlite(&args.input, &out_path) {
            eprintln!("error: conversion failed: {}", e);
            process::exit(3);
        }
    }
}

use pg2lite::convert_dump_to_sqlite;
