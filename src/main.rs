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

fn convert_dump_to_sqlite(input: &PathBuf, output: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    use regex::Regex;
    use rusqlite::Connection;
    use std::fs;

    let contents = fs::read_to_string(input)?;

    // Build statements by collecting lines until a semicolon terminator.
    let mut stmts: Vec<String> = Vec::new();
    let mut cur = String::new();
    for line in contents.lines() {
        let trimmed = line.trim_start();
        // skip SQL comments
        if trimmed.starts_with("--") {
            continue;
        }
        cur.push_str(line);
        cur.push('\n');
        if trimmed.ends_with(';') {
            let s = cur.trim().to_string();
            cur.clear();
            if !s.is_empty() {
                stmts.push(s);
            }
        }
    }
    if !cur.trim().is_empty() {
        stmts.push(cur);
    }

    let re_serial = Regex::new(r"(?i)\bSERIAL\b")?;
    let re_nextval = Regex::new(r"(?i)DEFAULT\s+nextval\('[^']+'::regclass\)")?;
    let re_e_quote = Regex::new(r"E'")?;

    let conn = Connection::open(output)?;
    let tx = conn.transaction()?;

    for stmt in stmts {
        let s = stmt.trim();
        if s.is_empty() {
            continue;
        }

        let s_upper = s.to_uppercase();
        // Skip unsupported statements
        if s_upper.starts_with("SET ")
            || s_upper.starts_with("SELECT PG_CATALOG.SETVAL")
            || s_upper.starts_with("COPY ")
            || s_upper.starts_with("ALTER TABLE")
            || s_upper.starts_with("COMMENT ON")
            || s_upper.starts_with("CREATE EXTENSION")
            || s_upper.starts_with("REVOKE ")
            || s_upper.starts_with("GRANT ")
        {
            continue;
        }

        let mut tstmt = s.to_string();

        if s_upper.starts_with("CREATE TABLE") {
            // Simplistic transformations for CREATE TABLE
            tstmt = re_serial.replace_all(&tstmt, "INTEGER").to_string();
            tstmt = re_nextval.replace_all(&tstmt, "").to_string();
            tstmt = tstmt.replace("BYTEA", "BLOB");
            tstmt = tstmt.replace("boolean", "INTEGER");
            tstmt = tstmt.replace("BOOLEAN", "INTEGER");
        } else if s_upper.starts_with("INSERT INTO") {
            // Convert boolean literals and remove E'' escapes
            tstmt = tstmt.replace("TRUE", "1").replace("FALSE", "0");
            tstmt = re_e_quote.replace_all(&tstmt, "'").to_string();
        }

        // Execute statement; continue on error but print a warning so the user can inspect.
        if let Err(e) = tx.execute_batch(&tstmt) {
            eprintln!("warning: failed to execute statement starting with '{}': {}", &tstmt.lines().next().unwrap_or(""), e);
        }
    }

    tx.commit()?;
    Ok(())
}
