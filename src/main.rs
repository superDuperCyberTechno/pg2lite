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

    /// Increase verbosity (-v, -vv, ...)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    /// Do not perform conversion; just print planned actions
    #[arg(long)]
    dry_run: bool,
}

fn main() {
    let args = Args::parse();

    if !args.input.exists() {
        eprintln!("error: input file '{}' does not exist", args.input.display());
        process::exit(2);
    }

    let out_path = match args.output.clone() {
        Some(p) => p,
        None => args.input.with_extension("sqlite"),
    };

    if args.dry_run {
        println!("[dry-run] Would convert '{}' -> '{}'", args.input.display(), out_path.display());
        if args.verbose > 0 {
            println!("[dry-run] verbosity level: {}", args.verbose);
        }
        return;
    }

    if args.verbose > 0 {
        eprintln!("Converting '{}' -> '{}'", args.input.display(), out_path.display());
    }

    // Currently we only compute and print the output path to keep the first steps minimal.
    println!("{}", out_path.display());
}
