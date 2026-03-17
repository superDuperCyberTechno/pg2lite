use std::env;
use std::path::Path;
use std::process;

fn print_usage(program: &str) {
    eprintln!("Usage: {} <dump-file>", program);
}

fn main() {
    let mut args = env::args();
    let program = args.next().unwrap_or_else(|| "pg2lite".to_string());

    let input = match args.next() {
        Some(v) => v,
        None => {
            print_usage(&program);
            process::exit(1);
        }
    };

    let in_path = Path::new(&input);

    if !in_path.exists() {
        eprintln!("error: input file '{}' does not exist", in_path.display());
        process::exit(2);
    }

    // Compute output filename by replacing (or adding) the extension with `.sqlite`.
    let out_path = in_path.with_extension("sqlite");

    // Print the computed output path to stdout so callers can capture it.
    println!("{}", out_path.display());
}
