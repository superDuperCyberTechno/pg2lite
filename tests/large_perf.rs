use std::path::PathBuf;
use std::time::Instant;

/// Ignored performance test — generates a large COPY block and runs conversion.
/// Use `cargo test -- --ignored` to run. Scale rows with `PG2LITE_PERF_ROWS` env var.
#[test]
#[ignore]
fn large_copy_perf() {
    let rows: usize = std::env::var("PG2LITE_PERF_ROWS").ok().and_then(|s| s.parse().ok()).unwrap_or(10_000);
    let tmp = std::env::temp_dir();
    let in_path = tmp.join("perf_large.pgsql");
    let out_path = tmp.join("perf_large.sqlite");
    let mut f = std::fs::File::create(&in_path).unwrap();
    use std::io::Write;
    writeln!(f, "CREATE TABLE t (id integer, val text);").unwrap();
    writeln!(f, "COPY t (id, val) FROM stdin;").unwrap();
    for i in 0..rows {
        writeln!(f, "{}\trow-{}", i, i).unwrap();
    }
    writeln!(f, "\\.").unwrap();
    let start = Instant::now();
    pg2lite::convert_dump_to_sqlite(&in_path, &out_path).expect("conversion");
    let dur = start.elapsed();
    println!("Converted {} rows in {:?}", rows, dur);
    let _ = std::fs::remove_file(in_path);
    let _ = std::fs::remove_file(out_path);
}
