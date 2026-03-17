use pg2lite::convert_dump_to_sqlite;
use std::path::PathBuf;

#[test]
fn convert_example_dump() {
    let in_path = PathBuf::from("examples/sample.pgsql");
    let out_path = PathBuf::from("target/examples/sample.sqlite");
    // remove if exists
    let _ = std::fs::remove_file(&out_path);
    convert_dump_to_sqlite(&in_path, &out_path).expect("conversion should succeed");
    assert!(out_path.exists());
    // cleanup
    let _ = std::fs::remove_file(&out_path);
}
