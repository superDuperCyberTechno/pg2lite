use pg2lite::convert_dump_to_sqlite;
use std::path::PathBuf;

#[test]
fn convert_copy_example() {
    let in_path = PathBuf::from("examples/sample_copy.pgsql");
    let out_path = PathBuf::from("target/examples/sample_copy.sqlite");
    let _ = std::fs::remove_file(&out_path);
    convert_dump_to_sqlite(&in_path, &out_path).expect("conversion should succeed");
    assert!(out_path.exists());
    // basic sanity: open DB and check product count
    let conn = rusqlite::Connection::open(&out_path).unwrap();
    let cnt: i64 = conn.query_row("SELECT COUNT(*) FROM products", [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 2);
    let _ = std::fs::remove_file(&out_path);
}
