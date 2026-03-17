use pg2lite::convert_dump_to_sqlite;
use std::path::PathBuf;

#[test]
fn convert_real_composite() {
    let in_path = PathBuf::from("examples/real_composite.pgsql");
    let out_path = PathBuf::from("target/examples/real_composite.sqlite");
    let _ = std::fs::remove_file(&out_path);
    convert_dump_to_sqlite(&in_path, &out_path).expect("conversion should succeed");
    let conn = rusqlite::Connection::open(&out_path).unwrap();
    let cnt: i64 = conn.query_row("SELECT COUNT(*) FROM project_members", [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 2);
    let _ = std::fs::remove_file(&out_path);
}
