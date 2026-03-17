use pg2lite::convert_dump_to_sqlite;
use std::path::PathBuf;

#[test]
fn convert_seq_owned() {
    let in_path = PathBuf::from("examples/seq_owned.pgsql");
    let out_path = PathBuf::from("target/examples/seq_owned.sqlite");
    let _ = std::fs::remove_file(&out_path);
    convert_dump_to_sqlite(&in_path, &out_path).expect("conversion should succeed");
    let conn = rusqlite::Connection::open(&out_path).unwrap();
    // check sqlite_sequence for users_seq should be at least 150
    let seq: i64 = conn.query_row("SELECT seq FROM sqlite_sequence WHERE name='users_seq'", [], |r| r.get(0)).unwrap();
    assert!(seq >= 150);
    let _ = std::fs::remove_file(&out_path);
}
