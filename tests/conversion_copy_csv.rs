use pg2lite::convert_dump_to_sqlite;
use std::path::PathBuf;

#[test]
fn convert_copy_csv_example() {
    let in_path = PathBuf::from("examples/sample_copy_csv.pgsql");
    let out_path = PathBuf::from("target/examples/sample_copy_csv.sqlite");
    let _ = std::fs::remove_file(&out_path);
    convert_dump_to_sqlite(&in_path, &out_path).expect("conversion should succeed");
    assert!(out_path.exists());
    let conn = rusqlite::Connection::open(&out_path).unwrap();
    let cnt: i64 = conn.query_row("SELECT COUNT(*) FROM csv_products", [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 3);
    let _ = std::fs::remove_file(&out_path);
}

#[test]
fn convert_copy_csv_multiline() {
    let in_path = PathBuf::from("examples/sample_copy_csv_real_multiline.pgsql");
    let out_path = PathBuf::from("target/examples/sample_copy_csv_real_multiline.sqlite");
    let _ = std::fs::remove_file(&out_path);
    convert_dump_to_sqlite(&in_path, &out_path).expect("conversion should succeed");
    let conn = rusqlite::Connection::open(&out_path).unwrap();
    let cnt: i64 = conn.query_row("SELECT COUNT(*) FROM mproducts", [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 2);
    // check that the first product description contains a newline
    let descr: String = conn.query_row("SELECT descr FROM mproducts WHERE id=1", [], |r| r.get(0)).unwrap();
    assert!(descr.contains("Second line"));
    let _ = std::fs::remove_file(&out_path);
}
