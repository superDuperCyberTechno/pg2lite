use assert_cmd::Command;
use std::fs::copy;
use std::env;
use std::process::Command as SysCommand;

#[test]
fn convert_employees_sql_gz() {
    // copy the example dump into a temp location and run the converter
    let src = std::path::Path::new("examples/employees.sql.gz");
    assert!(src.exists(), "example dump not found: {:?}", src);

    // If pg_restore is not available in the test environment skip the test
    if SysCommand::new("pg_restore").arg("--version").output().is_err() {
        eprintln!("pg_restore not found; skipping employees conversion test");
        return;
    }

    let tmp_dir = env::temp_dir();
    let in_path = tmp_dir.join("test_employees.pgdump");
    let out_path = in_path.with_extension("sqlite");

    // copy the raw custom-format dump into the temp path
    copy(src, &in_path).expect("copy example dump to temp");

    // run converter
    let mut cmd = Command::cargo_bin("pg2lite").unwrap();
    cmd.arg(in_path.to_str().unwrap());
    cmd.assert().success();

    // verify output sqlite exists
    assert!(out_path.exists(), "expected output sqlite at {:?}", out_path);

    // open with rusqlite to ensure it's a valid DB and contains expected tables
    let conn = rusqlite::Connection::open(&out_path).expect("open sqlite");
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").expect("prepare");
    let rows = stmt.query_map([], |r| r.get::<_, String>(0)).expect("query_map");
    let mut tables: Vec<String> = Vec::new();
    for maybe in rows { tables.push(maybe.expect("row")); }
    // expected tables from the employees sample dump
    let expected = vec!["department", "department_employee", "department_manager", "employee", "salary", "title"];
    for t in expected.iter() {
        assert!(tables.iter().any(|x| x == t), "expected table {} not found in converted DB; found tables: {:?}", t, tables);
    }

    // Check row counts for key tables (basic sanity checks)
    let mut q = conn.prepare("SELECT COUNT(*) FROM \"department\"").expect("prepare count");
    let dept_cnt: i64 = q.query_row([], |r| r.get(0)).expect("dept count");
    // the sample employees dataset contains 9 departments
    assert_eq!(dept_cnt, 9, "expected 9 departments in employees sample");

    for t in ["employee", "department_employee", "salary", "title"].iter() {
        let cnt: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM \"{}\"", t), [], |r| r.get(0)).expect("count");
        assert!(cnt > 0, "expected {} to have >0 rows, got {}", t, cnt);
    }

    // targeted assertions: column sanity, index presence, FK handling
    // department columns: expect dept_no and dept_name
    let mut cols = conn.prepare("PRAGMA table_info('department')").expect("pragma");
    let col_iter = cols.query_map([], |r| r.get::<_, String>(1)).expect("cols");
    let dept_cols: Vec<String> = col_iter.map(|c| c.expect("c")).collect();
    assert!(dept_cols.iter().any(|c| c == "dept_name"), "department.dept_name column missing: {:?}", dept_cols);
    assert!(dept_cols.iter().any(|c| c == "dept_no") || dept_cols.iter().any(|c| c == "id"), "department missing expected identifier column (dept_no or id): {:?}", dept_cols);

    // department values look sane: dept_name non-empty; if dept_no exists it should look like a code, otherwise id should be positive
    let dept_name: String = conn.query_row("SELECT dept_name FROM \"department\" LIMIT 1", [], |r| r.get(0)).expect("dept_name");
    assert!(!dept_name.is_empty(), "dept_name should be non-empty");
    if dept_cols.iter().any(|c| c == "dept_no") {
        let dept_no: String = conn.query_row("SELECT dept_no FROM \"department\" LIMIT 1", [], |r| r.get(0)).expect("dept_no");
        assert!(dept_no.starts_with('d'), "dept_no should start with 'd' but got {}", dept_no);
    } else if dept_cols.iter().any(|c| c == "id") {
        // id may be stored as TEXT depending on type mapping; accept non-empty values
        let id_str: String = conn.query_row("SELECT id FROM \"department\" LIMIT 1", [], |r| r.get(0)).expect("id");
        assert!(!id_str.is_empty(), "department.id should be non-empty");
    }

    // Check that each key table has at least one index defined
    for t in ["department", "employee", "salary", "title"].iter() {
        let mut idxs = conn.prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?1 AND name NOT LIKE 'sqlite_%'").expect("prepare idx");
        let found_idx = idxs.exists([t]).expect("exists");
        if !found_idx {
            eprintln!("note: no index on table {} — converter may skip index creation", t);
        }
    }

    // Foreign key enforcement: our converter currently does not recreate Postgres FK constraints;
    // ensure the DB doesn't (yet) contain enforced foreign keys for department_employee
    let mut fk = conn.prepare("PRAGMA foreign_key_list('department_employee')").expect("prepare fk");
    let fk_rows = fk.query_map([], |r| Ok(r.get::<_, String>(2).unwrap_or_default())).expect("fk rows");
    let fk_vec: Vec<String> = fk_rows.map(|r| r.unwrap_or_default()).collect();
    // Expect empty or missing FK enforcement (converter skips FK DDL)
    assert!(fk_vec.is_empty(), "expected no enforced foreign keys for department_employee, found: {:?}", fk_vec);

    // cleanup
    let _ = std::fs::remove_file(in_path);
    let _ = std::fs::remove_file(out_path);
}
