use assert_cmd::Command;
use predicates::prelude::*;
use std::env;
use std::fs::File;
use std::io::Write;

#[test]
fn missing_argument_shows_usage() {
    let mut cmd = Command::cargo_bin("pg2lite").unwrap();
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Usage"));
}

#[test]
fn non_existent_input_prints_error() {
    let mut cmd = Command::cargo_bin("pg2lite").unwrap();
    cmd.arg("no-such-file.pgdump");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("does not exist"));
}

#[test]
fn default_output_filename_is_computed() {
    // create a temporary file to act as input
    let tmp_dir = env::temp_dir();
    let in_path = tmp_dir.join("test_input.pgdump");
    let mut f = File::create(&in_path).unwrap();
    writeln!(f, "-- dummy").unwrap();

    let mut cmd = Command::cargo_bin("pg2lite").unwrap();
    cmd.arg(in_path.to_str().unwrap());
    let assert = cmd.assert().success();
    let expected = in_path.with_extension("sqlite").display().to_string();
    assert.stdout(predicate::str::contains(expected));

    let _ = std::fs::remove_file(in_path);
}

#[test]
fn verbose_flag_prints_progress() {
    // small file with a COPY so we produce some progress lines
    let tmp_dir = env::temp_dir();
    let in_path = tmp_dir.join("test_input_verbose.pgdump");
    let out_path = tmp_dir.join("test_input_verbose.sqlite");
    let mut f = File::create(&in_path).unwrap();
    writeln!(f, "CREATE TABLE v (id integer, val text);").unwrap();
    writeln!(f, "COPY v (id, val) FROM stdin WITH CSV;").unwrap();
    writeln!(f, "1,\"a\",\"b\"").unwrap();
    writeln!(f, "\\.").unwrap();

    let mut cmd = Command::cargo_bin("pg2lite").unwrap();
    cmd.arg(in_path.to_str().unwrap()).arg("-v");
    let assert = cmd.assert().success();
    assert.stderr(predicate::str::contains("processed").or(predicate::str::contains("flushed")));

    let _ = std::fs::remove_file(in_path);
    let _ = std::fs::remove_file(out_path);
}
