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
        .stderr(predicate::str::contains("USAGE"));
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
