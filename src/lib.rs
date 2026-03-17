use regex::Regex;
use rusqlite::Connection;
use std::path::PathBuf;
use std::fs;

/// Convert a PostgreSQL dump file at `input` into a SQLite database at `output`.
pub fn convert_dump_to_sqlite(input: &PathBuf, output: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(input)?;

    // Build statements by collecting lines until a semicolon terminator.
    let mut stmts: Vec<String> = Vec::new();
    let mut cur = String::new();
    for line in contents.lines() {
        let trimmed = line.trim_start();
        // skip SQL comments
        if trimmed.starts_with("--") {
            continue;
        }
        cur.push_str(line);
        cur.push('\n');
        if trimmed.ends_with(';') {
            let s = cur.trim().to_string();
            cur.clear();
            if !s.is_empty() {
                stmts.push(s);
            }
        }
    }
    if !cur.trim().is_empty() {
        stmts.push(cur);
    }

    let re_serial = Regex::new(r"(?i)\bSERIAL\b")?;
    let re_nextval = Regex::new(r"(?i)DEFAULT\s+nextval\('[^']+'::regclass\)")?;
    let re_e_quote = Regex::new(r"E'")?;

    let mut conn = Connection::open(output)?;
    let tx = conn.transaction()?;

    for stmt in stmts {
        let s = stmt.trim();
        if s.is_empty() {
            continue;
        }

        let s_upper = s.to_uppercase();
        // Skip unsupported statements
        if s_upper.starts_with("SET ")
            || s_upper.starts_with("SELECT PG_CATALOG.SETVAL")
            || s_upper.starts_with("COPY ")
            || s_upper.starts_with("ALTER TABLE")
            || s_upper.starts_with("COMMENT ON")
            || s_upper.starts_with("CREATE EXTENSION")
            || s_upper.starts_with("REVOKE ")
            || s_upper.starts_with("GRANT ")
        {
            continue;
        }

        let mut tstmt = s.to_string();

        if s_upper.starts_with("CREATE TABLE") {
            // Simplistic transformations for CREATE TABLE
            tstmt = re_serial.replace_all(&tstmt, "INTEGER").to_string();
            tstmt = re_nextval.replace_all(&tstmt, "").to_string();
            tstmt = tstmt.replace("BYTEA", "BLOB");
            tstmt = tstmt.replace("boolean", "INTEGER");
            tstmt = tstmt.replace("BOOLEAN", "INTEGER");
        } else if s_upper.starts_with("INSERT INTO") {
            // Convert boolean literals and remove E'' escapes
            tstmt = tstmt.replace("TRUE", "1").replace("FALSE", "0");
            tstmt = re_e_quote.replace_all(&tstmt, "'").to_string();
        }

        // Execute statement; continue on error but print a warning so the user can inspect.
        if let Err(e) = tx.execute_batch(&tstmt) {
            eprintln!("warning: failed to execute statement starting with '{}': {}", &tstmt.lines().next().unwrap_or(""), e);
        }
    }

    tx.commit()?;
    Ok(())
}
