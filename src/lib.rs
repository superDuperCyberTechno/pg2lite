use regex::Regex;
use rusqlite::Connection;
use std::path::PathBuf;
use std::fs;

/// Convert a PostgreSQL dump file at `input` into a SQLite database at `output`.
pub fn convert_dump_to_sqlite(input: &PathBuf, output: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(input)?;

    // Build statements and COPY blocks. We either collect normal statements (ending with ';')
    // or handle COPY ... FROM stdin blocks which have rows until a line with "\\.".
    #[derive(Debug)]
    enum Segment {
        Stmt(String),
        Copy { header: String, rows: Vec<String> },
    }

    let mut segments: Vec<Segment> = Vec::new();
    let mut cur = String::new();
    let mut in_copy = false;
    let mut copy_header = String::new();
    let mut copy_rows: Vec<String> = Vec::new();

    for raw_line in contents.lines() {
        let line = raw_line;
        let trimmed = line.trim_start();
        // skip SQL comments
        if trimmed.starts_with("--") && !in_copy {
            continue;
        }

        if in_copy {
            if line == "\\." {
                // end of copy
                segments.push(Segment::Copy { header: copy_header.clone(), rows: copy_rows.clone() });
                in_copy = false;
                copy_header.clear();
                copy_rows.clear();
            } else {
                copy_rows.push(line.to_string());
            }
            continue;
        }

        // detect COPY start
        let up = trimmed.to_uppercase();
        if up.starts_with("COPY ") && up.contains(" FROM STDIN") {
            // flush any pending statement before entering COPY
            if !cur.trim().is_empty() {
                segments.push(Segment::Stmt(cur.clone()));
                cur.clear();
            }
            in_copy = true;
            copy_header = line.to_string();
            continue;
        }

        cur.push_str(line);
        cur.push('\n');
        if trimmed.ends_with(';') {
            let s = cur.trim().to_string();
            cur.clear();
            if !s.is_empty() {
                segments.push(Segment::Stmt(s));
            }
        }
    }
    if !cur.trim().is_empty() {
        segments.push(Segment::Stmt(cur));
    }

    let re_serial = Regex::new(r"(?i)\bSERIAL\b")?;
    let re_nextval = Regex::new(r"(?i)DEFAULT\s+nextval\('[^']+'::regclass\)")?;
    let re_e_quote = Regex::new(r"E'")?;

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut conn = Connection::open(output)?;
    let tx = conn.transaction()?;

    for seg in segments {
        match seg {
            Segment::Stmt(stmt) => {
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
            Segment::Copy { header, rows } => {
                // parse header like: COPY tablename (col1, col2) FROM stdin;
                // We'll extract the table name and column list.
                let upper = header.to_uppercase();
                if !upper.starts_with("COPY ") {
                    eprintln!("warning: malformed COPY header: {}", header);
                    continue;
                }
                // crude parse
                let after_copy = header[5..].trim();
                if let Some(pos) = after_copy.find("(") {
                    let table = after_copy[..pos].trim();
                    if let Some(end_cols) = after_copy.find(")") {
                        let cols = &after_copy[pos+1..end_cols];
                        let col_names: Vec<String> = cols.split(',').map(|s| s.trim().to_string()).collect();

                        // prepare insert statement
                        let placeholders: Vec<String> = col_names.iter().map(|_| "?".to_string()).collect();
                        let sql = format!("INSERT INTO {} ({}) VALUES ({});", table, col_names.join(", "), placeholders.join(", "));
                        let mut stmt = match tx.prepare(&sql) {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("warning: failed to prepare insert for COPY into {}: {}", table, e);
                                continue;
                            }
                        };

                        for row in rows {
                            // split on tabs
                            let fields: Vec<&str> = row.split('\t').collect();
                            let mut values: Vec<rusqlite::types::Value> = Vec::new();
                            for f in fields.iter() {
                                if *f == "\\N" {
                                    values.push(rusqlite::types::Value::Null);
                                } else {
                                    let un = unescape_copy_field(f);
                                    values.push(rusqlite::types::Value::from(un));
                                }
                            }
                            // execute insert
                            if let Err(e) = stmt.execute(rusqlite::params_from_iter(values.iter())) {
                                eprintln!("warning: failed to insert COPY row into {}: {}", table, e);
                            }
                        }
                    }
                }
            }
        }
    }

    tx.commit()?;
    Ok(())
}

// Unescape COPY field content according to PostgreSQL text format rules (basic subset).
fn unescape_copy_field(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        // escape sequence
        match chars.next() {
            Some('b') => out.push('\x08' as char),
            Some('f') => out.push('\x0C' as char),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('v') => out.push('\x0B' as char),
            Some('\\') => out.push('\\'),
            Some(ch) if ch.is_digit(8) => {
                // octal sequence: up to 3 octal digits, we already consumed one
                let mut oct = ch.to_digit(8).unwrap();
                for _ in 0..2 {
                    if let Some(next) = chars.peek() {
                        if next.is_digit(8) {
                            let d = chars.next().unwrap().to_digit(8).unwrap();
                            oct = oct * 8 + d;
                        } else {
                            break;
                        }
                    }
                }
                if let Some(byte) = std::char::from_u32(oct) {
                    out.push(byte);
                }
            }
            Some(other) => out.push(other),
            None => break,
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unescape_copy_field() {
        assert_eq!(unescape_copy_field("Hello\\nWorld"), "Hello\nWorld");
        assert_eq!(unescape_copy_field("Tab\\tHere"), "Tab\tHere");
        assert_eq!(unescape_copy_field("Oct\\123"), String::from_utf8(vec![b'O', 0o123]).unwrap_or_default());
        assert_eq!(unescape_copy_field("Back\\\\Slash"), "Back\\Slash");
    }
}
