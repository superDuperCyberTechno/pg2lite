use regex::Regex;
use rusqlite::Connection;
use std::path::PathBuf;
use std::fs;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use rusqlite::params;
use sqlparser::ast::{Statement, ObjectName, Value as SQLValue, Expr, TableConstraint, ColumnOption};

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

    // Performance: tune SQLite for bulk import. These pragmas speed up large inserts
    // when creating a new DB from a trusted dump. They reduce durability guarantees
    // during import but are safe for producing a fresh DB file. We don't aggressively
    // restore old settings here because the DB is newly created in most cases.
    let _ = conn.execute_batch(
        "PRAGMA synchronous = OFF;\nPRAGMA journal_mode = MEMORY;\nPRAGMA temp_store = MEMORY;\n",
    );

    let tx = conn.transaction()?;

    // track tables that should be AUTOINCREMENT primary key: (table_name, pk_column)
    let mut autoinc_tables: Vec<(String, String)> = Vec::new();

    for seg in segments {
        match seg {
            Segment::Stmt(stmt) => {
                let s = stmt.trim();
                if s.is_empty() {
                    continue;
                }

                // Parse using sqlparser to get reliable ASTs for CREATE TABLE and INSERT
                let dialect = PostgreSqlDialect {};
                match Parser::parse_sql(&dialect, s) {
                    Ok(stmts) => {
                        for st in stmts {
                            match st {
                                Statement::CreateTable { name, columns, constraints, .. } => {
                                    // translate CreateTable into a SQLite-compatible DDL
                                    let tbl_name = name.to_string();
                                    // collect column info first so we can apply table-level constraints
                                    struct ColInfo { name: String, typ: String, not_null: bool, default_nextval: bool, is_primary: bool }
                                    let mut cols_info: Vec<ColInfo> = Vec::new();
                                    for col in columns.iter() {
                                        let mut typ = col.data_type.to_string();
                                        if typ.to_uppercase() == "SERIAL" {
                                            typ = "INTEGER".to_string();
                                        }
                                        typ = typ.replace("BYTEA", "BLOB");
                                        typ = typ.replace("BOOLEAN", "INTEGER").replace("boolean", "INTEGER");
                                        let mut not_null = false;
                                        let mut default_nextval = false;
                                        let mut is_primary = false;
                                        for opt in col.options.iter() {
                                            match &opt.option {
                                                ColumnOption::NotNull => not_null = true,
                                                ColumnOption::Default(expr) => {
                                                    let txt = expr.to_string().to_lowercase();
                                                    if txt.contains("nextval") {
                                                        default_nextval = true;
                                                    }
                                                }
                                                ColumnOption::Unique { is_primary: prim } => {
                                                    if *prim { is_primary = true; }
                                                }
                                                _ => {}
                                            }
                                        }
                                        cols_info.push(ColInfo { name: col.name.to_string(), typ, not_null, default_nextval, is_primary });
                                    }

                                    // apply table-level primary key constraints
                                    for c in constraints.iter() {
                                        if let TableConstraint::Unique { is_primary, columns, .. } = c {
                                            if *is_primary {
                                                for pk in columns {
                                                    for ci in cols_info.iter_mut() {
                                                        if ci.name == pk.to_string() {
                                                            ci.is_primary = true;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    let mut col_defs: Vec<String> = Vec::new();
                                    let pk_cols: Vec<String> = cols_info.iter().filter(|c| c.is_primary).map(|c| c.name.clone()).collect();
                                    let composite_pk = pk_cols.len() > 1;
                                    for ci in cols_info.iter() {
                                        // If single-column primary key and either declared SERIAL/DEFAULT nextval or type INTEGER,
                                        // make it INTEGER PRIMARY KEY AUTOINCREMENT for SQLite.
                                        if !composite_pk && ci.is_primary && (ci.default_nextval || ci.typ.to_uppercase() == "INTEGER") {
                                            let def = format!("{} INTEGER PRIMARY KEY AUTOINCREMENT", ci.name);
                                            col_defs.push(def);
                                            autoinc_tables.push((tbl_name.clone(), ci.name.clone()));
                                            continue;
                                        }
                                        let mut def = format!("{} {}", ci.name, ci.typ);
                                        if ci.not_null { def.push_str(" NOT NULL"); }
                                        // Do not add PRIMARY KEY here for composite PKs; will add table-level PK later
                                        if ci.is_primary && !composite_pk {
                                            def.push_str(" PRIMARY KEY");
                                        }
                                        col_defs.push(def);
                                    }
                                    // If composite primary key, append table-level PRIMARY KEY clause
                                    if composite_pk {
                                        let pk_clause = format!("PRIMARY KEY ({})", pk_cols.join(", "));
                                        col_defs.push(pk_clause);
                                    }

                                    let create_sql = format!("CREATE TABLE {} ({});", tbl_name, col_defs.join(", "));
                                    if let Err(e) = tx.execute_batch(&create_sql) {
                                        eprintln!("warning: failed to execute CREATE TABLE {}: {}", tbl_name, e);
                                    }
                                }
                                Statement::Insert { table_name, columns, source, .. } => {
                                    // Build INSERT statement with values from AST. Prepare the
                                    // statement once per INSERT source and reuse it for all rows
                                    // to avoid repeated parse/prepare overhead.
                                    let tbl = table_name.to_string();
                                    let columns_str = if columns.is_empty() { None } else { Some(columns.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ")) };
                                    if let sqlparser::ast::SetExpr::Values(values) = *source.body.clone() {
                                        // construct placeholder list based on first row's length
                                        if let Some(first_row) = values.rows.get(0) {
                                            let param_count = first_row.len();
                                            let placeholders: Vec<&str> = (0..param_count).map(|_| "?").collect();
                                            let sql = if let Some(ref cols) = columns_str {
                                                format!("INSERT INTO {} ({}) VALUES ({});", tbl, cols, placeholders.join(", "))
                                            } else {
                                                format!("INSERT INTO {} VALUES ({});", tbl, placeholders.join(", "))
                                            };

                                            match tx.prepare(&sql) {
                                                Ok(mut pst) => {
                                                    for row in values.rows {
                                                        let mut vals: Vec<rusqlite::types::Value> = Vec::new();
                                                        for expr in row {
                                                            match expr {
                                                                Expr::Value(SQLValue::Number(s, _)) => {
                                                                    vals.push(rusqlite::types::Value::from(s));
                                                                }
                                                                Expr::Value(SQLValue::SingleQuotedString(s)) => {
                                                                    vals.push(rusqlite::types::Value::from(s));
                                                                }
                                                                Expr::Value(SQLValue::Boolean(b)) => {
                                                                    vals.push(rusqlite::types::Value::from(if b {1} else {0}));
                                                                }
                                                                Expr::Value(SQLValue::Null) => {
                                                                    vals.push(rusqlite::types::Value::Null);
                                                                }
                                                                _ => {
                                                                    vals.push(rusqlite::types::Value::Null);
                                                                }
                                                            }
                                                        }
                                                        if let Err(e) = pst.execute(rusqlite::params_from_iter(vals.iter())) {
                                                            eprintln!("warning: failed to insert parsed row into {}: {}", tbl, e);
                                                        }
                                                    }
                                                }
                                                Err(e) => eprintln!("warning: failed to prepare insert for {}: {}", tbl, e),
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    // Fallback: try simple textual transforms for other statements
                                    let mut tstmt = s.to_string();
                                    tstmt = re_serial.replace_all(&tstmt, "INTEGER").to_string();
                                    tstmt = re_nextval.replace_all(&tstmt, "").to_string();
                                    tstmt = tstmt.replace("BYTEA", "BLOB");
                                    tstmt = tstmt.replace("boolean", "INTEGER");
                                    tstmt = tstmt.replace("BOOLEAN", "INTEGER");
                                    tstmt = re_e_quote.replace_all(&tstmt, "'").to_string();
                                    if let Err(e) = tx.execute_batch(&tstmt) {
                                        eprintln!("warning: failed to execute statement: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // parsing failed; fallback to previous behaviour
                        let mut tstmt = s.to_string();
                        tstmt = re_serial.replace_all(&tstmt, "INTEGER").to_string();
                        tstmt = re_nextval.replace_all(&tstmt, "").to_string();
                        tstmt = tstmt.replace("BYTEA", "BLOB");
                        tstmt = tstmt.replace("boolean", "INTEGER");
                        tstmt = tstmt.replace("BOOLEAN", "INTEGER");
                        if let Err(e2) = tx.execute_batch(&tstmt) {
                            eprintln!("warning: failed to execute statement after parse error: {} / {}", e, e2);
                        }
                    }
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

    // After committing, update sqlite_sequence for AUTOINCREMENT tables so future inserts continue correctly.
    for (table, pk) in autoinc_tables {
        let q = format!("SELECT MAX({}) FROM {}", pk, table);
        if let Ok(max_val) = conn.query_row(q.as_str(), [], |r| r.get::<_, Option<i64>>(0)) {
            if let Some(maxv) = max_val {
                // try to write into sqlite_sequence; ignore errors if table isn't present
                let _ = conn.execute("INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES (?1, ?2)", params![table, maxv]);
            }
        }
    }

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
        assert_eq!(unescape_copy_field("Oct\\123"), "OctS");
        assert_eq!(unescape_copy_field("Back\\\\Slash"), "Back\\Slash");
    }
}
