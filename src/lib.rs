use regex::Regex;
use rusqlite::Connection;
use std::path::PathBuf;
use std::fs;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use rusqlite::params;
use sqlparser::ast::{Statement, Value as SQLValue, Expr, TableConstraint, ColumnOption};
use std::env;
use csv::ReaderBuilder;
use std::process::Command;
use std::io::{Read, Seek};
use flate2::read::GzDecoder;
use tempfile::NamedTempFile;
use std::time::Instant;

/// Convert a PostgreSQL dump file at `input` into a SQLite database at `output`.
/// This wrapper preserves the original API and calls the verbose variant with `verbose=false`.
pub fn convert_dump_to_sqlite(input: &PathBuf, output: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    convert_dump_to_sqlite_with_verbose(input, output, false)
}

/// Convert a PostgreSQL dump file at `input` into a SQLite database at `output`.
/// When `verbose` is true, emit progress logs to stderr for COPY and batch INSERT operations.
pub fn convert_dump_to_sqlite_with_verbose(input: &PathBuf, output: &PathBuf, verbose: bool) -> Result<(), Box<dyn std::error::Error>> {
    // Support PostgreSQL custom-format dumps by detecting the magic header and
    // using `pg_restore` to extract plain SQL. If not a custom-format dump,
    // read the file contents directly.
    let mut contents = if is_custom_pg_dump(input)? {
        if verbose {
            eprintln!("detected custom-format dump; attempting to run pg_restore...");
        }
        run_pg_restore(input)?
    } else {
        fs::read_to_string(input)?
    };

    // Pre-clean common Postgres-specific tokens that confuse the splitter/parser.
    // Remove psql meta-commands that start with a backslash at the start of a line.
    contents = contents
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            // drop lines that start with backslash (psql meta-commands) except COPY terminator `\\.`
            !(t.starts_with('\\') && t != "\\.")
        })
        .collect::<Vec<_>>()
        .join("\n");

    // Case-insensitive removals for timezone qualifiers and USING btree
    let re_without_tz = Regex::new(r#"(?i)without\s+time\s+zone"#).unwrap();
    contents = re_without_tz.replace_all(&contents, "").to_string();
    let re_with_tz = Regex::new(r#"(?i)with\s+time\s+zone"#).unwrap();
    contents = re_with_tz.replace_all(&contents, "").to_string();
    let re_using_btree = Regex::new(r#"(?i)\s+USING\s+btree"#).unwrap();
    contents = re_using_btree.replace_all(&contents, "").to_string();

    // Normalize common type names (case-insensitive replacements)
    let re_char_vary = Regex::new(r#"(?i)character\s+varying"#).unwrap();
    contents = re_char_vary.replace_all(&contents, "text").to_string();
    // normalize timestamp(...) types to TEXT
    let re_timestamp = Regex::new(r#"(?i)timestamp\s*\([^)]*\)"#).unwrap();
    contents = re_timestamp.replace_all(&contents, "TEXT").to_string();
    // Normalize varchar/character varying/text with size to plain text
    let re_typed_size = Regex::new(r#"(?i)\b(?:character\s+varying|varchar|character|text)\s*\(\s*\d+\s*\)"#).unwrap();
    contents = re_typed_size.replace_all(&contents, "text").to_string();
    // Remove any remaining bare numeric size specifiers like `(255)` which can break SQL parsing in SQLite
    let re_size_only = Regex::new(r#"\(\s*\d+\s*\)"#).unwrap();
    contents = re_size_only.replace_all(&contents, "").to_string();

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

    // track tables that should be AUTOINCREMENT primary key: (table_name, pk_column, optional_sequence_last_value)
    let mut autoinc_tables: Vec<(String, String, Option<i64>)> = Vec::new();

    // First pass: collect sequence definitions, setval calls and OWNED BY mappings.
    use std::collections::HashMap;
    struct SeqInfo { start: Option<i64>, last_value: Option<i64>, owned: Option<(String, String)> }
    let mut sequences: HashMap<String, SeqInfo> = HashMap::new();
    // Note: avoid backreferences (not supported by Rust's regex). Accept optional single quotes around the name.
    let re_create_seq = Regex::new(r"(?i)CREATE\s+SEQUENCE\s+'?(?P<name>[^'\s;]+)'?(?:.*?START\s+WITH\s+(?P<start>\d+))?").unwrap();
    let re_setval = Regex::new(r"(?i)setval\s*\(\s*'(?P<name>[^']+)'\s*,\s*(?P<val>\d+)").unwrap();
    let re_alter_owned = Regex::new(r"(?i)ALTER\s+SEQUENCE\s+'?(?P<name>[^'\s]+)'?\s+OWNED\s+BY\s+'?(?P<table>[^'\s\.]+)'?\.(?P<col>[^'\)\s]+)").unwrap();
    let re_nextval_name = Regex::new(r"(?i)nextval\(\s*'(?P<name>[^']+)'::regclass\s*\)").unwrap();
    for seg in &segments {
        if let Segment::Stmt(s) = seg {
            // CREATE SEQUENCE
            for cap in re_create_seq.captures_iter(s) {
                let name = cap.name("name").unwrap().as_str().to_string();
                let start = cap.name("start").and_then(|m| m.as_str().parse().ok());
                sequences.entry(name).or_insert(SeqInfo { start, last_value: None, owned: None });
            }
            // setval calls
            for cap in re_setval.captures_iter(s) {
                let name = cap.name("name").unwrap().as_str().to_string();
                let val = cap.name("val").and_then(|m| m.as_str().parse().ok());
                sequences.entry(name.clone()).or_insert(SeqInfo { start: None, last_value: val, owned: None }).last_value = val;
            }
            // ALTER SEQUENCE ... OWNED BY
            for cap in re_alter_owned.captures_iter(s) {
                let name = cap.name("name").unwrap().as_str().to_string();
                let table = cap.name("table").unwrap().as_str().to_string();
                let col = cap.name("col").unwrap().as_str().to_string();
                sequences.entry(name.clone()).or_insert(SeqInfo { start: None, last_value: None, owned: Some((table.clone(), col.clone())) }).owned = Some((table.clone(), col.clone()));
            }
        }
    }

    for seg in segments {
        match seg {
            Segment::Stmt(stmt) => {
                let s = stmt.trim();
                // skip psql meta-commands that start with backslash
                if s.starts_with('\\') { continue; }
                // Skip unsupported or non-SQL statements early to avoid noisy errors in SQLite.
                let s_up = s.to_uppercase();
                if s_up.starts_with("SET ") || s_up.starts_with("RESET ") || s_up.starts_with("COMMENT ") || s_up.starts_with("GRANT ") || s_up.starts_with("REVOKE ") || s_up.starts_with("CREATE EXTENSION") {
                    continue;
                }
                if s_up.starts_with("ALTER TABLE") {
                    if s_up.contains("OWNER TO") {
                        continue;
                    }
                    // Try to handle common ALTER TABLE ... ADD CONSTRAINT cases.
                    // Convert UNIQUE constraints into CREATE UNIQUE INDEX statements
                    // and ignore PRIMARY KEY constraints (they are handled at table creation).
                    let re_alter_add = Regex::new(r"(?i)ALTER\s+TABLE\s+(?:ONLY\s+)?(?P<table>[^\s]+)\s+ADD\s+CONSTRAINT\s+(?P<name>[^\s]+)\s+(?P<type>UNIQUE|PRIMARY\s+KEY)\s*\((?P<cols>[^)]+)\)").unwrap();
                    if let Some(cap) = re_alter_add.captures(s) {
                        let raw_table = cap.name("table").unwrap().as_str();
                        let table = normalize_ident(raw_table);
                        let ctype = cap.name("type").unwrap().as_str().to_uppercase();
                        let cols = cap.name("cols").unwrap().as_str();
                        let col_list = cols.split(',').map(|c| c.trim().trim_matches('"').to_string()).collect::<Vec<_>>().join(", ");
                        let cname = cap.name("name").unwrap().as_str().trim_matches('"');
                        if ctype.contains("UNIQUE") {
                            let sql = format!("CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({});", cname, table, col_list);
                            if let Err(e) = tx.execute_batch(&sql) {
                                eprintln!("warning: failed to create UNIQUE index {} on {}: {}", cname, table, e);
                            }
                        } else {
                            // PRIMARY KEY via ALTER TABLE: usually redundant; skip
                        }
                        continue;
                    }
                    // Other ALTER TABLE variants are Postgres-specific; skip them.
                    continue;
                }
                if s_up.starts_with("ALTER SEQUENCE") || s_up.starts_with("CREATE SEQUENCE") {
                    continue;
                }
                if s_up.contains("PG_CATALOG.SETVAL") || s_up.contains("PG_CATALOG.SET_CONFIG") {
                    continue;
                }
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
                                        // Normalize table name by removing schema qualifiers and surrounding quotes
                                        let tbl_name = normalize_ident(&name.to_string());
                                    // collect column info first so we can apply table-level constraints
                                    struct ColInfo { name: String, typ: String, not_null: bool, default_nextval: bool, is_primary: bool, seq_name: Option<String> }
                                    let mut cols_info: Vec<ColInfo> = Vec::new();
                                    for col in columns.iter() {
                                        let mut typ = col.data_type.to_string();
                                        // normalize common Postgres types to SQLite-friendly types
                                        let t_up = typ.to_uppercase();
                                        if t_up.starts_with("SERIAL") || t_up.starts_with("BIGSERIAL") {
                                            typ = "INTEGER".to_string();
                                        } else if t_up.starts_with("BIGINT") || t_up.starts_with("INT8") || t_up == "INT" || t_up.starts_with("SMALLINT") || t_up.starts_with("INT") {
                                            // map integer family to INTEGER
                                            typ = "INTEGER".to_string();
                                        } else if t_up.starts_with("TIMESTAMP") || t_up.starts_with("DATE") || t_up.starts_with("TIME") {
                                            // map temporal types to TEXT for portability
                                            typ = "TEXT".to_string();
                                        } else if t_up.starts_with("CHARACTER VARYING") || t_up.starts_with("VARCHAR") || t_up.starts_with("CHARACTER") || t_up.starts_with("VARCHAR") {
                                            typ = "TEXT".to_string();
                                        }
                                        typ = typ.replace("BYTEA", "BLOB");
                                        typ = typ.replace("BOOLEAN", "INTEGER").replace("boolean", "INTEGER");
                                        // remove any size specifiers like (255) left from type normalization
                                        if let Ok(re_size) = Regex::new(r"\s*\(\s*\d+\s*\)") {
                                            typ = re_size.replace_all(&typ, "").to_string();
                                        }
                                        let mut not_null = false;
                                        let mut default_nextval = false;
                                        let mut is_primary = false;
                                        let mut seq_name: Option<String> = None;
                                        for opt in col.options.iter() {
                                            match &opt.option {
                                                ColumnOption::NotNull => not_null = true,
                                                ColumnOption::Default(expr) => {
                                                    let txt = expr.to_string();
                                                    // try to extract sequence name from nextval('seq'::regclass)
                                                    if let Some(cap) = re_nextval_name.captures(&txt) {
                                                        default_nextval = true;
                                                        seq_name = Some(cap.name("name").unwrap().as_str().to_string());
                                                    } else if txt.to_lowercase().contains("nextval") {
                                                        default_nextval = true;
                                                    }
                                                }
                                                ColumnOption::Unique { is_primary: prim } => {
                                                    if *prim { is_primary = true; }
                                                }
                                                _ => {}
                                            }
                                        }
                                        cols_info.push(ColInfo { name: col.name.to_string(), typ, not_null, default_nextval, is_primary, seq_name });
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
                                            // determine sequence last value if available
                                            let mut seq_last: Option<i64> = None;
                                            if let Some(seq) = &ci.seq_name {
                                                if let Some(si) = sequences.get(seq) {
                                                    seq_last = si.last_value.or(si.start);
                                                }
                                            }
                                            // also check sequences map for an owned sequence matching this table.col
                                            if seq_last.is_none() {
                                                for (_sname, si) in sequences.iter() {
                                                    if let Some((ref t, ref c)) = si.owned {
                                                        if t == &tbl_name && c == &ci.name {
                                                            seq_last = si.last_value.or(si.start);
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            let def = format!("{} INTEGER PRIMARY KEY AUTOINCREMENT", ci.name);
                                            col_defs.push(def);
                                            autoinc_tables.push((tbl_name.clone(), ci.name.clone(), seq_last));
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

                                    let mut create_sql = format!("CREATE TABLE IF NOT EXISTS {} ({});", tbl_name, col_defs.join(", "));
                                    // sanitize create_sql for Postgres-specific fragments that
                                    // may remain in the generated SQL (safety before execution)
                                    create_sql = remove_schema_qualifiers(&create_sql);
                                    create_sql = create_sql.replace("WITHOUT TIME ZONE", "");
                                    create_sql = create_sql.replace("WITH TIME ZONE", "");
                                    create_sql = create_sql.replace(" USING btree", "");
                                    if let Ok(re_ts) = Regex::new(r"(?i)timestamp\([^)]*\)") {
                                        create_sql = re_ts.replace_all(&create_sql, "TEXT").to_string();
                                    }
                                    if verbose { eprintln!("sanitized CREATE TABLE: {}", create_sql); }
                                    if let Err(e) = tx.execute_batch(&create_sql) {
                                        eprintln!("warning: failed to execute CREATE TABLE {}: {}", tbl_name, e);
                                        // Fallback: try executing a sanitized textual version of the
                                        // original CREATE TABLE statement (additional sanitization)
                                        let mut tstmt = remove_schema_qualifiers(&s.to_string());
                                        tstmt = tstmt.replace("WITHOUT TIME ZONE", "");
                                        tstmt = tstmt.replace("WITH TIME ZONE", "");
                                        tstmt = tstmt.replace(" USING btree", "");
                                        if let Ok(re_ts) = Regex::new(r"(?i)timestamp\([^)]*\)") {
                                            tstmt = re_ts.replace_all(&tstmt, "TEXT").to_string();
                                        }
                                        if verbose { eprintln!("fallback CREATE TABLE attempt: {}", tstmt); }
                                        if let Err(e2) = tx.execute_batch(&tstmt) {
                                            eprintln!("warning: fallback CREATE TABLE failed for {}: {}", tbl_name, e2);
                                        }
                                    }
                                }
                                Statement::Insert { table_name, columns, source, .. } => {
                                    // Build INSERT statement with values from AST. We'll batch rows
                                    // into multi-row INSERTs to reduce per-row overhead.
                                    let tbl = normalize_ident(&table_name.to_string());
                                    // Normalize column identifiers to match CREATE TABLE normalization
                                    let columns_str = if columns.is_empty() {
                                        None
                                    } else {
                                        Some(columns.iter().map(|c| normalize_ident(&c.to_string())).collect::<Vec<_>>().join(", "))
                                    };
                                    if verbose {
                                        if let Some(ref cols) = columns_str {
                                            eprintln!("sanitized INSERT target: {} ({})", tbl, cols);
                                        } else {
                                            eprintln!("sanitized INSERT target: {} (all columns)", tbl);
                                        }
                                    }
                                    if let sqlparser::ast::SetExpr::Values(values) = *source.body.clone() {
                                        // prepare batching parameters
                                        const SQLITE_MAX_VARS: usize = 32766;
                                        let default_batch: usize = env::var("PG2LITE_BATCH").ok().and_then(|s| s.parse().ok()).unwrap_or(500);
                                        if let Some(first_row) = values.rows.get(0) {
                                            let cols_per_row = first_row.len();
                                            let max_batch = std::cmp::max(1, SQLITE_MAX_VARS / cols_per_row);
                                            let batch_size = std::cmp::min(default_batch, max_batch);

                                            let mut batch: Vec<Vec<rusqlite::types::Value>> = Vec::with_capacity(batch_size);
                                            let flush_batch = |batch: &mut Vec<Vec<rusqlite::types::Value>>| {
                                                if batch.is_empty() { return; }
                                                let row_count = batch.len();
                                                let mut placeholders_row: Vec<String> = Vec::new();
                                                for _ in 0..cols_per_row {
                                                    placeholders_row.push("?".to_string());
                                                }
                                                let single = format!("({})", placeholders_row.join(","));
                                                let all = std::iter::repeat(single).take(row_count).collect::<Vec<_>>().join(",");
                                                let sql = if let Some(ref cols) = columns_str {
                                                    format!("INSERT INTO {} ({}) VALUES {};", tbl, cols, all)
                                                } else {
                                                    format!("INSERT INTO {} VALUES {};", tbl, all)
                                                };
                                                // flatten params
                                                let mut flat: Vec<rusqlite::types::Value> = Vec::with_capacity(row_count * cols_per_row);
                                                for r in batch.iter() { for v in r.iter() { flat.push(v.clone()); } }
                                                match tx.prepare(&sql) {
                                                    Ok(mut pst) => {
                                                        if let Err(e) = pst.execute(rusqlite::params_from_iter(flat.iter())) {
                                                            eprintln!("warning: failed batch insert into {}: {}", tbl, e);
                                                        }
                                                    }
                                                    Err(e) => eprintln!("warning: failed to prepare batch insert for {}: {}", tbl, e),
                                                }
                                                if verbose {
                                                    eprintln!("flushed {} rows into {}", row_count, tbl);
                                                }
                                                batch.clear();
                                            };

                                            for row in values.rows {
                                                let mut vals: Vec<rusqlite::types::Value> = Vec::with_capacity(cols_per_row);
                                                for expr in row {
                                                    match expr {
                                                        Expr::Value(SQLValue::Number(s, _)) => vals.push(rusqlite::types::Value::from(s)),
                                                        Expr::Value(SQLValue::SingleQuotedString(s)) => vals.push(rusqlite::types::Value::from(s)),
                                                        Expr::Value(SQLValue::Boolean(b)) => vals.push(rusqlite::types::Value::from(if b {1} else {0})),
                                                        Expr::Value(SQLValue::Null) => vals.push(rusqlite::types::Value::Null),
                                                        _ => vals.push(rusqlite::types::Value::Null),
                                                    }
                                                }
                                                batch.push(vals);
                                                if batch.len() >= batch_size {
                                                    flush_batch(&mut batch);
                                                }
                                            }
                                            flush_batch(&mut batch);
                                            if verbose {
                                                eprintln!("INSERT into {}: total batched rows processed (approx)", tbl);
                                            }
                                        }
                                    }
                                }
                                _ => {
                                    // Fallback: try simple textual transforms for other statements
                                    // before executing fallback textual transforms, remove schema qualifiers
                                    let mut tstmt = remove_schema_qualifiers(&s.to_string());
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
                        // skip psql meta-commands that start with backslash
                        if s.trim_start().starts_with('\\') { continue; }
                        let mut tstmt = s.to_string();
                        // remove schema qualifiers early on in fallback path as well
                        tstmt = remove_schema_qualifiers(&tstmt);
                        tstmt = re_serial.replace_all(&tstmt, "INTEGER").to_string();
                        tstmt = re_nextval.replace_all(&tstmt, "").to_string();
                        // remove timezone qualifiers that are Postgres-specific
                        tstmt = tstmt.replace("WITHOUT TIME ZONE", "");
                        tstmt = tstmt.replace("WITH TIME ZONE", "");
                        // strip USING btree from CREATE INDEX
                        tstmt = tstmt.replace(" USING btree", "");
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
                if verbose { eprintln!("processing COPY header: '{}' ({} rows captured)", header, rows.len()); }
                // parse header like: COPY tablename (col1, col2) FROM stdin;
                // We'll extract the table name and column list.
                let upper = header.to_uppercase();
                if !upper.starts_with("COPY ") {
                    eprintln!("warning: malformed COPY header: {}", header);
                    continue;
                }
                // crude parse to find table and column list; also detect CSV/FORMAT csv
                let after_copy = header[5..].trim();
                let is_csv = upper.contains(" CSV") || upper.contains("FORMAT CSV");
                if let Some(pos) = after_copy.find("(") {
                    let raw_table = after_copy[..pos].trim();
                    let table = normalize_ident(raw_table);
                    if let Some(end_cols) = after_copy.find(")") {
                        let cols = &after_copy[pos+1..end_cols];
                        let col_names: Vec<String> = cols.split(',').map(|s| normalize_ident(s.trim())).collect();

                        // prepare insert statement (we'll try column-specified form first, then fallback to VALUES-only)
                        let placeholders: Vec<String> = col_names.iter().map(|_| "?".to_string()).collect();
                        let sql = format!("INSERT INTO {} ({}) VALUES ({});", table, col_names.join(", "), placeholders.join(", "));
                        if verbose { eprintln!("sanitized COPY -> INSERT mapping: header='{}' -> sql='{}'", header, sql); }
                        let mut stmt = match tx.prepare(&sql) {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("warning: failed to prepare insert for COPY into {} using columns: {}; error: {}", table, col_names.join(", "), e);
                                // fallback: try without column list
                                let alt_sql = format!("INSERT INTO {} VALUES ({});", table, placeholders.join(", "));
                                if verbose { eprintln!("attempting fallback COPY INSERT SQL: {}", alt_sql); }
                                match tx.prepare(&alt_sql) {
                                    Ok(s2) => s2,
                                    Err(e2) => {
                                        eprintln!("warning: failed to prepare fallback insert for COPY into {}: {}", table, e2);
                                        continue;
                                    }
                                }
                            }
                        };

                        if is_csv {
                            // Stream CSV rows without joining into a single large string.
                            // Create a small reader that emits each row followed by a newline.
                            struct RowsReader {
                                rows: Vec<String>,
                                idx: usize,
                                pos: usize,
                            }
                            impl std::io::Read for RowsReader {
                                fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                                    if self.idx >= self.rows.len() {
                                        return Ok(0);
                                    }
                                    let mut written = 0usize;
                                    while written < buf.len() && self.idx < self.rows.len() {
                                        let cur = &self.rows[self.idx];
                                        let cur_bytes = cur.as_bytes();
                                        // write remaining bytes from current row
                                        if self.pos < cur_bytes.len() {
                                            let rem = cur_bytes.len() - self.pos;
                                            let take = std::cmp::min(rem, buf.len() - written);
                                            buf[written..written+take].copy_from_slice(&cur_bytes[self.pos..self.pos+take]);
                                            written += take;
                                            self.pos += take;
                                        }
                                        // if we've finished the row and there's room, write a newline and advance
                                        if self.pos >= cur_bytes.len() && written < buf.len() {
                                            buf[written] = b'\n';
                                            written += 1;
                                            self.idx += 1;
                                            self.pos = 0;
                                        }
                                        // if buffer full, break and return what we've written so far
                                    }
                                    Ok(written)
                                }
                            }

                            let reader = RowsReader { rows, idx: 0, pos: 0 };
                            let mut rdr = ReaderBuilder::new()
                                .has_headers(false)
                                .from_reader(reader);

                            // Batch insertion for CSV rows
                            const SQLITE_MAX_VARS: usize = 32766;
                            let default_batch: usize = env::var("PG2LITE_BATCH").ok().and_then(|s| s.parse().ok()).unwrap_or(500);
                            let cols_per_row = col_names.len().max(1);
                            let max_batch = std::cmp::max(1, SQLITE_MAX_VARS / cols_per_row);
                            let batch_size = std::cmp::min(default_batch, max_batch);

                            let mut batch: Vec<Vec<rusqlite::types::Value>> = Vec::with_capacity(batch_size);
                            let start = Instant::now();
                            let mut total_rows: usize = 0;
                            let flush_batch = |batch: &mut Vec<Vec<rusqlite::types::Value>>| {
                                if batch.is_empty() { return; }
                                let row_count = batch.len();
                                let mut placeholders_row: Vec<String> = Vec::new();
                                for _ in 0..cols_per_row { placeholders_row.push("?".to_string()); }
                                let single = format!("({})", placeholders_row.join(","));
                                let all = std::iter::repeat(single).take(row_count).collect::<Vec<_>>().join(",");
                                let sql = format!("INSERT INTO {} ({}) VALUES {};", table, col_names.join(", "), all);
                                // flatten params
                                let mut flat: Vec<rusqlite::types::Value> = Vec::with_capacity(row_count * cols_per_row);
                                for r in batch.iter() { for v in r.iter() { flat.push(v.clone()); } }
                                match tx.prepare(&sql) {
                                    Ok(mut pst) => {
                                        if let Err(e) = pst.execute(rusqlite::params_from_iter(flat.iter())) {
                                            eprintln!("warning: failed batch insert into {}: {}", table, e);
                                        }
                                    }
                                    Err(e) => eprintln!("warning: failed to prepare batch insert for {}: {}", table, e),
                                }
                                if verbose {
                                    eprintln!("flushed {} rows into {}", row_count, table);
                                }
                                batch.clear();
                            };

                            for result in rdr.records() {
                                match result {
                                    Ok(rec) => {
                                        let mut vals: Vec<rusqlite::types::Value> = Vec::with_capacity(cols_per_row);
                                        for f in rec.iter() {
                                            if f == "\\N" {
                                                vals.push(rusqlite::types::Value::Null);
                                            } else {
                                                vals.push(rusqlite::types::Value::from(f.to_string()));
                                            }
                                        }
                                        // if record has fewer fields, pad with NULLs
                                        while vals.len() < cols_per_row { vals.push(rusqlite::types::Value::Null); }
                                        batch.push(vals);
                                        total_rows += 1;
                                        if batch.len() >= batch_size {
                                            flush_batch(&mut batch);
                                        }
                                    }
                                    Err(e) => eprintln!("warning: failed to parse CSV COPY row: {}", e),
                                }
                            }
                            flush_batch(&mut batch);
                            if verbose {
                                let elapsed = start.elapsed();
                                eprintln!("COPY into {}: processed {} rows in {:?}", table, total_rows, elapsed);
                            }
                        } else {
                            let start = Instant::now();
                            let mut total_rows = 0usize;
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
                                total_rows += 1;
                            }
                            if verbose {
                                let elapsed = start.elapsed();
                                eprintln!("COPY into {}: processed {} rows in {:?}", table, total_rows, elapsed);
                            }
                        }
                    }
                }
            }
        }
    }

    tx.commit()?;

    // After committing, update sqlite_sequence for AUTOINCREMENT tables so future inserts continue correctly.
    for (table, pk, seq_last) in autoinc_tables {
        let q = format!("SELECT MAX({}) FROM {}", pk, table);
        if let Ok(max_val) = conn.query_row(q.as_str(), [], |r| r.get::<_, Option<i64>>(0)) {
            // compute final seq value as max(existing_max, declared seq_last)
            let mut final_seq: Option<i64> = seq_last;
            if let Some(maxv) = max_val {
                final_seq = Some(final_seq.map_or(maxv, |s| std::cmp::max(s, maxv)));
            }
            if let Some(seqv) = final_seq {
                let _ = conn.execute("INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES (?1, ?2)", params![table, seqv]);
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

// Normalize identifiers by removing schema qualifiers and surrounding double quotes.
fn normalize_ident(s: &str) -> String {
    let s = s.trim();
    // remove all double quotes and whitespace, then strip schema prefix
    let s = s.replace('"', "");
    let s = s.trim();
    if let Some(pos) = s.rfind('.') {
        return s[pos+1..].to_string();
    }
    s.to_string()
}

// Remove schema qualifiers from statements to avoid `public.` prefixes that SQLite doesn't accept.
fn remove_schema_qualifiers(s: &str) -> String {
    // Remove schema qualifiers like `public.table` or `"schema"."table"`.
    // This simple regex strips an optional quoted identifier or bare identifier
    // followed by a dot. It is intentionally conservative to avoid touching
    // other SQL tokens.
    let re = Regex::new(r#"(?i)"?[_A-Za-z][\w]*"?\."#).unwrap_or_else(|_| Regex::new(r#"(?i)"?[_A-Za-z][\w]*"?\."#).unwrap());
    re.replace_all(s, "").to_string()
}

// Detects PostgreSQL custom dump format by checking the file header for the
// custom-format magic bytes (starts with "PGDMP" in ASCII).
fn is_custom_pg_dump(path: &PathBuf) -> Result<bool, Box<dyn std::error::Error>> {
    let mut f = std::fs::File::open(path)?;
    let mut buf = [0u8; 5];
    let n = f.read(&mut buf)?;
    if n < 5 { return Ok(false); }
    // if file is gzipped, check gzip header first and attempt to read inner header
    if &buf[..2] == b"\x1f\x8b" {
        // rewind and try to decode a few bytes from the gzip stream
        f.rewind()?;
        let mut gz = GzDecoder::new(f);
        let mut ibuf = [0u8; 5];
        let m = gz.read(&mut ibuf)?;
        if m < 5 { return Ok(false); }
        return Ok(&ibuf == b"PGDMP");
    }
    Ok(&buf == b"PGDMP")
}

// Run `pg_restore --format=custom --file=- <path>` to emit SQL to stdout.
// Returns the SQL output as a String.
fn run_pg_restore(path: &PathBuf) -> Result<String, Box<dyn std::error::Error>> {
    // Ensure pg_restore is available
    let pg_restore = which::which("pg_restore").map_err(|_| {
        format!("pg_restore not found in PATH; required to extract custom-format PostgreSQL dumps")
    })?;
    // If the file is gzipped, decompress to a temp file and pass that to pg_restore
    let mut input_path = path.clone();
    // keep temp file alive while pg_restore runs to avoid premature deletion
    let mut _tmpf_opt: Option<NamedTempFile> = None;
    // quick gz detection
    let mut probe = std::fs::File::open(path)?;
    let mut hdr = [0u8;2];
    let _ = probe.read(&mut hdr)?;
    if &hdr == b"\x1f\x8b" {
        // verbose isn't available here; emit a generic hint via stderr
        eprintln!("detected gzip-compressed dump; decompressing for pg_restore");
        let mut f = std::fs::File::open(path)?;
        let mut gz = GzDecoder::new(&mut f);
        let mut tmpf = NamedTempFile::new()?;
        std::io::copy(&mut gz, &mut tmpf)?;
        input_path = tmpf.path().to_path_buf();
        _tmpf_opt = Some(tmpf);
    }

    let output = Command::new(pg_restore)
        .arg("-F")
        .arg("c")
        .arg("-f")
        .arg("-")
        .arg(input_path.as_os_str())
        .output()?;
    if !output.status.success() {
        return Err(format!("pg_restore failed: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
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
