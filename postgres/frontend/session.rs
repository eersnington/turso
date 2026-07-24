use std::num::NonZero;
use std::str;
use std::sync::Arc;

use crate::aliases;
use crate::catalog::{self, PostgresDialect};
use turso_core::{Connection, LimboError, Result, Statement, Value};
use turso_parser::ast;
use turso_pg_parser::translator::{
    is_comment_on, is_refresh_matview, try_extract_copy_from, try_extract_create_extension,
    try_extract_create_schema, try_extract_drop_schema, try_extract_set, try_extract_show,
    uses_pgvector_syntax, PgCopyFromStmt, PgCreateSchemaStmt, PgDropSchemaStmt,
    PostgreSQLTranslator,
};

use crate::copy::parse_copy_text_format;

#[derive(Clone)]
pub struct PgConnection {
    conn: Arc<Connection>,
}

/// Open a database with the PostgreSQL schema dialect, resolving the IO
/// backend from `vfs` or the path like [`turso_core::Database::open_new`].
pub fn open_database(
    path: &str,
    vfs: Option<&str>,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> Result<(Arc<dyn turso_core::IO>, Arc<turso_core::Database>)> {
    let io = match vfs {
        Some(vfs) => turso_core::Database::io_for_vfs(vfs)?,
        None => turso_core::Database::io_for_path(path)?,
    };
    let db = open_database_with_io(io.clone(), path, flags, opts)?;
    Ok((io, db))
}

/// Open a database with the PostgreSQL schema dialect on an existing IO
/// backend.
pub fn open_database_with_io(
    io: Arc<dyn turso_core::IO>,
    path: &str,
    flags: turso_core::OpenFlags,
    opts: turso_core::DatabaseOpts,
) -> Result<Arc<turso_core::Database>> {
    let file = io.open_file(path, flags, true)?;
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
    turso_core::Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(PostgresDialect))
            .storage(db_file)
            .flags(flags)
            .db_opts(opts),
    )
}

impl PgConnection {
    pub fn new(conn: Arc<Connection>) -> Self {
        aliases::install(&conn);
        Self { conn }
    }

    pub fn inner(&self) -> &Arc<Connection> {
        &self.conn
    }

    pub fn pgvector_installed(&self) -> bool {
        crate::pgvector::is_installed(&self.conn.current_schema())
    }

    /// Resolves one-based parameter positions that need nonstandard PostgreSQL types.
    pub fn parameter_metadata(&self, sql: &str) -> Result<crate::PgParameterMetadata> {
        let parsed = turso_pg_parser::parse(sql)
            .map_err(|error| LimboError::ParseError(error.to_string()))?;
        let mut vector_parameters =
            turso_pg_parser::translator::pgvector_parameter_indexes(&parsed);
        infer_vector_assignment_parameters(&self.conn, &parsed, &mut vector_parameters);
        vector_parameters.sort_unstable();
        Ok(crate::PgParameterMetadata {
            vector_parameters,
            oid_parameters: turso_pg_parser::translator::oid_parameter_indexes(&parsed),
            parameter_count: turso_pg_parser::translator::parameter_count(&parsed),
        })
    }

    pub fn prepare(&self, sql: impl AsRef<str>) -> Result<Statement> {
        prepare_statement(&self.conn, sql.as_ref())
    }

    pub fn query(&self, sql: impl AsRef<str>) -> Result<Option<Statement>> {
        let sql = sql.as_ref().trim();
        if sql.is_empty() {
            return Ok(None);
        }
        self.prepare(sql).map(Some)
    }

    pub fn execute(&self, sql: impl AsRef<str>) -> Result<()> {
        for stmt in self.query_runner(sql.as_ref().as_bytes()) {
            if let Some(mut stmt) = stmt? {
                stmt.run_ignore_rows()?;
            }
        }
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.conn.close()
    }

    pub fn pragma_update(&self, name: &str, value: impl std::fmt::Display) -> Result<()> {
        let sql = format!("PRAGMA {name} = {value}");
        let mut stmt = self.conn.prepare_internal(sql)?;
        stmt.run_ignore_rows()
    }

    pub fn query_runner<'a>(&'a self, sql: &'a [u8]) -> PgQueryRunner<'a> {
        PgQueryRunner::new(&self.conn, sql)
    }
}

fn infer_vector_assignment_parameters(
    conn: &Arc<Connection>,
    parsed: &pg_query::ParseResult,
    vector_parameters: &mut Vec<usize>,
) {
    use pg_query::protobuf::node::Node;
    use pg_query::NodeRef;

    fn add_parameter(node: Option<&pg_query::protobuf::Node>, parameters: &mut Vec<usize>) {
        let Some(Node::ParamRef(parameter)) = node.and_then(|node| node.node.as_ref()) else {
            return;
        };
        let Ok(index) = usize::try_from(parameter.number) else {
            return;
        };
        if index > 0 && !parameters.contains(&index) {
            parameters.push(index);
        }
    }

    let nodes = parsed.protobuf.nodes();
    let Some((root, _, _, _)) = nodes.first() else {
        return;
    };
    match root {
        NodeRef::InsertStmt(insert) => {
            let Some(relation) = &insert.relation else {
                return;
            };
            let Some(table) = conn
                .current_schema()
                .get_table(&relation.relname)
                .and_then(|table| table.btree())
            else {
                return;
            };
            let target_columns: Vec<_> = if insert.cols.is_empty() {
                table.columns().iter().collect()
            } else {
                insert
                    .cols
                    .iter()
                    .filter_map(|column| match column.node.as_ref() {
                        Some(Node::ResTarget(target)) => {
                            table.get_column(&target.name).map(|(_, column)| column)
                        }
                        _ => None,
                    })
                    .collect()
            };
            let Some(Node::SelectStmt(select)) = insert
                .select_stmt
                .as_deref()
                .and_then(|select| select.node.as_ref())
            else {
                return;
            };
            for values in &select.values_lists {
                let Some(Node::List(values)) = values.node.as_ref() else {
                    continue;
                };
                for (column, value) in target_columns.iter().zip(&values.items) {
                    if column.ty_str.eq_ignore_ascii_case("vector") {
                        add_parameter(Some(value), vector_parameters);
                    }
                }
            }
        }
        NodeRef::UpdateStmt(update) => {
            let Some(relation) = &update.relation else {
                return;
            };
            let Some(table) = conn
                .current_schema()
                .get_table(&relation.relname)
                .and_then(|table| table.btree())
            else {
                return;
            };
            for target in &update.target_list {
                let Some(Node::ResTarget(target)) = target.node.as_ref() else {
                    continue;
                };
                let is_vector = table
                    .get_column(&target.name)
                    .is_some_and(|(_, column)| column.ty_str.eq_ignore_ascii_case("vector"));
                if is_vector {
                    add_parameter(target.val.as_deref(), vector_parameters);
                }
            }
        }
        _ => {}
    }
}

pub struct PgQueryRunner<'a> {
    conn: &'a Arc<Connection>,
    stmts: Vec<String>,
    index: usize,
}

impl<'a> PgQueryRunner<'a> {
    fn new(conn: &'a Arc<Connection>, sql: &'a [u8]) -> Self {
        let sql = str::from_utf8(sql).unwrap_or("");
        Self {
            conn,
            stmts: split_statements(sql)
                .unwrap_or_else(|_| vec![sql.trim().to_string()])
                .into_iter()
                .filter(|stmt| !stmt.trim().is_empty())
                .collect(),
            index: 0,
        }
    }
}

impl Iterator for PgQueryRunner<'_> {
    type Item = Result<Option<Statement>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.stmts.len() {
            return None;
        }

        let sql = &self.stmts[self.index];
        self.index += 1;
        Some(prepare_statement(self.conn, sql).map(Some))
    }
}

pub fn split_statements(sql: &str) -> Result<Vec<String>> {
    match turso_pg_parser::split_statements(sql) {
        Ok(stmts) if stmts.is_empty() && !sql.trim().is_empty() => Ok(vec![sql.trim().to_string()]),
        Ok(stmts) => Ok(stmts),
        Err(_) => Ok(vec![sql.trim().to_string()]),
    }
}

fn prepare_statement(conn: &Arc<Connection>, sql: &str) -> Result<Statement> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(LimboError::InvalidArgument(
            "The supplied SQL string contains no statements".to_string(),
        ));
    }

    reject_sqlite_catalog_access(sql)?;

    if let Some(stmt) = try_prepare_special(conn, sql)? {
        return Ok(stmt);
    }

    let parse_result =
        turso_pg_parser::parse(sql).map_err(|e| LimboError::ParseError(e.to_string()))?;
    if parse_result
        .protobuf
        .nodes()
        .iter()
        .any(|(node, _, _, _)| {
            matches!(node, pg_query::NodeRef::RangeVar(relation) if relation.relname.starts_with("__turso_internal_"))
        })
    {
        return Err(LimboError::ParseError(
            "no such table: internal Turso catalog".to_string(),
        ));
    }
    if uses_pgvector_syntax(&parse_result) && !crate::pgvector::is_installed(&conn.current_schema())
    {
        return Err(LimboError::ParseError(
            "type \"vector\" does not exist; run CREATE EXTENSION vector first".to_string(),
        ));
    }
    let translator = PostgreSQLTranslator::new();
    let translated = translator
        .translate_with_prereqs(&parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?;
    reject_catalog_dml(&translated.stmt)?;

    for prereq in translated.prereqs {
        let input = prereq.to_string();
        let mut stmt = conn.prepare_translated_stmt(prereq, &input)?;
        stmt.run_ignore_rows()?;
    }

    conn.prepare_translated_stmt(translated.stmt, sql)
}

fn reject_catalog_dml(stmt: &ast::Stmt) -> Result<()> {
    let table_name = match stmt {
        ast::Stmt::Insert { tbl_name, .. } => Some(tbl_name.name.as_str()),
        ast::Stmt::Delete { tbl_name, .. } => Some(tbl_name.name.as_str()),
        ast::Stmt::Update(update) => Some(update.tbl_name.name.as_str()),
        _ => None,
    };

    let Some(table_name) = table_name else {
        return Ok(());
    };

    if !catalog::is_catalog_table_name(table_name) {
        return Ok(());
    }

    let verb = match stmt {
        ast::Stmt::Insert { .. } => "insert into",
        ast::Stmt::Delete { .. } => "delete from",
        ast::Stmt::Update { .. } => "update",
        _ => unreachable!(),
    };
    Err(LimboError::ParseError(format!(
        "cannot {verb} pg_catalog table \"{table_name}\""
    )))
}

fn reject_sqlite_catalog_access(sql: &str) -> Result<()> {
    let lower = sql.to_ascii_lowercase();
    for table_name in ["sqlite_master", "sqlite_schema"] {
        if lower.contains(table_name) {
            return Err(LimboError::ParseError(format!(
                "no such table: {table_name}"
            )));
        }
    }
    Ok(())
}

fn try_prepare_special(conn: &Arc<Connection>, sql: &str) -> Result<Option<Statement>> {
    let parse_result = match turso_pg_parser::parse(sql) {
        Ok(result) => result,
        Err(_) => return Ok(None),
    };

    if let Some(stmt) = try_extract_create_extension(&parse_result)
        .map_err(|e| LimboError::ParseError(e.to_string()))?
    {
        let if_not_exists = if stmt.if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };
        let sql = format!(
            "CREATE TABLE {if_not_exists}{} (version TEXT NOT NULL DEFAULT '{}')",
            crate::pgvector::INSTALLATION_MARKER,
            crate::pgvector::VERSION,
        );
        return Ok(Some(conn.prepare_internal_root(sql)?));
    }

    if let Some(set_stmt) = try_extract_set(&parse_result) {
        let pragma_sql = format!("PRAGMA {} = {}", set_stmt.name, set_stmt.value);
        return Ok(Some(conn.prepare(&pragma_sql)?));
    }

    if let Some(show_stmt) = try_extract_show(&parse_result) {
        let pragma_sql = format!("PRAGMA {}", show_stmt.name);
        return Ok(Some(conn.prepare(&pragma_sql)?));
    }

    if let Some(stmt) = try_extract_create_schema(&parse_result) {
        handle_pg_create_schema(conn, &stmt)?;
        return Ok(Some(noop_statement(conn)?));
    }

    if let Some(stmt) = try_extract_drop_schema(&parse_result) {
        handle_pg_drop_schema(conn, &stmt)?;
        return Ok(Some(noop_statement(conn)?));
    }

    if is_refresh_matview(&parse_result) {
        return Ok(Some(noop_statement(conn)?));
    }

    if is_comment_on(&parse_result) {
        return Ok(Some(noop_statement(conn)?));
    }

    if let Some(stmt) = try_extract_copy_from(&parse_result) {
        let rows_inserted = handle_pg_copy_from(conn, &stmt)?;
        let stmt = noop_statement(conn)?;
        stmt.set_n_change(rows_inserted as i64);
        return Ok(Some(stmt));
    }

    Ok(None)
}

fn noop_statement(conn: &Arc<Connection>) -> Result<Statement> {
    conn.prepare("SELECT 0 WHERE 0")
}

fn execute_sqlite_internal(conn: &Arc<Connection>, sql: impl AsRef<str>) -> Result<()> {
    let mut stmt = conn.prepare_internal(sql)?;
    stmt.run_ignore_rows()
}

fn handle_pg_create_schema(conn: &Arc<Connection>, stmt: &PgCreateSchemaStmt) -> Result<()> {
    let name = stmt.name.to_lowercase();
    if name == "public" {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        )));
    }

    if schema_exists(conn, &name)? {
        if stmt.if_not_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" already exists"
        )));
    }

    let path = schema_file_path(conn, &name);
    execute_sqlite_internal(
        conn,
        format!("ATTACH '{}' AS \"{}\"", path.replace('\'', "''"), name),
    )?;
    Ok(())
}

fn schema_file_path(conn: &Connection, schema_name: &str) -> String {
    let main_path = conn.db_file_path();
    let filename = format!("turso-postgres-schema-{schema_name}.db");
    if main_path == ":memory:" {
        filename
    } else {
        let parent = std::path::Path::new(&main_path)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        parent.join(&filename).to_string_lossy().to_string()
    }
}

fn handle_pg_drop_schema(conn: &Arc<Connection>, stmt: &PgDropSchemaStmt) -> Result<()> {
    let name = stmt.name.to_lowercase();
    if name == "public" {
        return handle_pg_drop_schema_public(conn, stmt.cascade);
    }

    if !schema_exists(conn, &name)? {
        if stmt.if_exists {
            return Ok(());
        }
        return Err(LimboError::ParseError(format!(
            "schema \"{name}\" does not exist"
        )));
    }

    if stmt.cascade {
        drop_all_tables_in_schema(conn, &name)?;
    }

    execute_sqlite_internal(conn, format!("DETACH \"{name}\""))?;
    Ok(())
}

fn handle_pg_drop_schema_public(conn: &Arc<Connection>, cascade: bool) -> Result<()> {
    let table_names = list_user_tables(conn, None)?;
    if !cascade && !table_names.is_empty() {
        return Err(LimboError::ParseError(
            "cannot drop schema \"public\" because other objects depend on it".to_string(),
        ));
    }

    for table_name in table_names {
        let mut stmt = prepare_statement(conn, &format!("DROP TABLE \"{table_name}\""))?;
        stmt.run_ignore_rows()?;
    }
    Ok(())
}

fn drop_all_tables_in_schema(conn: &Arc<Connection>, schema_name: &str) -> Result<()> {
    for table_name in list_user_tables(conn, Some(schema_name))? {
        let mut stmt = prepare_statement(
            conn,
            &format!("DROP TABLE \"{schema_name}\".\"{table_name}\""),
        )?;
        stmt.run_ignore_rows()?;
    }
    Ok(())
}

fn handle_pg_copy_from(conn: &Arc<Connection>, stmt: &PgCopyFromStmt) -> Result<usize> {
    let data = std::fs::read_to_string(&stmt.filename).map_err(|e| {
        LimboError::ParseError(format!("COPY FROM: cannot read '{}': {}", stmt.filename, e))
    })?;

    let table_name = match &stmt.schema_name {
        Some(schema) => format!("\"{schema}\".\"{}\"", stmt.table_name),
        None => format!("\"{}\"", stmt.table_name),
    };
    let column_names = get_table_columns(conn, &stmt.table_name, stmt.schema_name.as_deref())?;
    if column_names.is_empty() {
        return Err(LimboError::ParseError(format!(
            "COPY FROM: table '{}' not found or has no columns",
            stmt.table_name
        )));
    }

    let (insert_cols, num_columns) = match &stmt.columns {
        Some(cols) => {
            let col_list = cols
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            (format!(" ({col_list})"), cols.len())
        }
        None => (String::new(), column_names.len()),
    };

    let placeholders = (0..num_columns).map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_sql = format!("INSERT INTO {table_name}{insert_cols} VALUES ({placeholders})");

    let delimiter = stmt
        .delimiter
        .as_ref()
        .and_then(|d| d.chars().next())
        .unwrap_or('\t');
    let null_string = stmt.null_string.as_deref().unwrap_or("\\N");

    let mut rows = parse_copy_text_format(&data, delimiter, null_string, num_columns)?;
    if stmt.header && !rows.is_empty() {
        rows.remove(0);
    }

    let rows_inserted = rows.len();
    let mut begin = conn.prepare_sqlite("BEGIN")?;
    begin.run_ignore_rows()?;

    let result = (|| {
        let mut insert_stmt = conn.prepare_sqlite(&insert_sql)?;
        for row in &rows {
            for (i, val) in row.iter().enumerate() {
                let index = NonZero::new(i + 1).unwrap();
                match val {
                    Some(s) => insert_stmt.bind_at(index, Value::build_text(s.clone()))?,
                    None => insert_stmt.bind_at(index, Value::Null)?,
                }
            }
            insert_stmt.run_ignore_rows()?;
            insert_stmt.reset()?;
            insert_stmt.clear_bindings();
        }

        let mut commit = conn.prepare_sqlite("COMMIT")?;
        commit.run_ignore_rows()?;
        Ok(rows_inserted)
    })();

    if result.is_err() {
        if let Ok(mut rollback) = conn.prepare_sqlite("ROLLBACK") {
            let _ = rollback.run_ignore_rows();
        }
    }

    result
}

fn get_table_columns(
    conn: &Arc<Connection>,
    table_name: &str,
    schema_name: Option<&str>,
) -> Result<Vec<String>> {
    let sql = match schema_name {
        Some(schema) => format!("PRAGMA \"{schema}\".table_info('{table_name}')"),
        None => format!("PRAGMA table_info('{table_name}')"),
    };
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.get(1) {
            Some(Value::Text(t)) => Some(t.as_str().to_string()),
            _ => None,
        })
        .collect())
}

fn list_user_tables(conn: &Arc<Connection>, schema_name: Option<&str>) -> Result<Vec<String>> {
    let filter = "type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%'";
    let sql = match schema_name {
        Some(name) => format!("SELECT name FROM \"{name}\".sqlite_schema WHERE {filter}"),
        None => format!("SELECT name FROM sqlite_schema WHERE {filter}"),
    };
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.first() {
            Some(Value::Text(t)) => Some(t.as_str().to_string()),
            _ => None,
        })
        .collect())
}

fn schema_exists(conn: &Arc<Connection>, schema_name: &str) -> Result<bool> {
    let sql = format!(
        "SELECT 1 FROM pragma_database_list WHERE name = '{}'",
        schema_name.replace('\'', "''")
    );
    let mut stmt = conn.prepare_internal(&sql)?;
    let rows = stmt.run_collect_rows()?;
    Ok(!rows.is_empty())
}
