use std::fmt::Debug;
use std::num::NonZero;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use futures::{stream, Sink, SinkExt};
use postgres_types::Kind;
use tokio::net::TcpListener;
use tracing::{error, info};
use turso_core::Value;
use turso_pg::{split_statements, Connection, PgConnection};

use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, NoopHandler, PgWireServerHandlers, Type, DEFAULT_NAME,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::extendedquery::{Parse, ParseComplete};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use pgwire::types::format::FormatOptions;

pub struct TursoPgServer {
    address: String,
    db_file: String,
    conn: Arc<Mutex<PgConnection>>,
    interrupt_count: Arc<AtomicUsize>,
}

impl TursoPgServer {
    pub fn new(
        address: String,
        db_file: String,
        conn: Connection,
        interrupt_count: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            address,
            db_file,
            conn: Arc::new(Mutex::new(conn)),
            interrupt_count,
        }
    }

    pub fn run(&self) -> anyhow::Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.run_async())
    }

    async fn run_async(&self) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.address).await?;
        println!(
            "PostgreSQL server listening on {} (database: {})",
            self.address, self.db_file
        );

        let factory = Arc::new(TursoPgFactory {
            handler: Arc::new(TursoPgHandler {
                conn: self.conn.clone(),
                db_file: self.db_file.clone(),
                query_parser: Arc::new(NoopQueryParser::new()),
            }),
        });

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((socket, addr)) => {
                            info!("PostgreSQL client connected from {}", addr);
                            let factory_ref = factory.clone();
                            tokio::spawn(async move {
                                if let Err(e) = process_socket(socket, None, factory_ref).await {
                                    error!("Error processing connection from {}: {}", addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Error accepting connection: {}", e);
                        }
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    println!("\nShutting down PostgreSQL server...");
                    break;
                }
            }

            if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                println!("Shutting down PostgreSQL server...");
                break;
            }
        }

        Ok(())
    }
}

struct TursoPgHandler {
    conn: Arc<Mutex<PgConnection>>,
    db_file: String,
    query_parser: Arc<NoopQueryParser>,
}

impl TursoPgHandler {
    /// After a DROP SCHEMA query succeeds, delete the schema's database file.
    /// Uses simple string matching to detect DROP SCHEMA statements.
    fn cleanup_dropped_schema_file(&self, query: &str) {
        if self.db_file == ":memory:" {
            return;
        }
        // Simple detection: look for DROP SCHEMA pattern
        let trimmed = query.trim().to_lowercase();
        if !trimmed.starts_with("drop schema") {
            return;
        }
        // Extract schema name: "drop schema [if exists] <name> [cascade|restrict]"
        let rest = trimmed.strip_prefix("drop schema").unwrap().trim();
        let rest = rest
            .strip_prefix("if exists")
            .map(|s| s.trim())
            .unwrap_or(rest);
        // Take the first word as the schema name
        let name = rest
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches('"');
        if name.is_empty() || name == "public" {
            return;
        }
        let parent = std::path::Path::new(&self.db_file)
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let schema_file = parent.join(format!("turso-postgres-schema-{name}.db"));
        if schema_file.exists() {
            if let Err(e) = std::fs::remove_file(&schema_file) {
                tracing::warn!("Failed to delete schema file {:?}: {}", schema_file, e);
            } else {
                tracing::info!("Deleted schema file {:?}", schema_file);
            }
            // Also clean up WAL and SHM files
            let wal = schema_file.with_extension("db-wal");
            let shm = schema_file.with_extension("db-shm");
            let _ = std::fs::remove_file(wal);
            let _ = std::fs::remove_file(shm);
        }
    }
}

struct TursoPgFactory {
    handler: Arc<TursoPgHandler>,
}

impl PgWireServerHandlers for TursoPgFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(NoopHandler)
    }
}

#[async_trait]
impl SimpleQueryHandler for TursoPgHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap().clone();

        // Per the PostgreSQL simple query protocol, a query string may contain
        // multiple semicolon-separated statements. Split and execute each one.
        let statements = split_statements(query)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

        let mut responses = Vec::new();
        for sql in &statements {
            let mut stmt = conn
                .prepare(sql)
                .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

            self.cleanup_dropped_schema_file(sql);

            if stmt.num_columns() == 0 || is_pg_non_query(sql) {
                responses.push(execute_non_query(&mut stmt, sql)?);
            } else {
                let header = Arc::new(build_field_info(&stmt, &Format::UnifiedText));
                responses.push(execute_query(&mut stmt, header)?);
            }
        }

        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for TursoPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let conn = self.conn.lock().unwrap().clone();
        let metadata = conn
            .parameter_metadata(&message.query)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;
        let declares_vector_parameter = message
            .type_oids
            .contains(&(turso_pg::VECTOR_TYPE_OID as u32));
        if (declares_vector_parameter || !metadata.vector_parameters.is_empty())
            && !conn.pgvector_installed()
        {
            return Err(PgWireError::UserError(Box::new(error_info(
                "type \"vector\" does not exist; run CREATE EXTENSION vector first",
            ))));
        }
        let parameter_count = message.type_oids.len().max(metadata.parameter_count);
        let mut parameter_types = Vec::with_capacity(parameter_count);
        for index in 0..parameter_count {
            let oid = message.type_oids.get(index).copied().unwrap_or(0);
            let parameter_number = index + 1;
            let data_type = if oid == turso_pg::VECTOR_TYPE_OID as u32
                || (oid == 0 && metadata.vector_parameters.contains(&parameter_number))
            {
                Some(vector_pg_type())
            } else if oid == 0 && metadata.oid_parameters.contains(&parameter_number) {
                Some(Type::OID)
            } else if oid == 0 {
                None
            } else {
                Type::from_oid(oid)
            };
            parameter_types.push(data_type);
        }

        let statement = StoredStatement::new(
            message.name.unwrap_or_else(|| DEFAULT_NAME.to_owned()),
            message.query,
            parameter_types,
        );
        client.portal_store().put_statement(Arc::new(statement));
        client
            .send(PgWireBackendMessage::ParseComplete(ParseComplete::new()))
            .await?;
        Ok(())
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap().clone();
        let query = &portal.statement.statement;

        let mut stmt = conn
            .prepare(query)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

        // Clean up schema file after successful DROP SCHEMA
        self.cleanup_dropped_schema_file(query);

        // Bind parameters from the portal
        bind_portal_parameters(&mut stmt, portal)?;

        if stmt.num_columns() == 0 || is_pg_non_query(query) {
            return execute_non_query(&mut stmt, query);
        }

        let header = Arc::new(build_field_info(&stmt, &portal.result_column_format));
        execute_query(&mut stmt, header)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap().clone();
        let stmt = conn
            .prepare(&target.statement)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

        let param_types: Vec<Type> = target
            .parameter_types
            .iter()
            .map(|t| t.clone().unwrap_or(Type::TEXT))
            .collect();

        let fields = build_field_info(&stmt, &Format::UnifiedText);
        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let conn = self.conn.lock().unwrap().clone();
        let stmt = conn
            .prepare(&portal.statement.statement)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

        let fields = build_field_info(&stmt, &portal.result_column_format);
        Ok(DescribePortalResponse::new(fields))
    }
}

/// Build FieldInfo metadata from a prepared statement's column information.
fn build_field_info(stmt: &turso_core::Statement, format: &Format) -> Vec<FieldInfo> {
    (0..stmt.num_columns())
        .map(|i| {
            let name = stmt.get_column_name(i).into_owned();
            let pg_type = resolve_pg_type_for_column(stmt, i);
            FieldInfo::new(name, None, None, pg_type, format.format_for(i))
        })
        .collect()
}

fn vector_pg_type() -> Type {
    Type::new(
        "vector".to_string(),
        turso_pg::VECTOR_TYPE_OID as u32,
        Kind::Simple,
        "public".to_string(),
    )
}

/// Decide the PG wire type for a result column.
///
/// `get_column_type_info` is the single source of truth: it handles direct
/// table-column references (declared name, array depth, custom-type kind,
/// resolved primitive), bare literals (`SELECT 42` -> INTEGER), and typed
/// expressions like CAST. When it returns `Ok(None)` (no determined primitive)
/// or `Err` (custom types not enabled — won't happen in PG mode, but the wire
/// layer shouldn't panic if it does), the safe default is TEXT;
/// `encode_value` already handles per-value type mismatches.
fn resolve_pg_type_for_column(stmt: &turso_core::Statement, idx: usize) -> Type {
    use turso_core::ColumnTypeKind;

    let Some(info) = stmt.get_column_type_info(idx).ok().flatten() else {
        return Type::TEXT;
    };
    // STRUCT and UNION columns live as BLOBs on disk, but exposing them as
    // BYTEA would force clients to deal with raw bytes. Map them to JSONB so
    // libpq/psql/JDBC see structured data they can introspect.
    let mut base = match info.kind {
        ColumnTypeKind::Struct | ColumnTypeKind::Union => Type::JSONB,
        _ => {
            // Prefer the declared name (the user-visible type), then fall
            // back to the resolved base for custom/domain types whose
            // declared name isn't in the lookup table.
            let mapped = sqlite_type_to_pg_type(&info.declared_name);
            if mapped == Type::TEXT {
                info.base_type
                    .as_deref()
                    .map(sqlite_type_to_pg_type)
                    .unwrap_or(Type::TEXT)
            } else {
                mapped
            }
        }
    };
    if info.array_dimensions > 0 {
        base = scalar_pg_type_to_array_type(&base);
    }
    base
}

/// Map a scalar PG type to its array counterpart.
fn scalar_pg_type_to_array_type(scalar: &Type) -> Type {
    if *scalar == Type::INT4 {
        Type::INT4_ARRAY
    } else if *scalar == Type::INT8 {
        Type::INT8_ARRAY
    } else if *scalar == Type::FLOAT8 {
        Type::FLOAT8_ARRAY
    } else if *scalar == Type::BOOL {
        Type::BOOL_ARRAY
    } else if *scalar == Type::TEXT || *scalar == Type::VARCHAR {
        Type::TEXT_ARRAY
    } else if *scalar == Type::UUID {
        Type::UUID_ARRAY
    } else if *scalar == Type::JSON {
        Type::JSON_ARRAY
    } else if *scalar == Type::JSONB {
        Type::JSONB_ARRAY
    } else if *scalar == Type::DATE {
        Type::DATE_ARRAY
    } else if *scalar == Type::TIME {
        Type::TIME_ARRAY
    } else if *scalar == Type::TIMESTAMP {
        Type::TIMESTAMP_ARRAY
    } else if *scalar == Type::TIMESTAMPTZ {
        Type::TIMESTAMPTZ_ARRAY
    } else if *scalar == Type::INET {
        Type::INET_ARRAY
    } else if *scalar == Type::CIDR {
        Type::CIDR_ARRAY
    } else if *scalar == Type::MACADDR {
        Type::MACADDR_ARRAY
    } else if *scalar == Type::MACADDR8 {
        Type::MACADDR8_ARRAY
    } else if *scalar == Type::NUMERIC {
        Type::NUMERIC_ARRAY
    } else if *scalar == Type::BYTEA {
        Type::BYTEA_ARRAY
    } else if *scalar == Type::FLOAT4 {
        Type::FLOAT4_ARRAY
    } else {
        Type::TEXT_ARRAY
    }
}

/// Execute a query that returns rows and build a Query response.
fn execute_query(
    stmt: &mut turso_core::Statement,
    header: Arc<Vec<FieldInfo>>,
) -> PgWireResult<Response> {
    let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
    let header_clone = header.clone();

    stmt.run_with_row_callback(|row| {
        let mut encoder = DataRowEncoder::new(header_clone.clone());
        for (i, val) in row.get_values().enumerate() {
            let pg_type = header_clone
                .get(i)
                .map(|fi| fi.datatype().clone())
                .unwrap_or(Type::TEXT);
            let format = header_clone
                .get(i)
                .map(FieldInfo::format)
                .unwrap_or(FieldFormat::Text);
            encode_value(&mut encoder, val, &pg_type, format)?;
        }
        rows.push(encoder.finish());
        Ok(())
    })
    .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

    let data_stream = stream::iter(rows);
    Ok(Response::Query(QueryResponse::new(header, data_stream)))
}

/// Execute a non-SELECT statement and build an Execution response.
fn execute_non_query(stmt: &mut turso_core::Statement, query: &str) -> PgWireResult<Response> {
    stmt.run_ignore_rows()
        .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;

    let affected = stmt.n_change();
    let tag = command_tag(query, affected as usize);
    Ok(Response::Execution(tag))
}

/// Extract parameters from a Portal and bind them to a prepared statement.
///
/// PostgreSQL parameters ($1, $2, ...) map to portal parameters 0, 1, ...
/// The bytecode compiler may allocate internal parameter indices in a different
/// order than the $N numbering (e.g. if $2 appears before $1 in the SQL), so we
/// look up each parameter's internal index by name.
fn bind_portal_parameters(
    stmt: &mut turso_core::Statement,
    portal: &Portal<String>,
) -> PgWireResult<()> {
    for i in 0..portal.parameter_len() {
        let value = match &portal.parameters[i] {
            None => Value::Null,
            Some(bytes) => {
                let pg_type = portal
                    .statement
                    .parameter_types
                    .get(i)
                    .and_then(|t| t.as_ref())
                    .unwrap_or(&Type::UNKNOWN);
                pg_bytes_to_value(bytes, pg_type, portal.parameter_format.format_for(i))?
            }
        };
        // Portal parameter i corresponds to PostgreSQL $N where N = i + 1.
        // Look up the internal index that the bytecode compiler assigned to $N.
        let pg_param_name = format!("${}", i + 1);
        let idx = stmt
            .parameter_index(&pg_param_name)
            .unwrap_or_else(|| NonZero::new(i + 1).expect("parameter index must be non-zero"));
        stmt.bind_at(idx, value)
            .map_err(|e| PgWireError::UserError(Box::new(error_info(&e.to_string()))))?;
    }
    Ok(())
}

/// Convert text parameters and supported binary PostgreSQL formats to Core values.
fn pg_bytes_to_value(bytes: &[u8], pg_type: &Type, format: FieldFormat) -> PgWireResult<Value> {
    if pg_type.oid() == turso_pg::VECTOR_TYPE_OID as u32 {
        return match format {
            FieldFormat::Text => std::str::from_utf8(bytes)
                .map(|text| Value::from_text(text.to_owned()))
                .map_err(|e| {
                    PgWireError::UserError(Box::new(error_info(&format!(
                        "invalid UTF-8 in vector parameter: {e}"
                    ))))
                }),
            FieldFormat::Binary => decode_pgvector_binary(bytes),
        };
    }
    if format == FieldFormat::Binary && *pg_type == Type::OID {
        let bytes: [u8; 4] = bytes.try_into().map_err(|_| {
            PgWireError::UserError(Box::new(error_info(
                "invalid binary OID parameter: expected 4 bytes",
            )))
        })?;
        return Ok(Value::from_i64(i64::from(u32::from_be_bytes(bytes))));
    }
    let text = std::str::from_utf8(bytes).map_err(|e| {
        PgWireError::UserError(Box::new(error_info(&format!(
            "invalid UTF-8 in parameter: {e}"
        ))))
    })?;

    match *pg_type {
        Type::INT2 | Type::INT4 | Type::INT8 => {
            let i: i64 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(error_info(&format!(
                    "invalid integer parameter: {e}"
                ))))
            })?;
            Ok(Value::from_i64(i))
        }
        Type::OID => {
            let oid: u32 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(error_info(&format!("invalid OID parameter: {e}"))))
            })?;
            Ok(Value::from_i64(i64::from(oid)))
        }
        Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC => {
            let f: f64 = text.parse().map_err(|e| {
                PgWireError::UserError(Box::new(error_info(&format!(
                    "invalid float parameter: {e}"
                ))))
            })?;
            Ok(Value::from_f64(f))
        }
        Type::BOOL => match text {
            "t" | "true" | "TRUE" | "1" | "yes" | "on" => Ok(Value::from_i64(1)),
            "f" | "false" | "FALSE" | "0" | "no" | "off" => Ok(Value::from_i64(0)),
            _ => Err(PgWireError::UserError(Box::new(error_info(&format!(
                "invalid boolean parameter: {text}"
            ))))),
        },
        Type::BYTEA => {
            // PostgreSQL text format for bytea uses \x hex encoding
            if let Some(hex_str) = text.strip_prefix("\\x") {
                let data = decode_hex(hex_str).map_err(|e| {
                    PgWireError::UserError(Box::new(error_info(&format!(
                        "invalid bytea hex parameter: {e}"
                    ))))
                })?;
                Ok(Value::from_blob(data))
            } else {
                // Raw bytes as-is
                Ok(Value::from_blob(bytes.to_vec()))
            }
        }
        // UNKNOWN: try to infer type from text content (numeric-looking values
        // should be bound as numbers so comparisons with COUNT/SUM etc. work)
        Type::UNKNOWN => {
            if let Ok(i) = text.parse::<i64>() {
                Ok(Value::from_i64(i))
            } else if let Ok(f) = text.parse::<f64>() {
                Ok(Value::from_f64(f))
            } else if text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("t") {
                Ok(Value::from_i64(1))
            } else if text.eq_ignore_ascii_case("false") || text.eq_ignore_ascii_case("f") {
                Ok(Value::from_i64(0))
            } else {
                Ok(Value::from_text(text.to_owned()))
            }
        }
        // TEXT, VARCHAR, and all other types → text
        _ => Ok(Value::from_text(text.to_owned())),
    }
}

/// Decode pgvector's big-endian header and elements into Core's little-endian BLOB.
fn decode_pgvector_binary(bytes: &[u8]) -> PgWireResult<Value> {
    if bytes.len() < 4 {
        return Err(PgWireError::UserError(Box::new(error_info(
            "invalid vector binary parameter: expected a 4-byte header",
        ))));
    }
    let dimensions = usize::from(u16::from_be_bytes([bytes[0], bytes[1]]));
    let unused = u16::from_be_bytes([bytes[2], bytes[3]]);
    if !(turso_core::vector::MIN_DENSE_F32_DIMENSIONS
        ..=turso_core::vector::MAX_DENSE_F32_DIMENSIONS)
        .contains(&dimensions)
    {
        return Err(PgWireError::UserError(Box::new(error_info(&format!(
            "invalid vector binary parameter: dimensions must be between 1 and 16000, got {dimensions}"
        )))));
    }
    if unused != 0 {
        return Err(PgWireError::UserError(Box::new(error_info(
            "invalid vector binary parameter: unused header field must be zero",
        ))));
    }
    let expected_len = 4 + dimensions * size_of::<f32>();
    if bytes.len() != expected_len {
        return Err(PgWireError::UserError(Box::new(error_info(&format!(
            "invalid vector binary parameter: expected {expected_len} bytes, got {}",
            bytes.len()
        )))));
    }

    let mut blob = Vec::with_capacity(dimensions * size_of::<f32>());
    for chunk in bytes[4..].chunks_exact(size_of::<f32>()) {
        let value = f32::from_be_bytes(chunk.try_into().expect("chunks are exactly four bytes"));
        if !value.is_finite() {
            return Err(PgWireError::UserError(Box::new(error_info(
                "invalid vector binary parameter: elements must be finite",
            ))));
        }
        blob.extend_from_slice(&value.to_le_bytes());
    }
    Ok(Value::from_blob(blob))
}

/// Decode a hex string into bytes.
fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err("odd-length hex string".to_owned());
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|e| format!("invalid hex at position {i}: {e}"))
        })
        .collect()
}

fn encode_value(
    encoder: &mut DataRowEncoder,
    val: &Value,
    pg_type: &Type,
    format: FieldFormat,
) -> turso_core::Result<()> {
    if pg_type.oid() == turso_pg::VECTOR_TYPE_OID as u32 {
        return encode_pgvector_value(encoder, val, format);
    }
    match val {
        Value::Null => encoder
            .encode_field(&None::<i8>)
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
        Value::Numeric(turso_core::Numeric::Integer(i)) => {
            // Boolean columns: encode as true/false instead of 0/1
            if *pg_type == Type::BOOL {
                encoder
                    .encode_field(&(*i != 0))
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else if *pg_type == Type::OID {
                let oid = u32::try_from(*i).map_err(|_| {
                    turso_core::LimboError::ConversionError(format!(
                        "OID value is out of range: {i}"
                    ))
                })?;
                encoder
                    .encode_field(&oid)
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else {
                encoder
                    .encode_field(i)
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            }
        }
        Value::Numeric(turso_core::Numeric::Float(f)) => encoder
            .encode_field(&f64::from(*f))
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
        Value::Text(t) => {
            let text = t.value.as_ref();
            // For TIMESTAMPTZ columns, ensure timezone info is present so clients
            // parse the value correctly (as UTC, not local time).
            // TIMESTAMP (without TZ) should NOT have timezone suffix.
            if *pg_type == Type::CHAR {
                let value = text.as_bytes().first().copied().ok_or_else(|| {
                    turso_core::LimboError::ConversionError(
                        "PostgreSQL internal char cannot be empty".to_string(),
                    )
                })? as i8;
                encoder
                    .encode_field(&value)
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else if *pg_type == Type::TIMESTAMPTZ
                && !text.contains('+')
                && !text.contains('Z')
                && !text.ends_with("-00")
            {
                let with_tz = format!("{text}+00");
                encoder
                    .encode_field(&with_tz.as_str())
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else if pg_type.name().starts_with('_') {
                // Array types: pgwire's to_sql_text quotes strings containing
                // {, }, or commas when the type is Kind::Array. Since we store
                // array values as pre-formatted PG array literals (e.g.
                // "{1,2,3}"), encode with Type::TEXT to bypass the quoting.
                encoder
                    .encode_field_with_type_and_format(
                        &text,
                        &Type::TEXT,
                        FieldFormat::Text,
                        &FormatOptions::default(),
                    )
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            } else {
                encoder
                    .encode_field(&text)
                    .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
            }
        }
        Value::Blob(b) => encoder
            .encode_field(&b.as_slice())
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string())),
    }
}

/// Encode a Core dense-f32 vector using pgvector's requested text or binary format.
fn encode_pgvector_value(
    encoder: &mut DataRowEncoder,
    value: &Value,
    format: FieldFormat,
) -> turso_core::Result<()> {
    if matches!(value, Value::Null) {
        return encoder
            .encode_field_with_type_and_format(
                &None::<i8>,
                &Type::TEXT,
                format,
                &FormatOptions::default(),
            )
            .map_err(|e| turso_core::LimboError::InternalError(e.to_string()));
    }

    let vector = turso_core::vector::parse_vector(value, None)?;
    if vector.vector_type != turso_core::vector::vector_types::VectorType::Float32Dense {
        return Err(turso_core::LimboError::ConversionError(
            "PostgreSQL vector values must use dense float32 storage".to_string(),
        ));
    }
    match format {
        FieldFormat::Text => {
            let text = turso_core::vector::operations::text::vector_to_text(&vector);
            encoder
                .encode_field_with_type_and_format(
                    &text,
                    &Type::TEXT,
                    FieldFormat::Text,
                    &FormatOptions::default(),
                )
                .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
        }
        FieldFormat::Binary => {
            let dimensions = u16::try_from(vector.dims).map_err(|_| {
                turso_core::LimboError::ConversionError(
                    "vector dimensions exceed PostgreSQL binary format limits".to_string(),
                )
            })?;
            let mut bytes = Vec::with_capacity(4 + vector.dims * size_of::<f32>());
            bytes.extend_from_slice(&dimensions.to_be_bytes());
            bytes.extend_from_slice(&0u16.to_be_bytes());
            for value in vector.as_f32_slice() {
                bytes.extend_from_slice(&value.to_be_bytes());
            }
            encoder
                .encode_field_with_type_and_format(
                    &bytes,
                    &Type::BYTEA,
                    FieldFormat::Binary,
                    &FormatOptions::default(),
                )
                .map_err(|e| turso_core::LimboError::InternalError(e.to_string()))
        }
    }
}

fn sqlite_type_to_pg_type(type_str: &str) -> Type {
    let upper = type_str.to_uppercase();
    match upper.as_str() {
        "INTEGER" | "INT" | "INT4" | "SMALLINT" | "INT2" | "SERIAL" | "SMALLSERIAL" => Type::INT4,
        "BIGINT" | "INT8" | "BIGSERIAL" => Type::INT8,
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "NUMERIC"
        | "DECIMAL" => Type::FLOAT8,
        "TEXT" | "VARCHAR" | "CHAR" | "CHARACTER VARYING" | "CHARACTER" | "NAME" => Type::TEXT,
        "BLOB" | "BYTEA" => Type::BYTEA,
        "BOOLEAN" | "BOOL" => Type::BOOL,
        "UUID" => Type::UUID,
        "JSON" => Type::JSON,
        "JSONB" => Type::JSONB,
        "DATE" => Type::DATE,
        "TIME" | "TIMETZ" => Type::TIME,
        "TIMESTAMP" => Type::TIMESTAMP,
        "TIMESTAMPTZ" => Type::TIMESTAMPTZ,
        "INET" => Type::INET,
        "CIDR" => Type::CIDR,
        "MACADDR" => Type::MACADDR,
        "MACADDR8" => Type::MACADDR8,
        "OID" => Type::OID,
        "VECTOR" => vector_pg_type(),
        "INTERNAL_CHAR" => Type::CHAR,
        _ => {
            // Handle parameterized types like varchar(50), numeric(10,2)
            if upper.starts_with("VARCHAR") || upper.starts_with("CHAR") {
                Type::VARCHAR
            } else if upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL") {
                Type::NUMERIC
            } else {
                Type::TEXT
            }
        }
    }
}

/// PG statements handled by `try_prepare_pg()` that return a dummy SELECT
/// but should produce a command-tag response, not a result set.
fn is_pg_non_query(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("COPY")
        || upper.starts_with("CREATE SCHEMA")
        || upper.starts_with("DROP SCHEMA")
        || upper.starts_with("REFRESH MATERIALIZED VIEW")
        || upper.starts_with("COMMENT")
}

fn command_tag(query: &str, affected_rows: usize) -> Tag {
    let upper = query.trim().to_uppercase();
    if upper.starts_with("INSERT") {
        Tag::new("INSERT").with_oid(0).with_rows(affected_rows)
    } else if upper.starts_with("UPDATE") {
        Tag::new("UPDATE").with_rows(affected_rows)
    } else if upper.starts_with("DELETE") || upper.starts_with("TRUNCATE") {
        Tag::new("DELETE").with_rows(affected_rows)
    } else if upper.starts_with("CREATE VIEW") {
        Tag::new("CREATE VIEW")
    } else if upper.starts_with("CREATE INDEX") {
        Tag::new("CREATE INDEX")
    } else if upper.starts_with("CREATE SCHEMA") {
        Tag::new("CREATE SCHEMA")
    } else if upper.starts_with("CREATE EXTENSION") {
        Tag::new("CREATE EXTENSION")
    } else if upper.starts_with("CREATE") {
        Tag::new("CREATE TABLE")
    } else if upper.starts_with("DROP VIEW") {
        Tag::new("DROP VIEW")
    } else if upper.starts_with("DROP INDEX") {
        Tag::new("DROP INDEX")
    } else if upper.starts_with("DROP SCHEMA") {
        Tag::new("DROP SCHEMA")
    } else if upper.starts_with("DROP") {
        Tag::new("DROP TABLE")
    } else if upper.starts_with("ALTER") {
        Tag::new("ALTER TABLE")
    } else if upper.starts_with("BEGIN") || upper.starts_with("START") {
        Tag::new("BEGIN")
    } else if upper.starts_with("COMMIT") {
        Tag::new("COMMIT")
    } else if upper.starts_with("ROLLBACK") {
        Tag::new("ROLLBACK")
    } else if upper.starts_with("SAVEPOINT") {
        Tag::new("SAVEPOINT")
    } else if upper.starts_with("RELEASE") {
        Tag::new("RELEASE")
    } else if upper.starts_with("SET") {
        Tag::new("SET")
    } else if upper.starts_with("COPY") {
        Tag::new("COPY").with_rows(affected_rows)
    } else if upper.starts_with("COMMENT") {
        Tag::new("COMMENT")
    } else {
        Tag::new("OK")
    }
}

fn error_info(message: &str) -> ErrorInfo {
    ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct PgVector(Vec<f32>);

    impl postgres_types::ToSql for PgVector {
        fn to_sql(
            &self,
            ty: &Type,
            out: &mut bytes::BytesMut,
        ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
            if !Self::accepts(ty) {
                return Err("expected vector PostgreSQL type".into());
            }
            out.extend_from_slice(&(self.0.len() as u16).to_be_bytes());
            out.extend_from_slice(&0u16.to_be_bytes());
            for value in &self.0 {
                out.extend_from_slice(&value.to_be_bytes());
            }
            Ok(postgres_types::IsNull::No)
        }

        fn accepts(ty: &Type) -> bool {
            ty.oid() == turso_pg::VECTOR_TYPE_OID as u32
        }

        postgres_types::to_sql_checked!();
    }

    impl<'a> postgres_types::FromSql<'a> for PgVector {
        fn from_sql(
            ty: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            if !Self::accepts(ty) || raw.len() < 4 {
                return Err("invalid vector result".into());
            }
            let dimensions = usize::from(u16::from_be_bytes([raw[0], raw[1]]));
            if raw[2..4] != [0, 0] || raw.len() != 4 + dimensions * size_of::<f32>() {
                return Err("invalid vector result payload".into());
            }
            let values = raw[4..]
                .chunks_exact(size_of::<f32>())
                .map(|chunk| f32::from_be_bytes(chunk.try_into().unwrap()))
                .collect();
            Ok(Self(values))
        }

        fn accepts(ty: &Type) -> bool {
            ty.oid() == turso_pg::VECTOR_TYPE_OID as u32
        }
    }

    #[tokio::test]
    async fn extended_protocol_round_trips_binary_vector_with_inferred_oid() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let (_, database) = turso_pg::open_database(
            ":memory:",
            None,
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new().with_custom_types(true),
        )
        .unwrap();
        let connection = PgConnection::new(database.connect().unwrap());
        let server = Arc::new(TursoPgServer::new(
            address.to_string(),
            ":memory:".to_string(),
            connection,
            Arc::new(AtomicUsize::new(0)),
        ));
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move { server.run_async().await })
        };

        let (client, connection) = loop {
            match tokio_postgres::connect(
                &format!("host=127.0.0.1 port={} user=test", address.port()),
                tokio_postgres::NoTls,
            )
            .await
            {
                Ok(connected) => break connected,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
            }
        };
        let connection_task = tokio::spawn(connection);

        client
            .batch_execute(
                "CREATE EXTENSION vector; CREATE TABLE items (id bigint, embedding vector(3))",
            )
            .await
            .unwrap();
        let insert = client
            .prepare("INSERT INTO items (id, embedding) VALUES (1, $1)")
            .await
            .unwrap();
        assert_eq!(insert.params()[0].oid(), turso_pg::VECTOR_TYPE_OID as u32);
        client
            .execute(&insert, &[&PgVector(vec![1.0, 2.0, 3.0])])
            .await
            .unwrap();

        let select = client.prepare("SELECT embedding FROM items").await.unwrap();
        assert_eq!(
            select.columns()[0].type_().oid(),
            turso_pg::VECTOR_TYPE_OID as u32
        );
        let row = client.query_one(&select, &[]).await.unwrap();
        let vector: PgVector = row.get(0);
        assert_eq!(vector.0, vec![1.0, 2.0, 3.0]);

        connection_task.abort();
        server_task.abort();
    }

    #[test]
    fn test_pg_bytes_to_value_integer() {
        let val = pg_bytes_to_value(b"42", &Type::INT4, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_i64(42));

        let val = pg_bytes_to_value(b"-100", &Type::INT8, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_i64(-100));

        let val = pg_bytes_to_value(b"0", &Type::INT2, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_i64(0));
    }

    #[test]
    fn test_pg_bytes_to_value_float() {
        let val = pg_bytes_to_value(b"3.25", &Type::FLOAT8, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_f64(3.25));

        let val = pg_bytes_to_value(b"-0.5", &Type::FLOAT4, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_f64(-0.5));

        let val = pg_bytes_to_value(b"1.23", &Type::NUMERIC, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_f64(1.23));
    }

    #[test]
    fn test_pg_bytes_to_value_bool() {
        let val = pg_bytes_to_value(b"t", &Type::BOOL, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_i64(1));

        let val = pg_bytes_to_value(b"f", &Type::BOOL, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_i64(0));

        let val = pg_bytes_to_value(b"true", &Type::BOOL, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_i64(1));

        let val = pg_bytes_to_value(b"false", &Type::BOOL, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_i64(0));
    }

    #[test]
    fn test_pg_bytes_to_value_text() {
        let val = pg_bytes_to_value(b"hello world", &Type::TEXT, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_text("hello world".to_owned()));

        let val = pg_bytes_to_value(b"Alice", &Type::VARCHAR, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_text("Alice".to_owned()));
    }

    #[test]
    fn test_pg_bytes_to_value_bytea() {
        let val = pg_bytes_to_value(b"\\xDEADBEEF", &Type::BYTEA, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    }

    #[test]
    fn pgvector_binary_parameter_converts_to_internal_dense_f32_blob() {
        let mut bytes = vec![0, 2, 0, 0];
        bytes.extend_from_slice(&1.5f32.to_be_bytes());
        bytes.extend_from_slice(&(-2.0f32).to_be_bytes());

        let value = pg_bytes_to_value(&bytes, &vector_pg_type(), FieldFormat::Binary).unwrap();
        let Value::Blob(blob) = value else {
            panic!("expected vector blob");
        };
        assert_eq!(
            blob.as_slice(),
            [1.5f32.to_le_bytes(), (-2.0f32).to_le_bytes()].concat()
        );
    }

    #[test]
    fn pgvector_binary_parameter_rejects_malformed_payload() {
        let error = pg_bytes_to_value(
            &[0, 2, 0, 0, 0, 0, 0, 0],
            &vector_pg_type(),
            FieldFormat::Binary,
        )
        .unwrap_err();
        assert!(error.to_string().contains("expected 12 bytes"));
    }

    #[test]
    fn pgvector_binary_result_uses_pgvector_layout() {
        let fields = Arc::new(vec![FieldInfo::new(
            "embedding".to_string(),
            None,
            None,
            vector_pg_type(),
            FieldFormat::Binary,
        )]);
        let mut encoder = DataRowEncoder::new(fields);
        let mut blob = Vec::new();
        blob.extend_from_slice(&1.5f32.to_le_bytes());
        blob.extend_from_slice(&(-2.0f32).to_le_bytes());
        encode_value(
            &mut encoder,
            &Value::from_blob(blob),
            &vector_pg_type(),
            FieldFormat::Binary,
        )
        .unwrap();

        let row = encoder.finish().unwrap();
        assert_eq!(&row.data[..4], &12i32.to_be_bytes());
        assert_eq!(&row.data[4..8], &[0, 2, 0, 0]);
        assert_eq!(&row.data[8..12], &1.5f32.to_be_bytes());
        assert_eq!(&row.data[12..16], &(-2.0f32).to_be_bytes());
    }

    #[test]
    fn test_pg_bytes_to_value_unknown_type_as_text() {
        // Unknown types should be treated as text
        let val = pg_bytes_to_value(b"some-uuid-value", &Type::UUID, FieldFormat::Text).unwrap();
        assert_eq!(val, Value::from_text("some-uuid-value".to_owned()));
    }

    #[test]
    fn test_pg_bytes_to_value_integer_parse_error() {
        let result = pg_bytes_to_value(b"not_a_number", &Type::INT4, FieldFormat::Text);
        assert!(result.is_err());
    }

    #[test]
    fn test_pg_bytes_to_value_float_parse_error() {
        let result = pg_bytes_to_value(b"not_a_float", &Type::FLOAT8, FieldFormat::Text);
        assert!(result.is_err());
    }

    #[test]
    fn test_pg_bytes_to_value_bool_invalid() {
        let result = pg_bytes_to_value(b"maybe", &Type::BOOL, FieldFormat::Text);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_hex() {
        assert_eq!(
            decode_hex("DEADBEEF").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(decode_hex("00ff").unwrap(), vec![0x00, 0xFF]);
        assert_eq!(decode_hex("").unwrap(), Vec::<u8>::new());
        assert!(decode_hex("0").is_err()); // odd length
        assert!(decode_hex("GG").is_err()); // invalid hex
    }

    #[test]
    fn test_sqlite_type_to_pg_type() {
        assert_eq!(sqlite_type_to_pg_type("INTEGER"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("INT"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("INT4"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("SMALLINT"), Type::INT4);
        assert_eq!(sqlite_type_to_pg_type("BIGINT"), Type::INT8);
        assert_eq!(sqlite_type_to_pg_type("INT8"), Type::INT8);
        assert_eq!(sqlite_type_to_pg_type("REAL"), Type::FLOAT8);
        assert_eq!(sqlite_type_to_pg_type("TEXT"), Type::TEXT);
        assert_eq!(sqlite_type_to_pg_type("BLOB"), Type::BYTEA);
        assert_eq!(sqlite_type_to_pg_type("BOOLEAN"), Type::BOOL);
        assert_eq!(sqlite_type_to_pg_type("TIMESTAMP"), Type::TIMESTAMP);
        assert_eq!(sqlite_type_to_pg_type("TIMESTAMPTZ"), Type::TIMESTAMPTZ);
        assert_eq!(sqlite_type_to_pg_type("DATE"), Type::DATE);
        assert_eq!(sqlite_type_to_pg_type("JSON"), Type::JSON);
        assert_eq!(sqlite_type_to_pg_type("JSONB"), Type::JSONB);
        assert_eq!(sqlite_type_to_pg_type("UUID"), Type::UUID);
        // Unknown types map to TEXT
        assert_eq!(sqlite_type_to_pg_type("UNKNOWN"), Type::TEXT);
    }

    #[test]
    fn test_unknown_type_inference() {
        // UNKNOWN type should infer integers from numeric-looking strings
        let val = pg_bytes_to_value(b"42", &Type::UNKNOWN, FieldFormat::Text).unwrap();
        assert!(matches!(
            val,
            Value::Numeric(turso_core::Numeric::Integer(42))
        ));

        // UNKNOWN type should infer floats
        let val = pg_bytes_to_value(b"3.14", &Type::UNKNOWN, FieldFormat::Text).unwrap();
        if let Value::Numeric(turso_core::Numeric::Float(f)) = val {
            #[allow(clippy::approx_constant)]
            let expected = 3.14;
            assert!((f64::from(f) - expected).abs() < 0.001);
        } else {
            panic!("Expected Float");
        }

        // UNKNOWN type should keep text for non-numeric strings
        let val = pg_bytes_to_value(b"hello", &Type::UNKNOWN, FieldFormat::Text).unwrap();
        assert!(matches!(val, Value::Text(_)));
    }
}
