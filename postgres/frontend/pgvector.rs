//! PostgreSQL catalog identity and durable installation state for pgvector compatibility.

pub const EXTENSION_OID: i64 = 40_000;
pub const VECTOR_TYPE_OID: i64 = 40_001;
pub const VERSION: &str = "0.8.1";
pub const INSTALLATION_MARKER: &str = "__turso_internal_pgvector_extension";
pub const L2_FUNCTION_OID: i64 = 40_010;
pub const DOT_FUNCTION_OID: i64 = 40_011;
pub const COSINE_FUNCTION_OID: i64 = 40_012;
pub const L2_OPERATOR_OID: i64 = 40_020;
pub const DOT_OPERATOR_OID: i64 = 40_021;
pub const COSINE_OPERATOR_OID: i64 = 40_022;

pub fn is_installed(schema: &turso_core::schema::Schema) -> bool {
    schema.get_table(INSTALLATION_MARKER).is_some()
}
