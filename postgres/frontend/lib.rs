mod aliases;
mod catalog;
mod copy;
mod functions;
mod pgvector;
mod session;

pub use session::PgConnection as Connection;
pub const VECTOR_TYPE_OID: i64 = pgvector::VECTOR_TYPE_OID;

#[derive(Debug, Clone, PartialEq, Eq)]
/// PostgreSQL parameter positions and inferred wire types for a parsed statement.
pub struct PgParameterMetadata {
    pub vector_parameters: Vec<usize>,
    pub oid_parameters: Vec<usize>,
    pub parameter_count: usize,
}
pub use session::{
    open_database, open_database_with_io, split_statements, PgConnection, PgQueryRunner,
};
pub use turso_core::{
    Database, DatabaseOpts, Func, LimboError, Numeric, OpenFlags, PlatformIO, Result, StepResult,
};

pub mod vtab {
    pub use turso_core::VirtualTable;
}
