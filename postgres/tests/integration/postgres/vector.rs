use crate::common::TempDatabase;
use turso_core::{StepResult, Value};

#[turso_macros::test]
fn pgvector_installation_is_visible_to_new_connections(db: TempDatabase) {
    let first = db.connect_postgres();
    first.execute("CREATE EXTENSION vector").unwrap();
    drop(first);

    let second = db.connect_postgres();
    let mut rows = second
        .query("SELECT extname FROM pg_extension WHERE extname = 'vector'")
        .unwrap()
        .unwrap();
    assert!(matches!(rows.step().unwrap(), StepResult::Row));
    assert!(
        matches!(rows.row().unwrap().get_value(0), Value::Text(name) if name.value.as_ref() == "vector")
    );
}

#[turso_macros::test]
fn pgvector_installation_rollback_is_invisible_to_new_connections(db: TempDatabase) {
    let first = db.connect_postgres();
    first
        .execute("BEGIN; CREATE EXTENSION vector; ROLLBACK")
        .unwrap();
    drop(first);

    let second = db.connect_postgres();
    let error = second
        .execute("CREATE TABLE items (embedding vector(3))")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("run CREATE EXTENSION vector first"));
}

#[turso_macros::test]
fn pgvector_parameter_inference_uses_assignment_context(db: TempDatabase) {
    let conn = db.connect_postgres();
    conn.execute("CREATE EXTENSION vector; CREATE TABLE items (id bigint, embedding vector(3))")
        .unwrap();

    let insert = conn
        .parameter_metadata("INSERT INTO items (id, embedding) VALUES ($1, $2)")
        .unwrap();
    assert_eq!(insert.parameter_count, 2);
    assert_eq!(insert.vector_parameters, vec![2]);

    let update = conn
        .parameter_metadata("UPDATE items SET embedding = $2 WHERE id = $1")
        .unwrap();
    assert_eq!(update.parameter_count, 2);
    assert_eq!(update.vector_parameters, vec![2]);
}
