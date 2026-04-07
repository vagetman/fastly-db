use fastly::kv_store::InsertMode;
use fastly::KVStore;
use gluesql::prelude::*;
use uuid::Uuid;

const KV_STORE_NAME: &str = "sql_data";
const SQL_KEY_PREFIX: &str = "__sql_";

/// Load the database by replaying the SQL journal from KV Store.
/// Returns the Glue instance and the current journal entries.
pub async fn load_db() -> (Glue<MemoryStorage>, Vec<String>) {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    let log = load_sql_log();

    // Replay journal to reconstruct DB state
    for stmt in &log {
        glue.execute(stmt).await.ok();
    }

    (glue, log)
}

/// Execute SQL and return (json_result, has_mutations).
pub async fn execute_query(
    glue: &mut Glue<MemoryStorage>,
    sql: &str,
) -> Result<(String, bool), String> {
    let results = glue.execute(sql).await.map_err(|e| e.to_string())?;

    let mut all_rows: Vec<Vec<serde_json::Map<String, serde_json::Value>>> = Vec::new();
    let mut has_mutations = false;

    for payload in results {
        match payload {
            Payload::Select { labels, rows } => {
                let json_rows: Vec<serde_json::Map<String, serde_json::Value>> = rows
                    .into_iter()
                    .map(|row| {
                        labels
                            .iter()
                            .zip(row.into_iter())
                            .map(|(label, value)| (label.clone(), value_to_json(value)))
                            .collect()
                    })
                    .collect();
                all_rows.push(json_rows);
            }
            Payload::Create => {
                has_mutations = true;
            }
            Payload::Insert(n) => {
                has_mutations = true;
                let mut m = serde_json::Map::new();
                m.insert("inserted".into(), serde_json::Value::Number(n.into()));
                all_rows.push(vec![m]);
            }
            Payload::Update(n) => {
                has_mutations = true;
                let mut m = serde_json::Map::new();
                m.insert("updated".into(), serde_json::Value::Number(n.into()));
                all_rows.push(vec![m]);
            }
            Payload::Delete(n) => {
                has_mutations = true;
                let mut m = serde_json::Map::new();
                m.insert("deleted".into(), serde_json::Value::Number(n.into()));
                all_rows.push(vec![m]);
            }
            Payload::AlterTable => {
                has_mutations = true;
            }
            Payload::DropTable(_) => {
                has_mutations = true;
            }
            Payload::ShowVariable(variable) => {
                let mut m = serde_json::Map::new();
                match variable {
                    PayloadVariable::Tables(tables) => {
                        m.insert("tables".into(), serde_json::json!(tables));
                    }
                    PayloadVariable::Version(v) => {
                        m.insert("version".into(), serde_json::Value::String(v));
                    }
                    PayloadVariable::Functions(fns) => {
                        m.insert("functions".into(), serde_json::json!(fns));
                    }
                }
                all_rows.push(vec![m]);
            }
            Payload::ShowColumns(cols) => {
                let json_cols: Vec<serde_json::Map<String, serde_json::Value>> = cols
                    .into_iter()
                    .map(|(name, dtype)| {
                        let mut m = serde_json::Map::new();
                        m.insert("column".into(), serde_json::Value::String(name));
                        m.insert(
                            "type".into(),
                            serde_json::Value::String(format!("{:?}", dtype)),
                        );
                        m
                    })
                    .collect();
                all_rows.push(json_cols);
            }
            _ => {}
        }
    }

    let output = if all_rows.len() == 1 {
        serde_json::to_string_pretty(&all_rows.into_iter().next().unwrap())
    } else {
        serde_json::to_string_pretty(&all_rows)
    };

    output
        .map(|json| (json, has_mutations))
        .map_err(|e| e.to_string())
}

/// Append the user's SQL statements to a new sharded journal key (concurrent-safe).
pub fn append_to_log(new_sql: &str) {
    let kv = match KVStore::open(KV_STORE_NAME) {
        Ok(Some(kv)) => kv,
        _ => return,
    };
    let mut payload = String::new();
    for stmt in new_sql.split(';') {
        let stmt = stmt.trim();
        if !stmt.is_empty() {
            payload.push_str(stmt);
            payload.push('\n');
        }
    }
    if !payload.is_empty() {
        let key = format!("{}{}", SQL_KEY_PREFIX, Uuid::now_v7());
        kv.build_insert()
            .mode(InsertMode::Add)
            .execute(&key, payload)
            .ok();
    }
}

/// Query actual schema from GlueSQL's data dictionary tables.
pub async fn get_schema(glue: &mut Glue<MemoryStorage>) -> Result<String, String> {
    // Get all table names
    let results = glue
        .execute("SELECT TABLE_NAME FROM GLUE_TABLES")
        .await
        .map_err(|e| e.to_string())?;

    let mut tables: Vec<String> = Vec::new();
    for payload in results {
        if let Payload::Select { labels: _, rows } = payload {
            for row in rows {
                for val in row {
                    if let Value::Str(name) = val {
                        tables.push(name);
                    }
                }
            }
        }
    }

    // For each table, get column details
    let mut schema = serde_json::Map::new();
    for table in &tables {
        let col_sql = format!(
            "SELECT COLUMN_NAME, COLUMN_ID, NULLABLE, KEY, DEFAULT FROM GLUE_TABLE_COLUMNS WHERE TABLE_NAME = '{}'",
            table
        );
        if let Ok(col_results) = glue.execute(&col_sql).await {
            let mut columns: Vec<serde_json::Value> = Vec::new();
            for payload in col_results {
                if let Payload::Select { labels, rows } = payload {
                    for row in rows {
                        let col: serde_json::Map<String, serde_json::Value> = labels
                            .iter()
                            .zip(row.into_iter())
                            .map(|(label, value)| (label.clone(), value_to_json(value)))
                            .collect();
                        columns.push(serde_json::Value::Object(col));
                    }
                }
            }
            schema.insert(table.clone(), serde_json::Value::Array(columns));
        }
    }

    serde_json::to_string_pretty(&schema).map_err(|e| e.to_string())
}

fn load_sql_log() -> Vec<String> {
    let kv = match KVStore::open(KV_STORE_NAME) {
        Ok(Some(kv)) => kv,
        _ => return Vec::new(),
    };

    // List all journal keys with prefix, paginating if needed
    let mut keys: Vec<String> = Vec::new();
    let first_page = match kv.build_list().prefix(SQL_KEY_PREFIX).limit(1000).execute() {
        Ok(page) => page,
        Err(_) => return Vec::new(),
    };
    keys.extend(first_page.into_keys());

    // Sort keys — UUIDv7 hex sorts lexicographically by time
    keys.sort();

    // Lookup each key and collect SQL statements in order
    let mut stmts: Vec<String> = Vec::new();
    for key in &keys {
        if let Ok(mut resp) = kv.lookup(key) {
            let body = resp.take_body().into_string();
            stmts.extend(
                body.lines()
                    .map(|l| l.trim())
                    .filter(|l| !l.is_empty())
                    .map(|l| l.to_string()),
            );
        }
    }
    stmts
}

fn value_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::I8(n) => serde_json::json!(n),
        Value::I16(n) => serde_json::json!(n),
        Value::I32(n) => serde_json::json!(n),
        Value::I64(n) => serde_json::json!(n),
        Value::I128(n) => serde_json::json!(n.to_string()),
        Value::U8(n) => serde_json::json!(n),
        Value::U16(n) => serde_json::json!(n),
        Value::U32(n) => serde_json::json!(n),
        Value::U64(n) => serde_json::json!(n),
        Value::U128(n) => serde_json::json!(n.to_string()),
        Value::F32(n) => serde_json::json!(n),
        Value::F64(n) => serde_json::json!(n),
        Value::Str(s) => serde_json::Value::String(s),
        Value::Null => serde_json::Value::Null,
        other => serde_json::Value::String(format!("{:?}", other)),
    }
}
