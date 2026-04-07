use fastly::kv_store::InsertMode;
use fastly::KVStore;
use gluesql::prelude::*;
use uuid::Uuid;

const KV_STORE_NAME: &str = "sql_data";
const SQL_KEY_PREFIX: &str = "__sql_";
const SNAPSHOT_KEY: &str = "__meta_snapshot";
const COMPACT_KEY_THRESHOLD: usize = 50;
const COMPACT_AGE_SECS: u64 = 300;

pub struct JournalState {
    pub stmts: Vec<String>,
    pub all_keys: Vec<String>,
    pub delta_keys: Vec<String>,
    pub snapshot_gen: u64,
    pub current_boundary: Option<String>,
    pub prev_boundary: Option<String>,
}

/// Load the database by replaying the SQL journal from KV Store.
pub async fn load_db() -> (Glue<MemoryStorage>, JournalState) {
    let storage = MemoryStorage::default();
    let mut glue = Glue::new(storage);

    let journal = load_sql_log();

    for stmt in &journal.stmts {
        glue.execute(stmt).await.ok();
    }

    (glue, journal)
}

/// Try to compact the journal if enough delta keys have accumulated or
/// the oldest delta is older than 5 minutes.
/// Uses CAS (if_generation_match) to prevent concurrent compaction across regions.
/// Deletes only keys older than the previous compaction boundary (2-generation lag).
pub fn maybe_compact(journal: &JournalState) {
    if journal.delta_keys.is_empty() {
        return;
    }

    if !should_compact(journal) {
        return;
    }

    let kv = match KVStore::open(KV_STORE_NAME) {
        Ok(Some(kv)) => kv,
        _ => return,
    };

    let snapshot_body = journal.stmts.join("\n");
    let last_delta = match journal.delta_keys.last() {
        Some(k) => k.as_str(),
        None => return,
    };

    // Build metadata JSON: rotate current -> prev
    let meta = serde_json::json!({
        "current": last_delta,
        "prev": journal.current_boundary,
    });
    let meta_str = meta.to_string();

    // CAS write: only succeeds if snapshot generation hasn't changed since we read it.
    // Evaluated at the central storage backend — exactly one writer wins across all regions.
    if kv
        .build_insert()
        .mode(InsertMode::Overwrite)
        .if_generation_match(journal.snapshot_gen)
        .metadata(&meta_str)
        .execute(SNAPSHOT_KEY, snapshot_body)
        .is_err()
    {
        return;
    }

    // CAS succeeded. Delete delta keys that are older than the previous boundary
    // (2-generation lag: these were folded into at least the prior snapshot,
    // giving it time to propagate across all PoPs).
    if let Some(ref prev) = journal.prev_boundary {
        for key in &journal.all_keys {
            if key.as_str() <= prev.as_str() {
                kv.delete(key).ok();
            }
        }
    }
}

fn should_compact(journal: &JournalState) -> bool {
    // Condition A: enough delta keys
    if journal.delta_keys.len() >= COMPACT_KEY_THRESHOLD {
        return true;
    }

    // Condition B: oldest delta is older than COMPACT_AGE_SECS
    if let Some(oldest_key) = journal.delta_keys.first() {
        let uuid_str = oldest_key.trim_start_matches(SQL_KEY_PREFIX);
        if let Ok(oldest_uuid) = Uuid::parse_str(uuid_str) {
            if let Some(oldest_ts) = oldest_uuid.get_timestamp() {
                let now_uuid = Uuid::now_v7();
                if let Some(now_ts) = now_uuid.get_timestamp() {
                    let (oldest_secs, _) = oldest_ts.to_unix();
                    let (now_secs, _) = now_ts.to_unix();
                    if now_secs.saturating_sub(oldest_secs) >= COMPACT_AGE_SECS {
                        return true;
                    }
                }
            }
        }
    }

    false
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

fn load_sql_log() -> JournalState {
    let kv = match KVStore::open(KV_STORE_NAME) {
        Ok(Some(kv)) => kv,
        _ => {
            return JournalState {
                stmts: Vec::new(),
                all_keys: Vec::new(),
                delta_keys: Vec::new(),
                snapshot_gen: 0,
                current_boundary: None,
                prev_boundary: None,
            }
        }
    };

    let mut stmts: Vec<String> = Vec::new();
    let mut snapshot_gen: u64 = 0;
    let mut current_boundary: Option<String> = None;
    let mut prev_boundary: Option<String> = None;

    // Load compacted snapshot if it exists
    if let Ok(mut resp) = kv.build_lookup().execute(SNAPSHOT_KEY) {
        snapshot_gen = resp.current_generation();

        // Parse JSON metadata: {"current":"...","prev":"..."}
        if let Some(meta_bytes) = resp.metadata() {
            if let Ok(meta_str) = String::from_utf8(meta_bytes.to_vec()) {
                if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_str) {
                    current_boundary = meta
                        .get("current")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    prev_boundary = meta.get("prev").and_then(|v| v.as_str()).map(String::from);
                } else {
                    // Legacy plain-text metadata (pre-JSON format)
                    current_boundary = Some(meta_str);
                }
            }
        }

        let body = resp.take_body().into_string();
        stmts.extend(
            body.lines()
                .map(|l| l.trim())
                .filter(|l| !l.is_empty())
                .map(|l| l.to_string()),
        );
    }

    // List all delta keys, paginating through all results
    let mut keys: Vec<String> = Vec::new();
    let first_page = match kv.build_list().prefix(SQL_KEY_PREFIX).limit(1000).execute() {
        Ok(page) => page,
        Err(_) => {
            return JournalState {
                stmts,
                all_keys: Vec::new(),
                delta_keys: Vec::new(),
                snapshot_gen,
                current_boundary,
                prev_boundary,
            }
        }
    };
    let mut cursor = first_page.next_cursor().map(String::from);
    keys.extend(first_page.into_keys());
    while let Some(ref c) = cursor {
        match kv
            .build_list()
            .prefix(SQL_KEY_PREFIX)
            .limit(1000)
            .cursor(c)
            .execute()
        {
            Ok(page) => {
                cursor = page.next_cursor().map(String::from);
                keys.extend(page.into_keys());
            }
            Err(_) => break,
        }
    }

    // Sort keys — UUIDv7 hex sorts lexicographically by time
    keys.sort();

    // Keep only delta keys newer than the snapshot boundary
    let delta_keys: Vec<String> = if let Some(ref last) = current_boundary {
        keys.iter()
            .filter(|k| k.as_str() > last.as_str())
            .cloned()
            .collect()
    } else {
        keys.clone()
    };

    // Read each delta key
    for key in &delta_keys {
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

    JournalState {
        stmts,
        all_keys: keys,
        delta_keys,
        snapshot_gen,
        current_boundary,
        prev_boundary,
    }
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
