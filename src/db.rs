use fastly::kv_store::InsertMode;
use fastly::KVStore;
use gluesql::prelude::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use uuid::Uuid;

/// Trivial single-threaded block_on for WASM — no runtime needed.
fn block_on<F: Future>(mut fut: F) -> F::Output {
    // SAFETY: fut is a stack local, never moved after pinning.
    let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    // Single-poll executor: in WASM all I/O completes synchronously, so the
    // future is always Ready on the first poll. If it were Pending, there is
    // no reactor to wake us, so we panic rather than busy-loop.
    #[allow(clippy::never_loop)]
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => panic!("unexpected Pending in single-threaded WASM"),
        }
    }
}

const fn noop_waker() -> Waker {
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {});
    // SAFETY: vtable functions are no-ops; valid for a waker that is never
    // truly woken (single-threaded WASM with no pending futures).
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

const KV_STORE_NAME: &str = "sql_data";
const SQL_KEY_PREFIX: &str = "__sql_";
const SNAPSHOT_KEY_A: &str = "__meta_snapshot_a";
const SNAPSHOT_KEY_B: &str = "__meta_snapshot_b";
const COMPACT_KEY_THRESHOLD: usize = 50;
const COMPACT_AGE_SECS: u64 = 300;
const RELIST_INTERVAL: u32 = 10;

/// Paginate through all `__sql_*` keys in the KV store and return them sorted.
fn list_delta_keys(kv: &KVStore) -> Vec<String> {
    let first_page = match kv.build_list().prefix(SQL_KEY_PREFIX).limit(1000).execute() {
        Ok(page) => page,
        Err(_) => return Vec::new(),
    };
    let mut cursor = first_page.next_cursor();
    let mut keys: Vec<String> = first_page.into_keys();
    while let Some(ref c) = cursor {
        match kv
            .build_list()
            .prefix(SQL_KEY_PREFIX)
            .limit(1000)
            .cursor(c)
            .execute()
        {
            Ok(page) => {
                cursor = page.next_cursor();
                keys.extend(page.into_keys());
            }
            Err(_) => break,
        }
    }
    // UUIDv7 hex sorts lexicographically by time
    keys.sort();
    keys
}

pub struct JournalState {
    pub all_keys: Vec<String>,
    pub delta_keys: Vec<String>,
    pub gen_a: u64,
    pub gen_b: u64,
    pub boundary_a: Option<String>,
    pub boundary_b: Option<String>,
    pub refreshes_since_list: u32,
}

impl JournalState {
    /// The newer boundary (most recent snapshot). Used as the replay starting
    /// point when loading deltas.
    pub fn current_boundary(&self) -> Option<&String> {
        match (&self.boundary_a, &self.boundary_b) {
            (Some(a), Some(b)) => {
                if a.as_str() >= b.as_str() {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// The older boundary (fallback snapshot). Delta keys <= this boundary
    /// are safe to delete because both snapshots have folded them in.
    pub fn older_boundary(&self) -> Option<&String> {
        match (&self.boundary_a, &self.boundary_b) {
            (Some(a), Some(b)) => {
                if a.as_str() < b.as_str() {
                    Some(a)
                } else {
                    Some(b)
                }
            }
            _ => None,
        }
    }

    /// Returns (key, generation) of the stale slot — the one with the older
    /// boundary. This is the next compaction target.
    pub fn stale_slot(&self) -> (&str, u64) {
        match (&self.boundary_a, &self.boundary_b) {
            (Some(a), Some(b)) => {
                if a.as_str() < b.as_str() {
                    (SNAPSHOT_KEY_A, self.gen_a)
                } else {
                    (SNAPSHOT_KEY_B, self.gen_b)
                }
            }
            // If only one slot is populated, write to the empty one
            (Some(_), None) => (SNAPSHOT_KEY_B, self.gen_b),
            (None, Some(_)) => (SNAPSHOT_KEY_A, self.gen_a),
            // Neither populated — start with slot A
            (None, None) => (SNAPSHOT_KEY_A, self.gen_a),
        }
    }
}

/// Read a snapshot slot's metadata (boundary) and generation without reading
/// the body. Returns (generation, boundary).
fn read_slot_meta(kv: &KVStore, key: &str) -> (u64, Option<String>) {
    match kv.build_lookup().execute(key) {
        Ok(resp) => {
            let gen = resp.current_generation();
            let boundary = resp.metadata().and_then(|meta_bytes| {
                let meta_str = String::from_utf8(meta_bytes.to_vec()).ok()?;
                if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_str) {
                    meta.get("current")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                } else {
                    // Legacy plain-text metadata (pre-JSON format)
                    Some(meta_str)
                }
            });
            (gen, boundary)
        }
        Err(_) => (0, None),
    }
}

/// Deserialize a snapshot body into MemoryStorage.
/// Migration chain: bincode (current) → serde_json (previous) → SQL replay (legacy).
fn deserialize_snapshot(body_bytes: &[u8]) -> MemoryStorage {
    if let Ok(restored) = bincode::deserialize::<MemoryStorage>(body_bytes) {
        return restored;
    }
    if let Ok(body_str) = std::str::from_utf8(body_bytes) {
        if let Ok(restored) = serde_json::from_str::<MemoryStorage>(body_str) {
            return restored;
        }
        // Legacy SQL-text snapshot — replay each line
        let mut glue = Glue::new(MemoryStorage::default());
        for line in body_str.lines().map(|l| l.trim()).filter(|l| !l.is_empty()) {
            block_on(glue.execute(line)).ok();
        }
        return glue.storage;
    }
    MemoryStorage::default()
}

/// Load the database from KV Store. Reads both snapshot slots and uses the
/// newer one; falls back to the legacy single-key snapshot for migration.
pub fn load_db() -> (Glue<MemoryStorage>, JournalState) {
    let kv = match KVStore::open(KV_STORE_NAME) {
        Ok(Some(kv)) => kv,
        _ => {
            return (
                Glue::new(MemoryStorage::default()),
                JournalState {
                    all_keys: Vec::new(),
                    delta_keys: Vec::new(),
                    gen_a: 0,
                    gen_b: 0,
                    boundary_a: None,
                    boundary_b: None,
                    refreshes_since_list: 0,
                },
            )
        }
    };

    // Read metadata from both slots
    let (gen_a, boundary_a) = read_slot_meta(&kv, SNAPSHOT_KEY_A);
    let (gen_b, boundary_b) = read_slot_meta(&kv, SNAPSHOT_KEY_B);

    // Pick the newer slot (larger boundary = more recent)
    let snapshot_key = match (&boundary_a, &boundary_b) {
        (Some(a), Some(b)) => {
            if a.as_str() >= b.as_str() {
                Some(SNAPSHOT_KEY_A)
            } else {
                Some(SNAPSHOT_KEY_B)
            }
        }
        (Some(_), None) => Some(SNAPSHOT_KEY_A),
        (None, Some(_)) => Some(SNAPSHOT_KEY_B),
        (None, None) => None,
    };

    let mut storage = MemoryStorage::default();

    if let Some(key) = snapshot_key {
        if let Ok(mut resp) = kv.build_lookup().execute(key) {
            let body_bytes = resp.take_body().into_bytes();
            storage = deserialize_snapshot(&body_bytes);
        }
    }

    let mut glue = Glue::new(storage);

    // List all delta keys
    let keys = list_delta_keys(&kv);

    // Build a temporary JournalState to use current_boundary()
    let current_boundary = match (&boundary_a, &boundary_b) {
        (Some(a), Some(b)) => {
            if a.as_str() >= b.as_str() {
                Some(a)
            } else {
                Some(b)
            }
        }
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };

    // Keep only delta keys newer than the snapshot boundary
    let delta_keys: Vec<String> = current_boundary.map_or_else(
        || keys.clone(),
        |last| {
            keys.iter()
                .filter(|k| k.as_str() > last.as_str())
                .cloned()
                .collect()
        },
    );

    // Replay only delta keys on top of the snapshot
    for key in &delta_keys {
        if let Ok(mut resp) = kv.lookup(key) {
            let body = resp.take_body().into_string();
            for line in body.lines().map(|l| l.trim()).filter(|l| !l.is_empty()) {
                block_on(glue.execute(line)).ok();
            }
        }
    }

    let journal = JournalState {
        all_keys: keys,
        delta_keys,
        gen_a,
        gen_b,
        boundary_a,
        boundary_b,
        refreshes_since_list: 0,
    };

    (glue, journal)
}

/// Incrementally refresh the database by loading only new delta keys since
/// the last known state. If either snapshot generation changed (another sandbox
/// compacted), falls back to a full reload.
/// Returns true if a full reload was performed.
pub fn refresh_db(glue: &mut Glue<MemoryStorage>, journal: &mut JournalState) -> bool {
    let kv = match KVStore::open(KV_STORE_NAME) {
        Ok(Some(kv)) => kv,
        _ => return false,
    };

    // Check if either snapshot generation changed (another sandbox compacted)
    let current_gen_a = kv
        .build_lookup()
        .execute(SNAPSHOT_KEY_A)
        .map_or(0, |resp| resp.current_generation());
    let current_gen_b = kv
        .build_lookup()
        .execute(SNAPSHOT_KEY_B)
        .map_or(0, |resp| resp.current_generation());

    if current_gen_a != journal.gen_a || current_gen_b != journal.gen_b {
        // Another sandbox compacted — full reload needed
        let (new_glue, new_journal) = load_db();
        *glue = new_glue;
        *journal = new_journal;
        return true;
    }

    // Skip expensive paginated key listing most of the time.
    journal.refreshes_since_list += 1;
    if journal.refreshes_since_list < RELIST_INTERVAL && !journal.all_keys.is_empty() {
        return false;
    }
    journal.refreshes_since_list = 0;

    // Determine the boundary: newest key we've already seen
    let boundary = journal
        .delta_keys
        .last()
        .or(journal.current_boundary());

    // List all delta keys
    let keys = list_delta_keys(&kv);

    // Find new keys beyond our boundary
    let new_keys: Vec<String> = boundary.map_or_else(
        || keys.clone(),
        |b| {
            keys.iter()
                .filter(|k| k.as_str() > b.as_str())
                .cloned()
                .collect()
        },
    );

    if new_keys.is_empty() {
        // Update all_keys in case deletions happened
        journal.all_keys = keys;
        return false;
    }

    // Read and replay only the new delta keys
    for key in &new_keys {
        if let Ok(mut resp) = kv.lookup(key) {
            let body = resp.take_body().into_string();
            for line in body.lines().map(|l| l.trim()).filter(|l| !l.is_empty()) {
                block_on(glue.execute(line)).ok();
            }
        }
    }

    journal.delta_keys.extend(new_keys);
    journal.all_keys = keys;
    false
}

/// Try to compact the journal if enough delta keys have accumulated or
/// the oldest delta is older than 5 minutes.
/// Uses CAS (if_generation_match) to prevent concurrent compaction across regions.
/// Deletes only keys older than the previous compaction boundary (2-generation lag).
pub fn maybe_compact(journal: &mut JournalState, storage: &MemoryStorage) {
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

    do_compact(journal, &kv, storage);
}

/// Unconditionally compact: re-list delta keys from KV, re-read snapshot
/// generations, and fold everything into the snapshot. Called on sandbox
/// shutdown to catch writes that accumulated after the last `maybe_compact`.
pub fn force_compact(journal: &mut JournalState, storage: &MemoryStorage) {
    let kv = match KVStore::open(KV_STORE_NAME) {
        Ok(Some(kv)) => kv,
        _ => return,
    };

    // Re-read both snapshot generations — another sandbox may have compacted.
    journal.gen_a = kv
        .build_lookup()
        .execute(SNAPSHOT_KEY_A)
        .map_or(0, |resp| resp.current_generation());
    journal.gen_b = kv
        .build_lookup()
        .execute(SNAPSHOT_KEY_B)
        .map_or(0, |resp| resp.current_generation());

    // Re-list all delta keys and repopulate delta_keys.
    let keys = list_delta_keys(&kv);
    let boundary = journal.current_boundary().cloned();
    journal.delta_keys = boundary.as_ref().map_or_else(
        || keys.clone(),
        |b| {
            keys.iter()
                .filter(|k| k.as_str() > b.as_str())
                .cloned()
                .collect()
        },
    );
    journal.all_keys = keys;

    if journal.delta_keys.is_empty() {
        return;
    }

    do_compact(journal, &kv, storage);
}

/// Shared compaction logic: serialize MemoryStorage via CAS to the stale slot,
/// and delete old delta keys that both snapshots have folded in.
fn do_compact(journal: &mut JournalState, kv: &KVStore, storage: &MemoryStorage) {
    let snapshot_body = match bincode::serialize(storage) {
        Ok(bytes) => bytes,
        Err(_) => return,
    };
    let last_delta = match journal.delta_keys.last() {
        Some(k) => k.clone(),
        None => return,
    };

    let (slot_key, slot_gen) = journal.stale_slot();
    let slot_key = slot_key.to_owned();

    // Build metadata JSON: just the boundary for this slot
    let meta = serde_json::json!({ "current": &last_delta });
    let meta_str = meta.to_string();

    // CAS write: only succeeds if the slot's generation hasn't changed since we read it.
    // Evaluated at the central storage backend — exactly one writer wins across all regions.
    if kv
        .build_insert()
        .mode(InsertMode::Overwrite)
        .if_generation_match(slot_gen)
        .metadata(&meta_str)
        .execute(&slot_key, snapshot_body)
        .is_err()
    {
        return;
    }

    // CAS succeeded — update journal state for the written slot.
    let new_gen = kv
        .build_lookup()
        .execute(&slot_key)
        .map_or(0, |resp| resp.current_generation());
    if slot_key == SNAPSHOT_KEY_A {
        journal.gen_a = new_gen;
        journal.boundary_a = Some(last_delta);
    } else {
        journal.gen_b = new_gen;
        journal.boundary_b = Some(last_delta);
    }
    journal.delta_keys.clear();

    // Delete delta keys older than the older boundary.
    // Both snapshots have folded these in, so they're safe to remove.
    if let Some(older) = journal.older_boundary() {
        let older = older.clone();
        for key in &journal.all_keys {
            if key.as_str() <= older.as_str() {
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
pub fn execute_query(glue: &mut Glue<MemoryStorage>, sql: &str) -> Result<(String, bool), String> {
    let results = block_on(glue.execute(sql)).map_err(|e| e.to_string())?;

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

    let envelope = serde_json::json!({ "results": all_rows });
    let output = serde_json::to_string_pretty(&envelope);

    output
        .map(|json| (json, has_mutations))
        .map_err(|e| e.to_string())
}

/// Append the user's SQL to a new sharded journal key (concurrent-safe).
/// Returns an error if the KV store is unavailable or the write fails.
pub fn append_to_log(new_sql: &str) -> Result<(), String> {
    // Normalize newlines so the line-based journal format stays intact,
    // but do NOT split on ';' — that breaks string literals like 'a;b'.
    let sql = new_sql.replace(['\n', '\r'], " ");
    let sql = sql.trim();
    if sql.is_empty() {
        return Ok(());
    }

    let kv = KVStore::open(KV_STORE_NAME)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("KV store '{}' not found", KV_STORE_NAME))?;

    let key = format!("{}{}", SQL_KEY_PREFIX, Uuid::now_v7());
    kv.build_insert()
        .mode(InsertMode::Add)
        .execute(&key, sql)
        .map_err(|e| e.to_string())
}

/// Query actual schema from GlueSQL's data dictionary tables.
pub fn get_schema(glue: &mut Glue<MemoryStorage>) -> Result<String, String> {
    // Get all table names
    let results =
        block_on(glue.execute("SELECT TABLE_NAME FROM GLUE_TABLES")).map_err(|e| e.to_string())?;

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
        if let Ok(col_results) = block_on(glue.execute(&col_sql)) {
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
