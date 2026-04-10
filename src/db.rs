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
/// How many seconds a key must exist before the boundary can advance past it.
/// Prevents orphaning keys that KV LIST hasn't propagated yet.
const BOUNDARY_SAFETY_SECS: u64 = 60;

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
    /// Delta keys written by THIS sandbox via append_to_log.
    /// Used to prevent replay_missing_deltas from re-replaying our own writes
    /// (which would create duplicate rows), and to ensure force_compact and
    /// refresh_db preserve our acknowledged writes across snapshot reloads.
    pub local_keys: Vec<String>,
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
    #[allow(dead_code)]
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
                    local_keys: Vec::new(),
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
        local_keys: Vec::new(),
    };

    (glue, journal)
}

/// Incrementally refresh the database by loading only new delta keys since
/// the last known state. If either snapshot generation changed (another sandbox
/// compacted), updates metadata and replays missing deltas while preserving
/// the current in-memory state (which contains this sandbox's acknowledged writes).
/// Returns true if a generation change was detected.
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
        // Another sandbox compacted.  Do NOT replace in-memory state with
        // load_db() — that would discard our own acknowledged writes that may
        // not be in the new snapshot (KV LIST eventual consistency across PoPs).
        //
        // Instead: update metadata, ensure our local keys are "known", then
        // replay any delta keys from other sandboxes we haven't seen.
        let our_boundary = journal.current_boundary().cloned();

        // Update generations and boundaries to current values.
        let (_, boundary_a) = read_slot_meta(&kv, SNAPSHOT_KEY_A);
        let (_, boundary_b) = read_slot_meta(&kv, SNAPSHOT_KEY_B);
        journal.gen_a = current_gen_a;
        journal.gen_b = current_gen_b;
        journal.boundary_a = boundary_a;
        journal.boundary_b = boundary_b;

        // Put our local keys into delta_keys so they're treated as "known".
        {
            let known: std::collections::HashSet<&str> =
                journal.delta_keys.iter().map(|s| s.as_str()).collect();
            let to_add: Vec<String> = journal
                .local_keys
                .iter()
                .filter(|key| !known.contains(key.as_str()))
                .cloned()
                .collect();
            journal.delta_keys.extend(to_add);
            journal.delta_keys.sort();
        }

        // Re-list and replay keys > our old boundary that we haven't seen.
        let keys = list_delta_keys(&kv);
        let known: std::collections::HashSet<&str> =
            journal.delta_keys.iter().map(|s| s.as_str()).collect();
        let missing: Vec<String> = keys
            .iter()
            .filter(|k| {
                our_boundary
                    .as_ref()
                    .map_or(true, |b| k.as_str() > b.as_str())
                    && !known.contains(k.as_str())
            })
            .cloned()
            .collect();

        for key in &missing {
            if let Ok(mut resp) = kv.lookup(key) {
                let body = resp.take_body().into_string();
                for line in body.lines().map(|l| l.trim()).filter(|l| !l.is_empty()) {
                    block_on(glue.execute(line)).ok();
                }
            }
        }

        if !missing.is_empty() {
            journal.delta_keys.extend(missing);
            journal.delta_keys.sort();
        }
        journal.all_keys = keys;
        journal.refreshes_since_list = 0;
        return true;
    }

    // Skip expensive paginated key listing most of the time.
    journal.refreshes_since_list += 1;
    if journal.refreshes_since_list < RELIST_INTERVAL && !journal.all_keys.is_empty() {
        return false;
    }
    journal.refreshes_since_list = 0;

    // Determine the boundary: newest key we've already seen
    let boundary = journal.delta_keys.last().or(journal.current_boundary());

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
pub fn maybe_compact(journal: &mut JournalState) {
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

    do_compact(journal, &kv);
}

/// Unconditionally compact on sandbox shutdown.
/// CAS retries up to 3 times. If all fail, our local delta keys are still in
/// KV and will be picked up by the next compaction cycle (they're protected by
/// the conservative boundary — no compactor will set a boundary past them
/// until they're old enough to be visible in LIST, and age-gated deletion
/// won't remove them until COMPACT_AGE_SECS).
pub fn force_compact(journal: &mut JournalState) {
    let kv = match KVStore::open(KV_STORE_NAME) {
        Ok(Some(kv)) => kv,
        _ => return,
    };

    for _attempt in 0..3 {
        if do_compact(journal, &kv) {
            return;
        }
    }
}

/// Self-contained compaction with a conservative boundary.
///
/// Instead of serializing the sandbox's in-memory state (which may be
/// inconsistent with what's visible in KV LIST across PoPs), this function:
///
/// 1. Reads the latest snapshot from KV
/// 2. Lists ALL delta keys (fresh LIST call)
/// 3. Replays only keys between the old boundary and a conservative safe
///    boundary (latest key that is >= BOUNDARY_SAFETY_SECS old)
/// 4. CAS-writes the resulting state as a new snapshot
///
/// The conservative boundary ensures that recently-written delta keys from
/// other sandboxes — which may not yet be visible in LIST — are NEVER
/// covered by the boundary. They stay as delta keys and will be replayed
/// by future load_db calls. This prevents:
///   - DATA LOSS: boundary advancing past keys invisible in LIST
///   - DUPLICATES: snapshot only contains data for keys explicitly replayed
///
/// Returns true on CAS success.
fn do_compact(journal: &mut JournalState, kv: &KVStore) -> bool {
    // 1. Read fresh metadata from both slots
    let (gen_a, boundary_a) = read_slot_meta(kv, SNAPSHOT_KEY_A);
    let (gen_b, boundary_b) = read_slot_meta(kv, SNAPSHOT_KEY_B);

    // 2. Determine newer slot (source) and stale slot (target for writing)
    let (source_key, current_boundary) = match (&boundary_a, &boundary_b) {
        (Some(a), Some(b)) => {
            if a.as_str() >= b.as_str() {
                (Some(SNAPSHOT_KEY_A), Some(a.clone()))
            } else {
                (Some(SNAPSHOT_KEY_B), Some(b.clone()))
            }
        }
        (Some(a), None) => (Some(SNAPSHOT_KEY_A), Some(a.clone())),
        (None, Some(b)) => (Some(SNAPSHOT_KEY_B), Some(b.clone())),
        (None, None) => (None, None),
    };

    let (target_key, target_gen) = match (&boundary_a, &boundary_b) {
        (Some(a), Some(b)) => {
            if a.as_str() < b.as_str() {
                (SNAPSHOT_KEY_A, gen_a)
            } else {
                (SNAPSHOT_KEY_B, gen_b)
            }
        }
        (Some(_), None) => (SNAPSHOT_KEY_B, gen_b),
        (None, Some(_)) => (SNAPSHOT_KEY_A, gen_a),
        (None, None) => (SNAPSHOT_KEY_A, gen_a),
    };

    // 3. List all delta keys
    let keys = list_delta_keys(kv);

    // 4. Find eligible keys (> current boundary)
    let eligible: Vec<&String> = keys
        .iter()
        .filter(|k| current_boundary.as_ref().map_or(true, |b| k.as_str() > b.as_str()))
        .collect();

    if eligible.is_empty() {
        return false;
    }

    // 5. Conservative boundary: latest eligible key >= BOUNDARY_SAFETY_SECS old
    let now_secs = Uuid::now_v7()
        .get_timestamp()
        .map(|ts| ts.to_unix().0)
        .unwrap_or(0);

    let safe_boundary = eligible
        .iter()
        .rev()
        .find(|k| {
            let uuid_str = k.trim_start_matches(SQL_KEY_PREFIX);
            Uuid::parse_str(uuid_str)
                .ok()
                .and_then(|u| u.get_timestamp())
                .map(|ts| {
                    let (key_secs, _) = ts.to_unix();
                    now_secs.saturating_sub(key_secs) >= BOUNDARY_SAFETY_SECS
                })
                .unwrap_or(false)
        })
        .cloned()
        .cloned();

    let safe_boundary = match safe_boundary {
        Some(k) => k,
        None => return false, // All eligible keys are too recent
    };

    // 6. Load source snapshot from KV (not from in-memory state!)
    let storage = if let Some(key) = source_key {
        if let Ok(mut resp) = kv.build_lookup().execute(key) {
            let body_bytes = resp.take_body().into_bytes();
            deserialize_snapshot(&body_bytes)
        } else {
            MemoryStorage::default()
        }
    } else {
        MemoryStorage::default()
    };
    let mut glue = Glue::new(storage);

    // 7. Replay ONLY keys between current boundary and safe boundary
    for key in &keys {
        let after_boundary = current_boundary.as_ref().map_or(true, |b| key.as_str() > b.as_str());
        if after_boundary && key.as_str() <= safe_boundary.as_str() {
            if let Ok(mut resp) = kv.lookup(key) {
                let body = resp.take_body().into_string();
                for line in body.lines().map(|l| l.trim()).filter(|l| !l.is_empty()) {
                    block_on(glue.execute(line)).ok();
                }
            }
        }
    }

    // 8. Serialize the compaction snapshot
    let snapshot_body = match bincode::serialize(&glue.storage) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };

    // 9. CAS write to the stale slot
    let meta = serde_json::json!({ "current": &safe_boundary });
    let meta_str = meta.to_string();

    if kv
        .build_insert()
        .mode(InsertMode::Overwrite)
        .if_generation_match(target_gen)
        .metadata(&meta_str)
        .execute(target_key, snapshot_body)
        .is_err()
    {
        return false;
    }

    // 10. Update journal metadata
    let new_gen = kv
        .build_lookup()
        .execute(target_key)
        .map_or(0, |resp| resp.current_generation());

    if target_key == SNAPSHOT_KEY_A {
        journal.gen_a = new_gen;
        journal.gen_b = gen_b;
        journal.boundary_a = Some(safe_boundary.clone());
        journal.boundary_b = boundary_b;
    } else {
        journal.gen_a = gen_a;
        journal.gen_b = new_gen;
        journal.boundary_a = boundary_a;
        journal.boundary_b = Some(safe_boundary.clone());
    }

    // Only remove delta keys that are now covered by the boundary
    journal.delta_keys.retain(|k| k.as_str() > safe_boundary.as_str());
    journal.all_keys = keys;

    // 11. Age-gated deletion: only delete keys older than COMPACT_AGE_SECS
    //     that are behind the older boundary (both snapshots have folded them).
    if let Some(older) = journal.older_boundary() {
        let older = older.clone();
        for key in &journal.all_keys {
            if key.as_str() <= older.as_str() {
                let uuid_str = key.trim_start_matches(SQL_KEY_PREFIX);
                let old_enough = Uuid::parse_str(uuid_str)
                    .ok()
                    .and_then(|u| u.get_timestamp())
                    .map(|ts| {
                        let (key_secs, _) = ts.to_unix();
                        now_secs.saturating_sub(key_secs) >= COMPACT_AGE_SECS
                    })
                    .unwrap_or(false);

                if old_enough {
                    kv.delete(key).ok();
                }
            }
        }
    }

    true
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

/// Best-effort check whether the SQL likely mutates state.
/// Used to decide if we should take a pre-execution snapshot for rollback.
pub fn looks_like_mutation(sql: &str) -> bool {
    let s = sql.trim_start();
    if s.is_empty() {
        return false;
    }
    // Case-insensitive prefix check without allocations.
    let upper = s
        .bytes()
        .take(16)
        .map(|b| (b as char).to_ascii_uppercase())
        .collect::<String>();

    upper.starts_with("INSERT")
        || upper.starts_with("UPDATE")
        || upper.starts_with("DELETE")
        || upper.starts_with("CREATE")
        || upper.starts_with("DROP")
        || upper.starts_with("ALTER")
        || upper.starts_with("TRUNCATE")
}

/// Append the user's SQL to a new sharded journal key (concurrent-safe).
/// Returns an error if the KV store is unavailable or the write fails.
/// Append the user's SQL to a new sharded journal key (concurrent-safe).
/// Returns the delta key name on success so the caller can track it.
pub fn append_to_log(new_sql: &str) -> Result<String, String> {
    // Normalize newlines so the line-based journal format stays intact,
    // but do NOT split on ';' — that breaks string literals like 'a;b'.
    let sql = new_sql.replace(['\n', '\r'], " ");
    let sql = sql.trim();
    if sql.is_empty() {
        return Ok(String::new());
    }

    let kv = KVStore::open(KV_STORE_NAME)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("KV store '{}' not found", KV_STORE_NAME))?;

    // KV can occasionally return transient errors under load. Retry a few
    // times before failing the request.
    let mut last_err: Option<String> = None;
    for _ in 0..3 {
        let key = format!("{}{}", SQL_KEY_PREFIX, Uuid::now_v7());
        match kv.build_insert().mode(InsertMode::Add).execute(&key, sql) {
            Ok(()) => return Ok(key),
            Err(e) => last_err = Some(e.to_string()),
        }
    }

    Err(last_err.unwrap_or_else(|| "unknown KV error".to_string()))
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
