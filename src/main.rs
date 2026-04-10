mod db;

use fastly::http::serve::Serve;
use fastly::http::{Method, StatusCode};
use fastly::{mime, Error, Request, Response};
use gluesql::prelude::{Glue, MemoryStorage};
use std::time::Duration;

struct AppContext {
    glue: Option<Glue<MemoryStorage>>,
    journal: Option<db::JournalState>,
}

fn main() -> Result<(), Error> {
    let mut ctx = AppContext {
        glue: None,
        journal: None,
    };
    Serve::new()
        .with_max_requests(100)
        .with_timeout(Duration::from_secs(15))
        .run_with_context(handle, &mut ctx)
        .into_result()?;

    // Sandbox is shutting down — compact unconditionally to fold
    // any remaining delta keys into the snapshot.
    if let (Some(journal), Some(_glue)) = (ctx.journal.as_mut(), ctx.glue.as_ref()) {
        db::force_compact(journal);
    }

    Ok(())
}

fn ensure_db(ctx: &mut AppContext) {
    if ctx.glue.is_some() {
        // Incremental refresh — pick up new deltas from other sandboxes
        let glue = ctx.glue.as_mut().expect("db initialized by ensure_db");
        let journal = ctx
            .journal
            .as_mut()
            .expect("journal initialized by ensure_db");
        db::refresh_db(glue, journal);
    } else {
        // First request — full load
        let (glue, journal) = db::load_db();
        ctx.glue = Some(glue);
        ctx.journal = Some(journal);
    }
}

fn handle(mut req: Request, ctx: &mut AppContext) -> Result<Response, Error> {
    let path = req.get_path().to_owned();
    let method = req.get_method().clone();

    match (method, path.as_str()) {
        // Welcome page
        (Method::GET, "/") => Ok(Response::from_status(StatusCode::OK)
            .with_content_type(mime::TEXT_HTML_UTF_8)
            .with_body(include_str!("welcome-to-compute.html"))),

        // Execute SQL query (persistent)
        (Method::POST, "/query") => {
            let sql = req.take_body_str();
            ensure_db(ctx);
            let glue = ctx.glue.as_mut().expect("db initialized by ensure_db");

            // If this looks like a mutation, snapshot the current in-memory
            // state so we can roll back if persisting to KV fails.
            let pre_snapshot = if db::looks_like_mutation(&sql) {
                bincode::serialize(&glue.storage).ok()
            } else {
                None
            };

            match db::execute_query(glue, &sql) {
                Ok((json, has_mutations)) => {
                    if has_mutations {
                        match db::append_to_log(&sql) {
                            Ok(key) => {
                                // Track the delta key this sandbox wrote so that
                                // refresh_db/force_compact preserve it.
                                let journal = ctx
                                    .journal
                                    .as_mut()
                                    .expect("journal initialized by ensure_db");
                                if !key.is_empty() {
                                    journal.local_keys.push(key.clone());
                                    journal.delta_keys.push(key);
                                    journal.delta_keys.sort();
                                }
                            }
                            Err(err) => {
                                // Persistence failed: roll back in-memory mutation
                                // so a 500 never leaves an unpersisted write applied.
                                if let Some(bytes) = pre_snapshot.as_deref() {
                                    if let Ok(restored) = bincode::deserialize(bytes) {
                                        glue.storage = restored;
                                    }
                                } else {
                                    // Fallback: reload from KV (best effort).
                                    let (new_glue, new_journal) = db::load_db();
                                    ctx.glue = Some(new_glue);
                                    ctx.journal = Some(new_journal);
                                }

                                let body = serde_json::json!({
                                    "error": format!("query executed but failed to persist: {}", err)
                                })
                                .to_string();
                                return Ok(Response::from_status(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                )
                                .with_content_type(mime::APPLICATION_JSON)
                                .with_body(body));
                            }
                        }
                        let journal = ctx
                            .journal
                            .as_mut()
                            .expect("journal initialized by ensure_db");
                        db::maybe_compact(journal);
                    }
                    Ok(Response::from_status(StatusCode::OK)
                        .with_content_type(mime::APPLICATION_JSON)
                        .with_body(json))
                }
                Err(err) => {
                    let body = serde_json::json!({ "error": err }).to_string();
                    Ok(Response::from_status(StatusCode::BAD_REQUEST)
                        .with_content_type(mime::APPLICATION_JSON)
                        .with_body(body))
                }
            }
        }

        // Dynamic schema introspection
        (Method::GET, "/schema") => {
            ensure_db(ctx);
            let glue = ctx.glue.as_mut().expect("db initialized by ensure_db");
            match db::get_schema(glue) {
                Ok(json) => Ok(Response::from_status(StatusCode::OK)
                    .with_content_type(mime::APPLICATION_JSON)
                    .with_body(json)),
                Err(err) => {
                    let body = serde_json::json!({ "error": err }).to_string();
                    Ok(Response::from_status(StatusCode::INTERNAL_SERVER_ERROR)
                        .with_content_type(mime::APPLICATION_JSON)
                        .with_body(body))
                }
            }
        }

        // 404 for everything else
        _ => Ok(Response::from_status(StatusCode::NOT_FOUND).with_body_text_plain("Not found\n")),
    }
}
