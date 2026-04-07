mod db;

use fastly::http::{Method, StatusCode};
use fastly::{mime, Error, Request, Response};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// Trivial single-threaded block_on for WASM — no runtime needed.
fn block_on<F: Future>(mut fut: F) -> F::Output {
    // Safety: we never move `fut` after pinning, and WASM is single-threaded.
    let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    #[allow(clippy::never_loop)]
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(val) => return val,
            Poll::Pending => panic!("unexpected Pending in single-threaded WASM"),
        }
    }
}

fn noop_waker() -> Waker {
    const VTABLE: RawWakerVTable =
        RawWakerVTable::new(|p| RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {});
    // Safety: the vtable functions are all no-ops, which is valid for a single-threaded executor.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

#[fastly::main]
fn main(req: Request) -> Result<Response, Error> {
    block_on(handle(req))
}

async fn handle(mut req: Request) -> Result<Response, Error> {
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
            let (mut glue, _) = db::load_db().await;

            match db::execute_query(&mut glue, &sql).await {
                Ok((json, has_mutations)) => {
                    if has_mutations {
                        db::append_to_log(&sql);
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
            let (mut glue, _) = db::load_db().await;
            match db::get_schema(&mut glue).await {
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
