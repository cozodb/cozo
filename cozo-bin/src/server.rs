/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::{Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering};

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderName, Method, Request, Response, StatusCode};
use axum::response::sse::{Event, KeepAlive};
use axum::response::{Html, Sse};
use axum::routing::{get, post, put};
use axum::{Extension, Json, Router};
use clap::Args;
use futures::future::BoxFuture;
use futures::stream::Stream;
use itertools::Itertools;
use log::{error, info, warn};
use miette::miette;
// use miette::miette;
use rand::Rng;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::task::spawn_blocking;
use tower_http::auth::{AsyncAuthorizeRequest, AsyncRequireAuthorizationLayer};
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};

use cozo::{DataValue, DbInstance, format_error_as_json, MultiTransaction, NamedRows, ScriptMutability, SimpleFixedRule};

#[derive(Args, Debug)]
pub(crate) struct ServerArgs {
    /// Database engine, can be `mem`, `sqlite`, `rocksdb` and others.
    #[clap(short, long, default_value_t = String::from("mem"))]
    engine: String,

    /// Path to the directory to store the database
    #[clap(short, long, default_value_t = String::from("cozo.db"))]
    path: String,

    /// Restore from the specified backup before starting the server
    #[clap(long)]
    restore: Option<String>,

    /// Extra config in JSON format
    #[clap(short, long, default_value_t = String::from("{}"))]
    config: String,

    /// Address to bind the service to
    #[clap(short, long, default_value_t = String::from("127.0.0.1"))]
    bind: String,

    /// Port to use
    #[clap(short = 'P', long, default_value_t = 9070)]
    port: u16,

    /// When set, the content of the named table will be used as a token table
    #[clap(long)]
    token_table: Option<String>,
}

#[derive(Clone)]
struct DbState {
    db: DbInstance,
    rule_senders: Arc<Mutex<BTreeMap<u32, crossbeam::channel::Sender<miette::Result<NamedRows>>>>>,
    rule_counter: Arc<AtomicU32>,
    tx_counter: Arc<AtomicU32>,
    txs: Arc<Mutex<BTreeMap<u32, Arc<MultiTransaction>>>>,
}

#[derive(Clone)]
struct MyAuth {
    skip_auth: bool,
    auth_guard: String,
    token_table: Option<Arc<(String, DbInstance)>>,
}

impl AsyncAuthorizeRequest<Body> for MyAuth
{
    type RequestBody = Body;
    type ResponseBody = Body;
    type Future = BoxFuture<'static, Result<Request<Body>, Response<Self::ResponseBody>>>;

    fn authorize(&mut self, mut request: Request<Body>) -> Self::Future {
        let skip_auth = self.skip_auth;
        let auth_guard = self.auth_guard.clone();
        let token_table = self.token_table.clone();
        Box::pin(async move {
            if skip_auth {
                request.extensions_mut().insert(ScriptMutability::Mutable);
                return Ok(request);
            }

            let mutability = match request.headers().get("x-cozo-auth") {
                None => match request.uri().query() {
                    Some(q_str) => {
                        let mut bingo = false;
                        for pair in q_str.split('&') {
                            if let Some((k, v)) = pair.split_once('=') {
                                if k == "auth" {
                                    if v == auth_guard.as_str() {
                                        bingo = true
                                    }
                                    break;
                                }
                            }
                        }
                        if bingo {
                            Some(ScriptMutability::Mutable)
                        } else {
                            None
                        }
                    }
                    None => match token_table {
                        None => None,
                        Some(tt) => {
                            let (name, db) = tt.as_ref();
                            if let Some(auth_header) = request.headers().get("Authorization") {
                                if let Ok(auth_str) = auth_header.to_str() {
                                    if let Some(token) = auth_str.strip_prefix("Bearer ") {
                                        match db.run_script(
                                            &format!("?[mutable] := *{name} {{ token: $token, mutable }}"),
                                            BTreeMap::from([(String::from("token"), DataValue::from(token))]),
                                            ScriptMutability::Immutable,
                                        ) {
                                            Ok(rows) => match rows.rows.first() {
                                                None => None,
                                                Some(val) => {
                                                    if val[0].get_bool() == Some(true) {
                                                        Some(ScriptMutability::Mutable)
                                                    } else {
                                                        Some(ScriptMutability::Immutable)
                                                    }
                                                }
                                            },
                                            Err(err) => {
                                                eprintln!("Error: {}", err);
                                                None
                                            }
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        }
                    },
                },
                Some(data) => match data.to_str() {
                    Ok(s) => {
                        if s == auth_guard.as_str() {
                            Some(ScriptMutability::Mutable)
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                },
            };
            if let Some(mutability) = mutability {
                request.extensions_mut().insert(mutability);
                Ok(request)
            } else {
                let unauthorized_response = Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::empty())
                    .unwrap();

                Err(unauthorized_response)
            }
        })
    }
}

#[test]
fn x() {}

pub(crate) async fn server_main(args: ServerArgs) {
    let db = DbInstance::new(&args.engine, &args.path, &args.config).unwrap();
    if let Some(p) = &args.restore {
        if let Err(err) = db.restore_backup(p) {
            error!("{}", err);
            error!("Restore from backup failed, terminate");
            panic!()
        }
    }

    let skip_auth = args.bind == "127.0.0.1";

    let conf_path = if skip_auth {
        "".to_string()
    } else {
        format!("{}.{}.cozo_auth", args.path, args.engine)
    };
    let auth_guard = if skip_auth {
        "".to_string()
    } else {
        match tokio::fs::read_to_string(&conf_path).await {
            Ok(s) => s.trim().to_string(),
            Err(_) => {
                let s = rand::thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(64)
                    .map(char::from)
                    .collect();
                tokio::fs::write(&conf_path, &s).await.unwrap();
                s
            }
        }
    };

    let auth_obj = MyAuth {
        skip_auth,
        auth_guard,
        token_table: args.token_table.map(|t| Arc::new((t, db.clone()))),
    };

    let state = DbState {
        db,
        rule_senders: Default::default(),
        rule_counter: Default::default(),
        tx_counter: Default::default(),
        txs: Default::default(),
    };
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
        .allow_origin(Any)
        .allow_headers([header::CONTENT_TYPE, HeaderName::from_static("x-cozo-auth")]);

    let app = Router::new()
        .route("/text-query", post(text_query))
        .route("/export/:relations", get(export_relations))
        .route("/import", put(import_relations))
        .route("/backup", post(backup))
        .route("/import-from-backup", post(import_from_backup))
        .route("/changes/:relation", get(observe_changes))
        .route("/rules/:name", get(register_rule))
        .route(
            "/rule-result/:id",
            post(post_rule_result).delete(post_rule_err),
        ) // +keep alive
        .route("/transact", post(start_transact))
        .route("/transact/:id", post(transact_query).put(finish_query))
        .with_state(state)
        .layer(AsyncRequireAuthorizationLayer::new(auth_obj))
        .fallback(not_found)
        .route("/", get(root))
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(DefaultBodyLimit::disable());

    let addr = if Ipv6Addr::from_str(&args.bind).is_ok() {
        SocketAddr::from_str(&format!("[{}]:{}", args.bind, args.port)).unwrap()
    } else {
        SocketAddr::from_str(&format!("{}:{}", args.bind, args.port)).unwrap()
    };

    if args.bind != "127.0.0.1" {
        warn!("{}", include_str!("./security.txt"));
        info!("The auth token is in the file: {conf_path}");
    }

    info!(
        "Starting Cozo ({}-backed) API at http://{}",
        args.engine, addr
    );

    let listener = TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app.into_make_service()).await.unwrap();
}

#[derive(serde_derive::Deserialize)]
struct StartTransactPayload {
    write: bool,
}

async fn start_transact(
    State(st): State<DbState>,
    Query(payload): Query<StartTransactPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    let tx = st.db.multi_transaction(payload.write);
    let id = st.tx_counter.fetch_add(1, Ordering::SeqCst);
    st.txs.lock().unwrap().insert(id, Arc::new(tx));
    (StatusCode::OK, json!({"ok": true, "id": id}).into())
}

async fn transact_query(
    State(st): State<DbState>,
    Path(id): Path<u32>,
    Json(payload): Json<QueryPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    let tx = match st.txs.lock().unwrap().get(&id) {
        None => return (StatusCode::NOT_FOUND, json!({"ok": false}).into()),
        Some(tx) => tx.clone(),
    };
    let src = payload.script.clone();
    let result = spawn_blocking(move || {
        let params = payload
            .params
            .into_iter()
            .map(|(k, v)| (k, DataValue::from(v)))
            .collect();
        let query = payload.script;
        tx.run_script(&query, params)
    })
        .await;
    match result {
        Ok(Ok(res)) => (StatusCode::OK, res.into_json().into()),
        Ok(Err(err)) => (
            StatusCode::BAD_REQUEST,
            format_error_as_json(err, Some(&src)).into(),
        ),
        Err(err) => internal_error(err),
    }
}

#[derive(serde_derive::Deserialize)]
struct FinishTransactPayload {
    abort: bool,
}

async fn finish_query(
    State(st): State<DbState>,
    Path(id): Path<u32>,
    Json(payload): Json<FinishTransactPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    let tx = match st.txs.lock().unwrap().remove(&id) {
        None => return (StatusCode::NOT_FOUND, json!({"ok": false}).into()),
        Some(tx) => tx,
    };
    let res = if payload.abort {
        tx.abort()
    } else {
        tx.commit()
    };
    match res {
        Ok(_) => (StatusCode::OK, json!({"ok": true}).into()),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            json!({"ok": false, "message": err.to_string()}).into(),
        ),
    }
}

#[derive(serde_derive::Deserialize)]
struct QueryPayload {
    script: String,
    params: BTreeMap<String, serde_json::Value>,
    immutable: Option<bool>,
}

async fn text_query(
    Extension(mutability): Extension<ScriptMutability>,
    State(st): State<DbState>,
    Json(payload): Json<QueryPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    let params = payload
        .params
        .into_iter()
        .map(|(k, v)| (k, DataValue::from(v)))
        .collect();
    let immutable = match mutability {
        ScriptMutability::Mutable => payload.immutable.unwrap_or(false),
        ScriptMutability::Immutable => true,
    };
    let result = spawn_blocking(move || {
        st.db.run_script_fold_err(
            &payload.script,
            params,
            if immutable {
                ScriptMutability::Immutable
            } else {
                ScriptMutability::Mutable
            },
        )
    })
        .await;
    match result {
        Ok(res) => wrap_json(res),
        Err(err) => internal_error(err),
    }
}

async fn export_relations(
    State(st): State<DbState>,
    Path(relations): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    let relations = relations
        .split(',')
        .filter_map(|t| {
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        })
        .collect_vec();
    let result = spawn_blocking(move || st.db.export_relations(relations.iter())).await;
    match result {
        Ok(Ok(s)) => {
            let s: serde_json::Map<_, _> = s.into_iter().map(|(k, v)| (k, v.into_json())).collect();
            let ret = json!({"ok": true, "data": s});
            (StatusCode::OK, ret.into())
        }
        Ok(Err(err)) => {
            let ret = json!({"ok": false, "message": err.to_string()});
            (StatusCode::BAD_REQUEST, ret.into())
        }
        Err(err) => internal_error(err),
    }
}

async fn import_relations(
    State(st): State<DbState>,
    Json(payload): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let payload = match payload.as_object() {
        None => {
            return (
                StatusCode::BAD_REQUEST,
                json!({"ok": false, "message": "payload must be a JSON object"}).into(),
            );
        }
        Some(pl) => {
            let mut ret = BTreeMap::new();
            for (k, v) in pl {
                let nr = match NamedRows::from_json(v) {
                    Ok(p) => p,
                    Err(err) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            json!({"ok": false, "message": err.to_string()}).into(),
                        );
                    }
                };
                ret.insert(k.to_string(), nr);
            }
            ret
        }
    };

    let result = spawn_blocking(move || st.db.import_relations(payload)).await;
    match result {
        Ok(Ok(_)) => (StatusCode::OK, json!({"ok": true}).into()),
        Ok(Err(err)) => {
            let ret = json!({"ok": false, "message": err.to_string()});
            (StatusCode::BAD_REQUEST, ret.into())
        }
        Err(err) => internal_error(err),
    }
}

#[derive(serde_derive::Deserialize)]
struct BackupPayload {
    path: String,
}

async fn backup(
    State(st): State<DbState>,
    Json(payload): Json<BackupPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    let result = spawn_blocking(move || st.db.backup_db(payload.path)).await;

    match result {
        Ok(Ok(())) => {
            let ret = json!({"ok": true});
            (StatusCode::OK, ret.into())
        }
        Ok(Err(err)) => {
            let ret = json!({"ok": false, "message": err.to_string()});
            (StatusCode::BAD_REQUEST, ret.into())
        }
        Err(err) => internal_error(err),
    }
}

#[derive(serde_derive::Deserialize)]
struct BackupImportPayload {
    path: String,
    relations: Vec<String>,
}

async fn import_from_backup(
    State(st): State<DbState>,
    Json(payload): Json<BackupImportPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    let result =
        spawn_blocking(move || st.db.import_from_backup(&payload.path, &payload.relations)).await;

    match result {
        Ok(Ok(())) => {
            let ret = json!({"ok": true});
            (StatusCode::OK, ret.into())
        }
        Ok(Err(err)) => {
            let ret = json!({"ok": false, "message": err.to_string()});
            (StatusCode::BAD_REQUEST, ret.into())
        }
        Err(err) => internal_error(err),
    }
}

#[derive(serde_derive::Deserialize)]
struct RuleRegisterOptions {
    arity: usize,
}

async fn post_rule_result(
    State(st): State<DbState>,
    Path(id): Path<u32>,
    Json(res): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let res = match NamedRows::from_json(&res) {
        Ok(res) => res,
        Err(err) => {
            if let Some(ch) = st.rule_senders.lock().unwrap().remove(&id) {
                let _ = ch.send(Err(miette!("downstream posted malformed result")));
            }
            return (
                StatusCode::BAD_REQUEST,
                json!({"ok": false, "message": err.to_string()}).into(),
            );
        }
    };
    if let Some(ch) = st.rule_senders.lock().unwrap().remove(&id) {
        match ch.send(Ok(res)) {
            Ok(_) => (StatusCode::OK, json!({"ok": true}).into()),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"ok": false, "message": err.to_string()}).into(),
            ),
        }
    } else {
        (StatusCode::NOT_FOUND, json!({"ok": false}).into())
    }
}

async fn post_rule_err(
    State(st): State<DbState>,
    Path(id): Path<u32>,
) -> (StatusCode, Json<serde_json::Value>) {
    if let Some(ch) = st.rule_senders.lock().unwrap().remove(&id) {
        match ch.send(Err(miette!("downstream cancelled computation"))) {
            Ok(_) => (StatusCode::OK, json!({"ok": true}).into()),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({"ok": false, "message": err.to_string()}).into(),
            ),
        }
    } else {
        (StatusCode::NOT_FOUND, json!({"ok": false}).into())
    }
}

async fn register_rule(
    State(st): State<DbState>,
    Path(name): Path<String>,
    Query(rule_opts): Query<RuleRegisterOptions>,
) -> Sse<impl Stream<Item=Result<Event, Infallible>>> {
    let (rule, task_receiver) = SimpleFixedRule::rule_with_channel(rule_opts.arity);
    let (down_sender, mut down_receiver) = tokio::sync::mpsc::channel(1);
    let mut errored = None;

    if let Err(err) = st.db.register_fixed_rule(name.clone(), rule) {
        errored = Some(err);
    } else {
        let rule_senders = st.rule_senders.clone();
        let rule_counter = st.rule_counter.clone();

        rayon::spawn(move || {
            for (inputs, options, sender) in task_receiver {
                let id = rule_counter.fetch_add(1, Ordering::AcqRel);
                let inputs: serde_json::Value =
                    inputs.into_iter().map(|ip| ip.into_json()).collect();
                let options: serde_json::Value = options
                    .into_iter()
                    .map(|(k, v)| (k, serde_json::Value::from(v)))
                    .collect();
                if down_sender.blocking_send((id, inputs, options)).is_err() {
                    let _ = sender.send(Err(miette!("cannot send request to downstream")));
                } else {
                    rule_senders.lock().unwrap().insert(id, sender);
                }
            }
        });
    }

    struct Guard {
        name: String,
        db: DbInstance,
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            info!("dropping rules SSE {}", self.name);
            let _ = self.db.unregister_fixed_rule(&self.name);
        }
    }

    let stream = async_stream::stream! {
        if let Some(err) = errored {
            let item = json!({"type": "register-error", "error": err.to_string()});
            yield Ok(Event::default().json_data(item).unwrap());
        } else {
            info!("starting rule SSE {}", name);
            let _guard = Guard {db: st.db, name};
            while let Some((id, inputs, options)) = down_receiver.recv().await {
                let item = json!({"type": "request", "id": id, "inputs": inputs, "options": options});
                yield Ok(Event::default().json_data(item).unwrap());
            }
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn observe_changes(
    State(st): State<DbState>,
    Path(relation): Path<String>,
) -> Sse<impl Stream<Item=Result<Event, Infallible>>> {
    let (id, recv) = st.db.register_callback(&relation, None);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    struct Guard {
        id: u32,
        db: DbInstance,
        relation: String,
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            info!("dropping changes SSE {}: {}", self.relation, self.id);
            self.db.unregister_callback(self.id);
        }
    }

    spawn_blocking(move || {
        for data in recv {
            sender.blocking_send(data).unwrap();
        }
    });
    let stream = async_stream::stream! {
        info!("starting changes SSE {}: {}", relation, id);
        let _guard = Guard {id, db: st.db, relation};
        while let Some((op, new, old)) = receiver.recv().await {
            let item = json!({"op": op.to_string(), "new_rows": new.into_json(), "old_rows": old.into_json()});
            yield Ok(Event::default().json_data(item).unwrap());
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn root() -> Html<&'static str> {
    Html(include_str!("./index.html"))
}

fn internal_error<E>(err: E) -> (StatusCode, Json<serde_json::Value>)
    where
        E: std::error::Error,
{
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({"ok": false, "message": err.to_string()}).into(),
    )
}

fn wrap_json(json: serde_json::Value) -> (StatusCode, Json<serde_json::Value>) {
    let code = if let Some(serde_json::Value::Bool(true)) = json.get("ok") {
        StatusCode::OK
    } else {
        StatusCode::BAD_REQUEST
    };
    (code, json.into())
}

pub async fn not_found(uri: axum::http::Uri) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_FOUND,
        json!({"ok": false, "message": format!("No route {}", uri)}).into(),
    )
}
