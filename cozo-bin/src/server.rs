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
use std::sync::{mpsc, Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive};
use axum::response::{Html, Sse};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use clap::Args;
use futures::stream::{self, Stream};
use itertools::Itertools;
use log::{info, warn};
use serde_json::json;
use tokio::task::spawn_blocking;

use cozo::{DataValue, DbInstance, NamedRows};

#[derive(Args, Debug)]
pub(crate) struct ServerArgs {
    /// Database engine, can be `mem`, `sqlite`, `rocksdb` and others.
    #[clap(short, long, default_value_t = String::from("mem"))]
    engine: String,

    /// Path to the directory to store the database
    #[clap(short, long, default_value_t = String::from("cozo.db"))]
    path: String,

    // Restore from the specified backup before starting the server
    // #[clap(long)]
    // restore: Option<String>,
    /// Extra config in JSON format
    #[clap(short, long, default_value_t = String::from("{}"))]
    config: String,

    // When on, start REPL instead of starting a webserver
    // #[clap(short, long)]
    // repl: bool,
    /// Address to bind the service to
    #[clap(short, long, default_value_t = String::from("127.0.0.1"))]
    bind: String,

    /// Port to use
    #[clap(short = 'P', long, default_value_t = 9070)]
    port: u16,
}

type RuleCallbackStore = BTreeMap<usize, crossbeam::channel::Sender<miette::Result<NamedRows>>>;
type DbState = (DbInstance, Arc<Mutex<RuleCallbackStore>>);

pub(crate) async fn server_main(args: ServerArgs) {
    let db = DbInstance::new(&args.engine, args.path, &args.config).unwrap();
    let rule_channels: Arc<Mutex<RuleCallbackStore>> = Default::default();
    let state = (db, rule_channels);
    let app = Router::new()
        .fallback(not_found)
        .route("/", get(root))
        .route("/text-query", post(text_query))
        .route("/export/:relations", get(export_relations))
        .route("/import", put(import_relations))
        .route("/backup", post(backup))
        .route("/import-from-backup", post(import_from_backup))
        .route("/changes/:relation", get(observe_changes))
        .route("/rules/:name", get(register_rule)) // sse + post
        .route("/rules/:name/:id", post(rule_result))
        .with_state(state);
    let addr = if Ipv6Addr::from_str(&args.bind).is_ok() {
        SocketAddr::from_str(&format!("[{}]:{}", args.bind, args.port)).unwrap()
    } else {
        SocketAddr::from_str(&format!("{}:{}", args.bind, args.port)).unwrap()
    };

    if args.bind != "127.0.0.1" {
        warn!("{}", include_str!("./security.txt"));
    }

    info!(
        "Starting Cozo ({}-backed) API at http://{}",
        args.engine, addr
    );

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

#[derive(serde_derive::Deserialize)]
struct QueryPayload {
    script: String,
    params: BTreeMap<String, serde_json::Value>,
}

async fn text_query(
    State((db, _)): State<DbState>,
    Json(payload): Json<QueryPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    let params = payload
        .params
        .into_iter()
        .map(|(k, v)| (k, DataValue::from(v)))
        .collect();
    let result = spawn_blocking(move || db.run_script_fold_err(&payload.script, params)).await;
    match result {
        Ok(res) => wrap_json(res),
        Err(err) => internal_error(err),
    }
}

async fn export_relations(
    State((db, _)): State<DbState>,
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
    let result = spawn_blocking(move || db.export_relations(relations.iter())).await;
    match result {
        Ok(Ok(s)) => {
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
    State((db, _)): State<DbState>,
    Json(payload): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    let payload = match payload.as_object() {
        None => {
            return (
                StatusCode::BAD_REQUEST,
                json!({"ok": false, "message": "payload must be a JSON object"}).into(),
            )
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
                        )
                    }
                };
                ret.insert(k.to_string(), nr);
            }
            ret
        }
    };

    let result = spawn_blocking(move || db.import_relations(payload)).await;
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
    State((db, _)): State<DbState>,
    Json(payload): Json<BackupPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    let result = spawn_blocking(move || db.backup_db(payload.path)).await;

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
    State((db, _)): State<DbState>,
    Json(payload): Json<BackupImportPayload>,
) -> (StatusCode, Json<serde_json::Value>) {
    let result =
        spawn_blocking(move || db.import_from_backup(&payload.path, &payload.relations)).await;

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

async fn rule_result(
    State((store, _)): State<DbState>,
    Path(name): Path<String>,
    Path(id): Path<usize>,
) -> (StatusCode, Json<serde_json::Value>) {
    todo!()
}

async fn register_rule(
    State((db, cbs)): State<DbState>,
    Path(name): Path<String>,
    Query(rule_opts): Query<RuleRegisterOptions>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (id, recv) = db.register_callback(&name, None);
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
        info!("starting callback SSE {}: {}", name, id);
        let _guard = Guard {id, db, relation: name};
        while let Some((op, new, old)) = receiver.recv().await {
            let item = json!({"op": op.to_string(), "new_rows": new.into_json(), "old_rows": old.into_json()});
            yield Ok(Event::default().json_data(item).unwrap());
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn observe_changes(
    State((db, _)): State<DbState>,
    Path(relation): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (id, recv) = db.register_callback(&relation, None);
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
        let _guard = Guard {id, db, relation};
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
