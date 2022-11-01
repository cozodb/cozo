/*
 * Copyright 2022, The Cozo Project Authors. Licensed under MIT/Apache-2.0/BSD-3-Clause.
 */
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use lazy_static::lazy_static;
use neon::prelude::*;

use cozo::Db;

#[derive(Default)]
struct Handles {
    current: AtomicU32,
    dbs: Mutex<BTreeMap<u32, Db>>,
}

lazy_static! {
    static ref HANDLES: Handles = Handles::default();
}

fn open_db(mut cx: FunctionContext) -> JsResult<JsNumber> {
    let path = cx.argument::<JsString>(0)?.value(&mut cx);
    match Db::new(path) {
        Ok(db) => {
            let id = HANDLES.current.fetch_add(1, Ordering::AcqRel);
            let mut dbs = HANDLES.dbs.lock().unwrap();
            dbs.insert(id, db);
            Ok(cx.number(id))
        }
        Err(err) => {
            let s = cx.string(format!("{:?}", err));
            cx.throw(s)
        }
    }
}

fn close_db(mut cx: FunctionContext) -> JsResult<JsBoolean> {
    let id = cx.argument::<JsNumber>(0)?.value(&mut cx) as u32;
    let db = {
        let mut dbs = HANDLES.dbs.lock().unwrap();
        dbs.remove(&id)
    };
    Ok(cx.boolean(db.is_some()))
}

fn query_db(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let id = cx.argument::<JsNumber>(0)?.value(&mut cx) as u32;
    let db = {
        let db_ref = {
            let dbs = HANDLES.dbs.lock().unwrap();
            dbs.get(&id).cloned()
        };
        match db_ref {
            None => {
                let s = cx.string("database already closed");
                cx.throw(s)?
            }
            Some(db) => db,
        }
    };

    let query = cx.argument::<JsString>(1)?.value(&mut cx);
    let params_str = cx.argument::<JsString>(2)?.value(&mut cx);

    let params_map: serde_json::Value = match serde_json::from_str(&params_str) {
        Ok(m) => m,
        Err(_) => {
            let s = cx.string("the given params argument is not valid JSON");
            cx.throw(s)?
        }
    };
    let params_arg: BTreeMap<_, _> = match params_map {
        serde_json::Value::Object(m) => m.into_iter().collect(),
        _ => {
            let s = cx.string("the given params argument is not a JSON map");
            cx.throw(s)?
        }
    };

    let callback = cx.argument::<JsFunction>(3)?.root(&mut cx);

    let channel = cx.channel();

    std::thread::spawn(move || {
        let result = db.run_script(&query, &params_arg);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            let args = match result {
                Ok(json) => {
                    let json_str = cx.string(json.to_string());
                    vec![cx.null().upcast::<JsValue>(), json_str.upcast()]
                }
                Err(err) => {
                    let err = cx.string(format!("{:?}", err));
                    vec![err.upcast::<JsValue>()]
                }
            };

            callback.call(&mut cx, this, args)?;

            Ok(())
        });
    });

    Ok(cx.undefined())
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("open_db", open_db)?;
    cx.export_function("close_db", close_db)?;
    cx.export_function("query_db", query_db)?;
    Ok(())
}
