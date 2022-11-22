/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use lazy_static::lazy_static;
use neon::prelude::*;

use cozo::*;

#[derive(Default)]
struct Handles {
    current: AtomicU32,
    dbs: Mutex<BTreeMap<u32, DbInstance>>,
}

lazy_static! {
    static ref HANDLES: Handles = Handles::default();
}

fn open_db(mut cx: FunctionContext) -> JsResult<JsNumber> {
    let kind = cx.argument::<JsString>(0)?.value(&mut cx);
    let path = cx.argument::<JsString>(1)?.value(&mut cx);
    match DbInstance::new(&kind, &path, Default::default()) {
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
    let params = cx.argument::<JsString>(2)?.value(&mut cx);

    let callback = cx.argument::<JsFunction>(3)?.root(&mut cx);

    let channel = cx.channel();

    std::thread::spawn(move || {
        let result = db.run_script_str(&query, &params);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            let json_str = cx.string(result);
            callback.call(&mut cx, this, vec![json_str.upcast()])?;

            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn backup_db(mut cx: FunctionContext) -> JsResult<JsUndefined> {
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

    let path = cx.argument::<JsString>(1)?.value(&mut cx);

    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);

    let channel = cx.channel();

    std::thread::spawn(move || {
        let result = db.backup_db_str(&path);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            let json_str = cx.string(result);
            callback.call(&mut cx, this, vec![json_str.upcast()])?;

            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn restore_db(mut cx: FunctionContext) -> JsResult<JsUndefined> {
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

    let path = cx.argument::<JsString>(1)?.value(&mut cx);

    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);

    let channel = cx.channel();

    std::thread::spawn(move || {
        let result = db.restore_backup_str(&path);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            let json_str = cx.string(result);
            callback.call(&mut cx, this, vec![json_str.upcast()])?;

            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn export_relations(mut cx: FunctionContext) -> JsResult<JsUndefined> {
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

    let rels = cx.argument::<JsString>(1)?.value(&mut cx);

    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);

    let channel = cx.channel();

    std::thread::spawn(move || {
        let result = db.export_relations_str(&rels);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            let json_str = cx.string(result);
            callback.call(&mut cx, this, vec![json_str.upcast()])?;

            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn import_relation(mut cx: FunctionContext) -> JsResult<JsUndefined> {
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

    let data = cx.argument::<JsString>(1)?.value(&mut cx);

    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);

    let channel = cx.channel();

    std::thread::spawn(move || {
        let result = db.import_relation_str(&data);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            let json_str = cx.string(result);
            callback.call(&mut cx, this, vec![json_str.upcast()])?;

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
    cx.export_function("backup_db", backup_db)?;
    cx.export_function("restore_db", restore_db)?;
    cx.export_function("export_relations", export_relations)?;
    cx.export_function("import_relation", import_relation)?;
    Ok(())
}
