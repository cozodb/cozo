/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam::channel::Sender;
use lazy_static::lazy_static;
use miette::{miette, Result};
use neon::prelude::*;
use neon::types::buffer::TypedArray;
use serde_json::json;

use cozo::*;

fn rows2js<'a>(cx: &mut impl Context<'a>, rows: &[Vec<DataValue>]) -> JsResult<'a, JsArray> {
    let coll = cx.empty_array();
    for (j, row) in rows.iter().enumerate() {
        let cur = cx.empty_array();
        for (i, el) in row.iter().enumerate() {
            let el = value2js(cx, el)?;
            cur.set(cx, i as u32, el)?;
        }
        coll.set(cx, j as u32, cur)?;
    }
    Ok(coll)
}

fn named_rows2js<'a>(cx: &mut impl Context<'a>, nr: &NamedRows) -> JsResult<'a, JsObject> {
    let ret = cx.empty_object();
    if let Some(rows) = &nr.next {
        let converted = named_rows2js(cx, rows)?;
        ret.set(cx, "next", converted)?;
    };
    let headers = cx.empty_array();
    for (i, header) in nr.headers.iter().enumerate() {
        let converted = cx.string(header);
        headers.set(cx, i as u32, converted)?;
    }
    ret.set(cx, "headers", headers)?;
    let rows = rows2js(cx, &nr.rows)?;
    ret.set(cx, "rows", rows)?;
    Ok(ret)
}

fn js2value<'a>(
    cx: &mut impl Context<'a>,
    val: Handle<'a, JsValue>,
    coll: &mut DataValue,
) -> JsResult<'a, JsUndefined> {
    if val.downcast::<JsNull, _>(cx).is_ok() {
        *coll = DataValue::Null;
    } else if let Ok(n) = val.downcast::<JsNumber, _>(cx) {
        let n = n.value(cx);
        *coll = DataValue::from(n);
    } else if let Ok(b) = val.downcast::<JsBoolean, _>(cx) {
        let b = b.value(cx);
        *coll = DataValue::from(b);
    } else if val.downcast::<JsUndefined, _>(cx).is_ok() {
        *coll = DataValue::Null;
    } else if let Ok(s) = val.downcast::<JsString, _>(cx) {
        let s = s.value(cx);
        *coll = DataValue::Str(s.into());
    } else if let Ok(l) = val.downcast::<JsArray, _>(cx) {
        let n = l.len(cx);
        let mut ret = Vec::with_capacity(n as usize);
        for i in 0..n {
            let v: Handle<JsValue> = l.get(cx, i)?;
            let mut target = DataValue::Bot;
            js2value(cx, v, &mut target)?;
            ret.push(target);
        }
        *coll = DataValue::List(ret);
    } else if let Ok(b) = val.downcast::<JsBuffer, _>(cx) {
        let d = b.as_slice(cx);
        *coll = DataValue::Bytes(d.to_vec());
    } else if let Ok(obj) = val.downcast::<JsObject, _>(cx) {
        let names = obj.get_own_property_names(cx)?;
        let mut coll_inner = serde_json::Map::default();
        for i in 0..names.len(cx) {
            let name = names.get::<JsString, _, _>(cx, i)?.value(cx);
            let v = obj.get::<JsValue, _, _>(cx, &*name)?;
            let mut target = DataValue::Bot;
            js2value(cx, v, &mut target)?;
            coll_inner.insert(name, serde_json::Value::from(target));
        }
        *coll = DataValue::Json(JsonData(json!(coll_inner)));
    } else {
        let err = cx.string("Javascript value cannot be converted.");
        return cx.throw(err);
    }
    Ok(cx.undefined())
}

fn json2js<'a>(cx: &mut impl Context<'a>, val: &serde_json::Value) -> JsResult<'a, JsValue> {
    Ok(match val {
        serde_json::Value::Null => cx.null().as_value(cx),
        serde_json::Value::Bool(b) => cx.boolean(*b).as_value(cx),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                cx.number(i as f64).as_value(cx)
            } else if let Some(f) = n.as_f64() {
                cx.number(f).as_value(cx)
            } else {
                cx.undefined().as_value(cx)
            }
        }
        serde_json::Value::String(s) => cx.string(s).as_value(cx),
        serde_json::Value::Array(l) => {
            let target_l = cx.empty_array();
            for (i, el) in l.iter().enumerate() {
                let el = json2js(cx, el)?;
                target_l.set(cx, i as u32, el)?;
            }
            target_l.as_value(cx)
        }
        serde_json::Value::Object(m) => {
            let target_m = cx.empty_object();
            for (k, v) in m.iter() {
                let k = cx.string(k);
                let v = json2js(cx, v)?;
                target_m.set(cx, k, v)?;
            }
            target_m.as_value(cx)
        }
    })
}

fn value2js<'a>(cx: &mut impl Context<'a>, val: &DataValue) -> JsResult<'a, JsValue> {
    Ok(match val {
        DataValue::Null => cx.null().as_value(cx),
        DataValue::Bool(b) => cx.boolean(*b).as_value(cx),
        DataValue::Num(n) => match n {
            Num::Int(i) => cx.number(*i as f64).as_value(cx),
            Num::Float(f) => cx.number(*f).as_value(cx),
        },
        DataValue::Str(s) => cx.string(s).as_value(cx),
        DataValue::Bytes(b) => {
            let b = b.clone();
            JsBuffer::external(cx, b).as_value(cx)
        }
        DataValue::Uuid(uuid) => cx.string(uuid.0.to_string()).as_value(cx),
        DataValue::Regex(rx) => cx.string(rx.0.to_string()).as_value(cx),
        DataValue::List(l) => {
            let target_l = cx.empty_array();
            for (i, el) in l.iter().enumerate() {
                let el = value2js(cx, el)?;
                target_l.set(cx, i as u32, el)?;
            }
            target_l.as_value(cx)
        }
        DataValue::Set(l) => {
            let target_l = cx.empty_array();
            for (i, el) in l.iter().enumerate() {
                let el = value2js(cx, el)?;
                target_l.set(cx, i as u32, el)?;
            }
            target_l.as_value(cx)
        }
        DataValue::Validity(vld) => {
            let target_l = cx.empty_array();
            let ts = cx.number(vld.timestamp.0 .0 as f64);
            target_l.set(cx, 0, ts)?;
            let a = cx.boolean(vld.is_assert.0);
            target_l.set(cx, 1, a)?;
            target_l.as_value(cx)
        }
        DataValue::Bot => cx.undefined().as_value(cx),
        DataValue::Vec(v) => {
            let target_l = cx.empty_array();
            match v {
                Vector::F32(a) => {
                    for (i, el) in a.iter().enumerate() {
                        let el = cx.number(*el as f64);
                        target_l.set(cx, i as u32, el)?;
                    }
                }
                Vector::F64(a) => {
                    for (i, el) in a.iter().enumerate() {
                        let el = cx.number(*el);
                        target_l.set(cx, i as u32, el)?;
                    }
                }
            }
            target_l.as_value(cx)
        }
        DataValue::Json(JsonData(j)) => json2js(cx, j)?,
    })
}

fn js2params<'a>(
    cx: &mut impl Context<'a>,
    js_params: Handle<'a, JsObject>,
    collector: &mut BTreeMap<String, DataValue>,
) -> JsResult<'a, JsUndefined> {
    let keys = js_params.get_own_property_names(cx)?;
    let n_keys = keys.len(cx);
    for i in 0..n_keys {
        let key: Handle<JsString> = keys.get(cx, i)?;
        let key_str = key.value(cx);
        let val: Handle<JsValue> = js_params.get(cx, key)?;
        let mut value = DataValue::Bot;
        js2value(cx, val, &mut value)?;
        collector.insert(key_str, value);
    }
    Ok(cx.undefined())
}

fn js2rows<'a>(
    cx: &mut impl Context<'a>,
    rows: Handle<'a, JsArray>,
    collector: &mut Vec<Vec<DataValue>>,
) -> JsResult<'a, JsUndefined> {
    let n_rows = rows.len(cx);
    collector.reserve(n_rows as usize);
    for i in 0..n_rows {
        let row = rows.get::<JsArray, _, _>(cx, i)?;
        let n_cols = row.len(cx);
        let mut ret_row = Vec::with_capacity(n_cols as usize);
        for j in 0..n_cols {
            let col = row.get::<JsValue, _, _>(cx, j)?;
            let mut val = DataValue::Bot;
            js2value(cx, col, &mut val)?;
            ret_row.push(val);
        }
        collector.push(ret_row);
    }
    Ok(cx.undefined())
}

fn js2stored<'a>(
    cx: &mut impl Context<'a>,
    named_rows: Handle<'a, JsObject>,
    collector: &mut NamedRows,
) -> JsResult<'a, JsUndefined> {
    let headers_js = named_rows.get::<JsArray, _, _>(cx, "headers")?;
    let l = headers_js.len(cx);
    let mut headers = Vec::with_capacity(l as usize);
    for i in 0..l {
        let v = headers_js.get::<JsString, _, _>(cx, i)?.value(cx);
        headers.push(v);
    }
    let rows_js = named_rows.get::<JsArray, _, _>(cx, "rows")?;
    let mut rows = vec![];
    js2rows(cx, rows_js, &mut rows)?;
    collector.headers = headers;
    collector.rows = rows;
    Ok(cx.undefined())
}

fn params2js<'a>(
    cx: &mut impl Context<'a>,
    params: &BTreeMap<String, DataValue>,
) -> JsResult<'a, JsObject> {
    let obj = cx.empty_object();
    for (k, v) in params {
        let val = value2js(cx, v)?;
        obj.set(cx, k as &str, val)?;
    }
    Ok(obj)
}

#[derive(Default)]
struct Handles {
    nxt_db_id: AtomicU32,
    dbs: Mutex<BTreeMap<u32, DbInstance>>,
    cb_idx: AtomicU32,
    current_cbs: Mutex<BTreeMap<u32, Sender<Result<NamedRows>>>>,
    nxt_tx_id: AtomicU32,
    txs: Mutex<BTreeMap<u32, Arc<MultiTransaction>>>,
}

lazy_static! {
    static ref HANDLES: Handles = Handles::default();
}

fn open_db(mut cx: FunctionContext) -> JsResult<JsNumber> {
    let engine = cx.argument::<JsString>(0)?.value(&mut cx);
    let path = cx.argument::<JsString>(1)?.value(&mut cx);
    let options = cx.argument::<JsString>(2)?.value(&mut cx);
    match DbInstance::new(&engine, path, &options) {
        Ok(db) => {
            let id = HANDLES.nxt_db_id.fetch_add(1, Ordering::AcqRel);
            let mut dbs = HANDLES.dbs.lock().unwrap();
            dbs.insert(id, db);
            Ok(cx.number(id))
        }
        Err(err) => {
            let s = cx.string(format!("{err:?}"));
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

macro_rules! get_db {
    ($cx:expr) => {{
        let id = $cx.argument::<JsNumber>(0)?.value(&mut $cx) as u32;
        let db = {
            let db_ref = {
                let dbs = HANDLES.dbs.lock().unwrap();
                dbs.get(&id).cloned()
            };
            match db_ref {
                None => {
                    let s = $cx.string("database already closed");
                    $cx.throw(s)?
                }
                Some(db) => db,
            }
        };
        db
    }};
}

macro_rules! get_tx {
    ($cx:expr) => {{
        let id = $cx.argument::<JsNumber>(0)?.value(&mut $cx) as u32;
        let tx = {
            let tx_ref = {
                let txs = HANDLES.txs.lock().unwrap();
                txs.get(&id).cloned()
            };
            match tx_ref {
                None => {
                    let s = $cx.string("transaction closed");
                    $cx.throw(s)?
                }
                Some(tx) => tx,
            }
        };
        tx
    }};
}

macro_rules! remove_tx {
    ($cx:expr) => {{
        let id = $cx.argument::<JsNumber>(0)?.value(&mut $cx) as u32;
        let tx = {
            let tx_ref = {
                let mut txs = HANDLES.txs.lock().unwrap();
                txs.remove(&id)
            };
            match tx_ref {
                None => {
                    let s = $cx.string("transaction closed");
                    $cx.throw(s)?
                }
                Some(tx) => tx,
            }
        };
        tx
    }};
}

fn multi_transact(mut cx: FunctionContext) -> JsResult<JsNumber> {
    let db = get_db!(cx);
    let write = cx.argument::<JsBoolean>(1)?.value(&mut cx);
    let tx = db.multi_transaction(write);
    let id = HANDLES.nxt_tx_id.fetch_add(1, Ordering::AcqRel);
    HANDLES.txs.lock().unwrap().insert(id, Arc::new(tx));
    Ok(cx.number(id))
}

fn abort_tx(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let tx = remove_tx!(cx);
    match tx.abort() {
        Ok(_) => Ok(cx.undefined()),
        Err(err) => {
            let msg = cx.string(err.to_string());
            cx.throw(msg)
        }
    }
}

fn commit_tx(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let tx = remove_tx!(cx);
    match tx.commit() {
        Ok(_) => Ok(cx.undefined()),
        Err(err) => {
            let msg = cx.string(err.to_string());
            cx.throw(msg)
        }
    }
}

fn query_db(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let db = get_db!(cx);
    let query = cx.argument::<JsString>(1)?.value(&mut cx);
    let params_js = cx.argument::<JsObject>(2)?;
    let mut params = BTreeMap::new();
    js2params(&mut cx, params_js, &mut params)?;

    let callback = cx.argument::<JsFunction>(3)?.root(&mut cx);
    let immutable = cx.argument::<JsBoolean>(4)?.value(&mut cx);

    let channel = cx.channel();

    rayon::spawn(move || {
        let result = db.run_script(
            &query,
            params,
            if immutable {
                ScriptMutability::Immutable
            } else {
                ScriptMutability::Mutable
            },
        );
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            match result {
                Ok(nr) => {
                    let js_vals = named_rows2js(&mut cx, &nr)?.as_value(&mut cx);
                    let err = cx.undefined().as_value(&mut cx);
                    callback.call(&mut cx, this, vec![err, js_vals])?;
                }
                Err(err) => {
                    let reports = format_error_as_json(err, Some(&query)).to_string();
                    let err = cx.string(&reports).as_value(&mut cx);
                    callback.call(&mut cx, this, vec![err])?;
                }
            }
            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn query_tx(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let tx = get_tx!(cx);
    let query = cx.argument::<JsString>(1)?.value(&mut cx);
    let params_js = cx.argument::<JsObject>(2)?;
    let mut params = BTreeMap::new();
    js2params(&mut cx, params_js, &mut params)?;

    let callback = cx.argument::<JsFunction>(3)?.root(&mut cx);

    let channel = cx.channel();
    match tx
        .sender
        .send(TransactionPayload::Query((query.clone(), params)))
    {
        Ok(_) => {
            rayon::spawn(move || {
                let result = tx.receiver.recv();
                channel.send(move |mut cx| {
                    let callback = callback.into_inner(&mut cx);
                    let this = cx.undefined();
                    match result {
                        Ok(Ok(nr)) => {
                            let js_vals = named_rows2js(&mut cx, &nr)?.as_value(&mut cx);
                            let err = cx.undefined().as_value(&mut cx);
                            callback.call(&mut cx, this, vec![err, js_vals])?;
                        }
                        Ok(Err(err)) => {
                            let reports = format_error_as_json(err, Some(&query)).to_string();
                            let err = cx.string(&reports).as_value(&mut cx);
                            callback.call(&mut cx, this, vec![err])?;
                        }
                        Err(err) => {
                            let err = cx.string(err.to_string()).as_value(&mut cx);
                            callback.call(&mut cx, this, vec![err])?;
                        }
                    }
                    Ok(())
                });
            });

            Ok(cx.undefined())
        }
        Err(err) => {
            let msg = cx.string(err.to_string());
            cx.throw(msg)
        }
    }
}

fn backup_db(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let db = get_db!(cx);
    let path = cx.argument::<JsString>(1)?.value(&mut cx);
    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);
    let channel = cx.channel();

    rayon::spawn(move || {
        let result = db.backup_db(&path);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            if let Err(msg) = result {
                let reports = format_error_as_json(msg, None).to_string();
                let err = cx.string(&reports).as_value(&mut cx);
                callback.call(&mut cx, this, vec![err])?;
            } else {
                callback.call(&mut cx, this, vec![])?;
            }

            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn restore_db(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let db = get_db!(cx);
    let path = cx.argument::<JsString>(1)?.value(&mut cx);
    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);
    let channel = cx.channel();

    rayon::spawn(move || {
        let result = db.restore_backup(&path);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            if let Err(msg) = result {
                let reports = format_error_as_json(msg, None).to_string();
                let err = cx.string(&reports).as_value(&mut cx);
                callback.call(&mut cx, this, vec![err])?;
            } else {
                callback.call(&mut cx, this, vec![])?;
            }
            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn export_relations(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let db = get_db!(cx);
    let rels = cx.argument::<JsArray>(1)?;
    let mut relations = vec![];
    for i in 0..rels.len(&mut cx) {
        let r = rels.get::<JsString, _, _>(&mut cx, i)?.value(&mut cx);
        relations.push(r);
    }
    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);
    let channel = cx.channel();

    rayon::spawn(move || {
        let result = db.export_relations(relations.iter());
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            match result {
                Ok(ret) => {
                    let u = cx.undefined().as_value(&mut cx);
                    let data = cx.empty_object();
                    for (k, v) in ret {
                        let nv = named_rows2js(&mut cx, &v)?;
                        data.set(&mut cx, &k as &str, nv)?;
                    }
                    let data = data.as_value(&mut cx);
                    callback.call(&mut cx, this, vec![u, data])?;
                }
                Err(msg) => {
                    let reports = format_error_as_json(msg, None).to_string();
                    let err = cx.string(&reports).as_value(&mut cx);
                    callback.call(&mut cx, this, vec![err])?;
                }
            }
            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn import_relations(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let db = get_db!(cx);
    let data = cx.argument::<JsObject>(1)?;
    let callback = cx.argument::<JsFunction>(2)?.root(&mut cx);
    let channel = cx.channel();
    let mut rels = BTreeMap::new();
    let names = data.get_own_property_names(&mut cx)?;
    for name in names.to_vec(&mut cx)? {
        let name = name
            .downcast_or_throw::<JsString, _>(&mut cx)?
            .value(&mut cx);
        let val = data.get::<JsObject, _, _>(&mut cx, &name as &str)?;
        let mut nr = NamedRows::default();
        js2stored(&mut cx, val, &mut nr)?;
        rels.insert(name, nr);
    }

    rayon::spawn(move || {
        let result = db.import_relations(rels);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            if let Err(msg) = result {
                let reports = format_error_as_json(msg, None).to_string();
                let err = cx.string(&reports).as_value(&mut cx);
                callback.call(&mut cx, this, vec![err])?;
            } else {
                callback.call(&mut cx, this, vec![])?;
            }

            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn import_from_backup(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let db = get_db!(cx);
    let path = cx.argument::<JsString>(1)?.value(&mut cx);
    let rels = cx.argument::<JsArray>(2)?;
    let mut relations = vec![];
    for i in 0..rels.len(&mut cx) {
        let r = rels.get::<JsString, _, _>(&mut cx, i)?.value(&mut cx);
        relations.push(r);
    }

    let callback = cx.argument::<JsFunction>(3)?.root(&mut cx);
    let channel = cx.channel();

    rayon::spawn(move || {
        let result = db.import_from_backup(path, &relations);
        channel.send(move |mut cx| {
            let callback = callback.into_inner(&mut cx);
            let this = cx.undefined();
            if let Err(msg) = result {
                let reports = format_error_as_json(msg, None).to_string();
                let err = cx.string(&reports).as_value(&mut cx);
                callback.call(&mut cx, this, vec![err])?;
            } else {
                callback.call(&mut cx, this, vec![])?;
            }

            Ok(())
        });
    });

    Ok(cx.undefined())
}

fn register_callback(mut cx: FunctionContext) -> JsResult<JsNumber> {
    let db = get_db!(cx);
    let name = cx.argument::<JsString>(1)?.value(&mut cx);
    let capacity = cx.argument::<JsNumber>(3)?.value(&mut cx);
    let capacity = if capacity < 0. {
        None
    } else {
        Some(capacity as usize)
    };
    let callback = Arc::new(cx.argument::<JsFunction>(2)?.root(&mut cx));
    let channel = cx.channel();

    let (rid, recv) = db.register_callback(&name, capacity);
    rayon::spawn(move || {
        for (op, new, old) in recv {
            let cb = callback.clone();
            channel.send(move |mut cx| {
                let callback = cb.to_inner(&mut cx);
                let op = cx.string(op.as_str()).as_value(&mut cx);
                let new = rows2js(&mut cx, &new.rows)?.as_value(&mut cx);
                let old = rows2js(&mut cx, &old.rows)?.as_value(&mut cx);
                let this = cx.undefined();

                callback.call(&mut cx, this, vec![op, new, old])?;
                Ok(())
            });
        }
    });
    Ok(cx.number(rid))
}

fn unregister_callback(mut cx: FunctionContext) -> JsResult<JsBoolean> {
    let db = get_db!(cx);
    let id = cx.argument::<JsNumber>(1)?.value(&mut cx) as u32;
    let removed = db.unregister_callback(id);
    Ok(cx.boolean(removed))
}

fn register_named_rule(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let db = get_db!(cx);
    let name = cx.argument::<JsString>(1)?.value(&mut cx);
    let arity = cx.argument::<JsNumber>(2)?.value(&mut cx) as usize;
    let callback = Arc::new(cx.argument::<JsFunction>(3)?.root(&mut cx));
    let channel = cx.channel();
    let (rule_impl, recv) = SimpleFixedRule::rule_with_channel(arity);
    if let Err(err) = db.register_fixed_rule(name, rule_impl) {
        let msg = cx.string(err.to_string());
        return cx.throw(msg);
    }
    rayon::spawn(move || {
        for (inputs, options, sender) in recv {
            let id = HANDLES.cb_idx.fetch_add(1, Ordering::AcqRel);
            {
                HANDLES.current_cbs.lock().unwrap().insert(id, sender);
            }
            let cb = callback.clone();
            channel.send(move |mut cx| {
                let callback = cb.to_inner(&mut cx);
                let inputs_js = cx.empty_array();
                for (i, input) in inputs.into_iter().enumerate() {
                    let input_js = rows2js(&mut cx, &input.rows)?;
                    inputs_js.set(&mut cx, i as u32, input_js)?;
                }
                let inputs_js = inputs_js.as_value(&mut cx);
                let options_js = params2js(&mut cx, &options)?.as_value(&mut cx);
                let this = cx.undefined();
                let ret_id = cx.number(id).as_value(&mut cx);
                callback.call(&mut cx, this, vec![ret_id, inputs_js, options_js])?;

                Ok(())
            });
        }
    });

    Ok(cx.undefined())
}

fn respond_to_named_rule_invocation(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    let ret_id = cx.argument::<JsNumber>(0)?.value(&mut cx) as u32;
    let sender = {
        match HANDLES.current_cbs.lock().unwrap().remove(&ret_id) {
            None => {
                let msg = cx.string("fixed rule invocation sender should only be used once");
                return cx.throw(msg);
            }
            Some(s) => s,
        }
    };

    let send_err = |err| {
        let _ = sender.send(Err(miette!("Javascript fixed rule failed")));
        err
    };

    let payload = cx.argument::<JsValue>(1)?;
    if let Ok(msg) = payload.downcast::<JsString, _>(&mut cx) {
        let _ = sender.send(Err(miette!(msg.value(&mut cx))));
        return Ok(cx.undefined());
    }

    let data = payload.downcast_or_throw(&mut cx).map_err(send_err)?;
    let mut rows = vec![];
    js2rows(&mut cx, data, &mut rows).map_err(send_err)?;
    let nr = NamedRows::new(vec![], rows);
    if let Err(err) = sender.send(Ok(nr)) {
        let msg = err.to_string();
        let msg = cx.string(msg);
        return cx.throw(msg);
    }
    Ok(cx.undefined())
}

fn unregister_named_rule(mut cx: FunctionContext) -> JsResult<JsBoolean> {
    let db = get_db!(cx);
    let name = cx.argument::<JsString>(1)?.value(&mut cx);
    let removed = match db.unregister_fixed_rule(&name) {
        Ok(b) => b,
        Err(msg) => {
            let msg = cx.string(msg.to_string());
            return cx.throw(msg);
        }
    };
    Ok(cx.boolean(removed))
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    cx.export_function("open_db", open_db)?;
    cx.export_function("close_db", close_db)?;
    cx.export_function("query_db", query_db)?;
    cx.export_function("backup_db", backup_db)?;
    cx.export_function("restore_db", restore_db)?;
    cx.export_function("export_relations", export_relations)?;
    cx.export_function("import_relations", import_relations)?;
    cx.export_function("import_from_backup", import_from_backup)?;
    cx.export_function("register_callback", register_callback)?;
    cx.export_function("unregister_callback", unregister_callback)?;
    cx.export_function("register_named_rule", register_named_rule)?;
    cx.export_function(
        "respond_to_named_rule_invocation",
        respond_to_named_rule_invocation,
    )?;
    cx.export_function("unregister_named_rule", unregister_named_rule)?;
    cx.export_function("abort_tx", abort_tx)?;
    cx.export_function("commit_tx", commit_tx)?;
    cx.export_function("multi_transact", multi_transact)?;
    cx.export_function("query_tx", query_tx)?;
    Ok(())
}
