/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;
use std::thread;

use miette::{IntoDiagnostic, Result};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

use cozo::*;

fn py_to_named_rows(ob: &PyAny) -> PyResult<NamedRows> {
    let rows = ob.extract::<Vec<Vec<&PyAny>>>()?;
    let res: Vec<Vec<DataValue>> = rows
        .into_iter()
        .map(|row| row.into_iter().map(py_to_value).collect::<PyResult<_>>())
        .collect::<PyResult<_>>()?;

    Ok(NamedRows::new(vec![], res))
}

fn py_to_value(ob: &PyAny) -> PyResult<DataValue> {
    Ok(if ob.is_none() {
        DataValue::Null
    } else if let Ok(i) = ob.extract::<i64>() {
        DataValue::from(i)
    } else if let Ok(f) = ob.extract::<f64>() {
        DataValue::from(f)
    } else if let Ok(s) = ob.extract::<String>() {
        DataValue::from(s)
    } else if let Ok(b) = ob.extract::<Vec<u8>>() {
        DataValue::Bytes(b)
    } else if let Ok(l) = ob.downcast::<PyList>() {
        let mut coll = Vec::with_capacity(l.len());
        for el in l {
            let el = py_to_value(el)?;
            coll.push(el)
        }
        DataValue::List(coll)
    } else if let Ok(d) = ob.downcast::<PyDict>() {
        let mut coll = Vec::with_capacity(d.len());
        for (k, v) in d {
            let k = py_to_value(k)?;
            let v = py_to_value(v)?;
            coll.push(DataValue::List(vec![k, v]))
        }
        DataValue::List(coll)
    } else {
        return Err(PyException::new_err(format!(
            "Cannot convert {ob} into Cozo value"
        )));
    })
}

fn convert_params(ob: &PyDict) -> PyResult<BTreeMap<String, DataValue>> {
    let mut ret = BTreeMap::new();
    for (k, v) in ob {
        let k: String = k.extract()?;
        let v = py_to_value(v)?;
        ret.insert(k, v);
    }
    Ok(ret)
}

fn options_to_py(opts: BTreeMap<String, DataValue>, py: Python<'_>) -> PyResult<PyObject> {
    let ret = PyDict::new(py);

    for (k, v) in opts {
        let val = value_to_py(v, py);
        ret.set_item(k, val)?;
    }

    Ok(ret.into())
}

fn value_to_py(val: DataValue, py: Python<'_>) -> PyObject {
    match val {
        DataValue::Null => py.None(),
        DataValue::Bool(b) => b.into_py(py),
        DataValue::Num(num) => match num {
            Num::Int(i) => i.into_py(py),
            Num::Float(f) => f.into_py(py),
        },
        DataValue::Str(s) => s.as_str().into_py(py),
        DataValue::Bytes(b) => b.into_py(py),
        DataValue::Uuid(uuid) => uuid.0.to_string().into_py(py),
        DataValue::Regex(rx) => rx.0.as_str().into_py(py),
        DataValue::List(l) => {
            let vs: Vec<_> = l.into_iter().map(|v| value_to_py(v, py)).collect();
            vs.into_py(py)
        }
        DataValue::Set(l) => {
            let vs: Vec<_> = l.into_iter().map(|v| value_to_py(v, py)).collect();
            vs.into_py(py)
        }
        DataValue::Validity(vld) => {
            [vld.timestamp.0 .0.into_py(py), vld.is_assert.0.into_py(py)].into_py(py)
        }
        DataValue::Bot => py.None(),
    }
}

fn rows_to_py_rows(rows: Vec<Vec<DataValue>>, py: Python<'_>) -> PyObject {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|val| value_to_py(val, py))
                .collect::<Vec<_>>()
                .into_py(py)
        })
        .collect::<Vec<_>>()
        .into_py(py)
}

fn named_rows_to_py(named_rows: NamedRows, py: Python<'_>) -> PyObject {
    let rows = rows_to_py_rows(named_rows.rows, py);
    let headers = named_rows.headers.into_py(py);
    BTreeMap::from([("rows", rows), ("headers", headers)]).into_py(py)
}

#[pyclass]
struct CozoDbPy {
    db: Option<DbInstance>,
}

const DB_CLOSED_MSG: &str = r##"{"ok":false,"message":"database closed"}"##;

#[pymethods]
impl CozoDbPy {
    #[new]
    fn new(engine: &str, path: &str, options: &str) -> PyResult<Self> {
        match DbInstance::new(engine, path, options) {
            Ok(db) => Ok(Self { db: Some(db) }),
            Err(err) => Err(PyException::new_err(format!("{err:?}"))),
        }
    }
    pub fn run_script(&self, py: Python<'_>, query: &str, params: &PyDict) -> PyResult<PyObject> {
        if let Some(db) = &self.db {
            let params = convert_params(params)?;
            match py.allow_threads(|| db.run_script(query, params)) {
                Ok(rows) => Ok(named_rows_to_py(rows, py)),
                Err(err) => {
                    let reports = format_error_as_json(err, Some(query)).to_string();
                    Err(PyException::new_err(reports))
                }
            }
        } else {
            Err(PyException::new_err(DB_CLOSED_MSG))
        }
    }
    pub fn register_callback(&self, rel: &str, callback: &PyAny) -> PyResult<u32> {
        if let Some(db) = &self.db {
            let cb: Py<PyAny> = callback.into();
            let (id, ch) = db.register_callback(rel, None);
            thread::spawn(move || {
                for (op, new, old) in ch {
                    Python::with_gil(|py| {
                        let op = PyString::new(py, op.as_str()).into();
                        let new_py = rows_to_py_rows(new.rows, py);
                        let old_py = rows_to_py_rows(old.rows, py);
                        let args = PyTuple::new(py, [op, new_py, old_py]);
                        let callable = cb.as_ref(py);
                        if let Err(err) = callable.call1(args) {
                            eprintln!("{}", err);
                        }
                    })
                }
            });
            Ok(id)
        } else {
            Err(PyException::new_err(DB_CLOSED_MSG))
        }
    }
    pub fn register_fixed_rule(
        &self,
        name: String,
        arity: usize,
        callback: &PyAny,
    ) -> PyResult<()> {
        if let Some(db) = &self.db {
            let cb: Py<PyAny> = callback.into();
            let (rule_impl, receiver) = SimpleFixedRule::rule_with_channel(arity);
            match db.register_fixed_rule(name, rule_impl) {
                Ok(_) => {
                    thread::spawn(move || {
                        for (inputs, options, sender) in receiver {
                            let res = Python::with_gil(|py| -> Result<NamedRows> {
                                let py_inputs = PyList::new(
                                    py,
                                    inputs.into_iter().map(|nr| rows_to_py_rows(nr.rows, py)),
                                );
                                let py_opts = options_to_py(options, py).into_diagnostic()?;
                                let args =
                                    PyTuple::new(py, vec![PyObject::from(py_inputs), py_opts]);
                                let res = cb.as_ref(py).call1(args).into_diagnostic()?;
                                py_to_named_rows(res).into_diagnostic()
                            });
                            if sender.send(res).is_err() {
                                break;
                            }
                        }
                    });
                    Ok(())
                }
                Err(err) => Err(PyException::new_err(err.to_string())),
            }
        } else {
            Err(PyException::new_err(DB_CLOSED_MSG))
        }
    }
    pub fn unregister_callback(&self, id: u32) -> bool {
        if let Some(db) = &self.db {
            db.unregister_callback(id)
        } else {
            false
        }
    }
    pub fn unregister_fixed_rule(&self, name: &str) -> PyResult<bool> {
        if let Some(db) = &self.db {
            match db.unregister_fixed_rule(name) {
                Ok(b) => Ok(b),
                Err(err) => Err(PyException::new_err(err.to_string())),
            }
        } else {
            Ok(false)
        }
    }
    pub fn run_query(&self, py: Python<'_>, query: &str, params: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.run_script_str(query, params))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn export_relations(&self, py: Python<'_>, data: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.export_relations_str(data))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn import_relations(&self, py: Python<'_>, data: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.import_relations_str(data))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn backup(&self, py: Python<'_>, path: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.backup_db_str(path))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn restore(&self, py: Python<'_>, path: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.restore_backup_str(path))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn import_from_backup(&self, py: Python<'_>, data: &str) -> String {
        if let Some(db) = &self.db {
            py.allow_threads(|| db.import_from_backup_str(data))
        } else {
            DB_CLOSED_MSG.to_string()
        }
    }
    pub fn close(&mut self) -> bool {
        self.db.take().is_some()
    }
}

#[pymodule]
fn cozo_embedded(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<CozoDbPy>()?;
    Ok(())
}
